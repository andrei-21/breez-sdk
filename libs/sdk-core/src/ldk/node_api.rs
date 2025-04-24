use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use futures::Stream;

use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::channelmanager::PaymentId;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::lightning_invoice::{Bolt11InvoiceDescription, Description};
use ldk_node::payment::ConfirmationStatus;
use ldk_node::{Builder, Event, Node, PendingSweepBalance};

use core::str::FromStr;
use sdk_common::prelude::*;
use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex};

use crate::bitcoin::bech32::ToBase32;
use crate::bitcoin::secp256k1::ecdsa::RecoverableSignature;
use crate::bitcoin::secp256k1::Secp256k1;
use crate::bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey};
use crate::ldk::logger::Logger;
use crate::lightning::sign::{KeysManager, NodeSigner, Recipient};
use crate::lightning_invoice::RawBolt11Invoice;
use crate::node_api::{CreateInvoiceRequest, FetchBolt11Result, NodeAPI, NodeError, NodeResult};
use crate::{models::*, LspInformation};
use crate::{PrepareRedeemOnchainFundsRequest, PrepareRedeemOnchainFundsResponse};

pub(crate) struct Ldk {
    seed: [u8; 64],
    node: Arc<Node>,
    invoice_stream: Mutex<Option<mpsc::Receiver<Payment>>>,
}

impl Ldk {
    pub fn build(working_dir: String, seed: &[u8]) -> Self {
        let lsp = "0361984fe2a03cc594e97de423bf461096dd26a52e77feda68510377f360e430d4";
        let lsp = PublicKey::from_str(lsp).unwrap();

        let mut config = ldk_node::config::Config::default();
        config.anchor_channels_config = Some(ldk_node::config::AnchorChannelsConfig {
            trusted_peers_no_reserve: vec![lsp],
            per_channel_reserve_sats: 0,
        });
        config.trusted_peers_0conf = vec![lsp];

        let mut builder = Builder::from_config(config);

        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(seed);
        let seed = bytes;
        builder.set_entropy_seed_bytes(seed);
        builder.set_custom_logger(Arc::new(Logger {}));
        builder.set_storage_dir_path(working_dir);

        // builder.set_chain_source_esplora("https://blockstream.info/api".to_string(), None);
        // builder.set_gossip_source_rgs("https://rapidsync.lightningdevkit.org/snapshot".to_string());
        builder.set_network(ldk_node::bitcoin::Network::Regtest);
        builder.set_chain_source_bitcoind_rpc(
            "localhost".to_string(),
            18443,
            "btcuser".to_string(),
            "btcpass".to_string(),
        );
        builder.set_gossip_source_rgs("http://localhost:8011".to_string());
        let node = Arc::new(builder.build().unwrap());
        Self {
            seed,
            node,
            invoice_stream: Mutex::default(),
        }
    }
}

async fn stream_invoices(node: Arc<Node>, tx: mpsc::Sender<Payment>) {
    loop {
        let event = node.next_event_async().await;
        info!("Event: {event:?}");
        match event {
            Event::PaymentReceived { payment_id, .. } => {
                let payment = find_and_map_payment(&node, payment_id.unwrap());
                let _ = tx.send(payment).await;
            }

            Event::PaymentSuccessful { payment_id, .. } => {
                let payment = find_and_map_payment(&node, payment_id.unwrap());
                let _ = tx.send(payment).await;
            }
            Event::PaymentFailed {
                payment_id,
                payment_hash: _,
                reason,
            } => {
                let mut payment = find_and_map_payment(&node, payment_id.unwrap());
                payment.error = reason.map(|r| format!("{r:?}"));
                let _ = tx.send(payment).await;
            }
            Event::PaymentClaimable {
                payment_id: _,
                payment_hash: _,
                claimable_amount_msat: _,
                claim_deadline: _,
                custom_records: _,
            } => (),
            Event::PaymentForwarded { .. } => (),
            Event::ChannelPending {
                channel_id: _,
                user_channel_id: _,
                former_temporary_channel_id: _,
                counterparty_node_id: _,
                funding_txo: _,
            } => (),
            Event::ChannelReady {
                channel_id: _,
                user_channel_id: _,
                counterparty_node_id: _,
            } => (),
            Event::ChannelClosed {
                channel_id: _,
                user_channel_id: _,
                counterparty_node_id: _,
                reason: _,
            } => (),
        }
        if let Err(e) = node.event_handled() {
            error!("Failed to report that event was handled: {e}");
        }
    }
}

#[allow(unused_variables)]
#[tonic::async_trait]
impl NodeAPI for Ldk {
    /// Starts the node.
    async fn start_signer(&self, mut shutdown: mpsc::Receiver<()>) {
        debug!("Starting node");
        self.node.start().unwrap();
        debug!("LDK Node started");

        let node = Arc::clone(&self.node);
        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(async move { stream_invoices(node, tx).await });
        self.invoice_stream.lock().await.replace(rx);
        debug!("Event handling started");

        let node = Arc::clone(&self.node);
        let _ = tokio::spawn(async move {
            let _ = shutdown.recv().await;
            debug!("Received shutdown signal, stopping node");
            if let Err(e) = node.stop() {
                error!("{e}");
            }
            debug!("Node stopped");
        })
        .await;
        debug!("Node started");
    }

    /// Keeps background tasks running.
    async fn start_keep_alive(&self, shutdown: watch::Receiver<()>) {
        debug!("ldk: start_keep_alive()");
    }

    async fn node_credentials(&self) -> NodeResult<Option<NodeCredentials>> {
        Ok(None)
    }

    async fn configure_node(&self, close_to_address: Option<String>) -> NodeResult<()> {
        todo!()
    }

    async fn create_invoice(&self, request: CreateInvoiceRequest) -> NodeResult<String> {
        debug!("create_invoice: {request:?}");
        let description =
            Bolt11InvoiceDescription::Direct(Description::new(request.description).unwrap());
        let expiry = request.expiry.unwrap_or(3600);
        let payments = self.node.bolt11_payment();

        let result = match request.payer_amount_msat {
            Some(payer_amount_msat) => {
                let lsp_fees_msat = payer_amount_msat - request.amount_msat;
                payments.register_incoming_payment(
                    payer_amount_msat,
                    lsp_fees_msat,
                    &description,
                    expiry,
                )
            }
            None => payments.receive(request.amount_msat, &description, expiry),
        };

        result.map(|i| i.to_string()).map_err(to_node_error)
    }

    async fn sign_invoice(&self, invoice: RawBolt11Invoice) -> NodeResult<String> {
        let network = self.node.config().network;
        let xprv = ldk_node::bitcoin::bip32::Xpriv::new_master(network, &self.seed).unwrap();
        let ldk_seed_bytes: [u8; 32] = xprv.private_key.secret_bytes();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let key_manager = KeysManager::new(&ldk_seed_bytes, now.as_secs(), now.subsec_nanos());
        let signature = key_manager
            .sign_invoice(
                invoice.hrp.to_string().as_bytes(),
                &invoice.data.to_base32(),
                Recipient::Node,
            )
            .unwrap();
        let signed = invoice
            .sign(|_| Ok::<RecoverableSignature, ()>(signature))
            .unwrap()
            .to_string();
        debug!("sign_invoice: {signed:?}");
        Ok(signed)
    }

    async fn fetch_bolt11(&self, payment_hash: Vec<u8>) -> NodeResult<Option<FetchBolt11Result>> {
        todo!()
    }

    async fn pull_changed(
        &self,
        sync_state: Option<Value>,
        match_local_balance: bool,
    ) -> NodeResult<SyncResponse> {
        const MAX_PAYMENT_AMOUNT_MSAT: u64 = 4294967000;

        let balances = self.node.list_balances();
        debug!("Balances: {balances:?}");
        let pending_onchain_balance_sats: u64 = balances
            .pending_balances_from_channel_closures
            .into_iter()
            .map(get_balance)
            .sum();
        let connected_peers = self
            .node
            .list_peers()
            .iter()
            .filter(|p| p.is_connected)
            .map(|p| p.node_id.to_string())
            .collect();

        let channels = self.node.list_channels();
        let max_receivable_single_payment_amount_msat = channels
            .iter()
            .flat_map(|c| c.inbound_htlc_maximum_msat)
            .sum();
        debug!("Channels: {channels:?}");

        let channels = channels.into_iter().flat_map(map_channel).collect();

        let node_id = self.node.node_id();
        let payments = self
            .node
            .list_payments()
            .into_iter()
            .map(|p| to_payment(p, node_id))
            .collect();

        debug!("Channels: {channels:?}");

        let node_state = NodeState {
            id: self.node.node_id().to_string(),
            block_height: self.node.status().current_best_block.height,
            channels_balance_msat: balances.total_lightning_balance_sats * 1000,
            onchain_balance_msat: balances.total_onchain_balance_sats * 1000,
            pending_onchain_balance_msat: pending_onchain_balance_sats * 1000,
            utxos: Vec::new(),
            max_payable_msat: 0,
            max_receivable_msat: 0,
            max_single_payment_amount_msat: MAX_PAYMENT_AMOUNT_MSAT,
            //max_chan_reserve_msats: channels_balance - min(max_payable, channels_balance),
            max_chan_reserve_msats: 0,
            connected_peers,
            max_receivable_single_payment_amount_msat,
            total_inbound_liquidity_msats: 0,
        };
        let response = SyncResponse {
            sync_state: Value::Null,
            node_state,
            payments,
            channels,
        };
        Ok(response)
    }

    async fn send_pay(&self, bolt11: String, max_hops: u32) -> NodeResult<PaymentResponse> {
        todo!()
    }

    async fn send_payment(
        &self,
        bolt11: String,
        amount_msat: Option<u64>,
        label: Option<String>,
    ) -> NodeResult<Payment> {
        let invoice = ldk_node::lightning_invoice::Bolt11Invoice::from_str(&bolt11).unwrap();
        let payments = self.node.bolt11_payment();
        let payment_id = match amount_msat {
            Some(amount_msat) => payments.send_using_amount(&invoice, amount_msat, None),
            None => payments.send(&invoice, None),
        }
        .map_err(to_node_error)?;

        let payment = find_and_map_payment(&self.node, payment_id);
        Ok(payment)
    }

    async fn send_trampoline_payment(
        &self,
        bolt11: String,
        amount_msat: u64,
        label: Option<String>,
        trampoline_node_id: Vec<u8>,
    ) -> NodeResult<Payment> {
        todo!()
    }

    async fn send_spontaneous_payment(
        &self,
        node_id: String,
        amount_msat: u64,
        extra_tlvs: Option<Vec<TlvEntry>>,
        label: Option<String>,
    ) -> NodeResult<Payment> {
        let node_id = PublicKey::from_str(&node_id).unwrap();
        let payment_id = self
            .node
            .spontaneous_payment()
            .send(amount_msat, node_id, None)
            .map_err(to_node_error)?;
        let payment = find_and_map_payment(&self.node, payment_id);
        Ok(payment)
    }

    async fn node_id(&self) -> NodeResult<String> {
        Ok(self.node.node_id().to_string())
    }

    async fn redeem_onchain_funds(
        &self,
        to_address: String,
        sat_per_vbyte: u32,
    ) -> NodeResult<Vec<u8>> {
        todo!()
    }

    async fn prepare_redeem_onchain_funds(
        &self,
        req: PrepareRedeemOnchainFundsRequest,
    ) -> NodeResult<PrepareRedeemOnchainFundsResponse> {
        todo!()
    }

    async fn connect_peer(&self, id: String, addr: String) -> NodeResult<()> {
        let node_id = PublicKey::from_str(&id).unwrap();
        let address = SocketAddress::from_str(&addr).unwrap();
        let persist = false;
        self.node
            .connect(node_id, address, persist)
            .map_err(to_node_error)
    }

    async fn sign_message(&self, message: &str) -> NodeResult<String> {
        todo!()
    }

    async fn check_message(
        &self,
        message: &str,
        pubkey: &str,
        signature: &str,
    ) -> NodeResult<bool> {
        todo!()
    }

    async fn close_peer_channels(&self, node_id: String) -> NodeResult<Vec<String>> {
        let node_id = PublicKey::from_str(&node_id).unwrap();
        let channels = self
            .node
            .list_channels()
            .into_iter()
            .filter(|c| c.counterparty_node_id == node_id && c.is_channel_ready);
        for channel_id in channels {
            self.node
                .close_channel(&channel_id.user_channel_id, node_id)
                .map_err(to_node_error)?;
        }
        // TODO: Get closing tx ids.
        Ok(vec!["closing_tx_id".to_string()])
    }

    async fn stream_incoming_payments(&self) -> NodeResult<mpsc::Receiver<Payment>> {
        self.invoice_stream
            .lock()
            .await
            .take()
            .ok_or(NodeError::generic("Invoice stream is not initialized"))
    }

    async fn stream_log_messages(
        &self,
    ) -> NodeResult<mpsc::Receiver<gl_client::signer::model::greenlight::LogEntry>> {
        let (send, recv) = mpsc::channel(10);
        Ok(recv)
    }

    async fn static_backup(&self) -> NodeResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn generate_diagnostic_data(&self) -> NodeResult<Value> {
        todo!()
    }

    async fn execute_command(&self, command: String) -> NodeResult<Value> {
        todo!()
    }

    async fn max_sendable_amount(
        &self,
        payee_node_id: Option<Vec<u8>>,
        max_hops: u32,
        last_hop_hint: Option<&RouteHintHop>,
    ) -> NodeResult<Vec<MaxChannelAmount>> {
        todo!()
    }

    async fn derive_bip32_key(&self, path: Vec<ChildNumber>) -> NodeResult<ExtendedPrivKey> {
        let network = sdk_common::prelude::Network::Regtest;
        Ok(ExtendedPrivKey::new_master(network.into(), &self.seed)?
            .derive_priv(&Secp256k1::new(), &path)?)
    }

    async fn legacy_derive_bip32_key(&self, path: Vec<ChildNumber>) -> NodeResult<ExtendedPrivKey> {
        self.derive_bip32_key(path).await
    }

    async fn stream_custom_messages(
        &self,
    ) -> NodeResult<Pin<Box<dyn Stream<Item = Result<CustomMessage>> + Send>>> {
        todo!()
    }

    async fn send_custom_message(&self, message: CustomMessage) -> NodeResult<()> {
        todo!()
    }

    // Gets the routing hints related to all private channels that the node has
    async fn get_routing_hints(
        &self,
        lsp_info: &LspInformation,
    ) -> NodeResult<(Vec<RouteHint>, bool)> {
        todo!()
    }

    async fn get_open_peers(&self) -> NodeResult<HashSet<Vec<u8>>> {
        todo!()
    }
}

fn get_balance(balance: PendingSweepBalance) -> u64 {
    match balance {
        PendingSweepBalance::PendingBroadcast {
            channel_id: _,
            amount_satoshis,
        } => amount_satoshis,
        PendingSweepBalance::BroadcastAwaitingConfirmation {
            channel_id: _,
            latest_broadcast_height: _,
            latest_spending_txid: _,
            amount_satoshis,
        } => amount_satoshis,
        PendingSweepBalance::AwaitingThresholdConfirmations {
            channel_id: _,
            latest_spending_txid: _,
            confirmation_hash: _,
            confirmation_height: _,
            amount_satoshis,
        } => amount_satoshis,
    }
}

fn map_channel(channel: ldk_node::ChannelDetails) -> Option<Channel> {
    let funding_txo = channel.funding_txo?;
    let funding_txid = funding_txo.txid.to_string();
    let funding_outnum = Some(funding_txo.vout);

    let short_channel_id = channel.short_channel_id.map(format_scid);

    let state = match (channel.is_channel_ready, channel.is_usable) {
        // TODO: It might mean that ChannelState::Closed?
        (false, _) => ChannelState::PendingOpen,
        // TODO: If the peer is connected it might mean that ChannelState::PendingClose.
        (true, false) => ChannelState::Opened,
        (true, true) => ChannelState::Opened,
    };

    let spendable_msat = channel.outbound_capacity_msat;
    // TODO: Not sure about this math.
    let local_balance_msat =
        spendable_msat + channel.unspendable_punishment_reserve.unwrap_or_default();
    let receivable_msat = channel.inbound_capacity_msat;

    // TODO: Here we get only open channels I guess.
    let closed_at: Option<u64> = None;
    let closing_txid: Option<String> = None;

    let alias_local = channel.outbound_scid_alias.map(format_scid);
    let alias_remote = channel.inbound_scid_alias.map(format_scid);

    // TODO: Convert HTLCs.
    let htlcs = Vec::new();

    Some(Channel {
        funding_txid,
        short_channel_id,
        state,
        spendable_msat,
        local_balance_msat,
        receivable_msat,
        closed_at,
        funding_outnum,
        alias_local,
        alias_remote,
        closing_txid,
        htlcs,
    })
}

fn format_scid(id: u64) -> String {
    // TODO: It should be in this format 2531830x10x1 I guess.
    id.to_string()
}

fn hex<T: std::borrow::Borrow<[u8]>>(bytes: &T) -> String {
    hex::encode(bytes.borrow())
}

fn to_node_error(err: ldk_node::NodeError) -> NodeError {
    NodeError::generic(&format!("LDK Node error: {err}"))
}

fn find_and_map_payment(node: &Node, payment_id: PaymentId) -> Payment {
    let payment = node
        .list_payments_with_filter(|p| p.id == payment_id)
        .into_iter()
        .next()
        .unwrap();
    to_payment(payment, node.node_id())
}

fn to_payment(payment: ldk_node::payment::PaymentDetails, local_node_id: PublicKey) -> Payment {
    let payment_type = match payment.direction {
        ldk_node::payment::PaymentDirection::Inbound => PaymentType::Received,
        ldk_node::payment::PaymentDirection::Outbound => PaymentType::Sent,
    };
    Payment {
        id: hex(&payment.id),
        payment_type,
        payment_time: payment.latest_update_timestamp as i64,
        amount_msat: payment.amount_msat.unwrap_or_default(),
        fee_msat: payment.fee_paid_msat.unwrap_or_default(),
        status: to_payment_status(payment.status),
        error: None,
        description: None,
        details: to_payment_details(&payment, local_node_id),
        metadata: None,
    }
}

fn to_payment_status(status: ldk_node::payment::PaymentStatus) -> PaymentStatus {
    match status {
        ldk_node::payment::PaymentStatus::Pending => PaymentStatus::Pending,
        ldk_node::payment::PaymentStatus::Succeeded => PaymentStatus::Complete,
        ldk_node::payment::PaymentStatus::Failed => PaymentStatus::Failed,
    }
}

fn to_payment_details(
    payment: &ldk_node::payment::PaymentDetails,
    local_node_id: PublicKey,
) -> PaymentDetails {
    let destination_pubkey = match payment.direction {
        ldk_node::payment::PaymentDirection::Inbound => local_node_id.to_string(),
        ldk_node::payment::PaymentDirection::Outbound => String::new(),
    };
    match &payment.kind {
        ldk_node::payment::PaymentKind::Bolt11 {
            hash,
            preimage,
            secret: _,
        } => PaymentDetails::Ln {
            data: LnPaymentDetails {
                payment_hash: hex(hash),
                label: String::new(),
                destination_pubkey,
                payment_preimage: preimage.as_ref().map(hex).unwrap_or_default(),
                keysend: false,
                bolt11: String::new(),
                open_channel_bolt11: None,
                ..Default::default()
            },
        },
        ldk_node::payment::PaymentKind::Bolt11Jit {
            hash,
            preimage,
            secret: _,
            counterparty_skimmed_fee_msat: _,
            lsp_fee_limits: _,
        } => PaymentDetails::Ln {
            data: LnPaymentDetails {
                payment_hash: hex(hash),
                label: String::new(),
                destination_pubkey,
                payment_preimage: preimage.as_ref().map(hex).unwrap_or_default(),
                keysend: false,
                bolt11: String::new(),
                open_channel_bolt11: None,
                ..Default::default()
            },
        },
        ldk_node::payment::PaymentKind::Bolt12Offer { .. } => todo!(),
        ldk_node::payment::PaymentKind::Bolt12Refund { .. } => todo!(),
        // TODO: It is not necessary channel close.
        ldk_node::payment::PaymentKind::Onchain { txid, status } => PaymentDetails::ClosedChannel {
            data: ClosedChannelPaymentDetails {
                state: to_channel_state(status),
                funding_txid: String::new(),
                short_channel_id: None,
                closing_txid: Some(hex(txid)),
            },
        },
        ldk_node::payment::PaymentKind::Spontaneous { hash, preimage } => PaymentDetails::Ln {
            data: LnPaymentDetails {
                payment_hash: hex(hash),
                label: String::new(),
                destination_pubkey,
                payment_preimage: preimage.as_ref().map(hex).unwrap_or_default(),
                keysend: true,
                bolt11: String::new(),
                open_channel_bolt11: None,
                ..Default::default()
            },
        },
    }
}

fn to_channel_state(status: &ConfirmationStatus) -> ChannelState {
    match status {
        ConfirmationStatus::Confirmed { .. } => ChannelState::Closed,
        ConfirmationStatus::Unconfirmed => ChannelState::PendingClose,
    }
}
