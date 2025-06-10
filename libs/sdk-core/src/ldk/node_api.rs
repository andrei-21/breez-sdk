use anyhow::Result;
use core::str::FromStr;
use futures::Stream;
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::bitcoin::{Address, FeeRate};
use ldk_node::lightning::ln::channelmanager::PaymentId;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::lightning::offers::offer::Offer;
use ldk_node::lightning::util::persist::KVStore;
use ldk_node::lightning_invoice::{Bolt11InvoiceDescription, Description};
use ldk_node::lightning_types::payment::{PaymentHash, PaymentPreimage};
use ldk_node::payment::ConfirmationStatus;
use ldk_node::{Builder, Event, Node, PendingSweepBalance};
use rand::distributions::Alphanumeric;
use rand::Rng;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::{mpsc, watch, Mutex};
use vss_client::client::VssClient;
use vss_client::error::VssError;
use vss_client::util::retry::{ExponentialBackoffRetryPolicy, RetryPolicy};

use sdk_common::bitcoin::hashes::hex::ToHex;
use sdk_common::bitcoin::hashes::sha256::Hash as Sha256;
use sdk_common::bitcoin::hashes::Hash;
use sdk_common::prelude::*;

use crate::bitcoin::bech32::ToBase32;
use crate::bitcoin::secp256k1::ecdsa::RecoverableSignature;
use crate::bitcoin::secp256k1::Secp256k1;
use crate::bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey};
use crate::ldk::config::Config;
use crate::ldk::locking_store::LockingStore;
use crate::ldk::logger::Logger;
use crate::ldk::mirroring_store::MirroringStore;
use crate::ldk::vss_store::VssStore;
use crate::lightning::sign::{KeysManager, NodeSigner, Recipient};
use crate::lightning_invoice::RawBolt11Invoice;
use crate::node_api::{CreateInvoiceRequest, FetchBolt11Result, NodeAPI, NodeError, NodeResult};
use crate::{models::*, LspInformation};
use crate::{PrepareRedeemOnchainFundsRequest, PrepareRedeemOnchainFundsResponse};

type Store = Arc<dyn KVStore + Sync + Send>;

pub(crate) struct Ldk {
    seed: [u8; 64],
    node: Arc<Node>,
    payments_tx: mpsc::Sender<Payment>,
    payments_rx: Mutex<Option<mpsc::Receiver<Payment>>>,
    remote_lock_shutdown_tx: mpsc::Sender<()>,
    store: Store,
}

impl Ldk {
    pub async fn build(
        working_dir: String,
        seed: &[u8],
        network: &sdk_common::prelude::Network,
    ) -> Self {
        let lsp = "0361984fe2a03cc594e97de423bf461096dd26a52e77feda68510377f360e430d4";
        let lsp = PublicKey::from_str(lsp).unwrap();

        let mut config = ldk_node::config::Config::default();
        config.anchor_channels_config = Some(ldk_node::config::AnchorChannelsConfig {
            trusted_peers_no_reserve: vec![lsp],
            per_channel_reserve_sats: 0,
        });
        config.trusted_peers_0conf = vec![lsp];

        let instance_id_filename = Path::new(&working_dir).join("instance_id");
        let instance_id = fs::read_to_string(instance_id_filename.clone()).unwrap_or_else(|_| {
            let instance_id: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(8)
                .map(char::from)
                .collect();
            fs::write(&instance_id_filename, &instance_id).unwrap();
            instance_id
        });

        debug!("Building LDK Node");
        let mut builder = Builder::from_config(config);

        let seed_hash = Sha256::hash(seed).to_hex();
        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(seed);
        let seed = bytes;
        builder.set_entropy_seed_bytes(seed);
        builder.set_custom_logger(Arc::new(Logger {}));

        let config = match network {
            crate::prelude::Network::Bitcoin => Config::mainnet(),
            crate::prelude::Network::Regtest => Config::regtest(),
            network => panic!("Unsupported network {network}"),
        };

        builder.set_network(to_ldk_network(network));
        builder.set_chain_source_esplora(config.esplora_url, None);
        builder.set_gossip_source_rgs(config.rgs_url);

        let vss_client = VssClient::new(
            config.vss_url,
            ExponentialBackoffRetryPolicy::<VssError>::new(Duration::from_secs(1))
                .with_max_attempts(2),
        );
        let store_id = format!("{seed_hash}/ldk_node");
        let vss_store = VssStore::new(vss_client, store_id);
        let locking_store = LockingStore::new(instance_id, vss_store).await.unwrap();
        let locking_store = Arc::new(locking_store);

        let ls = Arc::clone(&locking_store);
        let (remote_lock_shutdown_tx, mut remote_lock_shutdown_rx) = mpsc::channel(1);
        tokio::task::spawn(async move {
            while let Ok(until) = ls.refresh_lock().await {
                tokio::select! {
                    _ = tokio::time::sleep_until(until) => (),
                    _ = remote_lock_shutdown_rx.recv() => {
                        match ls.unlock().await {
                            Ok(()) => info!("Remote lock was released"),
                            Err(e) => error!("Failed to release remote lock: {e}"),
                        };
                        break;
                    }
                };
            }
            // Explicitly drop the receiver to let the sender know we are done with releasing the lock.
            drop(remote_lock_shutdown_rx);
        });

        let store_filename = Path::new(&working_dir).join("ldk_node_storage.sql");
        let conn = Connection::open(store_filename).unwrap();
        let store = MirroringStore::new(tokio::runtime::Handle::current(), conn, locking_store)
            .await
            .unwrap();
        let store: Store = Arc::new(store);

        // The builder creates another tokio runtime inside and can drop it in case of errors.
        // But dropping runtime is not allowed here:
        // > Cannot drop a runtime in a context where blocking is not allowed.
        // > This happens when a runtime is dropped from within an asynchronous context.
        let node =
            tokio::task::block_in_place(|| builder.build_with_store(Arc::clone(&store))).unwrap();
        info!("LDK Node was built");

        let (payments_tx, payments_rx) = mpsc::channel(10);

        Self {
            seed,
            node: Arc::new(node),
            payments_tx,
            payments_rx: Mutex::new(Some(payments_rx)),
            remote_lock_shutdown_tx,
            store,
        }
    }
}

async fn stream_invoices(node: Arc<Node>, store: Store, tx: mpsc::Sender<Payment>) {
    loop {
        let event = tokio::select! {
            event = node.next_event_async() => event,
            _ = tx.closed() => {
                info!("Payments stream got closed, stopping stream_invoices loop");
                return;
            },
        };
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
                payment_hash,
                claimable_amount_msat,
                claim_deadline: _,
                custom_records: _,
            } => {
                let h = payment_hash.to_hex();
                match store.read("preimages", "", &h) {
                    Ok(preimage) => {
                        let preimage = PaymentPreimage(preimage.as_slice().try_into().unwrap());
                        if let Err(e) = node.bolt11_payment().claim_for_hash(
                            payment_hash,
                            claimable_amount_msat,
                            preimage,
                        ) {
                            error!("Failed to claim payment: {e}");
                        } else {
                            let _ = store.remove("preimages", "", &h, false);
                        }
                    }
                    Err(_e) => {
                        if let Err(e) = node.bolt11_payment().fail_for_hash(payment_hash) {
                            error!("Failed to fail payment: {e}");
                        }
                    }
                };
            }
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
        let store = Arc::clone(&self.store);
        let tx = self.payments_tx.clone();
        tokio::spawn(async move { stream_invoices(node, store, tx).await });
        debug!("Event handling started");

        tokio::select! {
            _ = shutdown.recv() => {
                debug!("Received shutdown signal, stopping node");
                if let Err(e) = self.node.stop() {
                    error!("{e}");
                }
                debug!("Node stopped");
                let _ = self.remote_lock_shutdown_tx.send(()).await;
                self.remote_lock_shutdown_tx.closed().await;
            },
            _ = self.remote_lock_shutdown_tx.closed() => {
                info!("Aborting node");
                std::process::exit(1);
            }
        };
    }

    async fn start_keep_alive(&self, _shutdown: watch::Receiver<()>) {}

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

        let preimage = match request.preimage {
            Some(p) => PaymentPreimage(p.as_slice().try_into().unwrap()),
            None => PaymentPreimage(rand::thread_rng().gen::<[u8; 32]>()),
        };
        let payment_hash: PaymentHash = preimage.into();
        self.store
            .write("preimages", "", &payment_hash.to_hex(), &preimage.0)
            .unwrap();

        self.node
            .bolt11_payment()
            .receive_for_hash(request.amount_msat, &description, expiry, payment_hash)
            .map(|i| i.to_string())
            .map_err(to_node_error)
    }

    async fn delete_invoice(&self, bolt11: String) -> NodeResult<()> {
        todo!()
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
        debug!("fetch_bolt11: {payment_hash:?}");
        let payment = self
            .node
            .list_payments_with_filter(|p| p.id.0 == *payment_hash)
            .into_iter()
            .next();
        // TODO: Get bolt11.
        Ok(None)
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
            max_receivable_msat: MAX_PAYMENT_AMOUNT_MSAT,
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

    async fn send_bolt12_payment(
        &self,
        offer: String,
        payment_id: String,
        amount_msat: Option<u64>,
    ) -> NodeResult<Payment> {
        let offer = Offer::from_str(&offer).unwrap();
        let payment_id = hex::decode(payment_id).unwrap();
        let payment_id = PaymentId(payment_id.as_slice().try_into().unwrap());

        let payments = self.node.bolt12_payment();
        let payment_id = match amount_msat {
            Some(amount) => payments.send_using_amount(&offer, payment_id, amount, None, None),
            None => payments.send(&offer, payment_id, None, None),
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
        let address = Address::from_str(&to_address)
            .unwrap()
            .require_network(self.node.config().network)
            .unwrap();
        let fee_rate = FeeRate::from_sat_per_vb(sat_per_vbyte as u64).unwrap();
        let txid = self
            .node
            .onchain_payment()
            .send_all_to_address(&address, false, Some(fee_rate))
            .map_err(to_node_error)?;
        let txid: &[u8] = txid.as_ref();
        Ok(txid.to_vec())
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
        self.payments_rx
            .lock()
            .await
            .take()
            .ok_or(NodeError::generic("Invoice stream has already started"))
    }

    async fn stream_log_messages(
        &self,
    ) -> NodeResult<mpsc::Receiver<gl_client::signer::model::greenlight::LogEntry>> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
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

    async fn max_sendable_amount<'a>(
        &self,
        payee_node_id: Option<Vec<u8>>,
        max_hops: u32,
        last_hop_hint: Option<&'a RouteHintHop>,
    ) -> NodeResult<Vec<MaxChannelAmount>> {
        todo!()
    }

    async fn derive_bip32_key(&self, path: Vec<ChildNumber>) -> NodeResult<ExtendedPrivKey> {
        let network = from_ldk_network(&self.node.config().network);
        Ok(ExtendedPrivKey::new_master(network.into(), &self.seed)?
            .derive_priv(&Secp256k1::new(), &path)?)
    }

    async fn legacy_derive_bip32_key(&self, path: Vec<ChildNumber>) -> NodeResult<ExtendedPrivKey> {
        self.derive_bip32_key(path).await
    }

    async fn stream_custom_messages(
        &self,
    ) -> NodeResult<Pin<Box<dyn Stream<Item = Result<CustomMessage>> + Send>>> {
        // For LSPS0.
        todo!()
    }

    async fn send_custom_message(&self, message: CustomMessage) -> NodeResult<()> {
        // For LSPS0.
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
        ldk_node::payment::PaymentKind::Bolt12Offer {
            hash,
            preimage,
            secret: _,
            offer_id,
            ..
        } => PaymentDetails::Ln {
            data: LnPaymentDetails {
                payment_hash: hash.as_ref().map(hex).unwrap_or_default(),
                label: String::new(),
                payment_preimage: preimage.as_ref().map(hex).unwrap_or_default(),
                keysend: false,
                bolt11: hex(offer_id),
                open_channel_bolt11: None,
                ..Default::default()
            },
        },
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

fn to_ldk_network(network: &crate::prelude::Network) -> ldk_node::bitcoin::network::Network {
    match network {
        crate::prelude::Network::Bitcoin => ldk_node::bitcoin::network::Network::Bitcoin,
        crate::prelude::Network::Testnet => ldk_node::bitcoin::network::Network::Testnet,
        crate::prelude::Network::Signet => ldk_node::bitcoin::network::Network::Signet,
        crate::prelude::Network::Regtest => ldk_node::bitcoin::network::Network::Regtest,
    }
}
fn from_ldk_network(network: &ldk_node::bitcoin::network::Network) -> crate::prelude::Network {
    match network {
        ldk_node::bitcoin::network::Network::Bitcoin => crate::prelude::Network::Bitcoin,
        ldk_node::bitcoin::network::Network::Testnet => crate::prelude::Network::Testnet,
        ldk_node::bitcoin::network::Network::Testnet4 => crate::prelude::Network::Testnet,
        ldk_node::bitcoin::network::Network::Signet => crate::prelude::Network::Signet,
        ldk_node::bitcoin::network::Network::Regtest => crate::prelude::Network::Regtest,
        network => panic!("Unexpected network {network}"),
    }
}
