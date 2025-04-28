use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use futures::Stream;

use ldk_node::{Builder, Node};
use sdk_common::prelude::*;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::bitcoin::secp256k1::Secp256k1;
use crate::bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey};
use crate::lightning_invoice::RawBolt11Invoice;
use crate::node_api::{CreateInvoiceRequest, FetchBolt11Result, NodeAPI, NodeResult};
use crate::{models::*, LspInformation};
use crate::{PrepareRedeemOnchainFundsRequest, PrepareRedeemOnchainFundsResponse};

pub(crate) struct Ldk {
    seed: [u8; 64],
    node: Arc<Node>,
}

impl Ldk {
    pub fn build(seed: &[u8]) -> Self {
        let mut builder = Builder::new();

        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(&seed);
        let seed = bytes;
        builder.set_entropy_seed_bytes(seed.clone());

        builder.set_network(ldk_node::bitcoin::Network::Testnet);
        builder.set_chain_source_esplora("https://blockstream.info/testnet/api".to_string(), None);
        builder.set_gossip_source_rgs(
            "https://rapidsync.lightningdevkit.org/testnet/snapshot".to_string(),
        );
        let node = Arc::new(builder.build().unwrap());
        Self { seed, node }
    }
}

#[allow(unused_variables)]
#[tonic::async_trait]
impl NodeAPI for Ldk {
    /// Starts the node.
    async fn start_signer(&self, mut shutdown: mpsc::Receiver<()>) {
        debug!("Starting node");
        self.node.start().unwrap();
        debug!("Node started");
        let node = Arc::clone(&self.node);
        let _ = tokio::spawn(async move {
            let _ = shutdown.recv().await;
            debug!("Received shutdown signal");
            if let Err(e) = node.stop() {
                error!("{e}");
            }
            debug!("Node stopped");
        })
        .await;
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
        todo!()
    }

    async fn fetch_bolt11(&self, payment_hash: Vec<u8>) -> NodeResult<Option<FetchBolt11Result>> {
        todo!()
    }

    // implement pull changes from greenlight
    async fn pull_changed(
        &self,
        sync_state: Option<Value>,
        match_local_balance: bool,
    ) -> NodeResult<SyncResponse> {
        let response = SyncResponse {
            sync_state: Value::Null,
            node_state: NodeState::default(),
            payments: Vec::new(),
            channels: Vec::new(),
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
        todo!()
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
        todo!()
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
        todo!()
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

    async fn sign_invoice(&self, invoice: RawBolt11Invoice) -> NodeResult<String> {
        todo!()
    }

    async fn close_peer_channels(&self, node_id: String) -> NodeResult<Vec<String>> {
        todo!()
    }

    async fn stream_incoming_payments(
        &self,
    ) -> NodeResult<mpsc::Receiver<gl_client::signer::model::greenlight::IncomingPayment>> {
        let (send, recv) = mpsc::channel(10);
        Ok(recv)
    }

    async fn stream_log_messages(
        &self,
    ) -> NodeResult<mpsc::Receiver<gl_client::signer::model::greenlight::LogEntry>> {
        let (send, recv) = mpsc::channel(10);
        Ok(recv)
    }

    async fn static_backup(&self) -> NodeResult<Vec<String>> {
        todo!()
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
        let network = sdk_common::prelude::Network::Testnet;
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
