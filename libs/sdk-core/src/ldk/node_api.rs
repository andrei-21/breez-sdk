use std::collections::HashSet;
use std::pin::Pin;

use anyhow::Result;
use futures::Stream;

use sdk_common::prelude::*;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tonic::Streaming;

use crate::bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey};
use crate::lightning_invoice::RawBolt11Invoice;
use crate::node_api::{CreateInvoiceRequest, FetchBolt11Result, NodeAPI, NodeResult};
use crate::{models::*, LspInformation};
use crate::{PrepareRedeemOnchainFundsRequest, PrepareRedeemOnchainFundsResponse};

pub(crate) struct Ldk;

impl Ldk {
    pub fn new() -> Self {
        Self {}
    }
}

#[allow(unused_variables)]
#[tonic::async_trait]
impl NodeAPI for Ldk {
    async fn node_credentials(&self) -> NodeResult<Option<NodeCredentials>> {
        todo!()
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
        todo!()
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
        todo!()
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

    /// Starts the signer that listens in a loop until the shutdown signal is received
    async fn start_signer(&self, shutdown: mpsc::Receiver<()>) {
        todo!()
    }

    async fn start_keep_alive(&self, shutdown: watch::Receiver<()>) {
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
    ) -> NodeResult<Streaming<gl_client::signer::model::greenlight::IncomingPayment>> {
        todo!()
    }

    async fn stream_log_messages(
        &self,
    ) -> NodeResult<Streaming<gl_client::signer::model::greenlight::LogEntry>> {
        todo!()
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
        todo!()
    }

    async fn legacy_derive_bip32_key(&self, path: Vec<ChildNumber>) -> NodeResult<ExtendedPrivKey> {
        todo!()
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
