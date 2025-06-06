use std::time::Duration;
use vss_client::client::VssClient;
use vss_client::error::VssError;
use vss_client::util::retry::{ExponentialBackoffRetryPolicy, MaxAttemptsRetryPolicy, RetryPolicy};

use sdk_common::bitcoin::hashes::hex::ToHex;
use sdk_common::bitcoin::hashes::sha256::Hash as Sha256;
use sdk_common::bitcoin::hashes::Hash;

use crate::backup::{BackupState, BackupTransport};
use crate::error::{SdkError, SdkResult};
use crate::ldk::versioned_store::VersionedStore;
use crate::ldk::vss_store::VssStore;

pub(crate) struct LdkBackupTransport {
    store: VssStore<MaxAttemptsRetryPolicy<ExponentialBackoffRetryPolicy<VssError>>>,
}

impl LdkBackupTransport {
    const KEY: &str = "backup";

    pub fn new(seed: &[u8]) -> Self {
        let seed_hash = Sha256::hash(seed).to_hex();
        let store_id = format!("{seed_hash}/backups");
        let vss_client = VssClient::new(
            "http://localhost:3080/vss".to_string(),
            ExponentialBackoffRetryPolicy::<VssError>::new(Duration::from_secs(1))
                .with_max_attempts(2),
        );
        let store = VssStore::new(vss_client, store_id);
        Self { store }
    }
}

#[tonic::async_trait]
impl BackupTransport for LdkBackupTransport {
    async fn pull(&self) -> SdkResult<Option<BackupState>> {
        debug!("Pulling backup");
        match self.store.get(Self::KEY.to_string()).await {
            Ok(Some((data, version))) => Ok(Some(BackupState {
                generation: version as u64,
                data,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(SdkError::generic(&e.to_string())),
        }
    }

    async fn push(&self, version: Option<u64>, hex: Vec<u8>) -> SdkResult<u64> {
        debug!("Pushing backup with version {version:?}");
        let version = version.unwrap_or_default() as i64;
        match self.store.put(Self::KEY.to_string(), &hex, version).await {
            Ok(()) => Ok((version + 1) as u64),
            Err(e) => Err(SdkError::generic(&e.to_string())),
        }
    }
}
