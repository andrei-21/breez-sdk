use crate::{
    backup::{BackupState, BackupTransport},
    error::SdkResult,
};

pub(crate) struct VssBackupTransport;

#[allow(unused_variables)]
#[tonic::async_trait]
impl BackupTransport for VssBackupTransport {
    async fn pull(&self) -> SdkResult<Option<BackupState>> {
        todo!()
    }

    async fn push(&self, version: Option<u64>, hex: Vec<u8>) -> SdkResult<u64> {
        todo!()
    }
}
