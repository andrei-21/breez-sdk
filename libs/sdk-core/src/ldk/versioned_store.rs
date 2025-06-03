use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait VersionedStore {
    async fn get(&self, key: String) -> Result<Option<(Vec<u8>, i64)>>;

    async fn put(&self, key: String, value: &[u8], version: i64) -> Result<()>;

    async fn delete(&self, key: String) -> Result<()>;

    async fn list(&self) -> Result<Vec<(String, i64)>>;
}
