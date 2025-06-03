use anyhow::Result;
use async_trait::async_trait;
use vss_client::client::VssClient;
use vss_client::error::VssError;
use vss_client::types::{
    DeleteObjectRequest, GetObjectRequest, GetObjectResponse, KeyValue, ListKeyVersionsRequest,
    PutObjectRequest,
};
use vss_client::util::retry::RetryPolicy;

use crate::ldk::versioned_store::VersionedStore;

pub struct VssStore<P: RetryPolicy<E = VssError> + Send + Sync> {
    client: VssClient<P>,
    store_id: String,
}

impl<P: RetryPolicy<E = VssError> + Send + Sync> VssStore<P> {
    pub fn new(client: VssClient<P>, store_id: String) -> Self {
        Self { client, store_id }
    }
}

#[async_trait]
impl<P: RetryPolicy<E = VssError> + Send + Sync> VersionedStore for VssStore<P> {
    async fn get(&self, key: String) -> Result<Option<(Vec<u8>, i64)>> {
        let request = GetObjectRequest {
            store_id: self.store_id.clone(),
            key,
        };

        match self.client.get_object(&request).await {
            Ok(GetObjectResponse { value: Some(kv) }) => Ok(Some((kv.value, kv.version))),
            Ok(GetObjectResponse { value: None }) => Ok(None),
            Err(VssError::NoSuchKeyError(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: String, value: &[u8], version: i64) -> Result<()> {
        let key_value = KeyValue {
            key,
            version,
            value: value.to_vec(),
        };

        let request = PutObjectRequest {
            store_id: self.store_id.clone(),
            transaction_items: vec![key_value],
            ..Default::default()
        };

        self.client.put_object(&request).await?;
        Ok(())
    }

    async fn delete(&self, key: String) -> Result<()> {
        let key_value = KeyValue {
            key,
            version: -1,
            value: Vec::new(),
        };

        let request = DeleteObjectRequest {
            store_id: self.store_id.clone(),
            key_value: Some(key_value),
        };

        self.client.delete_object(&request).await?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<(String, i64)>> {
        let request = ListKeyVersionsRequest {
            store_id: self.store_id.clone(),
            ..Default::default()
        };
        let response = self
            .client
            .list_key_versions(&request)
            .await?
            .key_versions
            .into_iter()
            .map(|kv| (kv.key, kv.version))
            .collect();
        Ok(response)
    }
}
