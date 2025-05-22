use anyhow::{bail, ensure, Result};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vss_client::client::VssClient;
use vss_client::error::VssError;
use vss_client::types::{GetObjectRequest, KeyValue, PutObjectRequest};
use vss_client::util::retry::RetryPolicy;
use vss_client::util::retry::{ExponentialBackoffRetryPolicy, MaxAttemptsRetryPolicy};

struct TimeLock {
    local_instance_id: String,
    locked_for_instance_id: String,
    locked_until: SystemTime,
    version: i64,
}

impl TimeLock {
    const KEY: &str = "lock";
    const LOCK_DURATION_SECS: u64 = 60;

    fn new(instance_id: String, latest_key_value: KeyValue) -> Result<Self> {
        let lock_data = LockData::decode(latest_key_value.value)?;
        Ok(Self {
            local_instance_id: instance_id,
            locked_for_instance_id: lock_data.instance_id,
            locked_until: lock_data.locked_until,
            version: latest_key_value.version,
        })
    }

    fn is_local_locked(&self) -> bool {
        SystemTime::now() < self.locked_until
            && self.local_instance_id == self.locked_for_instance_id
    }

    fn is_foreign_locked(&self) -> bool {
        SystemTime::now() < self.locked_until
            && self.local_instance_id != self.locked_for_instance_id
    }

    fn lock(&mut self) -> Result<KeyValue> {
        let locked_for_instance_id = &self.locked_for_instance_id;
        ensure!(
            !self.is_foreign_locked(),
            "Vss lock taken by {locked_for_instance_id}"
        );
        Ok(self.update_lock(SystemTime::now() + Duration::from_secs(Self::LOCK_DURATION_SECS)))
    }

    fn unlock(&mut self) -> Option<KeyValue> {
        if self.is_local_locked() {
            Some(self.update_lock(UNIX_EPOCH))
        } else {
            None
        }
    }

    fn update_lock(&mut self, locked_until: SystemTime) -> KeyValue {
        let lock_version = LockData {
            locked_until,
            instance_id: self.local_instance_id.clone(),
        };
        let key_value = KeyValue {
            key: Self::KEY.to_string(),
            version: self.version,
            value: lock_version.encode(),
        };
        self.version += 1;
        self.locked_for_instance_id = self.local_instance_id.clone();
        self.locked_until = locked_until;
        key_value
    }
}

struct LockData {
    locked_until: SystemTime,
    instance_id: String,
}

impl LockData {
    fn decode(data: Vec<u8>) -> Result<Self> {
        let data = String::from_utf8(data)?;
        let (timestamp, instance_id) = data.split_once("/").unwrap_or_default();
        let timestamp = match timestamp {
            "" => "0",
            timestamp => timestamp,
        };
        let timestamp: u64 = timestamp.parse()?;
        let timestamp = Duration::new(timestamp, 0);
        let locked_until = UNIX_EPOCH + timestamp;
        Ok(Self {
            locked_until,
            instance_id: instance_id.to_string(),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let locked_until = self
            .locked_until
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{locked_until}/{}", self.instance_id)
            .as_bytes()
            .to_vec()
    }
}

pub struct VssLock {
    store_id: String,
    client: VssClient<MaxAttemptsRetryPolicy<ExponentialBackoffRetryPolicy<VssError>>>,
    time_lock: TimeLock,
}

impl VssLock {
    pub async fn new(instance_id: String, store_id: String) -> Result<Self> {
        let client = VssClient::new(
            "http://localhost:3080/vss".to_string(),
            ExponentialBackoffRetryPolicy::<VssError>::new(Duration::from_secs(1))
                .with_max_attempts(2),
        );

        let key_value = match client
            .get_object(&GetObjectRequest {
                store_id: store_id.clone(),
                key: TimeLock::KEY.to_string(),
            })
            .await
        {
            Ok(response) => response.value.unwrap_or_default(),
            Err(VssError::NoSuchKeyError(_)) => Default::default(),
            Err(e) => bail!(e),
        };

        let mut time_lock = TimeLock::new(instance_id, key_value)?;

        // Locking.
        let key_value = time_lock.lock()?;
        client
            .put_object(&PutObjectRequest {
                store_id: store_id.clone(),
                global_version: None,
                transaction_items: vec![key_value],
                delete_items: Vec::new(),
            })
            .await?;

        Ok(Self {
            store_id,
            client,
            time_lock,
        })
    }

    async fn refresh(&mut self) -> Result<()> {
        let key_value = self.time_lock.lock()?;
        self.client
            .put_object(&PutObjectRequest {
                store_id: self.store_id.clone(),
                global_version: None,
                transaction_items: vec![key_value],
                delete_items: Vec::new(),
            })
            .await?;
        Ok(())
    }

    async fn unlock(&mut self) -> Result<()> {
        if let Some(key_value) = self.time_lock.unlock() {
            self.client
                .put_object(&PutObjectRequest {
                    store_id: self.store_id.clone(),
                    global_version: None,
                    transaction_items: vec![key_value],
                    delete_items: Vec::new(),
                })
                .await?;
        }
        Ok(())
    }
}

pub fn start_refresher(mut vss_lock: VssLock, mut shutdown_rx: tokio::sync::mpsc::Receiver<()>) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match vss_lock.refresh().await {
                        Ok(()) => info!("Vss lock was refreshed"),
                        Err(e) => {
                            error!("Failed to refresh vss lock: {e}");
                            break;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    info!("Releasing Vss lock");
                    match vss_lock.unlock().await {
                        Ok(()) => info!("Vss lock was released"),
                        Err(e) => error!("Failed to release vss lock: {e}"),
                    }
                    break;
                }
            }
        }
        drop(shutdown_rx);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn test_lock_data_encode() {
        let lock_data = LockData {
            locked_until: UNIX_EPOCH + Duration::from_secs(1234567890),
            instance_id: "test-instance-123".to_string(),
        };
        assert_eq!(
            lock_data.encode(),
            "1234567890/test-instance-123".as_bytes()
        );
    }

    #[test]
    fn test_lock_data_decode() {
        let decoded = LockData::decode("1234567890/test/instance".as_bytes().to_vec()).unwrap();
        assert_eq!(decoded.instance_id, "test/instance");
        assert_eq!(
            decoded.locked_until,
            UNIX_EPOCH + Duration::from_secs(1234567890)
        );
    }

    #[test]
    fn test_lock_data_decode_invalid_utf8() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = LockData::decode(invalid_utf8);
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_data_decode_invalid_format() {
        let invalid_format = "not-a-timestamp/instance-id".to_string();
        let result = LockData::decode(invalid_format.as_bytes().to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_data_decode_empty() {
        let result = LockData::decode("".as_bytes().to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_data_encode_decode_roundtrip() {
        let instance_id = "test-instance-123".to_string();
        let locked_until = SystemTime::now();

        let original = LockData {
            locked_until,
            instance_id: instance_id.clone(),
        };

        let encoded = original.encode();
        let decoded = LockData::decode(encoded).unwrap();

        assert_eq!(decoded.instance_id, instance_id);
        assert_eq!(decoded.locked_until, locked_until);
    }
}
