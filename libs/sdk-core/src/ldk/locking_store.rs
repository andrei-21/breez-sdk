use crate::ldk::versioned_store::VersionedStore;
use anyhow::{ensure, Result};
use async_trait::async_trait;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct LockingStore<S: VersionedStore + Send + Sync> {
    inner: S,
    tl: Mutex<TimeLock>,
}

impl<S: VersionedStore + Send + Sync> LockingStore<S> {
    const KEY: &str = "lock";

    pub async fn new(instance_id: String, store: S) -> Result<Self> {
        let (lock_data, version) = store.get(Self::KEY.to_string()).await?.unwrap_or_default();
        let lock_data = LockData::decode(lock_data)?;
        // if current_lock.instance_id.is_empty() || current_lock.instance_id == instance_id {
        // 	// Nothing to pull from remote.
        // }
        let mut tl = TimeLock::new(instance_id, lock_data, version)?;
        let (lock_data, version) = tl.next_lock();
        store
            .put(Self::KEY.to_string(), &lock_data.encode(), version)
            .await?;
        tl.update_lock(lock_data.locked_until, version + 1);

        let tl = Mutex::new(tl);

        let locking_store = Self { inner: store, tl };
        locking_store.lock().await?;

        Ok(locking_store)
    }

    pub async fn refresh_lock(&self) -> Result<tokio::time::Instant> {
        self.lock().await?;
        debug!("Remote lock refreshed");
        Ok(tokio::time::Instant::now() + Duration::from_secs(TimeLock::REFRESH_PERIOD_SECS))
    }

    async fn lock(&self) -> Result<()> {
        let (lock_data, version) = self.tl.lock().await.next_lock();
        self.inner
            .put(Self::KEY.to_string(), &lock_data.encode(), version)
            .await?;
        self.tl
            .lock()
            .await
            .update_lock(lock_data.locked_until, version + 1);
        Ok(())
    }

    pub async fn unlock(&self) -> Result<()> {
        if let Some((lock_data, version)) = self.tl.lock().await.unlock() {
            self.inner
                .put(Self::KEY.to_string(), &lock_data.encode(), version)
                .await?
        }
        Ok(())
    }

    async fn ensure_locked(&self) -> Result<()> {
        ensure!(
            self.tl.lock().await.is_locked(),
            "Remote lock was not aquired"
        );
        Ok(())
    }
}

#[async_trait]
impl<S: VersionedStore + Send + Sync> VersionedStore for LockingStore<S> {
    async fn get(&self, key: String) -> Result<Option<(Vec<u8>, i64)>> {
        self.ensure_locked().await?;
        self.inner.get(key).await
    }

    async fn put(&self, key: String, value: &[u8], version: i64) -> Result<()> {
        self.ensure_locked().await?;
        self.inner.put(key, value, version).await
    }

    async fn delete(&self, key: String) -> Result<()> {
        self.ensure_locked().await?;
        self.inner.delete(key).await
    }

    async fn list(&self) -> Result<Vec<(String, i64)>> {
        self.ensure_locked().await?;
        self.inner.list().await
    }
}

struct TimeLock {
    instance_id: String,
    locked_until: SystemTime,
    version: i64,
}

impl TimeLock {
    const LOCK_DURATION_SECS: u64 = 60;
    const REFRESH_PERIOD_SECS: u64 = 10;

    fn new(instance_id: String, latest_lock_data: LockData, latest_version: i64) -> Result<Self> {
        let remote_instance_id = latest_lock_data.instance_id;
        ensure!(
            instance_id == remote_instance_id || latest_lock_data.locked_until < SystemTime::now(),
            "Remote lock is aquired by {remote_instance_id}"
        );
        Ok(Self {
            instance_id,
            locked_until: UNIX_EPOCH,
            version: latest_version,
        })
    }

    fn is_locked(&self) -> bool {
        SystemTime::now() < self.locked_until
    }

    fn next_lock(&self) -> (LockData, i64) {
        let lock_data = LockData {
            locked_until: SystemTime::now() + Duration::from_secs(Self::LOCK_DURATION_SECS),
            instance_id: self.instance_id.clone(),
        };
        (lock_data, self.version)
    }

    fn unlock(&mut self) -> Option<(LockData, i64)> {
        if self.is_locked() {
            self.locked_until = UNIX_EPOCH;
            let lock_data = LockData {
                locked_until: self.locked_until,
                instance_id: self.instance_id.clone(),
            };
            return Some((lock_data, self.version));
        }
        None
    }

    fn update_lock(&mut self, locked_until: SystemTime, version: i64) {
        self.locked_until = locked_until;
        self.version = version;
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
