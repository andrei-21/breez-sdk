use crate::ldk::versioned_store::VersionedStore;
use ldk_node::bitcoin::io::ErrorKind;
use ldk_node::lightning::io;
use ldk_node::lightning::util::persist::KVStore;
use rusqlite::{params, Connection, OptionalExtension};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

pub struct MirroringStore<S: Deref<Target = T>, T: VersionedStore + Send + Sync> {
    handle: Handle,
    remote_client: S,
    conn: Arc<Mutex<Connection>>,
}

impl<S: Deref<Target = T>, T: VersionedStore + Send + Sync> MirroringStore<S, T> {
    pub async fn new(
        handle: Handle,
        conn: Connection,
        remote: S,
        // TODO: Track if this instance was the last one to hold the lock.
    ) -> anyhow::Result<Self> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS store (
                primary_ns TEXT NOT NULL,
                secondary_ns TEXT NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                local_version INTEGER NOT NULL,
                remote_version INTEGER NOT NULL DEFAULT -1,
                PRIMARY KEY (primary_ns, secondary_ns, key)
            )",
            [],
        )?;

        let i_was_the_last = false;
        let is_dirty = is_dirty(&conn)?;
        match (i_was_the_last, is_dirty) {
            (true, false) => (),  // Nothing to do.
            (true, true) => (),   // Upload local versions.
            (false, false) => (), // Download remote versions.
            (false, true) => (),  // Replace local with remote versions.
                                   // TODO: Mind malicious VSS server.
        };
        if !i_was_the_last || is_dirty {
            info!("Running reconcilation");
            reconcile(&conn, &*remote).await?;
        }

        Ok(Self {
            handle,
            conn: Arc::new(Mutex::new(conn)),
            remote_client: remote,
        })
    }
}

impl<S: Deref<Target = T>, T: VersionedStore + Send + Sync> KVStore for MirroringStore<S, T> {
    fn read(&self, primary_ns: &str, secondary_ns: &str, key: &str) -> io::Result<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM store WHERE primary_ns = ?1 AND secondary_ns = ?2 AND key = ?3",
            params![primary_ns, secondary_ns, key],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
        .ok_or(io::Error::new(ErrorKind::NotFound, "Not Found"))
    }

    fn write(
        &self,
        primary_ns: &str,
        secondary_ns: &str,
        key: &str,
        value: &[u8],
    ) -> io::Result<()> {
        debug!(
            "Writing {primary_ns}/{secondary_ns}/{key} {} bytes",
            value.len()
        );
        let conn = self.conn.lock().unwrap();

        let local_data: Option<(i64, Vec<u8>)> = conn.query_row(
            "SELECT local_version, value FROM store WHERE primary_ns = ?1 AND secondary_ns = ?2 AND key = ?3",
            params![primary_ns, secondary_ns, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let next_version = match local_data {
            None => {
                let next_version = 0;
                conn.execute(
                    "INSERT INTO store (primary_ns, secondary_ns, key, value, local_version, remote_version) VALUES (?1, ?2, ?3, ?4, ?5, -1)",
                    params![primary_ns, secondary_ns, key, value, next_version],
                ).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                next_version
            }
            Some((_, local_value)) if local_value == value => {
                debug!("Local value is the same, skipping writing");
                return Ok(());
            }
            Some((local_version, _)) => {
                debug!("Local value is different, writing");
                let next_version = local_version + 1;
                conn.execute(
                    "UPDATE store SET value = ?1, local_version = ?2 WHERE primary_ns = ?3 AND secondary_ns = ?4 AND key = ?5",
                    params![value, next_version, primary_ns, secondary_ns, key],
                ).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                next_version
            }
        };

        let full_key = format!("{primary_ns}/{secondary_ns}/{key}");
        let result = tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.remote_client.put(full_key, value, next_version))
        });
        if let Err(e) = result {
            error!("Error on remote: {e}");
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }

        conn.execute(
            "UPDATE store SET remote_version = local_version WHERE primary_ns = ?1 AND secondary_ns = ?2 AND key = ?3",
            params![primary_ns, secondary_ns, key],
        ).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }

    fn remove(
        &self,
        primary_ns: &str,
        secondary_ns: &str,
        key: &str,
        _lazy: bool,
    ) -> io::Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "DELETE FROM store WHERE primary_ns = ?1 AND secondary_ns = ?2 AND key = ?3",
            params![primary_ns, secondary_ns, key],
        )
        .unwrap();

        let full_key = format!("{primary_ns}/{secondary_ns}/{key}");
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.remote_client.delete(full_key))
                .unwrap()
        });

        Ok(())
    }

    fn list(&self, primary_ns: &str, secondary_ns: &str) -> io::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let keys: Vec<_> = conn
            .prepare("SELECT key FROM store WHERE primary_ns = ?1 AND secondary_ns = ?2")
            .unwrap()
            .query_map(params![primary_ns, secondary_ns], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();

        Ok(keys)
    }
}

fn is_dirty(conn: &Connection) -> rusqlite::Result<bool> {
    let dirty_rows: i64 = conn.query_row(
        "SELECT count(1) FROM store WHERE local_version != remote_version",
        [],
        |row| row.get(0),
    )?;
    Ok(dirty_rows > 0)
}

async fn reconcile<S: VersionedStore>(conn: &Connection, remote: &S) -> anyhow::Result<()> {
    conn.execute("DELETE FROM store", [])?;

    let remote_versions = remote.list().await?;

    for (full_key, version) in remote_versions {
        trace!("Downloading {full_key} @ {version} ...");
        let parts: Vec<&str> = full_key.splitn(3, '/').collect();
        let (primary, secondary, key) = match &parts[..] {
            [p, s, k] => (p.to_string(), s.to_string(), k.to_string()),
            _ => continue, // skip malformed keys
        };

        if let Some((value, version)) = remote.get(full_key).await? {
            trace!("Got {} bytes @ {version}", value.len());
            conn.execute(
                "INSERT INTO store (primary_ns, secondary_ns, key, value, local_version, remote_version) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                params![primary, secondary, key, value, version - 1],
            )?;
        }
    }
    Ok(())
}
