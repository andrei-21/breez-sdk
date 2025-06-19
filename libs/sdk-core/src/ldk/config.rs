pub(crate) struct Config {
    pub esplora_url: String,
    pub rgs_url: String,
    pub vss_url: String,
}

impl Config {
    pub fn mainnet() -> Self {
        Self {
            esplora_url: "https://blockstream.info/api".to_string(),
            rgs_url: "https://rapidsync.lightningdevkit.org/snapshot/v2".to_string(),
            vss_url: "http://localhost:4080/vss".to_string(),
        }
    }

    pub fn regtest() -> Self {
        Self {
            esplora_url: "http://localhost:30000".to_string(),
            rgs_url: "http://localhost:8011/v2".to_string(),
            vss_url: "http://localhost:3080/vss".to_string(),
        }
    }
}
