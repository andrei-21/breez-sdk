pub(crate) struct Config {
    pub esplora_url: String,
    pub rgs_url: String,
    pub vss_url: String,
    pub lsps2_id: &'static str,
    pub lsps2_address: &'static str,
}

impl Config {
    pub fn mainnet() -> Self {
        Self {
            esplora_url: "https://blockstream.info/api".to_string(),
            rgs_url: "https://rapidsync.lightningdevkit.org/snapshot/v2".to_string(),
            vss_url: "http://localhost:4080/vss".to_string(),
            lsps2_id: "038a9e56512ec98da2b5789761f7af8f280baf98a09282360cd6ff1381b5e889bf",
            lsps2_address: "64.23.162.51:9735",
        }
    }

    pub fn regtest() -> Self {
        Self {
            esplora_url: "http://localhost:30000".to_string(),
            rgs_url: "http://localhost:8011/v2".to_string(),
            vss_url: "http://localhost:3080/vss".to_string(),
            lsps2_id: "02b49b94e068e05c04c2ac98e096a06202d04920daec25d82f7898e21901f15d81",
            lsps2_address: "localhost:9735",
        }
    }
}
