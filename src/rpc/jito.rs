use std::{fmt::Formatter, sync::Arc};

use anyhow::{anyhow, Result};
use base64::{prelude::BASE64_STANDARD, Engine};
use futures_util::stream::StreamExt;
use log::{debug, error, info};
use rand::Rng;
use serde::{de, Deserialize};
use serde_json::{json, Value};
use solana_program::pubkey;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use solana_transaction_status::UiTransactionEncoding;
use tokio::{sync::RwLock, task::JoinHandle};

// https://jito-labs.gitbook.io/mev/searcher-services/recommendations#bundle-assembly
// - Ensure your tip is an instruction in the last transaction.
// - Run a balance assertion after tipping to ensure the transaction doesn't result in a loss.
// - Provide slot range checks to confirm bundles land within the expected range.
// - To enhance Solana's performance, randomly choose one of the eight tip accounts to transfer SOL to, maximizing the chance of parallelism.

// https://jito-labs.gitbook.io/mev/searcher-resources/bundles

const BUNDLES: &str = "/api/v1/bundles";
const TXNS: &str = "/api/v1/transactions";

#[derive(Debug, Deserialize)]
pub struct JitoResponse<T> {
    pub result: T,
}

pub struct JitoClient {
    client: reqwest::Client,
    bundles_endpoint: String,
    txns_endpoint: String,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum JitoRegion {
    Amsterdam,
    #[default]
    NewYork,
    Frankfurt,
    Tokyo,
}

impl JitoClient {
    pub fn new(region: Option<JitoRegion>) -> Self {
        let region = region.unwrap_or_default();
        let block_engine = match region {
            JitoRegion::Amsterdam => &JITO_BLOCK_ENGINES[0],
            JitoRegion::Frankfurt => &JITO_BLOCK_ENGINES[1],
            JitoRegion::NewYork => &JITO_BLOCK_ENGINES[2],
            JitoRegion::Tokyo => &JITO_BLOCK_ENGINES[3],
        };
        let bundles_endpoint = format!("{}{}", block_engine.url, BUNDLES);
        let txns_endpoint = format!("{}{}", block_engine.url, TXNS);
        JitoClient {
            client: reqwest::Client::new(),
            bundles_endpoint,
            txns_endpoint,
        }
    }

    pub fn build_bribe_ix(pubkey: &Pubkey, value: u64) -> solana_sdk::instruction::Instruction {
        solana_sdk::system_instruction::transfer(pubkey, pick_jito_recipient(), value)
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> Result<String> {
        let encoded = serialize_and_encode(tx, UiTransactionEncoding::Base58)?;
        Ok(self
            .make_request::<JitoResponse<String>>(
                &self.txns_endpoint,
                "sendTransaction",
                json!(vec![encoded]),
            )
            .await?
            .result)
    }

    /// Send a bundle to Jito. The signature of the first txn serves as the identifier for the entire bundle
    pub async fn send_bundle(&self, bundle: Vec<&VersionedTransaction>) -> Result<String> {
        let expected_length = bundle.len();
        let bundle = bundle
            .into_iter()
            .filter_map(|tx| serialize_and_encode(&tx, UiTransactionEncoding::Base58).ok()) // unwrap should be fine too
            .collect::<Vec<_>>();
        if bundle.len() != expected_length {
            return Err(anyhow!("Failed to serialize bundles"));
        }

        Ok(self
            .make_request::<JitoResponse<String>>(
                &self.bundles_endpoint,
                "sendBundle",
                json!([bundle]),
            )
            .await?
            .result)
    }

    async fn make_request<T>(&self, url: &str, method: &'static str, params: Value) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => return Err(anyhow!("fail to send request: {err}")),
        };

        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(err) => return Err(anyhow!("fail to read response content: {err:#}")),
        };

        if !status.is_success() {
            return Err(anyhow!("status code: {status}, response: {text}"));
        }

        let response: T = match serde_json::from_str(&text) {
            Ok(response) => response,
            Err(err) => {
                return Err(anyhow!(
                    "fail to deserialize response: {err:#}, response: {text}, status: {status}"
                ))
            }
        };

        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct JitoTips {
    #[serde(rename = "landed_tips_25th_percentile")]
    pub p25_landed: f64,

    #[serde(rename = "landed_tips_50th_percentile")]
    pub p50_landed: f64,

    #[serde(rename = "landed_tips_75th_percentile")]
    pub p75_landed: f64,

    #[serde(rename = "landed_tips_95th_percentile")]
    pub p95_landed: f64,

    #[serde(rename = "landed_tips_99th_percentile")]
    pub p99_landed: f64,
}

impl JitoTips {
    pub fn p75(&self) -> u64 {
        (self.p75_landed * 1e9f64) as u64
    }

    pub fn p50(&self) -> u64 {
        (self.p50_landed * 1e9f64) as u64
    }

    pub fn p25(&self) -> u64 {
        (self.p25_landed * 1e9f64) as u64
    }

    pub fn p95(&self) -> u64 {
        (self.p95_landed * 1e9f64) as u64
    }
}

impl std::fmt::Display for JitoTips {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tips(p25={},p50={},p75={},p95={},p99={})",
            (self.p25_landed * 1e9f64) as u64,
            (self.p50_landed * 1e9f64) as u64,
            (self.p75_landed * 1e9f64) as u64,
            (self.p95_landed * 1e9f64) as u64,
            (self.p99_landed * 1e9f64) as u64
        )
    }
}

pub fn subscribe_jito_tips(tips: Arc<RwLock<JitoTips>>) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn({
        let tips = tips.clone();
        async move {
            let url = "ws://bundles-api-rest.jito.wtf/api/v1/bundles/tip_stream";

            loop {
                let stream = match tokio_tungstenite::connect_async(url).await {
                    Ok((ws_stream, _)) => ws_stream,
                    Err(err) => {
                        error!("fail to connect to jito tip stream: {err:#}");
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let (_, read) = stream.split();

                read.for_each(|message| async {
                    let data = match message {
                        Ok(data) => data.into_data(),
                        Err(err) => {
                            error!("fail to read jito tips message: {err:#}");
                            return;
                        }
                    };

                    let data = match serde_json::from_slice::<Vec<JitoTips>>(&data) {
                        Ok(t) => t,
                        Err(err) => {
                            debug!("fail to parse jito tips: {err:#}");
                            return;
                        }
                    };

                    if data.is_empty() {
                        return;
                    }

                    let tip = data.first().unwrap();
                    log::trace!("tip: {}", tip);
                    *tips.write().await = *tip;
                })
                .await;

                info!("jito tip stream disconnected, retries in 5 seconds");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }

            #[allow(unreachable_code)]
            Ok(())
        }
    })
}

pub async fn jito_tip_stream(tips: Arc<RwLock<JitoTips>>) {
    subscribe_jito_tips(tips.clone());
    loop {
        let tips = *tips.read().await;
        debug!("tips: {}", tips);
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

pub const JITO_TIP_ACCOUNTS: [Pubkey; 8] = [
    pubkey!("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
    pubkey!("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe"),
    pubkey!("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"),
    pubkey!("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49"),
    pubkey!("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh"),
    pubkey!("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt"),
    pubkey!("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL"),
    pubkey!("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"),
];

pub struct BlockEngine<'a> {
    url: &'a str,
    #[allow(dead_code)]
    shred_receiver_addr: &'a str,
    #[allow(dead_code)]
    relayer_url: &'a str,
}

pub const JITO_BLOCK_ENGINES: [BlockEngine; 4] = [
    BlockEngine {
        url: "https://amsterdam.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "74.118.140.240:1002",
        relayer_url: "http://amsterdam.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://frankfurt.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "145.40.93.84:1002",
        relayer_url: "http://frankfurt.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://ny.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "141.98.216.96:1002",
        relayer_url: "http://ny.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://tokyo.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "202.8.9.160:1002",
        relayer_url: "http://tokyo.mainnet.relayer.jito.wtf:8100",
    },
];

pub const BUNDLE_ENDPOINT: &str = "/api/v1/bundles";
pub const TRANSACTIONS_ENDPOINT: &str = "/api/v1/transactions";

pub fn pick_jito_recipient() -> &'static Pubkey {
    &JITO_TIP_ACCOUNTS[rand::thread_rng().gen_range(0..JITO_TIP_ACCOUNTS.len())]
}

fn serialize_and_encode<T>(input: &T, encoding: UiTransactionEncoding) -> Result<String>
where
    T: serde::ser::Serialize,
{
    let serialized = bincode::serialize(input).map_err(|e| anyhow!("Serialization failed: {e}"))?;
    let encoded = match encoding {
        UiTransactionEncoding::Base58 => solana_sdk::bs58::encode(serialized).into_string(),
        UiTransactionEncoding::Base64 => BASE64_STANDARD.encode(serialized),
        _ => {
            return Err(anyhow!(
                "unsupported encoding: {encoding}. Supported encodings: base58, base64"
            ))
        }
    };
    Ok(encoded)
}
