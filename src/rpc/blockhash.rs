//! This service loops in the background to always obtain the most recent Blockhash. This is to prevent
//! latency in having to fetch it via RPC anytime we want to send a transaction.
//!
//! See https://solana.com/docs/core/transactions#recent-blockhash

use crate::types::BlockHashNotification;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use log::warn;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::RwLock;

const DEFAULT_BLOCKHASH_REFRESH_RATE: Duration = Duration::from_secs(20);

pub async fn get_blockhash_data_with_retry(
    rpc_client: &RpcClient,
    commitment: CommitmentConfig,
    retries: u8,
) -> anyhow::Result<(BlockHashNotification, u64)> {
    for i in 0..retries {
        match rpc_client
            .get_latest_blockhash_with_commitment(commitment)
            .await
        {
            Ok((blockhash, last_valid_block_height)) => {
                let blockhash_notif = BlockHashNotification {
                    blockhash,
                    last_valid_block_height,
                };
                return Ok((blockhash_notif, last_valid_block_height));
            }
            Err(e) => warn!("Retries = {}. Failed to get blockhash data", i),
        }
    }

    Err(anyhow!("Failed to get blockhash data after 3 retries"))
}

pub fn start_blockhash_polling_task(
    rpc_client: Arc<RpcClient>,
    blockhash_notif: Arc<RwLock<BlockHashNotification>>,
    current_block_height: Arc<AtomicU64>,
    commitment_config: CommitmentConfig,
    refresh_rate: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let duration = refresh_rate.unwrap_or(DEFAULT_BLOCKHASH_REFRESH_RATE);
        loop {
            if let Ok((blockhash, last_valid_block_height)) = rpc_client
                .get_latest_blockhash_with_commitment(commitment_config)
                .await
            {
                *blockhash_notif.write().await = BlockHashNotification {
                    blockhash,
                    last_valid_block_height,
                };
            }

            if let Ok(block_height) = rpc_client.get_block_height().await {
                current_block_height.store(block_height, Ordering::Relaxed);
            }
            tokio::time::sleep(duration).await;
        }
    })
}
