use super::types::{
    ConfirmTransactionData, ConfirmTransactionResponse, NotConfirmedReason, SendTransactionData,
    TransactionSourceId,
};

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use log::{error, trace};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::UnboundedSender;

#[allow(clippy::too_many_arguments)]
pub fn start_transaction_confirmation_task(
    rpc_client: Arc<RpcClient>,
    current_block_height: Arc<AtomicU64>,
    unconfirmed_transactions_map: Arc<DashMap<Signature, SendTransactionData>>,
    subscribers_map: Arc<DashMap<TransactionSourceId, UnboundedSender<ConfirmTransactionData>>>,
    broadcast_confirmation_sender: Sender<ConfirmTransactionData>,
    num_confirmed_transactions: Arc<AtomicUsize>,
    num_unconfirmed_transactions: Arc<AtomicUsize>,
    exit_signal: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn({
        let exit_signal = Arc::clone(&exit_signal);
        let unconfirmed_transactions_map = Arc::clone(&unconfirmed_transactions_map);
        let current_block_height = Arc::clone(&current_block_height);
        let subscribers_map = Arc::clone(&subscribers_map);
        async move {
            // check transactions that are not expired or have just expired between two checks
            let mut last_block_height = current_block_height.load(Ordering::Relaxed);

            while !exit_signal.load(Ordering::Relaxed) {
                if !unconfirmed_transactions_map.is_empty() {
                    let current_block_height = current_block_height.load(Ordering::Relaxed);
                    let transactions_to_verify: Vec<Signature> = unconfirmed_transactions_map
                        .iter()
                        .filter(|x| {
                            let is_not_expired = current_block_height <= x.last_valid_block_height;
                            // transaction expired between last and current check
                            let is_recently_expired = last_block_height
                                <= x.last_valid_block_height
                                && current_block_height > x.last_valid_block_height;
                            is_not_expired || is_recently_expired
                        })
                        .map(|x| *x.key())
                        .collect();
                    for signatures in
                        transactions_to_verify.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS)
                    {
                        // TODO: Schedule a retry here?
                        match rpc_client.get_signature_statuses(signatures).await {
                            Ok(result) => {
                                let statuses = result.value;
                                for (signature, status) in
                                    signatures.iter().zip(statuses.into_iter())
                                {
                                    if let Some((status, data)) = status
                                        .filter(|status| {
                                            status.satisfies_commitment(rpc_client.commitment())
                                        })
                                        .and_then(|status| {
                                            unconfirmed_transactions_map
                                                .remove(signature)
                                                .map(|(_, data)| (status, data))
                                        })
                                    {
                                        num_confirmed_transactions.fetch_add(1, Ordering::Relaxed);
                                        trace!(
                                            "Confirmed transaction with signature: {}",
                                            signature
                                        );
                                        let error = match status.err {
                                            Some(TransactionError::AlreadyProcessed) | None => None,
                                            Some(error) => Some(error),
                                        };

                                        let sender = subscribers_map.get(&data.source_id).unwrap();
                                        let id = data.txn_id;
                                        let confirmation_data = ConfirmTransactionData {
                                            data,
                                            response: ConfirmTransactionResponse::Confirmed {
                                                error,
                                                confirmation_slot: status.slot,
                                            },
                                        };
                                        if let Err(e) = sender.send(confirmation_data.clone()) {
                                            error!(
                                                "Failed sending confirmed tx {} over channel: {}",
                                                id, e
                                            );
                                        }
                                        _ = broadcast_confirmation_sender.send(confirmation_data);
                                    };
                                }
                            }
                            Err(e) => {
                                error!("Error getting signature statuses for confirmation: {}", e)
                            }
                        }
                    }

                    /////////////////////////////////////////////////////////////////////////////////////////////////
                    // The opposite of the above. We take transactions that have expired, and not recently
                    let expired_txns = unconfirmed_transactions_map
                        .iter()
                        .filter(|x| {
                            // Note: We're less conservative here so we get non-confirmations faster
                            let is_expired = current_block_height > x.last_valid_block_height;
                            let is_recently_expired = last_block_height
                                <= x.last_valid_block_height
                                && current_block_height > x.last_valid_block_height;
                            is_expired && !is_recently_expired
                            //is_expired
                        })
                        .map(|x| x.signature)
                        .collect::<Vec<_>>();

                    for signature in expired_txns {
                        trace!(
                            "Failed to confirm transaction with signature: {}",
                            signature
                        );
                        num_unconfirmed_transactions.fetch_add(1, Ordering::Relaxed);
                        let removed_entry = unconfirmed_transactions_map.remove(&signature);

                        match removed_entry {
                            Some((_, data)) => {
                                let entry = subscribers_map
                                    .get(&data.source_id)
                                    .expect("is valid confirmation id");
                                let sender = entry.value();
                                let id = data.source_id;
                                let confirmation_data = ConfirmTransactionData {
                                    data,
                                    response: ConfirmTransactionResponse::Unconfirmed {
                                        reason: NotConfirmedReason::Expired,
                                    },
                                };
                                if let Err(e) = sender.send(confirmation_data.clone()) {
                                    error!(
                                        "Failed sending confirmed tx {} over channel: {}",
                                        id, e
                                    );
                                }
                                _ = broadcast_confirmation_sender.send(confirmation_data);
                            }
                            None => {
                                unreachable!(
                                    "attempted duplicate removal of signature {}",
                                    signature
                                )
                            }
                        }
                    }
                    //////////////////////////////////////////////////////////////////////////////////////////////////

                    last_block_height = current_block_height;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            Ok::<_, anyhow::Error>(())
        }
    })
}
