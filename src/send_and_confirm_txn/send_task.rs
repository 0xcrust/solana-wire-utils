use super::confirm_task::start_transaction_confirmation_task;
use super::types::{
    ConfirmTransactionData, SendTransactionData, TransactionId, TransactionSourceId,
};
use crate::jito::JitoClient;
use crate::priofee::PriorityDetails;
use crate::types::BlockHashNotification;
use crate::utils::spawn_retry_task;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use dashmap::DashMap;
use log::{error, trace};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::SerializableTransaction;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_sdk::signers::Signers;
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;

const DEFAULT_SEND_TXN_ATTEMPTS: usize = 10;

#[derive(Clone)]
pub struct SendTransactionService {
    new_transactions_sender: UnboundedSender<SendTransactionData>,
    confirmation_subscribers_map:
        Arc<DashMap<TransactionSourceId, UnboundedSender<ConfirmTransactionData>>>,
    broadcast_confirmation_sender: Sender<ConfirmTransactionData>,
    blockhash_notification: Arc<RwLock<BlockHashNotification>>,
    num_attempted_txns: Arc<AtomicUsize>,
    num_confirmed_txns: Arc<AtomicUsize>,
    num_unconfirmed_txns: Arc<AtomicUsize>,
}

impl SendTransactionService {
    /// Returns an handle for sending transactions, and a channel that receives confirmations for
    /// only the transactions we send
    pub fn subscribe(
        &self,
    ) -> (
        SendTransactionsHandle,
        UnboundedReceiver<ConfirmTransactionData>,
    ) {
        let handle = SendTransactionsHandle {
            id: TransactionSourceId::next(),
            blockhash_notification: Arc::clone(&self.blockhash_notification),
            new_transactions_sender: self.new_transactions_sender.clone(),
            num_sent_txns: Arc::new(AtomicUsize::new(0)),
        };
        let (confirmation_sender, confirmation_receiver) = unbounded_channel();
        self.confirmation_subscribers_map
            .insert(handle.id, confirmation_sender);
        (handle, confirmation_receiver)
    }

    /// Returns an handle for sending transactions, and a channel that receives confirmations for
    /// all transactions
    pub fn subscribe_all(&self) -> (SendTransactionsHandle, Receiver<ConfirmTransactionData>) {
        let handle = SendTransactionsHandle {
            id: TransactionSourceId::next(),
            blockhash_notification: Arc::clone(&self.blockhash_notification),
            new_transactions_sender: self.new_transactions_sender.clone(),
            num_sent_txns: Arc::new(AtomicUsize::new(0)),
        };
        (handle, self.broadcast_confirmation_sender.subscribe())
    }

    pub fn attempted_txns_count(&self) -> usize {
        self.num_attempted_txns.load(Ordering::Relaxed)
    }

    pub fn confirmed_txns_count(&self) -> usize {
        self.num_confirmed_txns.load(Ordering::Relaxed)
    }

    pub fn unconfirmed_txns_count(&self) -> usize {
        self.num_unconfirmed_txns.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct SendTransactionsHandle {
    id: TransactionSourceId,
    blockhash_notification: Arc<RwLock<BlockHashNotification>>,
    new_transactions_sender: UnboundedSender<SendTransactionData>,
    num_sent_txns: Arc<AtomicUsize>,
}

pub struct SentTransactionDetails {
    pub signature: Signature,
    pub txn_id: TransactionId,
}

impl SendTransactionsHandle {
    pub async fn send_transaction<T: Signers>(
        &self,
        mut tx: VersionedTransaction,
        signers: &T,
        send_to_jito: bool,
    ) -> Result<SentTransactionDetails, anyhow::Error> {
        // https://docs.triton.one/chains/solana/sending-txs
        let blockhash_data = self.blockhash_notification.read().await;
        tx.message.set_recent_blockhash(blockhash_data.blockhash);
        let tx = VersionedTransaction::try_new(tx.message, signers)
            .expect("Signing transaction should succeed");
        let signature = *tx.get_signature();

        let send_transaction_data = SendTransactionData {
            signature,
            sent_at: std::time::Instant::now(),
            last_valid_block_height: blockhash_data.last_valid_block_height,
            priority_details: PriorityDetails::from_versioned_transaction(&tx),
            transaction: tx,
            txn_id: TransactionId::next(),
            source_id: self.id,
            send_to_jito,
        };
        let txn_id = send_transaction_data.txn_id;
        self.new_transactions_sender.send(send_transaction_data)?;
        self.num_sent_txns.fetch_add(1, Ordering::Relaxed);
        Ok(SentTransactionDetails { signature, txn_id })
    }

    pub async fn resend_transaction<T: Signers>(
        &self,
        mut tx: VersionedTransaction,
        signers: &T,
        txn_id: TransactionId,
        send_to_jito: bool,
    ) -> Result<SentTransactionDetails, anyhow::Error> {
        // https://docs.triton.one/chains/solana/sending-txs
        let blockhash_data = self.blockhash_notification.read().await;
        tx.message.set_recent_blockhash(blockhash_data.blockhash);
        let tx = VersionedTransaction::try_new(tx.message, signers)
            .expect("Signing transaction should succeed");
        let signature = *tx.get_signature();

        let send_transaction_data = SendTransactionData {
            signature,
            sent_at: std::time::Instant::now(),
            last_valid_block_height: blockhash_data.last_valid_block_height,
            priority_details: PriorityDetails::from_versioned_transaction(&tx),
            transaction: tx,
            txn_id,
            source_id: self.id,
            send_to_jito,
        };
        let txn_id = send_transaction_data.txn_id;
        self.new_transactions_sender.send(send_transaction_data)?;
        self.num_sent_txns.fetch_add(1, Ordering::Relaxed);
        Ok(SentTransactionDetails { signature, txn_id })
    }

    pub fn id(&self) -> TransactionSourceId {
        self.id
    }
}

pub fn start_send_transaction_service(
    rpc_client: Arc<RpcClient>,
    send_txn_client: Option<Arc<RpcClient>>,
    jito_client: Option<Arc<JitoClient>>,
    current_blockheight: Arc<AtomicU64>,
    blockhash_notification: Arc<RwLock<BlockHashNotification>>,
    commitment: CommitmentConfig,
    send_txn_attempts: Option<usize>,
) -> (
    SendTransactionService,
    Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) {
    let send_txn_client = send_txn_client.unwrap_or(Arc::clone(&rpc_client));
    let unconfirmed_transactions_queue = Arc::new(DashMap::<Signature, SendTransactionData>::new());
    let subscribers_map = Arc::new(DashMap::new());
    let (broadcast_confirmation_sender, _broadcast_confirmation_receiver) = channel(1000);
    let (new_transactions_sender, mut new_transactions_receiver) =
        unbounded_channel::<SendTransactionData>();
    let confirmation_exit_signal = Arc::new(AtomicBool::new(false));
    let num_attempted_txns = Arc::new(AtomicUsize::new(0));
    let num_confirmed_txns = Arc::new(AtomicUsize::new(0));
    let num_unconfirmed_txns = Arc::new(AtomicUsize::new(0));

    let send_txn_attempts = send_txn_attempts.unwrap_or(DEFAULT_SEND_TXN_ATTEMPTS);
    let send_txns_task = tokio::task::spawn({
        let unconfirmed_transactions_queue = Arc::clone(&unconfirmed_transactions_queue);
        let num_attempted_transactions = Arc::clone(&num_attempted_txns);
        let jito_client = jito_client.clone();
        async move {
            while let Some(data) = new_transactions_receiver.recv().await {
                num_attempted_transactions.fetch_add(1, Ordering::Relaxed);
                unconfirmed_transactions_queue.insert(data.signature, data.clone());

                if data.send_to_jito && jito_client.is_some() {
                    let jito_client = jito_client.clone().expect("jito_client");
                    let transaction = Arc::new(data.transaction.clone());
                    let txn_id = data.txn_id;
                    spawn_retry_task(
                        move || {
                            let jito_client = jito_client.clone();
                            let transaction = Arc::clone(&transaction);
                            async move {
                                match jito_client.send_transaction(&transaction).await {
                                    Ok(_signature) => {
                                        trace!("Sent txn {} to Jito", txn_id);
                                        Ok(())
                                    }
                                    Err(e) => Err(anyhow!("Error sending to Jito: {}", e)),
                                }
                            }
                        },
                        "send-jito-txn".to_string(),
                        3,
                        1000,
                    );

                    continue;
                }

                let data = data.clone();
                let send_txn_client = Arc::clone(&send_txn_client);
                tokio::task::spawn(async move {
                    for i in 0..send_txn_attempts {
                        match send_txn_client
                            .send_transaction_with_config(
                                &data.transaction,
                                RpcSendTransactionConfig {
                                    skip_preflight: true,
                                    preflight_commitment: Some(commitment.commitment),
                                    max_retries: Some(0),
                                    ..RpcSendTransactionConfig::default()
                                },
                            )
                            .await
                        {
                            Ok(_signature) => {
                                trace!("Sent {} to RPC", data.txn_id);
                                return;
                            }
                            Err(e) => {
                                if i == send_txn_attempts - 1 {
                                    error!("Failed sending {} via RPC: {}", data.txn_id, e);
                                }
                            }
                        }
                    }
                });
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    let confirm_txns_task = start_transaction_confirmation_task(
        rpc_client,
        current_blockheight,
        unconfirmed_transactions_queue,
        Arc::clone(&subscribers_map),
        broadcast_confirmation_sender.clone(),
        Arc::clone(&num_confirmed_txns),
        Arc::clone(&num_unconfirmed_txns),
        confirmation_exit_signal,
    );

    let service = SendTransactionService {
        new_transactions_sender,
        confirmation_subscribers_map: subscribers_map,
        broadcast_confirmation_sender,
        blockhash_notification,
        num_attempted_txns,
        num_confirmed_txns,
        num_unconfirmed_txns,
    };

    (service, vec![confirm_txns_task, send_txns_task])
}
