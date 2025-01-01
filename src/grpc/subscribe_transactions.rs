use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use dashmap::DashSet;
use futures::StreamExt;
use log::error;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};
use yellowstone_grpc_proto::tonic::service::Interceptor;

async fn create_grpc_connection(
    endpoint: &String,
    x_token: &Option<String>,
) -> anyhow::Result<GeyserGrpcClient<impl Interceptor + Sized>> {
    super::grpc_connector::connect_with_timeout_with_buffers(
        endpoint.to_string(),
        x_token.to_owned(),
        None,
        Some(Duration::from_secs(15)),
        Some(Duration::from_secs(15)),
        super::grpc_connector::GeyserGrpcClientBufferConfig::default(),
    )
    .await
    .context("Failed to connect to grpc source")
}

pub struct GrpcTransactions {
    keys: Arc<DashSet<Pubkey>>,
    accounts_notifier: tokio::sync::watch::Sender<Vec<String>>,
    receiver: tokio::sync::broadcast::Receiver<SubscribeUpdateTransactionInfo>,
}

impl GrpcTransactions {
    pub fn drop_accounts(&self, requests: Vec<Pubkey>) -> anyhow::Result<()> {
        let mut changed = false;
        for req in requests {
            if self.keys.remove(&req).is_some() {
                changed = true;
            }
        }

        if changed {
            let accounts = self
                .keys
                .iter()
                .map(|key| key.to_string())
                .collect::<Vec<_>>();
            if let Err(e) = self.accounts_notifier.send(accounts) {
                error!(
                    "Error updating accounts for Grpc txns streaming task: {}",
                    e
                );
            }
        }

        Ok(())
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SubscribeUpdateTransactionInfo> {
        self.receiver.resubscribe()
    }

    pub fn watch_accounts(&self, requests: Vec<Pubkey>) -> anyhow::Result<()> {
        log::trace!("Got watch accounts request");
        let mut changed = false;
        for req in requests {
            if self.keys.insert(req) {
                changed = true;
            }
        }

        log::trace!("changed={}", changed);
        if changed {
            let accounts = self
                .keys
                .iter()
                .map(|key| key.to_string())
                .collect::<Vec<_>>();
            log::trace!("Sending new accounts to account-notifier");
            if let Err(e) = self.accounts_notifier.send(accounts) {
                error!(
                    "Error updating accounts for Grpc txns streaming task: {}",
                    e
                );
            }
        }

        Ok(())
    }
}

pub fn grpc_txns_task(
    grpc_endpoint: String,
    grpc_x_token: Option<String>,
    // accounts: Vec<String>,
    grpc_task_name: String,
) -> (GrpcTransactions, JoinHandle<anyhow::Result<()>>) {
    let (accounts_notifier, mut accounts_watch) = tokio::sync::watch::channel(vec![]);
    let (broadcast_txn_sender, broadcast_txn_receiver) = tokio::sync::broadcast::channel(1000);

    log::debug!("Starting GRPC transactions task");
    let main_task = tokio::task::spawn({
        let task_name = grpc_task_name.clone();
        let txn_sender = broadcast_txn_sender;
        async move {
            match accounts_watch.changed().await {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "GRPC transactions streaming task failed: {}. exiting task",
                        e
                    );
                    return Err(anyhow!("accounts watch sender dropped!"));
                }
            }
            let accounts = accounts_watch.borrow_and_update().clone();

            let has_started = Arc::new(tokio::sync::Notify::new());
            let mut current_task = grpc_watch_transactions_task_inner(
                grpc_endpoint.clone(),
                grpc_x_token.clone(),
                accounts,
                Arc::clone(&has_started),
                task_name.clone(),
                txn_sender.clone(),
            );

            while accounts_watch.changed().await.is_ok() {
                log::trace!("New accounts received. Resetting txns-updating task");
                // Introduce delays to prevent creating new tasks spuriously
                tokio::time::sleep(Duration::from_secs(1)).await;
                let accounts = accounts_watch.borrow_and_update().clone();
                let has_started = Arc::new(tokio::sync::Notify::new());

                let new_task = grpc_watch_transactions_task_inner(
                    grpc_endpoint.clone(),
                    grpc_x_token.clone(),
                    accounts,
                    Arc::clone(&has_started),
                    task_name.clone(),
                    txn_sender.clone(),
                );

                if tokio::time::timeout(Duration::from_secs(60), has_started.notified())
                    .await
                    .is_err()
                {
                    error!("Updated txns-watching task failed to start");
                    new_task.abort();
                    continue;
                }

                log::trace!("Resetting txns watching GRPC task");
                current_task.abort();
                current_task = new_task;
            }
            log::info!("Exiting GRPC txns-watching main task :(");

            Ok(())
        }
    });

    let grpc_accounts = GrpcTransactions {
        keys: Arc::new(DashSet::new()),
        accounts_notifier,
        receiver: broadcast_txn_receiver,
    };

    (grpc_accounts, main_task)
}

fn grpc_watch_transactions_task_inner(
    grpc_endpoint: String,
    grpc_x_token: Option<String>,
    accounts: Vec<String>,
    has_started: Arc<Notify>,
    task_name: String,
    txn_sender: tokio::sync::broadcast::Sender<SubscribeUpdateTransactionInfo>,
) -> JoinHandle<anyhow::Result<()>> {
    log::trace!("Starting inner GRPC watch transactions task");
    tokio::task::spawn({
        async move {
            if accounts.is_empty() {
                log::info!("Empty accounts. Exiting watch-transactions task");
                return Ok(());
            }

            'outer: loop {
                let mut txns_filter: HashMap<String, SubscribeRequestFilterTransactions> =
                    HashMap::new();
                log::debug!("accounts: {:#?}", accounts);
                txns_filter.insert(
                    task_name.to_string(),
                    SubscribeRequestFilterTransactions {
                        vote: Some(false),
                        account_include: accounts.clone(),
                        signature: None,
                        account_exclude: vec![],
                        account_required: vec![],
                        // failed: Some(true), // this makes it only return failed txns
                        ..Default::default()
                    },
                );

                let txns_subscription = SubscribeRequest {
                    transactions: txns_filter,
                    commitment: Some(CommitmentLevel::Confirmed.into()),
                    ..Default::default()
                };

                log::trace!("Connecting to GRPC, endpoint={}", grpc_endpoint);
                let Ok(mut client) = create_grpc_connection(&grpc_endpoint, &grpc_x_token).await
                else {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                };
                log::trace!("Connection complete, sending subscribe request");
                let Ok(mut txns_stream) = client.subscribe_once(txns_subscription).await else {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                };
                log::trace!("Sent subscribe-request successfully");

                while let Some(message) = txns_stream.next().await {
                    let Ok(message) = message else {
                        // disconnected. retry the main loop and connect again
                        break;
                    };
                    log::trace!("Got new grpc message!");

                    let Some(update) = message.update_oneof else {
                        continue;
                    };

                    has_started.notify_one();

                    match update {
                        UpdateOneof::Transaction(update) => {
                            log::trace!("Received transaction update");
                            if let Some(ref transaction) = update.transaction {
                                let signature =
                                    Signature::try_from(transaction.signature.as_slice())
                                        .expect("valid signature");
                                log::trace!("GRPC txns task got new transaction {}", signature);

                                if txn_sender.send(transaction.clone()).is_err() {
                                    error!(
                                        "Receiver end of GRPC txns channel closed. Exiting task"
                                    );
                                    break 'outer;
                                }
                            }
                        }
                        UpdateOneof::Ping(_) => {
                            log::trace!("Received ping from GRPC stream");
                        }
                        _ => {
                            log::error!("Received unexpected message from GRPC stream");
                        }
                    }
                }
                log::error!("Grpc stream disconnected. Reconnecting..");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(())
        }
    })
}
