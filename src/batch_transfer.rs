use crate::rpc::jito::JitoClient;
use crate::send_and_confirm_txn::SendTransactionsHandle;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use log::info;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::{Message, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct BatchTokenTransfer {
    transfer_amounts: Arc<RwLock<Vec<(Pubkey, u64)>>>,
    copy_keypair: Arc<Keypair>,
    rpc_client: Arc<RpcClient>,
    destination_wallet: Pubkey,
    send_txns_handle: SendTransactionsHandle,
    jito_tip_amount: Arc<AtomicU64>,
}

impl BatchTokenTransfer {
    pub fn new(
        keypair: Arc<Keypair>,
        rpc_client: Arc<RpcClient>,
        destination: Pubkey,
        send_txns_handle: SendTransactionsHandle,
    ) -> Self {
        BatchTokenTransfer {
            transfer_amounts: Default::default(),
            copy_keypair: keypair,
            rpc_client,
            destination_wallet: destination,
            send_txns_handle,
            jito_tip_amount: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn set_jito_tip_amount(&self, tip: u64) {
        self.jito_tip_amount.store(tip, Ordering::Release);
    }

    pub async fn add_transfer_amount(&self, mint: Pubkey, amount: u64) {
        if amount != 0 {
            self.transfer_amounts.write().await.push((mint, amount));
        }
    }

    pub async fn process_transfers(&self) -> anyhow::Result<()> {
        let mut list = self.transfer_amounts.write().await;
        if list.is_empty() {
            return Ok(());
        }
        let chunk_size = 7;
        info!(
            "Cleaning up dust for {} tokens in chunks of {}",
            list.len(),
            chunk_size
        );
        let copy_wallet = self.copy_keypair.pubkey();
        for (i, chunk) in list.chunks(chunk_size).enumerate() {
            info!("Processing dust: chunk {}. length: {}", i, chunk.len());
            let mut instructions = Vec::with_capacity(chunk.len());
            for (token, amount) in chunk {
                // For now, always use the spl_token program
                let source_ata =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &copy_wallet,
                        &token,
                        &spl_token::ID,
                    );
                let destination_ata =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &self.destination_wallet,
                        &token,
                        &spl_token::ID,
                    );
                let create_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &copy_wallet,
                    &self.destination_wallet,
                    &token,
                    &spl_token::ID,
                );
                instructions.push(create_ix);
                instructions.push(spl_token::instruction::transfer(
                    &spl_token::ID,
                    &source_ata,
                    &destination_ata,
                    &copy_wallet,
                    &[],
                    *amount,
                )?);
            }

            let tip = self.jito_tip_amount.load(Ordering::Acquire);
            let jito_tip_ix = if tip != 0 {
                Some(JitoClient::build_bribe_ix(&copy_wallet, tip))
            } else {
                None
            };
            let message = VersionedMessage::Legacy(Message::new(&instructions, Some(&copy_wallet)));
            let txn = VersionedTransaction {
                signatures: vec![Signature::default()],
                message,
            };

            // todo: CUs can be hardcoded
            let dynamic_cu = self
                .rpc_client
                .simulate_transaction_with_config(
                    &txn,
                    RpcSimulateTransactionConfig {
                        sig_verify: false,
                        replace_recent_blockhash: true,
                        commitment: Some(CommitmentConfig::confirmed()),
                        ..Default::default()
                    },
                )
                .await?
                .value
                .units_consumed;

            let mut instructions = if let Some(cu) = dynamic_cu.and_then(|cu| u32::try_from(cu).ok()) {
                let compute_ix =
                    solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
                        cu + 50_000,
                    );
                let mut vec = vec![compute_ix];
                vec.extend(instructions);
                vec
            } else {
                instructions
            };
            jito_tip_ix.map(|ix| instructions.push(ix));

            let message = VersionedMessage::Legacy(Message::new(&instructions, Some(&copy_wallet)));
            let txn = VersionedTransaction {
                signatures: vec![Signature::default()],
                message,
            };

            let details = self
                .send_txns_handle
                .send_transaction(txn, &[&self.copy_keypair], true)
                .await?;
            info!("Transfer tx: https://solscan.io/tx/{}", details.signature);
        }
        list.clear();

        Ok(())
    }
}

