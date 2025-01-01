use crate::priofee::PriorityDetails;
use std::collections::HashMap;
use std::str::FromStr;

use anyhow::anyhow;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::instruction::CompiledInstruction;
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::message::{AccountKeys, VersionedMessage};
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::TransactionError;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiInnerInstructions, UiInstruction,
    UiLoadedAddresses, UiTransactionStatusMeta,
};
use yellowstone_grpc_proto::convert_from::{create_tx_meta, create_tx_versioned};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

/// Type to unify transaction payloads from both RPC and GRPC
#[derive(Debug, Clone)]
pub struct ITransaction {
    /// The transaction signature
    pub signature: Signature,

    /// The transaction message
    pub message: VersionedMessage,

    /// The slot corresponding to this transaction's entry
    pub slot: Slot,

    /// Loaded addresses for this transaction
    pub loaded_addresses: Option<LoadedAddresses>,

    /// The transaction meta
    pub meta: Option<UiTransactionStatusMeta>,

    /// Error that might have occured during the transaction
    pub err: Option<TransactionError>,

    /// Priority fee details for this transaction
    pub priority_details: PriorityDetails,

    /// Compute units consumed by this transaction
    pub compute_units_consumed: Option<u64>,

    /// The blockhash used in sending the transaction
    pub recent_blockhash: Hash,

    /// Map of an instruction index to its inner instructions
    pub inner_instructions: Option<HashMap<u8, Vec<CompiledInstruction>>>,

    /// Whether or not this transaction is a vote transaction
    pub is_vote_transaction: bool,

    /// Address-lookup-tables needed for this transaction's keys
    pub address_table_lookups: Vec<MessageAddressTableLookup>,

    /// Readable accounts in the transaction
    pub readable_accounts: Vec<Pubkey>,

    /// Writable accounts in the transaction
    pub writable_accounts: Vec<Pubkey>,

    /// Blocktime
    pub block_time: Option<i64>,
}

impl ITransaction {
    pub fn is_legacy(&self) -> bool {
        matches!(self.message, VersionedMessage::Legacy(_))
    }

    pub fn is_versioned(&self) -> bool {
        matches!(self.message, VersionedMessage::V0(_))
    }

    pub async fn extract_or_load_additional_keys(
        &mut self,
        client: &RpcClient,
    ) -> anyhow::Result<()> {
        if let Some(loaded) = self.extract_loaded_addresses() {
            self.loaded_addresses = Some(loaded);
        } else {
            self.load_keys(client).await?;
        }
        Ok(())
    }

    pub async fn load_keys(&mut self, client: &RpcClient) -> anyhow::Result<()> {
        match self.message.address_table_lookups() {
            Some(lookups) if !lookups.is_empty() => match lookup_addresses(client, lookups).await {
                Ok(addresses) => {
                    self.loaded_addresses = Some(addresses);
                    Ok(())
                }
                Err(e) => Err(anyhow!("Failed lookup for tx {}: {}", self.signature, e)),
            },
            _ => Ok(()),
        }
    }

    pub fn account_keys(&self) -> AccountKeys {
        AccountKeys::new(
            self.message.static_account_keys(),
            self.loaded_addresses.as_ref(),
        )
    }

    fn extract_loaded_addresses(&self) -> Option<LoadedAddresses> {
        self.meta.as_ref().and_then(extract_loaded_addresses)
    }
}

pub async fn lookup_addresses(
    client: &RpcClient,
    lookups: &[MessageAddressTableLookup],
) -> Result<LoadedAddresses, anyhow::Error> {
    let mut loaded_addresses = vec![];
    let keys = lookups.iter().map(|t| t.account_key).collect::<Vec<_>>();
    let lookup_accounts = client.get_multiple_accounts(&keys).await?;
    debug_assert!(lookup_accounts.len() == lookups.len());
    for (i, account) in lookup_accounts.into_iter().enumerate() {
        if account.is_none() {
            return Err(anyhow!("Failed to get account for address table lookup"));
        }
        let account = account.unwrap();
        let lookup_table = AddressLookupTable::deserialize(&account.data)
            .map_err(|_| anyhow!("failed to deserialize account lookup table"))?;
        loaded_addresses.push(LoadedAddresses {
            writable: lookups[i]
                .writable_indexes
                .iter()
                .map(|idx| {
                    lookup_table
                        .addresses
                        .get(*idx as usize)
                        .copied()
                        .ok_or(anyhow!(
                            "account lookup went out of bounds of address lookup table"
                        ))
                })
                .collect::<Result<_, _>>()?,
            readonly: lookups[i]
                .readonly_indexes
                .iter()
                .map(|idx| {
                    lookup_table
                        .addresses
                        .get(*idx as usize)
                        .copied()
                        .ok_or(anyhow!(
                            "account lookup went out of bounds of address lookup table"
                        ))
                })
                .collect::<Result<_, _>>()?,
        });
    }
    Ok(LoadedAddresses::from_iter(loaded_addresses))
}

impl TryFrom<SubscribeUpdateTransaction> for ITransaction {
    type Error = anyhow::Error;
    fn try_from(value: SubscribeUpdateTransaction) -> Result<Self, Self::Error> {
        let slot = value.slot;
        if value.transaction.as_ref().is_none() {
            return Err(anyhow!("No transaction data in Geyser update"));
        }
        let transaction_info = value.transaction.unwrap();
        if transaction_info.transaction.is_none() {
            return Err(anyhow!("No transaction data in Geyser update"));
        }
        let transaction = transaction_info.transaction.unwrap();
        let versioned_tx = create_tx_versioned(transaction).map_err(|e| anyhow!(e))?;
        let is_vote_transaction = versioned_tx.message.instructions().iter().any(|i| {
            i.program_id(versioned_tx.message.static_account_keys())
                .eq(&solana_sdk::vote::program::id())
                && limited_deserialize::<VoteInstruction>(&i.data)
                    .map(|vi| vi.is_simple_vote())
                    .unwrap_or(false)
        });

        let priority_details = PriorityDetails::from_versioned_transaction(&versioned_tx);
        let signature = Signature::try_from(transaction_info.signature)
            .map_err(|_| anyhow!("Failed converting signature from u8 array"))?;
        let address_table_lookups = versioned_tx
            .message
            .address_table_lookups()
            .map(|x| x.to_vec())
            .unwrap_or_default();

        let meta: Option<UiTransactionStatusMeta> = transaction_info
            .meta
            .and_then(|meta| create_tx_meta(meta).ok().map(|m| m.into()));
        let err = meta.as_ref().and_then(|m| m.err.clone());
        let inner_instructions = meta.as_ref().and_then(extract_compiled_inner_instructions);
        let recent_blockhash = *versioned_tx.message.recent_blockhash();
        let compute_units_consumed = meta
            .as_ref()
            .and_then(|meta| meta.compute_units_consumed.clone().into());

        let mut readable_accounts = vec![];
        let mut writable_accounts = vec![];
        let keys = versioned_tx.message.static_account_keys();
        for (index, key) in keys.iter().enumerate() {
            if versioned_tx.message.is_maybe_writable(index) {
                writable_accounts.push(*key);
            } else {
                readable_accounts.push(*key);
            }
        }

        Ok(ITransaction {
            signature,
            slot,
            message: versioned_tx.message,
            loaded_addresses: None,
            meta,
            err,
            priority_details,
            compute_units_consumed,
            recent_blockhash,
            inner_instructions,
            is_vote_transaction,
            address_table_lookups,
            readable_accounts,
            writable_accounts,
            block_time: Some(0),
        })
    }
}

impl TryFrom<EncodedConfirmedTransactionWithStatusMeta> for ITransaction {
    type Error = anyhow::Error;

    fn try_from(
        encoded_transaction: EncodedConfirmedTransactionWithStatusMeta,
    ) -> Result<Self, Self::Error> {
        let versioned_tx = encoded_transaction
            .transaction
            .transaction
            .decode()
            .ok_or(anyhow!("failed decoding encoded transaction"))?;
        let meta = encoded_transaction.transaction.meta.as_ref();
        let is_vote_transaction = versioned_tx.message.instructions().iter().any(|i| {
            i.program_id(versioned_tx.message.static_account_keys())
                .eq(&solana_sdk::vote::program::id())
                && limited_deserialize::<VoteInstruction>(&i.data)
                    .map(|vi| vi.is_simple_vote())
                    .unwrap_or(false)
        });
        let priority_details = PriorityDetails::from_versioned_transaction(&versioned_tx);
        let inner_instructions = meta.and_then(extract_compiled_inner_instructions);
        let err = meta.and_then(|m| m.err.clone());
        let recent_blockhash = *versioned_tx.message.recent_blockhash();
        let address_table_lookups = versioned_tx
            .message
            .address_table_lookups()
            .map(|x| x.to_vec())
            .unwrap_or_default();
        let compute_units_consumed = encoded_transaction
            .transaction
            .meta
            .as_ref()
            .and_then(|meta| meta.compute_units_consumed.clone().into());
        let mut readable_accounts = vec![];
        let mut writable_accounts = vec![];
        let keys = versioned_tx.message.static_account_keys();
        for (index, key) in keys.iter().enumerate() {
            if versioned_tx.message.is_maybe_writable(index) {
                writable_accounts.push(*key);
            } else {
                readable_accounts.push(*key);
            }
        }

        Ok(ITransaction {
            signature: versioned_tx.signatures[0],
            slot: encoded_transaction.slot,
            message: versioned_tx.message,
            meta: encoded_transaction.transaction.meta.clone(),
            err,
            loaded_addresses: None,
            priority_details,
            recent_blockhash,
            inner_instructions,
            is_vote_transaction,
            address_table_lookups,
            compute_units_consumed,
            readable_accounts,
            writable_accounts,
            block_time: encoded_transaction.block_time,
        })
    }
}

fn extract_compiled_inner_instructions(
    meta: &UiTransactionStatusMeta,
) -> Option<HashMap<u8, Vec<CompiledInstruction>>> {
    let inner_instructions: Option<&Vec<UiInnerInstructions>> =
        meta.inner_instructions.as_ref().into();
    Some(HashMap::from_iter(inner_instructions?.iter().map(
        |inner_ix| {
            (
                inner_ix.index,
                inner_ix
                    .instructions
                    .iter()
                    .filter_map(|ix| match ix {
                        UiInstruction::Compiled(ix) => Some(CompiledInstruction {
                            program_id_index: ix.program_id_index,
                            accounts: ix.accounts.clone(),
                            data: solana_sdk::bs58::decode(&ix.data).into_vec().unwrap(),
                        }),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )
        },
    )))
}

fn extract_loaded_addresses(meta: &UiTransactionStatusMeta) -> Option<LoadedAddresses> {
    let addresses: Option<&UiLoadedAddresses> = meta.loaded_addresses.as_ref().into();
    let ui_loaded_addresses = addresses?;
    Some(LoadedAddresses {
        readonly: ui_loaded_addresses
            .readonly
            .iter()
            .map(|s| Pubkey::from_str(s.as_str()).unwrap())
            .collect(),
        writable: ui_loaded_addresses
            .writable
            .iter()
            .map(|s| Pubkey::from_str(s.as_str()).unwrap())
            .collect(),
    })
}
