use crate::priofee::PriorityDetails;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use solana_sdk::transaction::VersionedTransaction;

/// Incremented for each transaction that is generated and sent through the wire
static NEXT_TRANSACTION_ID: AtomicUsize = AtomicUsize::new(1);
/// Incremented for each service that sends transactions through the wire. All
/// transactions that are sent by the same service will share this source-id.
///
/// It is used by the transaction-sending service to send confirmation for a txn
/// back to its originator
static NEXT_TRANSACTION_SOURCE_ID: AtomicUsize = AtomicUsize::new(1);

/// The Unique ID of a single sent-transaction.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TransactionId(usize);

impl TransactionId {
    /// Creates a new `TransactionId`.
    pub fn next() -> Self {
        TransactionId(NEXT_TRANSACTION_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The ID of a single subscriber to a sent-transaction.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TransactionSourceId(usize);

impl TransactionSourceId {
    /// Creates a new `TransactionSourceId`.
    pub fn next() -> Self {
        TransactionSourceId(NEXT_TRANSACTION_SOURCE_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for TransactionSourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct SendTransactionData {
    pub signature: Signature,
    pub last_valid_block_height: u64,
    pub sent_at: Instant,
    pub priority_details: PriorityDetails,
    pub transaction: VersionedTransaction,
    pub txn_id: TransactionId,
    pub source_id: TransactionSourceId,
    pub send_to_jito: bool,
}

#[derive(Debug, Clone)]
pub enum NotConfirmedReason {
    Expired,
    TimedOut,
    RpcError(String),
}

#[derive(Debug, Clone)]
pub enum ConfirmTransactionResponse {
    Confirmed {
        error: Option<TransactionError>,
        confirmation_slot: u64,
    },
    Unconfirmed {
        reason: NotConfirmedReason,
    },
}

#[derive(Clone, Debug)]
pub struct ConfirmTransactionData {
    pub data: SendTransactionData,
    pub response: ConfirmTransactionResponse,
}
