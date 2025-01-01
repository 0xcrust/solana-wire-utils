use std::str::FromStr;
use std::sync::Arc;

use futures_util::StreamExt;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};

const CONCURRENT_GET_TRANSACTION_REQUESTS: usize = 20;

#[derive(Copy, Clone, Debug)]
pub enum TransactionHistoryOrder {
    /// Order from left to right in the timeline; oldest transactions first
    Forward,
    /// Order from right to left in the timeline; newest transactions first
    Backward,
}

#[derive(Copy, Clone, Debug)]
/// Limits for a transaction search.
///
/// A specified limit will take precedence in terminating a search. That is, if a limit of 1000 is
/// specified, then the search will return as soon as 1000 signatures are yielded, irrespective of
/// whether or not we've reached the `until` signature.
pub enum TransactionHistoryLimit {
    /// Search backwards until this timestamp is reached.
    Blocktime(i64),
    /// Search backwards for the last n transactions
    N(usize),
}

#[derive(Debug)]
pub enum TerminationReason {
    BlockTime,
    N,
    UntilSig,
}

pub async fn get_most_recent_transaction_for_wallet(
    client: &RpcClient,
    address: Pubkey,
) -> anyhow::Result<Option<Signature>> {
    let config = GetConfirmedSignaturesForAddress2Config {
        before: None,
        until: None,
        limit: Some(1),
        commitment: Some(CommitmentConfig::confirmed()),
    };
    let signatures = client
        .get_signatures_for_address_with_config(&address, config)
        .await?;

    Ok(signatures
        .first()
        .map(|sig| Signature::from_str(&sig.signature).unwrap()))
}

/// Get the last n historical transactions for a wallet. If `recent-signature` is not provided, it defaults to the
/// latest transaction sent by the wallet and starts its search backwards from that signature.
///
/// If `order_by_earliest` is not provided, it defaults to `true` and returns results with the earliest transaction
/// coming first
///
/// *before*: start searching backwards from this signature
///
/// *after*: search until this signature
///
/// *limits*: list of limits. termination occurs once one of the conditions is met
pub async fn get_signatures_for_address(
    client: &RpcClient,
    commitment_config: Option<CommitmentConfig>,
    address: Pubkey,
    mut before: Option<Signature>, // starts searching backwards from this
    until: Option<Signature>,      // search until this signature
    limits: Vec<TransactionHistoryLimit>, // applied on a OR basis
    order: Option<TransactionHistoryOrder>,
) -> anyhow::Result<(
    Vec<RpcConfirmedTransactionStatusWithSignature>,
    TerminationReason,
)> {
    let order = order.unwrap_or(TransactionHistoryOrder::Backward);
    let mut limit = None;
    let mut blocktime_limit = None;

    for l in limits {
        match l {
            TransactionHistoryLimit::Blocktime(ts) => blocktime_limit = Some(ts),
            TransactionHistoryLimit::N(n) => limit = Some(n),
        }
    }

    let mut signatures = if let Some(n) = limit {
        Vec::with_capacity(n)
    } else {
        Vec::new()
    };

    let termination_reason;
    loop {
        if limit == Some(0) {
            termination_reason = TerminationReason::N;
            break;
        }
        if before == until && before.is_some() {
            termination_reason = TerminationReason::UntilSig;
            break;
        }

        // if `before` is specified then it is not included in the result. If not specified then it defaults to the most recent signature,
        // and includes that in the response.

        // if `until` is specified then it is not included in the result.
        let config = GetConfirmedSignaturesForAddress2Config {
            before,
            until,
            limit: limit.map(|count| std::cmp::min(count, 1000)),
            commitment: commitment_config,
        };

        let results = client
            .get_signatures_for_address_with_config(&address, config)
            .await?;

        let Some(earliest) = results.last() else {
            termination_reason = TerminationReason::UntilSig;
            break;
        };

        if let Some(ts) = blocktime_limit {
            if earliest.block_time < blocktime_limit {
                let target_idx = results.partition_point(|v| {
                    if let Some(t) = v.block_time {
                        t < ts
                    } else {
                        false
                    }
                });
                signatures.extend(results[target_idx..].to_vec());
                termination_reason = TerminationReason::BlockTime;
                break;
            }
        }
        limit.as_mut().map(|c| *c = c.saturating_sub(results.len()));
        before = Some(Signature::from_str(&earliest.signature)?);
        signatures.extend(results);
    }

    // Now we have all the signatures, ordered from newest to oldest(right to left in the timeline)
    let signatures = match order {
        TransactionHistoryOrder::Backward => signatures,
        TransactionHistoryOrder::Forward => signatures.into_iter().rev().collect(),
    };

    Ok((signatures, termination_reason))
}

pub async fn get_transactions_for_signatures(
    client: Arc<RpcClient>,
    signatures: Vec<Signature>,
    concurrency: Option<usize>,
) -> anyhow::Result<Vec<EncodedConfirmedTransactionWithStatusMeta>> {
    let concurrency = concurrency.unwrap_or(CONCURRENT_GET_TRANSACTION_REQUESTS);
    let config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base58),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let mut transactions = Vec::with_capacity(signatures.len());
    let mut stream = futures::stream::iter(signatures)
        .map(|signature| {
            let client = Arc::clone(&client);
            async move {
                let tx = client
                    .get_transaction_with_config(&signature, config)
                    .await?;
                Ok::<_, anyhow::Error>(tx)
            }
        })
        .buffered(concurrency);

    while let Some(transaction) = stream.next().await {
        transactions.push(transaction?)
    }

    Ok(transactions)
}
