use anchor_spl::token_2022::spl_token_2022;
use mpl_token_metadata::accounts::Metadata;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{program_pack::Pack, pubkey::Pubkey};
use spl_token::state::Mint;

use super::account::get_multiple_account_data;

pub async fn get_info_for_mints(
    rpc_client: &RpcClient,
    mints: &[Pubkey],
) -> anyhow::Result<Vec<Option<RawMintInfo>>> {
    let mint_accounts = get_multiple_account_data(rpc_client, mints).await?;
    let metadatas = mints
        .iter()
        .map(|mint| Metadata::find_pda(mint).0)
        .collect::<Vec<_>>();
    let metadata_accounts = get_multiple_account_data(rpc_client, &metadatas).await?;

    let mut final_results = Vec::with_capacity(mints.len());
    for ((mint_account, metadata_account), mint) in mint_accounts
        .into_iter()
        .zip(metadata_accounts.into_iter())
        .zip(mints)
    {
        let Some(mint_account) = mint_account else {
            log::error!("No mint account found for mint {}", mint);
            final_results.push(None);
            continue;
        };

        let mint_state = match Mint::unpack(&mint_account.data[..Mint::LEN]) {
            Ok(mint) => mint,
            Err(e) => {
                log::error!(
                    "Failed to deserialize mint account for mint {}: {}",
                    mint,
                    e
                );
                final_results.push(None);
                continue;
            }
        };
        let (name, symbol, uri) = if let Some(account) = metadata_account {
            match Metadata::from_bytes(&account.data) {
                Ok(metadata) => (
                    Some(metadata.name.trim_end_matches('\u{0000}').to_string()),
                    Some(metadata.symbol.trim_end_matches('\u{0000}').to_string()),
                    Some(metadata.uri.trim_end_matches('\u{0000}').to_string()),
                ),
                Err(e) => {
                    log::error!(
                        "Failed to deserialize metadata account for mint {}: {}",
                        mint,
                        e
                    );
                    (None, None, None)
                }
            }
        } else {
            log::error!("No metadata account found for mint {}", mint);
            (None, None, None)
        };

        final_results.push(Some(RawMintInfo {
            pubkey: *mint,
            decimals: mint_state.decimals,
            supply: mint_state.supply,
            program_id: mint_account.owner,
            token_2022: mint_account.owner == spl_token_2022::ID,
            mint_authority: mint_state.mint_authority.into(),
            freeze_authority: mint_state.freeze_authority.into(),
            name,
            symbol,
            uri,
        }))
    }

    Ok(final_results)
}

#[derive(Debug, Clone)]
pub struct RawMintInfo {
    pub pubkey: Pubkey,
    pub decimals: u8,
    pub supply: u64,
    pub program_id: Pubkey,
    pub token_2022: bool,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub uri: Option<String>,
}
