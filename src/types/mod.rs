mod transaction;
pub use transaction::*;

use solana_sdk::hash::Hash;
use solana_sdk::slot_history::Slot;

#[derive(Debug, Clone, Default)]
pub struct SlotNotification {
    pub processed_slot: Slot,
    pub estimated_processed_slot: Slot,
}

pub struct BlockHashNotification {
    pub blockhash: Hash,
    pub last_valid_block_height: u64,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum ComputeUnitLimits {
    #[default]
    Dynamic,
    Fixed(u64),
}

#[derive(Copy, Clone, Debug)]
pub enum PriorityFeeConfig {
    DynamicMultiplier(u64),
    FixedCuPrice(u64),
    JitoTip(u64),
}
