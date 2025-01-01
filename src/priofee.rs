use solana_program_runtime::compute_budget_processor::{
    process_compute_budget_instructions, DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT,
    MAX_COMPUTE_UNIT_LIMIT,
};
use solana_program_runtime::prioritization_fee::{PrioritizationFeeDetails, PrioritizationFeeType};
use solana_sdk::borsh1::try_from_slice_unchecked;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Copy, Debug, Default, Clone)]
pub struct PriorityDetails {
    pub priority_fee: u64,
    pub compute_unit_limit: u32,
    pub compute_unit_price: u64,
}

impl PriorityDetails {
    pub fn new(priority_fee: u64, compute_unit_limit: u32, compute_unit_price: u64) -> Self {
        PriorityDetails {
            priority_fee,
            compute_unit_limit,
            compute_unit_price,
        }
    }

    pub fn from_cu_price(cu_price_microlamports: u64, cu_limit: Option<u32>) -> Self {
        let cu_limit = cu_limit.unwrap_or(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT);
        let fee = PrioritizationFeeType::ComputeUnitPrice(cu_price_microlamports);
        let details = PrioritizationFeeDetails::new(fee, cu_limit as u64);
        PriorityDetails {
            priority_fee: details.get_fee(),
            compute_unit_limit: cu_limit,
            compute_unit_price: cu_price_microlamports,
        }
    }

    pub fn from_versioned_transaction(transaction: &VersionedTransaction) -> Self {
        if let Err(_) = transaction.sanitize() {
            return PriorityDetails {
                priority_fee: 0,
                compute_unit_limit: DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT,
                compute_unit_price: 0,
            };
        }
        priority_details_from_instructions(
            transaction.message.instructions(),
            transaction.message.static_account_keys(),
        )
    }

    pub fn from_versioned_message(message: &VersionedMessage) -> Self {
        if let Err(_) = message.sanitize() {
            return PriorityDetails {
                priority_fee: 0,
                compute_unit_limit: DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT,
                compute_unit_price: 0,
            };
        }
        priority_details_from_instructions(message.instructions(), message.static_account_keys())
    }

    pub fn from_instructions(
        instructions: &[CompiledInstruction],
        static_account_keys: &[Pubkey],
    ) -> Self {
        priority_details_from_instructions(instructions, static_account_keys)
    }

    pub fn from_iterator<'a>(
        instructions: impl Iterator<Item = (&'a Pubkey, &'a CompiledInstruction)>,
    ) -> Self {
        let compute_unit_limit = DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT;

        let compute_limits = process_compute_budget_instructions(instructions);
        match compute_limits {
            Ok(compute_limits) => {
                let fee =
                    PrioritizationFeeType::ComputeUnitPrice(compute_limits.compute_unit_price);
                let details =
                    PrioritizationFeeDetails::new(fee, compute_limits.compute_unit_limit as u64);
                PriorityDetails {
                    priority_fee: details.get_fee(),
                    compute_unit_limit: compute_limits.compute_unit_limit,
                    compute_unit_price: compute_limits.compute_unit_price,
                }
            }
            Err(_) => PriorityDetails {
                priority_fee: 0,
                compute_unit_limit,
                compute_unit_price: 0,
            },
        }
    }
}

fn priority_details_from_instructions(
    instructions: &[CompiledInstruction],
    static_account_keys: &[Pubkey],
) -> PriorityDetails {
    let mut compute_unit_limit = DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT;

    let instructions = instructions.iter().map(|ix| {
        if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
            try_from_slice_unchecked(&ix.data)
        {
            compute_unit_limit = limit.min(MAX_COMPUTE_UNIT_LIMIT);
        }
        (
            static_account_keys
                .get(usize::from(ix.program_id_index))
                .expect("program id index is sanitized"),
            ix,
        )
    });
    let compute_limits = process_compute_budget_instructions(instructions);
    match compute_limits {
        Ok(compute_limits) => {
            let fee = PrioritizationFeeType::ComputeUnitPrice(compute_limits.compute_unit_price);
            let details =
                PrioritizationFeeDetails::new(fee, compute_limits.compute_unit_limit as u64);
            PriorityDetails {
                priority_fee: details.get_fee(),
                compute_unit_limit: compute_limits.compute_unit_limit,
                compute_unit_price: compute_limits.compute_unit_price,
            }
        }
        Err(_) => PriorityDetails {
            priority_fee: 0,
            compute_unit_limit,
            compute_unit_price: 0,
        },
    }
}
