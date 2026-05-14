//! # pump_fun
//!
//! Pump.fun event parsing with instruction discriminators.

use serde::{Deserialize, Serialize};

/// Pump.fun program ID.
pub const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// Instruction discriminators for pump.fun program (from audit).
pub mod discriminators {
    /// Buy instruction discriminator (first 8 bytes of SHA256("global:buy")).
    pub const BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
    /// Sell instruction discriminator (first 8 bytes of SHA256("global:sell")).
    pub const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
    /// Create instruction discriminator (first 8 bytes of SHA256("global:create")).
    pub const CREATE: [u8; 8] = [24, 30, 200, 165, 86, 54, 28, 123];
}

/// Parsed pump.fun buy instruction data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpBuyEvent {
    pub mint: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub token_amount: u64,
    pub max_sol_cost: u64,
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
}

/// Parsed pump.fun sell instruction data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpSellEvent {
    pub mint: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub token_amount: u64,
    pub min_sol_output: u64,
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
}

/// Parsed pump.fun create (new token launch) event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpCreateEvent {
    pub mint: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
}

/// All possible pump.fun events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PumpFunEvent {
    Buy(PumpBuyEvent),
    Sell(PumpSellEvent),
    Create(PumpCreateEvent),
}

/// Attempt to classify a pump.fun instruction by its discriminator.
pub fn classify_instruction(data: &[u8]) -> Option<&'static [u8; 8]> {
    if data.len() < 8 {
        return None;
    }
    let disc: [u8; 8] = data[..8].try_into().ok()?;
    if disc == discriminators::BUY {
        Some(&discriminators::BUY)
    } else if disc == discriminators::SELL {
        Some(&discriminators::SELL)
    } else if disc == discriminators::CREATE {
        Some(&discriminators::CREATE)
    } else {
        None
    }
}

/// Parse a pump.fun buy instruction from raw instruction data.
pub fn parse_buy_event(
    _data: &[u8],
    _accounts: &[String],
    _signature: String,
    _slot: u64,
    _block_time: Option<i64>,
) -> Option<PumpBuyEvent> {
    // TODO: Deserialize accounts and data fields from the instruction.
    todo!("Deserialize buy instruction accounts and data")
}

/// Parse a pump.fun sell instruction from raw instruction data.
pub fn parse_sell_event(
    _data: &[u8],
    _accounts: &[String],
    _signature: String,
    _slot: u64,
    _block_time: Option<i64>,
) -> Option<PumpSellEvent> {
    todo!("Deserialize sell instruction accounts and data")
}

/// Parse a pump.fun create instruction from raw instruction data.
pub fn parse_create_event(
    _data: &[u8],
    _accounts: &[String],
    _signature: String,
    _slot: u64,
    _block_time: Option<i64>,
) -> Option<PumpCreateEvent> {
    todo!("Deserialize create instruction accounts and data")
}
