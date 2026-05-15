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
///
/// Pump.fun buy instruction data layout (after 8-byte discriminator):
///   - [8..16] u64: token_amount
///   - [16..24] u64: max_sol_cost
///
/// Accounts (in order):
///   0: mint
///   1: bonding_curve
///   2: associated_bonding_curve
///   3: associated_user
pub fn parse_buy_event(
    data: &[u8],
    accounts: &[String],
    signature: String,
    slot: u64,
    block_time: Option<i64>,
) -> Option<PumpBuyEvent> {
    if data.len() < 24 || accounts.len() < 4 {
        return None;
    }
    let token_amount = u64::from_le_bytes(data[8..16].try_into().ok()?);
    let max_sol_cost = u64::from_le_bytes(data[16..24].try_into().ok()?);

    Some(PumpBuyEvent {
        mint: accounts[0].clone(),
        bonding_curve: accounts[1].clone(),
        associated_bonding_curve: accounts[2].clone(),
        token_amount,
        max_sol_cost,
        signature,
        slot,
        block_time,
    })
}

/// Parse a pump.fun sell instruction from raw instruction data.
///
/// Pump.fun sell instruction data layout (after 8-byte discriminator):
///   - [8..16] u64: token_amount
///   - [16..24] u64: min_sol_output
pub fn parse_sell_event(
    data: &[u8],
    accounts: &[String],
    signature: String,
    slot: u64,
    block_time: Option<i64>,
) -> Option<PumpSellEvent> {
    if data.len() < 24 || accounts.len() < 4 {
        return None;
    }
    let token_amount = u64::from_le_bytes(data[8..16].try_into().ok()?);
    let min_sol_output = u64::from_le_bytes(data[16..24].try_into().ok()?);

    Some(PumpSellEvent {
        mint: accounts[0].clone(),
        bonding_curve: accounts[1].clone(),
        associated_bonding_curve: accounts[2].clone(),
        token_amount,
        min_sol_output,
        signature,
        slot,
        block_time,
    })
}

/// Parse a pump.fun create instruction from raw instruction data.
///
/// Pump.fun create instruction data layout (after 8-byte discriminator):
///   - [8..12] u32: name_len
///   - [12..12+name_len] bytes: name
///   - [next 4] u32: symbol_len
///   - [next+symbol_len] bytes: symbol
///   - [next 4] u32: uri_len
///   - [next+uri_len] bytes: uri
pub fn parse_create_event(
    data: &[u8],
    accounts: &[String],
    signature: String,
    slot: u64,
    block_time: Option<i64>,
) -> Option<PumpCreateEvent> {
    if data.len() < 12 || accounts.len() < 4 {
        return None;
    }

    let mut offset = 8;

    // Read name.
    let name_len = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?) as usize;
    offset += 4;
    if data.len() < offset + name_len { return None; }
    let name = String::from_utf8_lossy(&data[offset..offset+name_len]).to_string();
    offset += name_len;

    // Read symbol.
    if data.len() < offset + 4 { return None; }
    let symbol_len = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?) as usize;
    offset += 4;
    if data.len() < offset + symbol_len { return None; }
    let symbol = String::from_utf8_lossy(&data[offset..offset+symbol_len]).to_string();
    offset += symbol_len;

    // Read uri.
    if data.len() < offset + 4 { return None; }
    let uri_len = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?) as usize;
    offset += 4;
    if data.len() < offset + uri_len { return None; }
    let uri = String::from_utf8_lossy(&data[offset..offset+uri_len]).to_string();

    Some(PumpCreateEvent {
        mint: accounts[0].clone(),
        bonding_curve: accounts[1].clone(),
        associated_bonding_curve: accounts[2].clone(),
        name,
        symbol,
        uri,
        creator: accounts.get(3).cloned().unwrap_or_default(),
        signature,
        slot,
        block_time,
    })
}
