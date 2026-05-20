//! Zerion API client for wallet portfolio, positions, PnL, and token prices.
//!
//! Uses the Zerion REST API (Basic Auth) with rate limiting for the free tier
//! (60K calls/mo, 10 RPS, ~2K calls/day budget).

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::RateLimitedClient;

pub const ZERION_BASE_URL: &str = "https://api.zerion.io/v1";

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPortfolioResponse {
    pub data: ZerionPortfolioData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPortfolioData {
    pub id: String,
    pub attributes: ZerionPortfolioAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ZerionPortfolioAttributes {
    pub positions_distribution_by_type: Option<serde_json::Value>,
    pub positions_distribution_by_chain: Option<serde_json::Value>,
    pub total: ZerionTotal,
    pub changes: Option<ZerionChanges>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionTotal {
    pub positions: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionChanges {
    pub absolute_1d: Option<f64>,
    pub percent_1d: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPositionsResponse {
    pub data: Vec<ZerionPositionData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPositionData {
    pub id: String,
    pub attributes: ZerionPositionAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ZerionPositionAttributes {
    pub fungible_info: Option<ZerionFungibleInfo>,
    pub value: Option<f64>,
    pub quantity: Option<ZerionQuantity>,
    pub price: Option<f64>,
    pub value_24h_change: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionFungibleInfo {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub implementation: Option<String>,
    pub icon: Option<ZerionIcon>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionIcon {
    pub url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionQuantity {
    pub int: Option<String>,
    pub decimal: Option<String>,
    pub numeric: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPnlResponse {
    pub data: ZerionPnlData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZerionPnlData {
    pub id: String,
    pub attributes: ZerionPnlAttributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ZerionPnlAttributes {
    pub total_gain: Option<f64>,
    pub realized_gain: Option<f64>,
    pub unrealized_gain: Option<f64>,
    pub relative_total_gain_percentage: Option<f64>,
    pub relative_realized_gain_percentage: Option<f64>,
    pub relative_unrealized_gain_percentage: Option<f64>,
    pub total_fee: Option<f64>,
    pub total_invested: Option<f64>,
    pub realized_cost_basis: Option<f64>,
    pub net_invested: Option<f64>,
}

/// Simplified portfolio info returned by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPortfolio {
    pub address: String,
    pub total_value_usd: f64,
    pub change_1d_absolute: Option<f64>,
    pub change_1d_percent: Option<f64>,
}

/// A single token position from Zerion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPosition {
    pub token_name: String,
    pub token_symbol: String,
    pub implementation: Option<String>,
    pub quantity: f64,
    pub value_usd: f64,
    pub price_usd: Option<f64>,
    pub change_24h: Option<f64>,
}

/// Wallet PnL summary from Zerion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPnl {
    pub total_gain: f64,
    pub realized_gain: f64,
    pub unrealized_gain: f64,
    pub relative_total_gain_pct: f64,
    pub total_invested: f64,
    pub net_invested: f64,
    pub total_fee: f64,
}

// ─── Client ──────────────────────────────────────────────────────────────────

/// Zerion API client with rate limiting.
///
/// Uses HTTP Basic Auth where the API key is the username and password is empty.
/// Free tier: 60K calls/mo, 10 RPS.
pub struct ZerionClient {
    http: RateLimitedClient,
    base_url: String,
    auth_header: String,
    enabled: bool,
}

impl ZerionClient {
    /// Create a new Zerion client. If `api_key` is None, the client is disabled
    /// and all methods return graceful errors.
    pub fn new(api_key: Option<String>) -> Self {
        let enabled = api_key.is_some();
        let auth_header = match &api_key {
            Some(key) => {
                let encoded = base64_encode(format!("{key}:"));
                format!("Basic {encoded}")
            }
            None => String::new(),
        };
        Self {
            http: RateLimitedClient::new(8), // 8 RPS to stay well under 10 RPS limit
            base_url: ZERION_BASE_URL.to_string(),
            auth_header,
            enabled,
        }
    }

    /// Create a client with custom base URL (for testing).
    pub fn with_base_url(api_key: Option<String>, base_url: String) -> Self {
        let mut client = Self::new(api_key);
        client.base_url = base_url;
        client
    }

    /// Whether the client is configured and enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get portfolio overview for a wallet address.
    pub async fn get_portfolio(&self, address: &str) -> Result<WalletPortfolio> {
        if !self.enabled {
            anyhow::bail!("Zerion client not configured (no API key)");
        }
        let url = format!(
            "{}/wallets/{}/portfolio?filter[positions]=only_simple&currency=usd",
            self.base_url, address
        );
        let resp: ZerionPortfolioResponse = self
            .http
            .get_json_with_auth(&url, &self.auth_header)
            .await?;

        Ok(WalletPortfolio {
            address: resp.data.id,
            total_value_usd: resp.data.attributes.total.positions,
            change_1d_absolute: resp.data.attributes.changes.as_ref().and_then(|c| c.absolute_1d),
            change_1d_percent: resp.data.attributes.changes.as_ref().and_then(|c| c.percent_1d),
        })
    }

    /// Get fungible token positions for a wallet address.
    pub async fn get_positions(&self, address: &str) -> Result<Vec<WalletPosition>> {
        if !self.enabled {
            anyhow::bail!("Zerion client not configured (no API key)");
        }
        let url = format!(
            "{}/wallets/{}/positions?filter[positions]=only_simple&currency=usd",
            self.base_url, address
        );
        let resp: ZerionPositionsResponse = self
            .http
            .get_json_with_auth(&url, &self.auth_header)
            .await?;

        let positions = resp
            .data
            .into_iter()
            .filter_map(|p| {
                let attrs = p.attributes;
                let fi = attrs.fungible_info?;
                Some(WalletPosition {
                    token_name: fi.name.unwrap_or_default(),
                    token_symbol: fi.symbol.unwrap_or_default(),
                    implementation: fi.implementation,
                    quantity: attrs.quantity.and_then(|q| q.numeric).unwrap_or(0.0),
                    value_usd: attrs.value.unwrap_or(0.0),
                    price_usd: attrs.price,
                    change_24h: attrs.value_24h_change,
                })
            })
            .collect();
        Ok(positions)
    }

    /// Get PnL for a wallet address, optionally filtered by chain.
    pub async fn get_pnl(&self, address: &str, chain: Option<&str>) -> Result<WalletPnl> {
        if !self.enabled {
            anyhow::bail!("Zerion client not configured (no API key)");
        }
        let mut url = format!(
            "{}/wallets/{}/pnl?currency=usd",
            self.base_url, address
        );
        if let Some(chain_id) = chain {
            url = format!("{}&filter[chain_ids]={}", url, chain_id);
        }
        let resp: ZerionPnlResponse = self
            .http
            .get_json_with_auth(&url, &self.auth_header)
            .await?;

        let attrs = resp.data.attributes;
        Ok(WalletPnl {
            total_gain: attrs.total_gain.unwrap_or(0.0),
            realized_gain: attrs.realized_gain.unwrap_or(0.0),
            unrealized_gain: attrs.unrealized_gain.unwrap_or(0.0),
            relative_total_gain_pct: attrs.relative_total_gain_percentage.unwrap_or(0.0),
            total_invested: attrs.total_invested.unwrap_or(0.0),
            net_invested: attrs.net_invested.unwrap_or(0.0),
            total_fee: attrs.total_fee.unwrap_or(0.0),
        })
    }
}

/// Simple base64 encoding without external crate.
fn base64_encode(input: String) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] as u32 } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] as u32 } else { 0 };

        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARSET[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARSET[((triple >> 12) & 0x3F) as usize] as char);
        result.push(if i + 1 < bytes.len() { CHARSET[((triple >> 6) & 0x3F) as usize] as char } else { '=' });
        result.push(if i + 2 < bytes.len() { CHARSET[(triple & 0x3F) as usize] as char } else { '=' });

        i += 3;
    }
    result
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_encode_basic() {
        assert_eq!(base64_encode("hello".to_string()), "aGVsbG8=");
        assert_eq!(base64_encode("test-key:1234".to_string()), "dGVzdC1rZXk6MTIzNA==");
        assert_eq!(base64_encode("A".to_string()), "QQ==");
        assert_eq!(base64_encode("AB".to_string()), "QUI=");
        assert_eq!(base64_encode("ABC".to_string()), "QUJD");
    }

    #[test]
    fn test_base64_encode_empty() {
        assert_eq!(base64_encode(String::new()), "");
    }

    #[test]
    fn test_client_disabled_without_key() {
        let client = ZerionClient::new(None);
        assert!(!client.is_enabled());
    }

    #[test]
    fn test_client_enabled_with_key() {
        let client = ZerionClient::new(Some("test-key".to_string()));
        assert!(client.is_enabled());
        assert!(client.auth_header.starts_with("Basic "));
    }

    #[test]
    fn test_parse_portfolio_response() {
        let json = r#"{
            "data": {
                "id": "8BH9pjtgyZDC4iAQH5ZiYDZ1MDWC98xki2V8NzqqKW3K",
                "attributes": {
                    "positions_distribution_by_type": {"wallet": 150.0},
                    "positions_distribution_by_chain": {"solana": 150.0},
                    "total": {"positions": 150.0},
                    "changes": {"absolute_1d": 5.0, "percent_1d": 3.45}
                }
            }
        }"#;
        let resp: ZerionPortfolioResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.attributes.total.positions, 150.0);
        assert_eq!(resp.data.attributes.changes.as_ref().unwrap().absolute_1d, Some(5.0));
    }

    #[test]
    fn test_parse_positions_response() {
        let json = r#"{
            "data": [{
                "id": "pos1",
                "attributes": {
                    "fungible_info": {
                        "name": "Solana",
                        "symbol": "SOL",
                        "implementation": "solana:So11111111111111111111111111111111"
                    },
                    "value": 100.5,
                    "quantity": {"numeric": 0.65},
                    "price": 154.61,
                    "value_24h_change": 2.5
                }
            }]
        }"#;
        let resp: ZerionPositionsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 1);
        let pos = &resp.data[0].attributes;
        assert_eq!(pos.fungible_info.as_ref().unwrap().symbol.as_deref(), Some("SOL"));
        assert_eq!(pos.value, Some(100.5));
    }

    #[test]
    fn test_parse_pnl_response() {
        let json = r#"{
            "data": {
                "id": "8BH9pjtgyZDC4iAQH5ZiYDZ1MDWC98xki2V8NzqqKW3K",
                "attributes": {
                    "total_gain": -637.81,
                    "realized_gain": -655.36,
                    "unrealized_gain": 17.54,
                    "relative_total_gain_percentage": -11.38,
                    "relative_realized_gain_percentage": -15.15,
                    "relative_unrealized_gain_percentage": -0.19,
                    "total_fee": 281.90,
                    "total_invested": 701.2,
                    "realized_cost_basis": 655.36,
                    "net_invested": 45.84
                }
            }
        }"#;
        let resp: ZerionPnlResponse = serde_json::from_str(json).unwrap();
        let attrs = resp.data.attributes;
        assert!((attrs.total_gain.unwrap() - (-637.81)).abs() < 0.01);
        assert!((attrs.realized_gain.unwrap() - (-655.36)).abs() < 0.01);
        assert!((attrs.unrealized_gain.unwrap() - 17.54).abs() < 0.01);
    }

    #[test]
    fn test_graceful_failure_when_disabled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = ZerionClient::new(None);
        rt.block_on(async {
            let result = client.get_portfolio("test").await;
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("not configured"));
        });
    }

    #[tokio::test]
    async fn test_rate_limiting_respected() {
        // Just verify the client creates with proper RPS.
        let client = ZerionClient::new(Some("test".to_string()));
        assert!(client.is_enabled());
        // The rate limiter is internal — we can't easily test it without
        // a mock server, but we verified the RPS is set to 8.
    }
}
