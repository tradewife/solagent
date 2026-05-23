//! Helius-backed wallet watcher that monitors known wallets and emits events.
//!
//! Uses polling (not webhooks) so it works without a public HTTP endpoint.
//! Periodically fetches recent parsed transactions for each watched wallet
//! and emits `WalletBuy` / `WalletSell` events on the event bus.
//!
//! Swap detection uses the Helius `events.swap` field (authoritative) and
//! falls back to `tokenTransfers` + `nativeTransfers` analysis.

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use solagent_core::{Chain, Event, EventBus};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

use crate::helius::{HeliusSdkClient, ParsedTransaction, SwapEvent};

type LastSeenMap = DashMap<String, i64>;

/// Configuration for the wallet watcher.
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    pub poll_interval: Duration,
    pub max_wallets: usize,
    pub min_value_usd: f64,
    /// Delay between individual wallet polls within a cycle.
    pub stagger_delay: Duration,
    /// Duration to sleep the entire poll cycle when a 429 is encountered.
    pub rate_limit_backoff: Duration,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(120),
            max_wallets: 10,
            min_value_usd: 0.0,
            stagger_delay: Duration::from_secs(3),
            rate_limit_backoff: Duration::from_secs(30),
        }
    }
}

/// A wallet being monitored.
#[derive(Debug, Clone)]
pub struct WatchedWallet {
    pub address: String,
    pub chain: Chain,
    pub score: f64,
}

/// Parsed swap details extracted from a transaction.
struct SwapDetails {
    /// The SPL token mint that was bought or sold.
    token_mint: String,
    /// True if the wallet bought the token (sent SOL, received token).
    is_buy: bool,
    /// SOL amount in lamports involved in the swap.
    sol_lamports: u64,
}

/// Helius-based wallet watcher that polls for new transactions and emits events.
#[derive(Clone)]
pub struct WalletWatcher {
    helius: Arc<HeliusSdkClient>,
    event_bus: EventBus,
    config: WatcherConfig,
    watched: Arc<RwLock<Vec<WatchedWallet>>>,
    last_seen: Arc<LastSeenMap>,
}

impl WalletWatcher {
    pub fn new(helius: Arc<HeliusSdkClient>, event_bus: EventBus, config: WatcherConfig) -> Self {
        Self {
            helius,
            event_bus,
            config,
            watched: Arc::new(RwLock::new(Vec::new())),
            last_seen: Arc::new(DashMap::new()),
        }
    }

    pub async fn set_watched_wallets(&self, wallets: Vec<WatchedWallet>) -> Result<()> {
        let limited: Vec<WatchedWallet> = wallets
            .into_iter()
            .take(self.config.max_wallets)
            .collect();
        tracing::info!(count = limited.len(), "Setting watched wallets");
        let mut guard = self.watched.write().await;
        *guard = limited;
        Ok(())
    }

    pub async fn add_wallet(&self, wallet: WatchedWallet) -> Result<()> {
        let mut guard = self.watched.write().await;
        if guard.len() >= self.config.max_wallets {
            anyhow::bail!("Watch list is full (max {})", self.config.max_wallets);
        }
        guard.push(wallet);
        Ok(())
    }

    pub async fn remove_wallet(&self, address: &str) -> Result<bool> {
        let mut guard = self.watched.write().await;
        let before = guard.len();
        guard.retain(|w| w.address != address);
        Ok(guard.len() < before)
    }

    pub async fn watched_count(&self) -> usize {
        self.watched.read().await.len()
    }

    /// Run the polling loop until `shutdown` fires.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut ticker = interval(self.config.poll_interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.poll_all().await {
                        tracing::error!(error = %e, "Wallet poll cycle failed");
                    }
                }
                _ = shutdown.changed() => {
                    tracing::info!("Wallet watcher shutting down");
                    return;
                }
            }
        }
    }

    async fn poll_all(&self) -> Result<()> {
        let wallets = self.watched.read().await.clone();
        for wallet in &wallets {
            if let Err(e) = self.poll_wallet(wallet).await {
                let err_str = e.to_string();
                if err_str.contains("429") {
                    tracing::warn!(
                        wallet = &wallet.address[..wallet.address.len().min(12)],
                        "Rate limited by Helius, backing off for {:?}",
                        self.config.rate_limit_backoff
                    );
                    tokio::time::sleep(self.config.rate_limit_backoff).await;
                } else {
                    tracing::warn!(
                        wallet = &wallet.address[..wallet.address.len().min(12)],
                        error = %e,
                        "Failed to poll wallet"
                    );
                }
            }
            // Stagger requests to respect Helius free tier (~1 req/sec sustained).
            // 10 wallets × 3s = 30s per cycle, leaving 90s idle before next cycle.
            tokio::time::sleep(self.config.stagger_delay).await;
        }
        Ok(())
    }

    async fn poll_wallet(&self, wallet: &WatchedWallet) -> Result<()> {
        let txs = self.helius.get_transactions(&wallet.address, None).await?;

        let last_ts = self
            .last_seen
            .get(&wallet.address)
            .map(|g| *g)
            .unwrap_or(0);

        let mut max_ts = last_ts;

        for tx in &txs {
            let ts = tx.timestamp.unwrap_or(0);
            if ts <= last_ts {
                continue;
            }
            max_ts = max_ts.max(ts);

            for event in self.extract_events(tx, wallet) {
                self.event_bus.publish(event);
            }
        }

        if max_ts > last_ts {
            self.last_seen.insert(wallet.address.clone(), max_ts);
        }

        Ok(())
    }

    /// Extract WalletBuy/WalletSell events from a parsed transaction.
    ///
    /// Strategy (in order of reliability):
    /// 1. `events.swap` -- authoritative structured swap data from Helius
    /// 2. `tokenTransfers` -- SPL token movements, cross-referenced with nativeTransfers
    /// 3. Skip non-swap transactions
    fn extract_events(&self, tx: &ParsedTransaction, wallet: &WatchedWallet) -> Vec<Event> {
        let ts = tx
            .timestamp
            .and_then(|t| DateTime::from_timestamp(t, 0))
            .unwrap_or_else(Utc::now);

        // Strategy 1: Use events.swap if available.
        if let Some(events) = &tx.events
            && let Some(swap) = &events.swap
            && let Some(details) = self.parse_swap_event(swap, &wallet.address)
        {
            let amount_sol = details.sol_lamports as f64 / 1_000_000_000.0;
            let event = if details.is_buy {
                Event::WalletBuy {
                    wallet: wallet.address.clone(),
                    token_address: details.token_mint,
                    chain: wallet.chain,
                    amount_usd: amount_sol,
                    timestamp: ts,
                }
            } else {
                Event::WalletSell {
                    wallet: wallet.address.clone(),
                    token_address: details.token_mint,
                    chain: wallet.chain,
                    amount_usd: amount_sol,
                    timestamp: ts,
                }
            };
            return vec![event];
        }

        // Strategy 2: Analyze tokenTransfers for swap patterns.
        if let Some(token_transfers) = &tx.token_transfers {
            let mut events = Vec::new();

            // Find transfers where the wallet is the sender or receiver.
            for transfer in token_transfers {
                let is_sender = transfer.from_user_account.as_deref() == Some(&wallet.address);
                let is_receiver = transfer.to_user_account.as_deref() == Some(&wallet.address);

                if !is_sender && !is_receiver {
                    continue;
                }

                // Check if there's a corresponding native transfer indicating SOL movement.
                let sol_amount = self.find_matching_native_transfer(
                    tx,
                    &wallet.address,
                    is_sender,
                );

                // For buys: wallet receives tokens, sends SOL.
                // For sells: wallet sends tokens, receives SOL.
                let (is_buy, amount_sol) = if is_receiver && !is_sender {
                    (true, sol_amount)
                } else if is_sender && !is_receiver {
                    (false, sol_amount)
                } else {
                    continue;
                };

                if amount_sol < self.config.min_value_usd {
                    continue;
                }

                let event = if is_buy {
                    Event::WalletBuy {
                        wallet: wallet.address.clone(),
                        token_address: transfer.mint.clone(),
                        chain: wallet.chain,
                        amount_usd: amount_sol,
                        timestamp: ts,
                    }
                } else {
                    Event::WalletSell {
                        wallet: wallet.address.clone(),
                        token_address: transfer.mint.clone(),
                        chain: wallet.chain,
                        amount_usd: amount_sol,
                        timestamp: ts,
                    }
                };
                events.push(event);
            }

            return events;
        }

        Vec::new()
    }

    /// Parse the Helius SwapEvent to determine what the wallet bought/sold.
    ///
    /// The SwapEvent has:
    /// - `nativeInput`: SOL sent in (buy) -- has account + amount in lamports
    /// - `nativeOutput`: SOL received (sell)
    /// - `tokenInputs`: tokens sent in (sell) -- has mint + userAccount
    /// - `tokenOutputs`: tokens received (buy) -- has mint + userAccount
    fn parse_swap_event(&self, swap: &SwapEvent, wallet_address: &str) -> Option<SwapDetails> {
        // Check if wallet sent SOL (buy) or received SOL (sell).
        let sent_sol = swap.native_input.as_ref().and_then(|n| {
            if n.account == wallet_address {
                Some(n.amount.as_u64().unwrap_or(0))
            } else {
                None
            }
        });

        let received_sol = swap.native_output.as_ref().and_then(|n| {
            if n.account == wallet_address {
                Some(n.amount.as_u64().unwrap_or(0))
            } else {
                None
            }
        });

        if let Some(sol_lamports) = sent_sol {
            // Wallet sent SOL -> buying tokens.
            // The token output with this wallet's userAccount is what was bought.
            let token_mint = swap.token_outputs.iter()
                .find(|t| t.user_account == wallet_address)
                .map(|t| t.mint.clone())?;

            return Some(SwapDetails {
                token_mint,
                is_buy: true,
                sol_lamports,
            });
        }

        if let Some(sol_lamports) = received_sol {
            // Wallet received SOL -> selling tokens.
            let token_mint = swap.token_inputs.iter()
                .find(|t| t.user_account == wallet_address)
                .map(|t| t.mint.clone())?;

            return Some(SwapDetails {
                token_mint,
                is_buy: false,
                sol_lamports,
            });
        }

        // No SOL movement for this wallet -- might be a token-to-token swap.
        // Check if wallet appears in tokenInputs (selling) or tokenOutputs (buying).
        let sold_token = swap.token_inputs.iter()
            .find(|t| t.user_account == wallet_address);
        let bought_token = swap.token_outputs.iter()
            .find(|t| t.user_account == wallet_address);

        if let Some(sold) = sold_token {
            return Some(SwapDetails {
                token_mint: sold.mint.clone(),
                is_buy: false,
                sol_lamports: 0,
            });
        }

        if let Some(bought) = bought_token {
            return Some(SwapDetails {
                token_mint: bought.mint.clone(),
                is_buy: true,
                sol_lamports: 0,
            });
        }

        None
    }

    /// Find a matching native SOL transfer for a given wallet.
    ///
    /// For buys (wallet sending SOL): look for nativeTransfers where wallet is from.
    /// For sells (wallet receiving SOL): look for nativeTransfers where wallet is to.
    fn find_matching_native_transfer(
        &self,
        tx: &ParsedTransaction,
        wallet_address: &str,
        is_buy: bool,
    ) -> f64 {
        let native_transfers = match &tx.native_transfers {
            Some(t) => t,
            None => return 0.0,
        };

        for transfer in native_transfers {
            let matches = if is_buy {
                transfer.from_user_account.as_deref() == Some(wallet_address)
            } else {
                transfer.to_user_account.as_deref() == Some(wallet_address)
            };

            if matches {
                return transfer.amount.as_f64().unwrap_or(0.0) / 1_000_000_000.0;
            }
        }

        0.0
    }
}
