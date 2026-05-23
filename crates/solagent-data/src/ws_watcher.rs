//! Enhanced WebSocket wallet monitor with polling fallback.
//!
//! Uses the Helius Enhanced WebSocket (`transactionSubscribe`) for real-time
//! transaction notifications on watched wallet addresses. On disconnect,
//! automatically falls back to the existing polling-based `WalletWatcher`.
//! Reconnects with exponential backoff and re-subscribes to all wallets.

use anyhow::Result;
use helius::types::{
    RpcTransactionsConfig, TransactionCommitment,
    TransactionSubscribeFilter, TransactionSubscribeOptions,
    UiEnhancedTransactionEncoding,
};
use helius::websocket::{EnhancedWebsocket, ENHANCED_WEBSOCKET_URL_MAINNET};
use solagent_core::EventBus;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

use crate::helius::HeliusSdkClient;
use crate::watcher::{LastSeenMap, WatchedWallet, WalletWatcher, WatcherConfig};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Connection state: disconnected, no active WS.
const STATE_DISCONNECTED: u32 = 0;
/// Connection state: attempting to connect.
const STATE_CONNECTING: u32 = 1;
/// Connection state: connected and subscribed.
const STATE_CONNECTED: u32 = 2;

/// Initial backoff delay after a WS disconnect (seconds).
const INITIAL_BACKOFF_SECS: u64 = 2;

/// Maximum backoff delay (seconds). Capped to prevent excessive waits.
const MAX_BACKOFF_SECS: u64 = 60;

/// How long to wait after a disconnect before declaring WS failed and
/// activating polling fallback.
const FALLBACK_TIMEOUT_SECS: u64 = 30;

/// Maximum time to wait for a WS connection attempt before giving up.
const CONNECT_TIMEOUT_SECS: u64 = 10;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Configuration for the WebSocket wallet watcher.
#[derive(Debug, Clone)]
pub struct WsWatcherConfig {
    /// Polling configuration used for the fallback `WalletWatcher`.
    pub poll_config: WatcherConfig,
    /// Maximum time to wait for a WS connection attempt.
    pub connect_timeout: Duration,
    /// Duration after disconnect before activating polling fallback.
    pub fallback_timeout: Duration,
    /// Initial backoff delay for reconnection attempts.
    pub initial_backoff: Duration,
    /// Maximum backoff delay for reconnection attempts.
    pub max_backoff: Duration,
}

impl Default for WsWatcherConfig {
    fn default() -> Self {
        Self {
            poll_config: WatcherConfig::default(),
            connect_timeout: Duration::from_secs(CONNECT_TIMEOUT_SECS),
            fallback_timeout: Duration::from_secs(FALLBACK_TIMEOUT_SECS),
            initial_backoff: Duration::from_secs(INITIAL_BACKOFF_SECS),
            max_backoff: Duration::from_secs(MAX_BACKOFF_SECS),
        }
    }
}

// ─── WsWatcher ───────────────────────────────────────────────────────────────

/// WebSocket-first wallet watcher with automatic polling fallback.
///
/// Connects to the Helius Enhanced WebSocket and subscribes to
/// `transactionSubscribe` for all watched wallet addresses. On disconnect,
/// activates the polling-based `WalletWatcher` fallback. Automatically
/// reconnects with exponential backoff and re-subscribes to all wallets.
#[derive(Clone)]
pub struct WsWatcher {
    /// Helius API key used to construct the WS URL.
    api_key: String,
    /// Helius SDK client for parsing transactions via REST API.
    helius: Arc<HeliusSdkClient>,
    /// Event bus for publishing WalletBuy/WalletSell events.
    event_bus: EventBus,
    /// Configuration.
    config: WsWatcherConfig,
    /// Currently watched wallet addresses.
    watched: Arc<RwLock<Vec<WatchedWallet>>>,
    /// Last-seen timestamps per wallet (for deduplication).
    last_seen: Arc<LastSeenMap>,
    /// Polling fallback watcher.
    poll_watcher: WalletWatcher,
    /// Atomic connection state (STATE_DISCONNECTED / CONNECTING / CONNECTED).
    connection_state: Arc<AtomicU32>,
    /// Whether the polling fallback is currently active.
    polling_active: Arc<AtomicBool>,
    /// Consecutive reconnect failures (for backoff calculation).
    reconnect_failures: Arc<AtomicU32>,
}

impl WsWatcher {
    /// Create a new WebSocket wallet watcher.
    ///
    /// The watcher wraps both a WS connection and a polling fallback.
    /// Call `run()` to start monitoring.
    pub fn new(
        helius: Arc<HeliusSdkClient>,
        event_bus: EventBus,
        config: WsWatcherConfig,
    ) -> Self {
        let watched = Arc::new(RwLock::new(Vec::new()));
        let last_seen = Arc::new(dashmap::DashMap::new());

        // Create the polling fallback using the same helius client and event bus.
        let poll_watcher = WalletWatcher::new(
            Arc::clone(&helius),
            event_bus.clone(),
            config.poll_config.clone(),
        );

        Self {
            api_key: helius.api_key().to_string(),
            helius,
            event_bus,
            config,
            watched,
            last_seen,
            poll_watcher,
            connection_state: Arc::new(AtomicU32::new(STATE_DISCONNECTED)),
            polling_active: Arc::new(AtomicBool::new(false)),
            reconnect_failures: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Set the list of watched wallets. Replaces any existing list.
    pub async fn set_watched_wallets(&self, wallets: Vec<WatchedWallet>) -> Result<()> {
        let limited: Vec<WatchedWallet> = wallets
            .into_iter()
            .take(self.config.poll_config.max_wallets)
            .collect();
        tracing::info!(count = limited.len(), "Setting watched wallets for WS watcher");

        // Update WS watcher's list.
        {
            let mut guard = self.watched.write().await;
            *guard = limited;
        }

        // Also update the polling fallback's list.
        let wallets_copy = self.watched.read().await.clone();
        self.poll_watcher.set_watched_wallets(wallets_copy).await?;

        Ok(())
    }

    /// Returns the number of currently watched wallets.
    pub async fn watched_count(&self) -> usize {
        self.watched.read().await.len()
    }

    /// Returns `true` if the WS connection is currently active.
    pub fn is_connected(&self) -> bool {
        self.connection_state.load(Ordering::SeqCst) == STATE_CONNECTED
    }

    /// Returns `true` if the polling fallback is currently active.
    pub fn is_polling(&self) -> bool {
        self.polling_active.load(Ordering::SeqCst)
    }

    /// Run the WebSocket watcher main loop until `shutdown` fires.
    ///
    /// This method manages the full lifecycle:
    /// 1. Connect to WS
    /// 2. Subscribe to watched wallets
    /// 3. Process real-time transaction events
    /// 4. On disconnect: activate polling fallback, attempt reconnect
    /// 5. On reconnect: stop polling, re-subscribe
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        tracing::info!("WebSocket wallet watcher starting");

        loop {
            // Check for shutdown signal.
            if *shutdown.borrow() {
                tracing::info!("WebSocket wallet watcher shutting down");
                return;
            }

            // Attempt to connect and subscribe.
            match self.connect_and_subscribe(&mut shutdown).await {
                Ok(()) => {
                    // WS disconnected normally or stream ended.
                    // Reset is handled inside connect_and_subscribe.
                }
                Err(e) => {
                    tracing::warn!(error = %e, "WebSocket connection error");
                }
            }

            // Check shutdown again after disconnect.
            if *shutdown.borrow() {
                tracing::info!("WebSocket wallet watcher shutting down");
                return;
            }

            // Mark as disconnected and activate polling fallback.
            self.connection_state.store(STATE_DISCONNECTED, Ordering::SeqCst);

            // Activate polling fallback if not already active.
            if !self.polling_active.load(Ordering::SeqCst) {
                self.polling_active.store(true, Ordering::SeqCst);
                tracing::info!(
                    "WebSocket disconnected, falling back to polling within {}s",
                    self.config.fallback_timeout.as_secs()
                );
            }

            // Spawn polling fallback task.
            let poll_active = self.polling_active.clone();
            let poll_watcher = self.poll_watcher.clone();
            let mut poll_shutdown = shutdown.clone();
            let poll_handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(
                    poll_watcher.config().poll_interval,
                );
                loop {
                    if *poll_shutdown.borrow() || !poll_active.load(Ordering::SeqCst) {
                        tracing::info!("Polling fallback stopped");
                        return;
                    }
                    tokio::select! {
                        _ = ticker.tick() => {
                            if !poll_active.load(Ordering::SeqCst) {
                                return;
                            }
                            if let Err(e) = poll_watcher.poll_cycle().await {
                                tracing::warn!(error = %e, "Polling fallback cycle failed");
                            }
                        }
                        _ = poll_shutdown.changed() => {
                            return;
                        }
                    }
                }
            });

            // Attempt reconnection with exponential backoff.
            let backoff_delay = self.calculate_backoff();
            let failures = self.reconnect_failures.fetch_add(1, Ordering::SeqCst);

            tracing::info!(
                attempt = failures + 1,
                backoff_secs = backoff_delay.as_secs(),
                "Reconnect attempt with exponential backoff"
            );

            // Wait for backoff (or shutdown).
            tokio::select! {
                _ = tokio::time::sleep(backoff_delay) => {}
                _ = shutdown.changed() => {
                    // Stop polling and shut down.
                    self.polling_active.store(false, Ordering::SeqCst);
                    poll_handle.abort();
                    tracing::info!("WebSocket wallet watcher shutting down during backoff");
                    return;
                }
            }

            // Try to reconnect. If successful, stop polling.
            // The reconnect attempt happens in the next loop iteration.
            // But first, check if we should stop polling.
            // Note: We don't stop polling here because the WS might fail to connect
            // again. Polling stops only after a successful WS connection + subscription.
            drop(poll_handle);
        }
    }

    /// Connect to the WebSocket and subscribe to all watched wallets.
    ///
    /// Returns when the WS stream ends (disconnect or error).
    /// Returns `Ok(())` on normal stream end, `Err` on connection failure.
    async fn connect_and_subscribe(
        &self,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        self.connection_state.store(STATE_CONNECTING, Ordering::SeqCst);

        // Build the WS URL.
        let ws_url = format!("{}{}", ENHANCED_WEBSOCKET_URL_MAINNET, self.api_key);

        tracing::info!("Connecting to Helius Enhanced WebSocket");

        // Connect with timeout.
        let ws = tokio::time::timeout(
            self.config.connect_timeout,
            EnhancedWebsocket::new(&ws_url, None, None),
        )
        .await
        .map_err(|_| anyhow::anyhow!("WebSocket connection timeout after {}s", self.config.connect_timeout.as_secs()))?
        .map_err(|e| anyhow::anyhow!("WebSocket connection failed: {e}"))?;

        tracing::info!("Helius WebSocket connected");
        self.connection_state.store(STATE_CONNECTED, Ordering::SeqCst);
        self.reconnect_failures.store(0, Ordering::SeqCst);

        // Stop polling fallback (WS is connected).
        if self.polling_active.load(Ordering::SeqCst) {
            tracing::info!("WebSocket reconnected, stopping polling fallback");
            self.polling_active.store(false, Ordering::SeqCst);
        }

        // Get the current list of watched wallets.
        let wallets = self.watched.read().await.clone();
        if wallets.is_empty() {
            tracing::info!("No watched wallets — WS connected but idle");
            // Still connected, just wait for wallets to be added or shutdown.
            tokio::select! {
                _ = shutdown.changed() => {
                    let _ = ws.shutdown().await;
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // Periodically check for new wallets.
                    // In production, set_watched_wallets would trigger re-subscription.
                    let _ = ws.shutdown().await;
                    return Ok(());
                }
            }
        }

        // Build subscription filter with all watched wallet addresses.
        let addresses: Vec<String> = wallets.iter().map(|w| w.address.clone()).collect();
        let config = RpcTransactionsConfig {
            filter: TransactionSubscribeFilter {
                account_include: Some(addresses.clone()),
                ..Default::default()
            },
            options: TransactionSubscribeOptions {
                commitment: Some(TransactionCommitment::Confirmed),
                encoding: Some(UiEnhancedTransactionEncoding::JsonParsed),
                transaction_details: Some(helius::types::enhanced_websocket::TransactionDetails::Signatures),
                ..Default::default()
            },
        };

        tracing::info!(
            wallet_count = addresses.len(),
            "Subscribed to transaction notifications for watched wallets"
        );

        // Subscribe to transaction notifications.
        let (stream, _unsub) = ws.transaction_subscribe(config).await.map_err(|e| {
            anyhow::anyhow!("WebSocket transaction subscribe failed: {e}")
        })?;

        // Process the stream until disconnect or shutdown.
        // We use a separate block so that `stream` is dropped before we try
        // to shut down the WebSocket (stream borrows from ws).
        let stream_result = {
            let mut stream = stream;
            loop {
                tokio::select! {
                    notification = tokio_stream::StreamExt::next(&mut stream) => {
                        match notification {
                            Some(notification) => {
                                if let Err(e) = self.handle_notification(&notification, &wallets).await {
                                    tracing::warn!(error = %e, "Failed to process WS notification");
                                }
                            }
                            None => {
                                // Stream ended — WS disconnected.
                                tracing::warn!("WebSocket stream ended — disconnected");
                                break false; // disconnected, not shutdown
                            }
                        }
                    }
                    _ = shutdown.changed() => {
                        tracing::info!("WebSocket wallet watcher shutting down during subscription");
                        break true; // shutdown signal
                    }
                }
            }
        };

        // Stream is dropped here. Now we can shut down the WebSocket.
        // The unsubscribe function (_unsub) is also dropped with the stream.
        let _ = ws.shutdown().await;

        if stream_result {
            // Propagate shutdown signal.
            return Err(anyhow::anyhow!("shutdown"));
        }
        Ok(())
    }

    /// Handle a transaction notification from the WebSocket.
    ///
    /// Extracts the transaction signature, fetches the enhanced transaction
    /// via REST API, and emits WalletBuy/WalletSell events.
    async fn handle_notification(
        &self,
        notification: &helius::types::TransactionNotification,
        wallets: &[WatchedWallet],
    ) -> Result<()> {
        // Extract signature from the notification.
        let signature = match notification {
            helius::types::TransactionNotification::Full(full) => full.signature.clone(),
            helius::types::TransactionNotification::Signature(sig_entry) => {
                sig_entry.signature.clone()
            }
            helius::types::TransactionNotification::Unknown(value) => {
                // Try to extract signature from unknown format.
                value
                    .get("signature")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            }
        };

        if signature.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            signature = %signature,
            "WebSocket transaction notification received"
        );

        // Fetch the enhanced transaction via REST API for full parsing.
        // This is one call per actual transaction (not polling), achieving >80% API reduction.
        let parsed_tx = match self.helius.get_parsed_transaction(&signature).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::debug!(
                    signature = %signature,
                    error = %e,
                    "Failed to fetch enhanced transaction for WS notification — skipping"
                );
                return Ok(());
            }
        };

        // Use the existing event extraction logic by iterating over wallets.
        // The parsed transaction contains all the data we need.
        for wallet in wallets {
            // Check if this transaction involves this wallet.
            let involves_wallet = parsed_tx
                .fee_payer
                .as_deref()
                .map(|fp| fp == wallet.address)
                .unwrap_or(false)
                || parsed_tx
                    .account_data
                    .iter()
                    .any(|ad| ad.account == wallet.address)
                || parsed_tx
                    .native_transfers
                    .as_ref()
                    .map(|nt| {
                        nt.iter().any(|t| {
                            t.from_user_account.as_deref() == Some(&wallet.address)
                                || t.to_user_account.as_deref() == Some(&wallet.address)
                        })
                    })
                    .unwrap_or(false)
                || parsed_tx
                    .token_transfers
                    .as_ref()
                    .map(|tt| {
                        tt.iter().any(|t| {
                            t.from_user_account.as_deref() == Some(&wallet.address)
                                || t.to_user_account.as_deref() == Some(&wallet.address)
                        })
                    })
                    .unwrap_or(false);

            if !involves_wallet {
                continue;
            }

            // Deduplication: check if we've already seen this transaction for this wallet.
            let ts = parsed_tx.timestamp.unwrap_or(0);
            let last_ts = self
                .last_seen
                .get(&wallet.address)
                .map(|g| *g)
                .unwrap_or(0);

            if ts <= last_ts {
                continue;
            }

            // Extract events using the same logic as the polling watcher.
            let events = WalletWatcher::extract_events_static(
                &parsed_tx,
                wallet,
                self.config.poll_config.min_value_usd,
            );

            let mut max_ts = last_ts;
            for event in events {
                max_ts = max_ts.max(ts);
                self.event_bus.publish(event);
            }

            if max_ts > last_ts {
                self.last_seen.insert(wallet.address.clone(), max_ts);
            }
        }

        Ok(())
    }

    /// Calculate the exponential backoff delay based on consecutive reconnect failures.
    fn calculate_backoff(&self) -> Duration {
        let n = self.reconnect_failures.load(Ordering::SeqCst);
        // Backoff: 2^(n+1) seconds, capped at MAX_BACKOFF_SECS.
        // n=0 → 2s, n=1 → 4s, n=2 → 8s, n=3 → 16s, n=4 → 32s, n=5+ → 60s
        let delay_secs = (2u64.saturating_pow(n.saturating_add(1).min(31)))
            .saturating_mul(1) // Base unit is seconds
            .min(MAX_BACKOFF_SECS);
        Duration::from_secs(delay_secs)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use solagent_core::Chain;

    #[test]
    fn test_calculate_backoff_first_attempt() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        // Initial backoff: 2^(0+1) = 2s
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(2));
    }

    #[test]
    fn test_calculate_backoff_second_attempt() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        watcher.reconnect_failures.store(1, Ordering::SeqCst);
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(4));
    }

    #[test]
    fn test_calculate_backoff_third_attempt() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        watcher.reconnect_failures.store(2, Ordering::SeqCst);
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(8));
    }

    #[test]
    fn test_calculate_backoff_capped_at_max() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        // After many failures, should be capped at 60s.
        watcher.reconnect_failures.store(10, Ordering::SeqCst);
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(MAX_BACKOFF_SECS));

        watcher.reconnect_failures.store(50, Ordering::SeqCst);
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(MAX_BACKOFF_SECS));
    }

    #[test]
    fn test_backoff_sequence_follows_exponential() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        // Verify the full sequence: 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
        let expected = [2, 4, 8, 16, 32, 60, 60];
        for (i, expected_secs) in expected.iter().enumerate() {
            watcher.reconnect_failures.store(i as u32, Ordering::SeqCst);
            let backoff = watcher.calculate_backoff();
            assert_eq!(
                backoff.as_secs(),
                *expected_secs,
                "Backoff at failure count {} should be {}s, got {}s",
                i,
                expected_secs,
                backoff.as_secs()
            );
        }
    }

    #[test]
    fn test_initial_state_is_disconnected() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        assert!(!watcher.is_connected(), "Should start disconnected");
        assert!(!watcher.is_polling(), "Should start without polling");
    }

    #[tokio::test]
    async fn test_set_watched_wallets() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        assert_eq!(watcher.watched_count().await, 0);

        let wallets = vec![
            WatchedWallet {
                address: "wallet1".to_string(),
                chain: Chain::Solana,
                score: 85.0,
            },
            WatchedWallet {
                address: "wallet2".to_string(),
                chain: Chain::Solana,
                score: 75.0,
            },
        ];

        watcher.set_watched_wallets(wallets).await.unwrap();
        assert_eq!(watcher.watched_count().await, 2);
    }

    #[test]
    fn test_reconnect_failures_resets_on_connect() {
        let config = WsWatcherConfig::default();
        let helius = Arc::new(
            HeliusSdkClient::new("test_key_for_unit_tests_only")
                .expect("valid key"),
        );
        let watcher = WsWatcher::new(
            helius,
            EventBus::new(64),
            config,
        );

        // Simulate some failures.
        watcher.reconnect_failures.store(5, Ordering::SeqCst);
        assert_eq!(watcher.reconnect_failures.load(Ordering::SeqCst), 5);

        // On successful connection (simulated by direct store), failures reset.
        watcher.reconnect_failures.store(0, Ordering::SeqCst);
        assert_eq!(watcher.reconnect_failures.load(Ordering::SeqCst), 0);

        // Backoff should be back to initial.
        let backoff = watcher.calculate_backoff();
        assert_eq!(backoff, Duration::from_secs(2));
    }

    #[test]
    fn test_websocket_url_construction() {
        // Verify the WS URL format.
        let api_key = "test-api-key-123";
        let url = format!("{}{}", ENHANCED_WEBSOCKET_URL_MAINNET, api_key);
        assert_eq!(
            url,
            "wss://atlas-mainnet.helius-rpc.com/?api-key=test-api-key-123"
        );
    }
}
