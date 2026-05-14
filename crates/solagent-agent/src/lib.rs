//! # solagent-agent
//!
//! Autonomous agent with state machine, main event loop, and decision logging.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use solagent_core::chrono::{DateTime, Utc};
use solagent_core::serde_json;
use solagent_core::uuid::Uuid;
use solagent_core::{Chain, EventBus, Event, Signal, TokenInfo};
use std::sync::Arc;

// ─── Agent State Machine ─────────────────────────────────────────────────────

/// Agent states in the trading lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentState {
    /// Scanning for new tokens / opportunities.
    Scanning,
    /// Evaluating a candidate token (running strategies + safety).
    Evaluating,
    /// Running risk checks on an approved signal.
    RiskCheck,
    /// Executing a trade.
    Executing,
    /// Monitoring open positions.
    Monitoring,
}

impl std::fmt::Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentState::Scanning => write!(f, "SCANNING"),
            AgentState::Evaluating => write!(f, "EVALUATING"),
            AgentState::RiskCheck => write!(f, "RISK_CHECK"),
            AgentState::Executing => write!(f, "EXECUTING"),
            AgentState::Monitoring => write!(f, "MONITORING"),
        }
    }
}

// ─── Decision Log ────────────────────────────────────────────────────────────

/// A single decision entry with full reasoning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub state: AgentState,
    pub token_address: Option<String>,
    pub signals: Vec<Signal>,
    pub safety_score: Option<u8>,
    pub risk_report: Option<serde_json::Value>,
    pub action: String,
    pub reasoning: String,
    pub outcome: Option<String>,
}

// ─── Agent Configuration ─────────────────────────────────────────────────────

/// Agent-specific configuration beyond the base Config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub scan_interval_secs: u64,
    pub monitor_interval_secs: u64,
    pub max_concurrent_evaluations: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 30,
            monitor_interval_secs: 60,
            max_concurrent_evaluations: 5,
        }
    }
}

// ─── Agent ───────────────────────────────────────────────────────────────────

/// The autonomous SolAgent trading agent.
pub struct Agent {
    state: Arc<tokio::sync::RwLock<AgentState>>,
    config: AgentConfig,
    event_bus: EventBus,
    decisions: Arc<tokio::sync::RwLock<Vec<Decision>>>,
    running: Arc<tokio::sync::RwLock<bool>>,
}

impl Agent {
    /// Create a new agent.
    pub fn new(config: AgentConfig, event_bus: EventBus) -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(AgentState::Scanning)),
            config,
            event_bus,
            decisions: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            running: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    /// Get the current agent state.
    pub async fn state(&self) -> AgentState {
        *self.state.read().await
    }

    /// Transition to a new state.
    pub async fn transition(&self, new_state: AgentState) {
        let old = *self.state.read().await;
        tracing::info!(old_state = %old, new_state = %new_state, "State transition");
        *self.state.write().await = new_state;
    }

    /// Log a decision.
    pub async fn log_decision(&self, decision: Decision) {
        tracing::info!(
            state = %decision.state,
            action = %decision.action,
            token = ?decision.token_address,
            "Decision logged"
        );
        self.decisions.write().await.push(decision);
    }

    /// Run the scanning phase — discover new tokens.
    async fn scan(&self) -> Result<Vec<TokenInfo>> {
        self.transition(AgentState::Scanning).await;
        todo!("Fetch new tokens from DexScreener / Helius webhooks")
    }

    /// Run the evaluation phase — apply strategies and safety checks.
    async fn evaluate(&self, _token: &TokenInfo) -> Result<EvaluationResult> {
        self.transition(AgentState::Evaluating).await;
        todo!("Run confluence scorer + safety scorer on token")
    }

    /// Run risk check phase.
    async fn risk_check(&self, _evaluation: &EvaluationResult) -> Result<bool> {
        self.transition(AgentState::RiskCheck).await;
        todo!("Run risk manager checks")
    }

    /// Execute a trade.
    async fn execute(&self, _evaluation: &EvaluationResult) -> Result<()> {
        self.transition(AgentState::Executing).await;
        todo!("Dispatch to execution engine")
    }

    /// Monitor open positions (check stop-loss, take-profit).
    async fn monitor(&self) -> Result<()> {
        self.transition(AgentState::Monitoring).await;
        todo!("Check open positions against SL/TP")
    }

    /// Run the agent's main event loop.
    /// This uses tokio::select! to multiplex between different signal channels.
    pub async fn run(&self) -> Result<()> {
        *self.running.write().await = true;

        let mut event_rx = self.event_bus.subscribe();
        let mut scan_interval = tokio::time::interval(
            std::time::Duration::from_secs(self.config.scan_interval_secs),
        );
        let mut monitor_interval = tokio::time::interval(
            std::time::Duration::from_secs(self.config.monitor_interval_secs),
        );

        tracing::info!("Agent starting main loop");

        while *self.running.read().await {
            tokio::select! {
                // Periodic scan for new tokens.
                _ = scan_interval.tick() => {
                    tracing::debug!("Scan tick");
                    match self.scan().await {
                        Ok(tokens) => {
                            tracing::info!(count = tokens.len(), "Scan discovered tokens");
                            for token in &tokens {
                                self.event_bus.publish(Event::TokenDiscovered {
                                    token: token.clone(),
                                    timestamp: Utc::now(),
                                });
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Scan failed");
                        }
                    }
                }

                // Periodic position monitoring.
                _ = monitor_interval.tick() => {
                    if let Err(e) = self.monitor().await {
                        tracing::error!(error = %e, "Monitor failed");
                    }
                }

                // React to events from the event bus.
                event = event_rx.recv() => {
                    match event {
                        Ok(event) => {
                            tracing::debug!(?event, "Received event");
                            self.handle_event(event).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                            tracing::warn!(count, "Event bus lagged, skipping events");
                        }
                        Err(_) => {
                            tracing::warn!("Event channel closed");
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("Agent main loop exited");
        Ok(())
    }

    /// Handle a single event from the event bus.
    async fn handle_event(&self, event: Event) {
        match event {
            Event::TokenDiscovered { token, timestamp: _ } => {
                tracing::info!(
                    token = %token.address,
                    symbol = %token.symbol,
                    "Processing discovered token"
                );
                // Evaluate the token.
                if let Ok(result) = self.evaluate(&token).await {
                    if result.passed {
                        self.log_decision(Decision {
                            id: Uuid::new_v4(),
                            timestamp: Utc::now(),
                            state: AgentState::Evaluating,
                            token_address: Some(token.address.clone()),
                            signals: result.signals.clone(),
                            safety_score: Some(result.safety_score),
                            risk_report: None,
                            action: "evaluate_pass".to_string(),
                            reasoning: format!(
                                "Confluence: {}, Safety: {}",
                                result.confluence_score, result.safety_score
                            ),
                            outcome: None,
                        }).await;

                        // Proceed to risk check.
                        if let Ok(approved) = self.risk_check(&result).await {
                            if approved {
                                if let Err(e) = self.execute(&result).await {
                                    tracing::error!(error = %e, "Execution failed");
                                }
                            }
                        }
                    }
                }
            }
            Event::SignalFired { signal, timestamp: _ } => {
                tracing::info!(
                    token = %signal.token_address,
                    strategy = %signal.strategy,
                    score = signal.score,
                    "Signal fired"
                );
            }
            Event::CircuitBreaker { message, timestamp: _ } => {
                tracing::warn!(message, "Circuit breaker triggered!");
                // Stop the agent.
                *self.running.write().await = false;
            }
            _ => {
                tracing::debug!(?event, "Unhandled event type");
            }
        }
    }

    /// Stop the agent.
    pub async fn stop(&self) {
        *self.running.write().await = false;
        tracing::info!("Agent stop requested");
    }

    /// Get all logged decisions.
    pub async fn decisions(&self) -> Vec<Decision> {
        self.decisions.read().await.clone()
    }
}

// ─── Evaluation Result ───────────────────────────────────────────────────────

/// Result of evaluating a token through strategies + safety.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub token_address: String,
    pub chain: Chain,
    pub confluence_score: u8,
    pub safety_score: u8,
    pub signals: Vec<Signal>,
    pub passed: bool,
    pub reasoning: String,
}
