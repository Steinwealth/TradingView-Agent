# TradingView Agent

**Version:** 3.7.0  
**Last Updated:** February 2026  
**Status:** Production ready (DEMO mode supported)  
**Hosting target:** Google Cloud Run (serverless)  
**Core design:** FastAPI webhook relay • Declarative YAML config • Firestore-backed state • Virtual partitions

---

## 1. Overview

TradingView Agent is a production-grade execution layer for TradingView strategies.

It listens for TradingView webhook alerts and converts them into instant, margin-aware broker executions across supported crypto futures and equities accounts (Coinbase, Binance, Kraken, etc.).

Its purpose is simple:

**Eliminate execution latency and reliability risk between TradingView signals and broker fills.**

It works with any TradingView strategy that emits JSON via `alert()` (e.g. Ichimoku, trend-following, ORB). This system is not a trading strategy itself; it is the execution, monitoring, and capital orchestration layer that sits between signal generation (TradingView) and broker infrastructure.

### The Problem It Solves

A TradingView strategy can be profitable in backtests — but live execution often fails due to:

- Alert delivery delays
- Missed or duplicate webhooks
- Manual trade entry latency
- Broker-side exit failures
- Intraday → overnight margin changes
- Capital collisions between strategies

Profitable signals are meaningless if execution is unreliable.

TradingView Agent exists to make execution deterministic, monitored, and risk-enforced.

### What the Agent Does

- Receives TradingView webhook alerts instantly.
- Routes them to configured broker accounts.
- Sizes positions using real-time margin rates.
- Enforces partition-level capital isolation.
- Verifies broker position state before exits.
- Runs a 30-second exit monitor to mirror Pine Script logic.
- Auto-flattens opposite positions.
- Prevents duplicate exits.
- Sends real-time Telegram trade alerts and daily summaries.
- Scales to zero when no positions are open.

It is not a strategy.

It is the execution and monitoring engine behind your strategy.

### Why This Exists

Most retail trading workflows couple research, signal generation, and execution in a single environment. This platform decouples them into independent layers, enabling production-grade monitoring, capital isolation, and cloud-native deployment.

---

## 2. Exit Reliability Layer

TradingView exit alerts can fail, arrive late, or be rate-limited.

To mitigate this, the Agent runs its own 30-second monitoring loop that can replicate Pine Script exit logic, including:

- RSI-based exits
- Trailing stops
- Breakeven logic
- Stop loss / take profit
- End-of-day auto-close
- Ichimoku cloud exits

Before any close order is sent, the Agent verifies broker position state to prevent duplicate exits.

This creates a redundant exit system:

**TradingView Exit → Webhook → Agent**  
*or*  
**Agent Monitor → Broker Verification → Close**

Execution reliability does not depend on a single webhook.

---

## 3. Real-Time Trade Notifications

The Agent integrates with Telegram (and configurable messaging channels) to provide:

- Entry alerts (symbol, side, size, leverage)
- Exit alerts (reason + source)
- Commission cap warnings
- Margin guard triggers
- Contract expiry warnings
- End-of-day performance summaries
- Partition-level P&L reports

Alerts include execution source:

- **TradingView Webhook**
- **Agent Exit Monitor**
- **Afterhours Margin Checker**

This ensures full visibility into live trading activity without logging into the broker.

---

## 4. System Characteristics

- **Event-driven webhook architecture** — TradingView alerts (JSON) trigger validation, routing, sizing, and execution; no polling.
- **Stateless compute with stateful Firestore reconciliation** — Cloud Run instances are stateless; partition state, trade history, and open positions persist in Firestore and are restored on startup.
- **Idempotent order handling** — Payload metadata (`bar_time`, `cid`, `client_tag`) supports deduplication; broker position verification avoids double exits.
- **30-second monitoring loop for exits** — Agent-level exit monitor runs every 30 seconds; executes breakeven, RSI, trailing stop, stop loss, take profit, EOD auto-close, and Ichimoku cloud exits with duplicate-exit prevention.
- **Declarative YAML configuration** — Strategies, accounts, partitions, risk limits, and exit presets are defined in `config.yaml`; secrets live in Google Secret Manager.
- **Scale-to-zero serverless deployment** — Service never scales to zero while any position is open; when flat during the weekend window (Fri 4am PT–Mon 1am PT with entry blocking), Cloud Run scales to zero; Cloud Scheduler wakes Monday 4am ET.
- **Broker ↔ ledger reconciliation guarantees** — Startup and periodic reconciliation keep Firestore partition state aligned with live broker balance and positions; manual reconcile available via admin endpoint.

### Design Principles

- Separation of concerns — Strategy research remains in TradingView; execution and risk orchestration live in the Agent.
- Stateless compute, durable state — Cloud Run instances remain ephemeral; Firestore guarantees persistence.
- Idempotency first — Every execution path validates broker state before action.
- Capital isolation by design — Virtual partitions prevent cross-strategy capital bleed.
- Fail-safe over fail-fast — Exits always run; new entries halt on anomaly.
- Cost-aware architecture — Designed to scale to zero when flat.

---

## 5. Architecture

```
  TradingView (Pine strategy)
         │  alert() → JSON webhook
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │  TradingView Agent (Cloud Run / FastAPI)                 │
  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │
  │  │ Webhook     │  │ Partition    │  │ Broker adapters │  │
  │  │ validation  │→ │ manager      │→ │ (Coinbase /     │  │
  │  │ & routing   │  │ & sizing     │  │  Binance / DEX) │  │
  │  └─────────────┘  └──────────────┘  └────────┬────────┘  │
  │         │                  │                       │       │
  │         ▼                  ▼                       ▼       │
  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │
  │  │ Exit        │  │ Firestore   │  │ Telegram        │  │
  │  │ monitor     │  │ (state)     │  │ (alerts / EOD)  │  │
  │  │ (30s loop)  │  └──────────────┘  └─────────────────┘  │
  │  └─────────────┘                                            │
  └──────────────────────────────────────────────────────────┘
         │
         ▼
  Broker API (demo ledger or live)
```

---

## 6. Capabilities

- **Execution** — Coinbase Advanced Trade (nano futures) and Binance USDⓈ-M; optional Base DEX spot bridge. Multi-account routing; margin-based position sizing with intraday/afterhours rate selection; ADV cap. Idempotent webhook handling with broker position verification before exits.
- **Capital Management** — Virtual partitions (2–4 slices per account), isolated or cooperative styles; two-layer balance (virtual P&L per partition, shared real broker cash). Min-contract buffer for small accounts; partition-level capital enforcement.
- **Monitoring & Safety** — 30-second agent-level exit loop (breakeven, RSI, trailing stop, stop loss, take profit, EOD auto-close, Ichimoku cloud). Contract expiry monitoring; commission cap auto-halt; duplicate-exit prevention and opposite-position auto-flatten. Telegram execution and EOD alerts.
- **Infrastructure** — Declarative YAML config; Firestore-backed state; scale-to-zero serverless (weekend entry-blocking window); broker ↔ ledger reconciliation.

> Always validate in DEMO mode before promoting to LIVE deployment.

For preset and alert details see `docs/strategies/EASY_ICHIMOKU_GUIDE.md`.

### Execution Guarantees

- **Idempotent handling and broker verification** — Payload metadata (`bar_time`, `cid`, `client_tag`) supports request deduplication; every exit path checks broker position state before sending close orders, preventing double-closing and phantom exits. Agent exit monitor and TradingView webhook exits are coordinated under a single verification gate.
- **Duplicate exit prevention** — Agent exit monitor and TradingView webhook exits share the same broker-verification step; only one close is executed per position.
- **Opposite position auto-flatten** — On entry in the opposite direction (e.g. SELL when LONG exists), the Agent closes the existing position before opening the new one; coordinated with Agent-level exits and broker verification.
- **Commission cap auto-halt** — If a live fill's commission exceeds `max_commission_rate_pct` (default 0.04% of notional), new entries are blocked; exits continue. Telegram alert emitted; resume via `/trading/enable` or redeploy.
- **Contract expiry monitoring** — Background monitor logs warnings as nano futures approach expiry; entries can be disabled before settlement while exits remain allowed.
- **Auto-close for afterhours margin transitions** — Daily check at 3:55 PM ET closes positions that cannot afford afterhours margin rates; prevents margin calls when intraday positions would exceed overnight requirements.
- **Partition-level capital enforcement** — Per-partition virtual balance, deployed cash, and min-contract buffer (default 30%) enforce capital limits; insufficient balance yields a single skip alert with required wallet size; executions resume when balance is sufficient.

### Non-Goals

The Agent does **not** define trading logic (entries/exits are determined by TradingView or the configured agent-level exit presets). It does not replace the broker's margin or risk systems; it enforces allocation and sizing within broker limits. Strategy research, backtesting, and chart setup remain in TradingView and strategy repos; the Agent's scope is execution, monitoring, and capital orchestration only. It does not provide discretionary overrides, signal optimization, or backtesting functionality — those belong in research systems.

### Key Capabilities

- **Single-alert TradingView workflow** – One webhook per chart using `{{strategy.order.action}}` placeholders drives buy/sell/exit logic.
- **Multi-account routing** – Multiple accounts can subscribe to the same `strategy_id`, each with its own broker, leverage and mode.
- **Dynamic position sizing** – Global and per-strategy capital allocation settings (percent or fixed) align live sizing with backtests.
- **ADV Cap (Slip Guard)** – Aggregates projected exposure across partitions and trims or skips orders to keep requests inside liquidity guardrails (default 1% ADV; entries are never blocked when trimmed—orders execute at reduced size or are skipped with logging).
- **Multi-broker adapters** – Coinbase (Advanced Trade), Binance USDⓈ-M, and Base DEX spot are supported today; others can be added via `brokers/*.py`.
- **Virtual account partitions** – 2–4 virtual slices per broker account with independent balances, leverage, strategy lists and trade histories.
- **Partition styles** – `isolated` compounding or `cooperative` rebalance-to-split; switchable through `config.yaml`.
- **Cooperative routing** – In `cooperative` mode the agent assigns each new entry signal to the next available partition (FIFO). A partition must be idle (no open position) before it receives another signal, ensuring four concurrent trades max without symbol affinity and preventing leg skips when unused capital exists.
- **Partition split options** – Presets such as `25/25/25/25`, `33/33/33`, `50/25/25`, and `50/50` can be applied to match risk preferences.
- **Contract expiry protection** – Background monitor probes contract metadata and logs warnings before expiry (e.g., BIPZ2030). Use alerts to rotate contracts and temporarily disable entries.
- **Demo & live modes** – Each account can run in `DEMO` (virtual ledger) or `LIVE` against real brokers without code changes.
- **Min-contract buffer & small accounts** – Configurable buffer (default 30%) above broker minimums; if a partition's balance is below the required wallet size, the Agent skips once and sends a single Telegram alert with the exact amount needed. See `docs/features/SMALL_ACCOUNT_TRADING_GUIDE.md`.
- **Risk management** – Capital caps, ADV guard, optional minimum balances per partition, strategy enable/disable toggles, and daily summaries for oversight.

### Partition Styles

| Style | How Trades Route | What Compounds | EOD Focus |
|-------|------------------|----------------|-----------|
| **Isolated** (default) | Each partition listens to specific `strategy_ids`. Only those partitions receive the alert. | Virtual balance + drawdown stay fully independent per partition/strategy. | EOD shows per-partition results (one-to-one with strategy charts). |
| **Cooperative** | All partitions share the same strategy list. Incoming signals go to the next idle partition (FIFO). | Real account equity is rebalanced back to the configured split after each trade. | EOD highlights per-strategy performance; partitions only appear in the open-positions snapshot. |

Use isolated when you want hard capital silos per chart, and cooperative when you want multiple slots that take signals in order to minimize skips while keeping per-trade capital capped.

### Preset TradingView Symbols (Coinbase nano futures)

| Symbol | Description | Notes |
|--------|-------------|-------|
| `COINBASE:BIPZ2030` | BTC nano futures (Dec 2030) | Default chart for BTC preset. |
| `COINBASE:ETPZ2030` | ETH nano futures (Dec 2030) | ETH preset reference chart. |
| `COINBASE:SLPZ2030` | SOL nano futures (Dec 2030) | SOL preset reference chart. |
| `COINBASE:XPPZ2030` | XRP nano futures (Dec 2030) | XRP preset reference chart. |

> Update the preset names if Coinbase rolls the contract (e.g., to `_2031`). The contract monitor will surface expirations ahead of time.

### Historical Enhancer Logging

When TradingView alerts include the `historical_enhancer` field, the agent stores the payload verbatim for downstream analytics. Storage backends:

- **File mode (default):** JSONL files in a configurable directory, auto-pruned based on `HISTORICAL_ENHANCER_RETENTION_DAYS`.
- **Firestore mode:** Set `HISTORICAL_ENHANCER_STORAGE=firestore` to persist entries in the configured collection (defaults to `historical_enhancer`).

Disable logging by omitting the payload from alerts or adjusting the helper in the codebase.

---

## 7. Runtime Overview

1. **Webhook ingress** – Validates payload, resolves strategy, target accounts, and partitions.
2. **Broker routing** – Selects broker client per account (Coinbase Advanced Trade demo/live, Binance, Base DEX).
3. **Position sizing** – Margin-optimized within allocation using API rates (intraday/afterhours by entry time); ADV cap; max capital and per-position caps. Auto-close for afterhours oversize: see §8 and Execution Guarantees.
4. **Execution** – Market order (demo ledger or live broker); tracks legs and deployed cash. Close-opposite-before-opening when entry is opposite direction; coordinated with Agent exits (Execution Guarantees). Alerts include source (TradingView webhook / Agent Exit / Afterhours Position Checker) and exit reason.
5. **Agent-level exit monitoring** – 30-second loop over open positions: breakeven, RSI, trailing stop, stop loss, take profit, EOD auto-close, Ichimoku cloud exits. Duplicate-exit prevention and broker position verification (Execution Guarantees). See `docs/AGENT_EXITS_AND_MONITORING_GUIDE.md` for presets.
6. **Post-trade** – Firestore state and trade history updated; Telegram alerts with source and exit reason.
7. **Daily summary** – Scheduler aggregates realized P&L, mark-to-market for open legs, EOD report to Telegram.

---

## 8. Risk Management & Contract Expiry

- **Dynamic position sizing:** Global and per-strategy `capital_allocation_pct` keeps live sizing aligned with TradingView simulations. The Agent uses API margin rates (intraday vs afterhours by entry time). A daily 3:55 PM ET check auto-closes positions that cannot afford afterhours margin (see Execution Guarantees).
- **ADV Cap (Slip Guard):** Enforces a cap on total per-symbol exposure across partitions. When projected exposure exceeds the cap the agent trims or skips orders and logs/announces the adjustment (default 1% ADV; Telegram alerts when capped).
- **Per-partition safeguards:** Configure `min_balance_usd` to auto-disable aggressive slices, or set `enabled: false` on partitions during downtime.
- **Strategy toggles:** Disable strategies in `config.yaml` to prevent routing without redeploying alerts.
- **Contract expiry protection:** The contract monitor logs warnings as nano futures approach expiry (e.g., BIPZ2030). Use the alerts to roll TradingView charts to the new contract and temporarily disable entries before final settlement.
- **Demo vs live parity:** Both modes use the same sizing/guardrails. Demo entries stage trades in the ledger; flipping to LIVE simply points orders at the broker.

Additional safeguards (daily loss halts, liquidation telemetry) are scaffolded but not yet exposed as API endpoints; consult the roadmap below for status.

### Overnight & Weekend Margin Policy (CFM/Derivatives)

- **80/20 default** at 3× (≥20% free margin post-fill). Margin rate selection and auto-close are described in §8 and Execution Guarantees.
- **24/7 trading:** `general.disable_entries_fri_sun: false` allows entries over weekends; margin logic and auto-close protect against overnight margin calls.
- **Weekend entry blocking:** `general.disable_entries_fri_sun: true` blocks new entries Fri 4am PT–Mon 1am PT (exits always allowed); enables scale-to-zero.
- **Scale-to-zero:** Service stays up until all positions close, then scales to zero. Cloud Scheduler wakes Monday 4:00 AM ET. See `docs/features/DEPLOYMENT_SCALE_TO_ZERO.md`.
- **Commission cap** (default 0.04%) and broker retry/verify for blocked exits (see Execution Guarantees).

---

## 9. System Costs (Typical Monthly)

| Component | Cost | Notes |
|-----------|------|-------|
| TradingView Plus | $24.95 | Required for webhook alerts. |
| Google Cloud Run | ~$0.00–$15 | Free tier for scale-to-zero (min-instances: 0). Always-on (min-instances: 1) ≈ $15/mo. |
| Secret Manager | ~$0.18 | First 6 secrets ≈ $0.18/mo; add $0.03 per extra secret. |
| Firestore | ~$0.10–$0.30 | Depends on write volume for demo ledger/history. |
| Coinbase Futures fees | 0.04% per side | Matches strategy backtest/forward test; demo mode can mirror this rate. |
| Artifact Registry | ~$0.50 | Keep latest few source deploy images; add cleanup policy if needed. |
| Cloud Scheduler | ~$0.10 | 6 jobs for scale-to-zero automation and daily EOD reports (negligible cost). |
| Telegram | $0.00 | Bot + chat notifications are free. |

Expected baseline: ~$25–$26 per month with scale-to-zero (dominated by the TradingView subscription). Always-on deployment adds ~$15/mo. Live trading incurs additional broker commissions according to trade volume.

**Cost Optimization:** Enable scale-to-zero (`min-instances: 0`) and weekend entry blocking (`disable_entries_fri_sun: true`) to save 40–50% on Cloud Run costs. See `docs/features/DEPLOYMENT_SCALE_TO_ZERO.md` for setup.

---

## 10. Quick Start

### 10.1 Prerequisites

- TradingView Plus (webhooks enabled).
- Google Cloud project with Billing enabled.
- gcloud CLI authenticated (`gcloud auth login`).
- Coinbase Advanced Trade API credentials (API key name + EdDSA private key). Binance keys are optional.
- Telegram bot + chat ID if you want alerting (optional but recommended).

### 10.2 Secrets (Google Secret Manager)

```bash
# Set your GCP project ID
PROJECT_ID="YOUR_GCP_PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# Coinbase Advanced Trade (required)
gcloud secrets create coinbase-api-key-1 --replication-policy=automatic
gcloud secrets versions add coinbase-api-key-1 --data-file=/path/to/api_key_name.txt

gcloud secrets create coinbase-private-key-1 --replication-policy=automatic
gcloud secrets versions add coinbase-private-key-1 --data-file=/path/to/private_key.pem

# Telegram (optional)
gcloud secrets create telegram-bot-token --replication-policy=automatic
gcloud secrets versions add telegram-bot-token --data-file=/path/to/bot_token.txt

gcloud secrets create telegram-chat-id --replication-policy=automatic
gcloud secrets versions add telegram-chat-id --data-file=/path/to/chat_id.txt
```

> **Tip:** If you plan to enable Binance or another broker, create matching secrets (e.g., `binance-api-key-1`) and reference them in `config.yaml`.

### 10.3 Configure `config.yaml`

Key sections to review:

```yaml
general:
  local_timezone: America/Los_Angeles     # Used for Telegram timestamps & scheduler
  capital_allocation_pct: 80.0           # Default % of partition equity per entry (70% recommended for live 3×)
  max_position_allocation_pct: 100.0    # Cap for total capital deployed per symbol
  min_contract_buffer_pct: 30.0         # Buffer above broker minimums (default 30%)
  disable_entries_fri_sun: true         # Block new entries Fri 4am PT–Mon 1am PT (exits allowed); enables scale-to-zero
  demo_starting_balance: 2000.0         # Starting balance for DEMO accounts (USD)

strategies:
  - id: ichimoku-coinbase-btc-5m
    name: Easy Ichimoku Coinbase BTC 5m
    enabled: true
    sizing:
      capital_alloc_pct: 90.0           # Optional per-strategy override

accounts:
  - id: coinbase-main
    label: Coinbase Main Account
    enabled: true
    mode: DEMO                            # Switch to LIVE after validation
    broker: coinbase_futures
    api_key: ${COINBASE_API_KEY_1}
    api_secret: ${COINBASE_PRIVATE_KEY_1}
    partition_mode: enabled
    partition_style: isolated             # isolated | cooperative
    partition_split: "25/25/25/25"
    partitions:
      - id: "25%-1-3x"
        allocation_pct: 25.0
        leverage: 3
        strategy_ids:
          - ichimoku-coinbase-btc-5m
          - ichimoku-coinbase-eth-5m
          - ichimoku-coinbase-sol-5m
          - ichimoku-coinbase-xrp-5m
      # ... additional partitions as needed
```

> **Tip:** For isolated style, give each partition a single `strategy_id` so the capital stays dedicated to that chart. For cooperative style, list every strategy on each partition so the FIFO queue can pick any open slot.

### Trading Enable/Disable Toggle

Set the global master switch in `config.yaml` when you want to deploy in LIVE mode but ignore entry signals:

```yaml
risk_management:
  max_total_positions: 10
  max_leverage_global: 5
  enable_trading: false   # Pauses all trading after deploy
  max_commission_rate_pct: 0.04   # Block new entries if broker fees >0.04% of notional
```

- Deploy with `enable_trading: false` to run health/balance checks safely. Flip it back to `true` (and redeploy) when you're ready to process webhooks.
- At runtime you can also call the admin endpoints (body `{ "admin_secret": "..." }`):
  - `POST /trading/disable` — pauses trading without touching open positions.
  - `POST /trading/enable` — resumes trading after tests.
  - `POST /kill-switch` — force-closes all positions and disables trading (hard stop).
  - `POST /reconcile/run` — force a broker↔ledger reconcile and rebalance partitions immediately.

### Commission Cap Auto-Halt

Set `risk_management.max_commission_rate_pct` (default `0.04` = 0.04%) to guard against unexpected broker fee hikes.  
If any live fill reports a commission higher than that percentage of the filled notional:

- The agent blocks new entries (exits still run so positions can close normally).
- Telegram posts **"COMMISSION CAP TRIGGERED"** with the observed/allowed rates.
- Clear the cap via `/trading/enable` (or redeploy) once the broker confirms fees are back under the cap.

### 10.4 Deploy to Cloud Run

**Option 1: Using cloudbuild.yaml (Recommended – Scale-to-Zero Enabled)**

```bash
# From the repository root
gcloud builds submit --config cloudbuild.yaml
```

**Option 2: Manual Deployment**

```bash
# From the repository root
gcloud run deploy tradingview-agent \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --min-instances 0 \
  --max-instances 3 \
  --memory 512Mi \
  --cpu 1 \
  --timeout 3600 \
  --update-secrets \
      COINBASE_API_KEY_1=coinbase-api-key-1:latest,\
      COINBASE_PRIVATE_KEY_1=coinbase-private-key-1:latest,\
      TELEGRAM_BOT_TOKEN=telegram-bot-token:latest,\
      TELEGRAM_CHAT_ID=telegram-chat-id:latest

# Retrieve the base URL
AGENT_URL=$(gcloud run services describe tradingview-agent \
  --region us-central1 \
  --format='value(status.url)')
echo "Agent URL: $AGENT_URL"
echo "Webhook endpoint: $AGENT_URL/webhook"
```

**After Deployment: Set Up Cloud Scheduler (Scale-to-Zero)**

```bash
./setup_cloud_scheduler.sh
```

This creates Cloud Scheduler jobs for automatic scale-to-zero on weekends and daily EOD reports. See `docs/features/DEPLOYMENT_SCALE_TO_ZERO.md` for details.

> If you do not pass Telegram secrets the notifier will stay disabled and log the rendered messages.

### 10.5 TradingView Alert (single alert per chart)

- **Condition:** `Any alert() function call` from your Pine strategy.
- **Webhook URL:** `https://YOUR-AGENT-URL.run.app/webhook`
- **Message:** leave blank when your Pine script calls `alert()` with payloads.

Typical Pine alert body:

```pine
alert(
  '{"strategy_id":"ichimoku-coinbase-eth-5m","side":"{{strategy.order.action}}","price":{{close}},"symbol":"{{ticker}}","bar_time":"{{time}}"}'
)
```

---

## 11. Webhook Payload Contract

`models/webhook_payload.WebhookPayload` accepts:

| Field | Required | Notes |
|-------|----------|-------|
| `strategy_id` | Yes | Maps to entries in `strategies` section of `config.yaml`. |
| `side` | Yes | Accepts `buy`, `sell`, `exit` (aliases: long/short/close). |
| `price` | Optional | Used for logging / validation (no pricing decisions are made). |
| `symbol` | Optional | Used in alerts when supplied. |
| `bar_time`, `timestamp`, `cid`, `client_tag` | Optional | Metadata (improves idempotency). |
| `leverage`, `qty_pct`, `qty_usdt`, `qty_units` | Optional | Overrides; defaults come from the partition & strategy config. |
| `tp_pct`, `sl_pct`, `tp_price`, `sl_price` | Optional | Currently logged only (no automatic TP/SL placement in demo mode). |
| `historical_enhancer` | Optional | Logged verbatim for research (no enrichment at runtime). |

Payloads that omit required fields return HTTP 400 with validation details.

---

## 12. REST Endpoints (core)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Service heartbeat (version + timestamp). |
| `GET` | `/health` | Health: strategies, accounts, partition summary, heartbeat. |
| `POST` | `/webhook` | TradingView alert entry point. |
| `GET` | `/demo/balance/{account_id}` | Demo balance, P&L, drawdown. |
| `GET` | `/demo/positions/{account_id}` | Demo open positions. |
| `POST` | `/demo/reset/{account_id}` | Reset demo account (partition ledger unchanged). |
| `GET` | `/pnl/accounts` | Realized P&L by strategy + account (Firestore). |
| `GET` | `/scale-check` | Scale-to-zero eligibility (open positions, weekend). Used by Cloud Scheduler. |
| `GET` | `/eod/trigger` | Trigger EOD summary (Cloud Scheduler; requires `admin_secret`). |

Admin/runtime: `POST /trading/disable`, `POST /trading/enable`, `POST /kill-switch`, `POST /reconcile/run`. Full endpoint list: see `docs/TRADINGVIEW_AGENT_GUIDE.md` and `docs/AGENT_FEATURES_GUIDE.md`.

---

## 13. Supported Strategies & Presets

- **Easy Ichimoku v14 (Coinbase nano futures, 5m & 1h):** Production-ready presets for BTC, ETH, SOL, XRP. Alert payloads include `strategy_id` and optional `historical_enhancer`. See `docs/strategies/EASY_ICHIMOKU_GUIDE.md` for preset and alert details.
- **Multi-account capability:** A single `strategy_id` can drive multiple accounts (e.g., demo + live, or different leverage partitions) without changing the TradingView alert.
- **Custom strategies:** Any TradingView script that calls `strategy.entry()` / `strategy.close()` and sends JSON via `alert()` can integrate. Map the custom `strategy_id` to a YAML strategy and partitions accordingly.
- **Preset documentation:** `docs/strategies/EASY_ICHIMOKU_GUIDE.md`, `docs/brokers/COINBASE_SETUP_GUIDE.md`, `docs/TRADINGVIEW_STRATEGY_ALERTS_GUIDE.md`.

---

## 14. Testing & Validation Checklist

1. **Deploy in DEMO** – Confirm `/health` returns `status: degraded` only until the first heartbeat; after the broker monitor runs it should flip to `healthy`.
2. **Reset ledger** – `POST /demo/reset/{account_id}` before starting a new test cycle (use the account id from your config, e.g. `coinbase-main`).
3. **Send test webhook** – Trigger TradingView alert or POST manually to `/webhook`; watch Cloud Run logs for partition initialization and execution details.
4. **Verify demo balance** – `GET /demo/balance/{account_id}` should show the starting balance from `general.demo_starting_balance` in config (default **$2000**), then update after exits.
5. **Check capital exhaustion flow** – Fire two entries back to back; second may be skipped with a Telegram note when partition capital is exhausted. ADV cap adjustments scale the fill size and still execute (entries are never blocked by ADV cap).
6. **Validate exit logic** – Ensure `close`/`exit` alerts liquidate every leg for the symbol and release deployed cash.
7. **Inspect Firestore** – Confirm partition documents and trade history persist across redeploys.
8. **Review end-of-day summary** – Daily Telegram report should display correct daily and cumulative numbers. If no trades occurred, expect `+0.00%` placeholders.
9. **Flip to LIVE (when ready)** – Change `mode: LIVE` for the account, redeploy, and run through a limited live test with small size.

---

## 15. Roadmap & Known Gaps

- Expanded REST surface (`/positions`, `/partitions`, `/kill-switch`, `/balances`).
- Automatic stop-loss / take-profit placement for live brokers.
- Risk dashboards (liquidation distance, daily loss halts surfaced via API).
- Artifact Registry cleanup automation.
- Multi-broker orchestration templates for parallel Coinbase + Binance deployment.

Contributions or issue reports are welcome – open a PR or note gaps alongside observed behaviour.

---

## 16. Documentation Map

- `docs/TRADINGVIEW_AGENT_GUIDE.md` – end-to-end deployment + operations manual.
- `docs/SETTINGS.md` – field-by-field explanation of `config.yaml` and environment toggles.
- `docs/AGENT_FEATURES_GUIDE.md` – partition styles, strategy ledger, broker safety loop, and other agent features.
- `docs/AGENT_EXITS_AND_MONITORING_GUIDE.md` – exit monitoring, position sync, and health checks.
- `docs/TRADINGVIEW_STRATEGY_ALERTS_GUIDE.md` – 4-panel layout, presets, and webhook alert setup.
- `docs/AGENT_TELEGRAM_ALERTS_GUIDE.md` – alert examples and formatting notes.
- `docs/features/DEPLOYMENT_SCALE_TO_ZERO.md` – scale-to-zero setup, weekend entry blocking, Cloud Scheduler jobs, cost savings.
- `docs/features/VIRTUAL_PARTITIONS_COMPLETE.md` – virtual partitions: two-layer balance, sync, liquidation protection.
- `docs/features/SMALL_ACCOUNT_TRADING_GUIDE.md` – min-contract buffer, partition-aware skips, scaling from small wallets.
- `docs/brokers/COINBASE_SETUP_GUIDE.md` – Coinbase Derivatives setup, product IDs, contract minimums, presets.
- `docs/brokers/MARGIN_RISK_AND_LEVERAGE_GUIDE.md` – intraday vs overnight margin, buffer policy, guardrails.
- `docs/brokers/*.md` – other broker-specific setup (Binance, etc.).
