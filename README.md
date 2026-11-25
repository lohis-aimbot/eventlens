<p align="center">
  <img src="docs/assets/eventlens-banner.svg" alt="EventLens — From macro events to auditable intraday decisions" width="100%" />
</p>

<p align="center">
  <a href="https://github.com/lohis-aimbot/eventlens/actions/workflows/tests.yml"><img src="https://github.com/lohis-aimbot/eventlens/actions/workflows/tests.yml/badge.svg?branch=main" alt="CI Checks" /></a>
  <img src="https://img.shields.io/badge/Python-3.11%2B-4D6BFE?style=flat-square&logo=python&logoColor=white" alt="Python 3.11+" />
  <img src="https://img.shields.io/badge/Tests-148_Passing-10b981?style=flat-square" alt="148 Passing Tests" />
  <a href="https://lohis-aimbot.github.io/eventlens/dashboard/"><img src="https://img.shields.io/badge/Web_Dashboard-Live_PWA-2563eb?style=flat-square" alt="Live Web Dashboard" /></a>
  <a href="docs/blueprint-v2.md"><img src="https://img.shields.io/badge/Blueprint-v2_Stage_A-6366f1?style=flat-square" alt="Commander Blueprint v2" /></a>
  <img src="https://img.shields.io/badge/Execution-Demo_Only_Locked-0f172a?style=flat-square" alt="Demo-Only Locked" />
</p>

<p align="center">
  <strong>English</strong> | <a href="README_CN.md">简体中文</a> &nbsp;·&nbsp;
  <a href="https://lohis-aimbot.github.io/eventlens/dashboard/"><strong>Live Web Dashboard ↗</strong></a> &nbsp;·&nbsp;
  <a href="docs/blueprint-v2.md">Blueprint v2</a> &nbsp;·&nbsp;
  <a href="https://lohis-aimbot.github.io/eventlens/">Visual Overview ↗</a>
</p>

<p align="center">
  <a href="#1-introduction">Introduction</a> ·
  <a href="#2-system-architecture--safety-boundaries">Architecture</a> ·
  <a href="#3-empirical-evaluation--live-telemetry">Empirical Evaluation</a> ·
  <a href="#4-next-phase-research-blueprint-v2">Blueprint v2</a> ·
  <a href="#5-live-bilingual-web-dashboard">Web Dashboard</a> ·
  <a href="#6-quickstart--reproducible-verification">Quickstart</a> ·
  <a href="#7-repository-map--documentation">Repository</a>
</p>

---

## News & Research Log

- **`[2026/11/25]`** Published **[Commander Blueprint v2](docs/blueprint-v2.md)** ([中文版](docs/blueprint-v2_CN.md)), launching **Stage A**: an offline Colab Quant & Reward Lab on 14+ days of continuous real cloud EUR/USD ticks & Tier-1 macro news, an interactive desktop/mobile `Chart Lab`, and a **Gemini vs. DeepSeek** out-of-sample benchmark.
- **`[2026/11/24]`** Completed forensic autopsy of the Nov 11–24 Pepperstone Demo run (`6 trades, 0W/6L, -11.62 USD`). Activated `--kill-switch` on the GCP VM to halt unvalidated Demo entries while continuing uninterrupted `24×5` quote & macro feed capture.
- **`[2026/11/19]`** Released `single-pass-agent-v4.1`, `tier1-eurusd-filter-v2` (cutting daily LLM evaluations from `80/day` to `~24–27/day`), tighter `8.0p/6.0p` trailing profit locks, NY pre-close entry cutoffs, and a redesigned main-screen Web Dashboard with inline CoT reasoning drawers.
- **`[2026/11/11]`** Built headless `.NET 6` `EventLensCliBridge.algo` on Ubuntu 24.04 (`cTrader.Automate 1.0.21`), passed disconnected metadata checks, `demo-reconcile`, and bounded `1,000 EUR` Pepperstone Demo `demo-smoke`, entering `24×5` cloud observation.

---

## 1. Introduction

**EventLens** is a neuro-symbolic, empirical intraday macro trading & research system focused on **EUR/USD**, organized around one auditable pipeline:

> **Macro Event (Arrival-Clocked) → Persistent Thesis Memory → Deterministic Risk Gate → Shadow / Demo Execution**

Instead of treating an LLM as an unconstrained black-box trader, EventLens enforces a strict architectural separation between **semantic reasoning** and **deterministic execution safety**:

- **Zero Credential Custody:** The LLM (`DeepSeek` today, benchmarked against `Gemini` in Stage A) only receives structured context and outputs bounded JSON proposals—it never touches broker keys or raw order endpoints.
- **Deterministic Risk & Exit Authority:** Pure Python guards (`DeterministicHardRiskEngine`) govern position sizing, spread/staleness checks, cross-day loss baselines, `--kill-switch` enforcement, and model-free pip SL/TP/trailing/timeout exits.
- **Anti-Lookahead Data Integrity:** Every Tier-1 headline and broker tick preserves distinct `source_at`, `published_at`, `received_at`, and `available_at` UTC clocks in SQLite, preventing inadvertent future-information leakage during replay or evaluation.
- **Scientific Accountability:** Forward failures are quarantined, audited trade-by-trade (`MFE`/`MAE` + post-entry drift), and resolved through out-of-sample offline evaluation rather than unverified parameter guesswork.

---

## 2. System Architecture & Safety Boundaries

```mermaid
flowchart LR
    subgraph LiveRuntime["Live 24x5 Cloud Runtime (GCP Ubuntu 24.04 + Docker)"]
        News["Tier-1 English Feeds<br/>(investingLive, ECB, Fed, BEA, FF)"] --> Filter["tier1-eurusd-filter-v2<br/>Dedup & Multi-Event Bundler"]
        Bridge["EventLensCliBridge (.NET 6)<br/>Pepperstone Demo Bid/Ask Quotes"] --> Quotes["QuoteStore<br/>Clock & Razor Spread Gate"]
        Filter --> Agent["SinglePassTradingAgent v4.1<br/>+ SQLite Thesis Memory"]
        Quotes --> Agent
        Agent --> Risk["DeterministicHardRiskEngine<br/>Sizing · Loss Floor · --kill-switch"]
        Risk --> Ledger["SQLiteShadowLedger /<br/>CLI Demo Execution (!IsLive)"]
        Ledger --> Dash["Sanitized Snapshot Sync -> GitHub Pages"]
    end

    subgraph StageALab["Stage A Offline Research Feedback Loop"]
        Ledger --> Export["14+ Day Read-Only<br/>SQLite Research Export"]
        Export --> Lab["Chart Lab (Human + 3-Tier Labels)<br/>& Colab Net-R Reward Lab"]
        Lab -->|"Out-of-Sample Validated Policy"| Agent
    end
```

### Subsystem Breakdown

| Layer | Canonical Modules | Implemented Responsibilities & Safeguards |
|---|---|---|
| **Information Intake** | `src/eventlens/sources/english_tier1.py`, `src/eventlens/observer.py`, `src/eventlens/latency.py` | Captures Tier-1 English macro feeds; `tier1-eurusd-filter-v2` drops session roundups, option expiries, and non-G2 noise; tracks 4D feed health and strict arrival clocks without fabricating missing release timestamps |
| **LLM & Thesis Memory** | `src/eventlens/trading/agent.py`, `src/eventlens/deepseek.py`, `src/eventlens/memory.py`, `src/eventlens/sqlite_memory.py` | Executes `single-pass-agent-v4.1` structured JSON proposals (`IGNORE_EVENT`, `HOLD`, `SET_TARGET`, `CLOSE`) backed by immutable SQLite evidence/thesis revisions and idempotent event-bundle crash recovery |
| **Quotes & Cost Policy** | `src/eventlens/quotes.py`, `src/eventlens/sources/ctrader_demo.py`, `ctrader/EventLensCliBridge/` | Streams `12h`-rotated Pepperstone Demo EUR/USD ticks from a containerized `.NET 6` cBot (`--exit-on-stop`); enforces `QuoteQualityPolicy` freshness checks and `UsdPerOneLot` / `UsdPerMillionUsdVolume` Razor commission accounting |
| **Hard Risk & Execution** | `src/eventlens/trading/risk.py`, `src/eventlens/trading/portfolio.py`, `src/eventlens/trading/execution.py`, `src/eventlens/trading/ctrader_cli_broker.py` | Enforces `q`-scaled risk budgets, conservative cross-day `demo_daily_state` loss guards, FX weekend sleep, NY pre-close entry cutoffs, `--kill-switch`, `ClosingDealId`-verified Demo settlement, and hard `!Account.IsLive` blocks |
| **Runtime & Web UI** | `src/eventlens/runtime.py`, `src/eventlens/shadow_cli.py`, `src/eventlens/dashboard.py`, `docs/dashboard/index.html` | Single-process `ShadowRunner`, model-free protective exit monitoring, read-only SQLite diagnostics (`?mode=ro`), and bilingual PWA dashboard synchronization |

---

## 3. Empirical Evaluation & Live Telemetry

EventLens treats **unfiltered forward telemetry** as first-class scientific evidence—both what succeeded in engineering and what failed in strategy expectancy.

### Nov 11 – Nov 24, 2026 Live Pepperstone Demo Autopsy (`200 USD / 200x` Razor)

- **Cumulative Result:** Equity moved from **`176.10 USD` → `164.48 USD`** (`-6.6%`, `24.48 USD` above the `140 USD` hard floor); **`6` trades (`0W / 6L`, `0%` win rate)**; net PnL **`-11.62 USD`** (gross `-10.90 USD`, commissions `0.72 USD`), avg `-9.1 pips`.
- **Pre-Filter Noise Reduction (`v4.1`):** Cut daily LLM calls from **`80/day`** (hitting budget cap) down to **`~24–27/day`** (blocking 48 non-G2/single-stock headlines, 37 rapid revisions, 19 empty deltas, and 17 wraps).

| Date | Ver | Side | Trigger & Session | Net Pips / Exit | MFE / MAE | 1h / 4h / 12h Drift | Empirical Root Cause |
|---|---|---|---|---|---|---|---|
| `11-13` | `v3.2` | Short | Fed Collins | `-6.7p` (`15m` exit) | `+0.2p` / `-8.3p` | `-5.7p / +1.8p / +5.4p` | Chased `30m` `-20.3p` local floor |
| `11-18` | `v4.0` | Short | European news wrap | `-14.0p` (`14p` SL) | `+6.1p` / `-13.9p` | `-13.1p / -2.8p / -14.5p` | Session-wrap noise |
| `11-19` | `v4.0` | Short | FOMC Minutes | `-3.7p` (NY cutoff) | **`+8.5p`** / `-6.6p` | `+5.5p / -2.1p / +0.9p` | `22p` trailing trigger too wide |
| `11-19` | `v4.0` | Short | NYT Iran strikes (`03:11Z` Asian) | `-14.0p` (`14p` SL) | `+1.0p` / `-13.7p` | `-4.4p / -15.4p / -32.3p` | Asian chase reversed on London open |
| `11-19` | `v4.0` | Long | Philly Fed (`6.3` vs `20.0`) | `-14.1p` (`14p` SL) | **`+8.5p`** / `-13.7p` | `+4.0p / -15.6p / -26.2p` | Tier-2 spike faded vs USD trend |
| `11-24` | `v4.1` | Short | Fed Williams (`23:09Z` Asian) | `-2.0p` (`5h` exit) | `+3.9p` / `-13.0p` | `+1.3p / -2.5p / +4.6p` | Illiquid Asian session |

### Three Scientific & Engineering Takeaways

1. **Payoff-Structure Mismatch:** Maximum Favorable Excursion across all 6 trades stayed inside `[+0.2p, +8.5p]` (zero reached `+10p`), showing `14p SL / 42p TP` brackets were misaligned with actual intraday macro follow-through when lacking `1H/4H/Daily` trend alignment and liquidity-session gating.
2. **Discovered Singleton `thesis_id` Deadlock:** `max_reviews_per_thesis = 3` (`src/eventlens/runtime.py:181,3607`, wired at `src/eventlens/shadow_cli.py:1672`) counts lifetime reviews per `thesis_id`. Because `v4` mapped all watches onto singleton `MARKET_STATE_WATCH_THESIS_ID = "market-state-watch"` (`src/eventlens/trading/agent.py:35`), `step_reviews()` froze after Nov 16—leaving **42 `v4.1` pullback `watch_condition`s** unreviewed (`0` triggers). Held intentionally for bundled deployment after Stage A validation.
3. **Disciplined Risk Halt:** Rather than overfitting live stops on a 6-trade sample, `--kill-switch` was engaged on Nov 24 while keeping `24×5` tick (`~377 MB`) and Tier-1 news (`~2.45 GB`) collection online.

### Epistemic Boundary Matrix

| Verification Tier | Current Evidence |
|---|---|
| **Offline-Verified** | **`148` automated tests** (`pytest -q`), strict `mypy src`, `ruff check .`, and `ruff format --check .` covering Pydantic contracts, risk gates, Razor commission policy, bridge rotation, close-deal race guards, and live-account locks |
| **Live-Observed** | Ubuntu 24.04 GCP `demo-reconcile`, bounded `1,000 EUR` `demo-smoke`, continuous `24×5` EUR/USD tick & Tier-1 macro SQLite accumulation (`Nov 11–present`), and the full 6-trade Nov 11–24 forward evaluation |
| **Active Research (Stage A)** | Offline reverse-engineering of clean `MFE/MAE` opportunities, human K-line labeling (`Chart Lab`), `±60m` bidirectional attribution, `Net-R` reward benchmarking, and **Gemini vs. DeepSeek** out-of-sample trials |
| **Hard-Blocked** | Real-money Live broker endpoints and accounts (`!Account.IsLive` enforced in code & execution bridge) |

---

## 4. Next-Phase Research Blueprint (v2)

| Stage | Core Design & Methodology | Gate / Artifact |
|---|---|---|
| **Stage 0 · Safety Halt** | Activate `--kill-switch` while running `24×5` quote (`live-quotes.sqlite`) and news (`observer.sqlite`) streams | Active (`0` open Demo positions) |
| **Stage 1 · Two-Phase Ladder** | **Phase A:** Empirical discovery & reward benchmark on 14+ day real cloud data; **Phase B:** Scale to 2–5Y macro bars & decouple LLM semantic factors from a Walk-Forward ML/reward policy (`60% / 20% / 20%`) | Phase B unlocked only after positive Phase A out-of-sample alpha |
| **Stage 2 · Dual Labeling & Attribution** | Algorithmic 3-tier markers (`90m >= 10p`, `4h >= 15p`, `8h >= 25p`) + human `Buy/Sell/Avoid` tags; **4 FX sessions**; bidirectional **`-60m` pre-arrival** (front-running/lag) + **`0–15m / 15–60m` post-arrival** news attribution | `Chart Lab` JSON + Colab dataset |
| **Stage 3 · Net-R Reward & Exits** | Standardized net `R` after spread & Razor fees with `>70%` drawdown penalty, miss/avoid shaping, **4 exit templates** (`10/15p`, `14/24p+trail`, `18/36p`, `ATR`) + `30m/1h/4h` drift, evaluated on a **`70% / 30%` OOS split** | Reproducible Colab Reward Lab |
| **Stage 4 · Deliverables & Arena** | Read-only 14-day export pipeline → desktop/mobile `Chart Lab` Tab → **Gemini vs. DeepSeek** blind OOS showdown → coordinated release of validated strategy + `max_reviews_per_thesis` fix | Explicit operator sign-off to lift `--kill-switch` |

Full technical specification → **[docs/blueprint-v2.md](docs/blueprint-v2.md)**

---

## 5. Live Bilingual Web Dashboard

Monitor live equity, open/closed Demo positions, triggering news catalysts, full model Chain-of-Thought (`CoT`) reasoning, and decision audit logs directly from the browser:

- **Hosted Live Dashboard:** **[https://lohis-aimbot.github.io/eventlens/dashboard/](https://lohis-aimbot.github.io/eventlens/dashboard/)**
- **Project Visual Overview:** **[https://lohis-aimbot.github.io/eventlens/](https://lohis-aimbot.github.io/eventlens/)**
- **Capabilities:** Main-screen trade ledger (`#mainTradesSection`), clickable open position hero card, inline 3-tab CoT inspection drawer, daily FX PnL curves, bilingual **`中文 / EN`** toggle, Light/Dark themes, mobile **PWA** install (`manifest.webmanifest`), and upcoming **Tab 3 (`Chart Lab`)** for Stage A K-line annotation.

---

## 6. Quickstart & Reproducible Verification

Requires **Python 3.11+**. All CI verification steps run locally with **no broker credentials or LLM API keys**:

```bash
git clone https://github.com/lohis-aimbot/eventlens.git
cd eventlens

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

# Run full quality & test gate (CI parity: 148 tests)
ruff check .
ruff format --check .
mypy src
pytest -q
python -m build
```

### Primary CLI (`eventlens-shadow`)

```bash
# Inspect all runtime subcommands and risk/strategy options
eventlens-shadow --help
eventlens-shadow start --help

# Launch the local read-only Web Dashboard against local SQLite databases
eventlens-shadow dashboard
```

| Subcommand | Role |
|---|---|
| `start` / `stop` / `recover` | Run the single-process intraday Shadow / opt-in Demo loop, drain cleanly on `SIGTERM`, or reconcile crash state |
| `status` / `report` | Open SQLite databases strictly read-only (`?mode=ro`) to audit equity, decisions, latency, and events |
| `demo-reconcile` / `demo-smoke` / `demo-unlock` | Reconcile `EventLensCliBridge` Demo state, run bounded `1,000–2,000 EUR` smoke verification, or unlock ambiguous states |
| `export-dashboard` / `dashboard` | Export sanitized static snapshots (`dashboard-live`) or serve the local read-only dashboard |

Headless `24×5` Linux provisioning & Docker `.NET 6` bridge runbook → **[docs/cloud-24x5-deployment.md](docs/cloud-24x5-deployment.md)**

---

## 7. Repository Map & Documentation

<details>
<summary><strong>Repository structure (verified 21 source modules &amp; deployment tree)</strong></summary>

```text
eventlens/
├── src/eventlens/
│   ├── contracts.py                 Immutable Pydantic schemas & strict UTC clock checks
│   ├── observer.py                  Tier-1 macro capture, revisions & 4D stream health
│   ├── latency.py                   Clock-lag decomposition utilities
│   ├── quotes.py                    QuoteStore, as-of alignment & Razor spread/cost policy
│   ├── memory.py / sqlite_memory.py Durable SQLite evidence & immutable thesis revisions
│   ├── deepseek.py                  Structured JSON LLM client adapter
│   ├── runtime.py                   IntradayEventRuntime, stateful reviews & ShadowRunner
│   ├── shadow_cli.py                eventlens-shadow CLI entrypoint & subcommands
│   ├── dashboard.py                 Sanitized dashboard snapshot exporter & local server
│   ├── sources/
│   │   ├── english_tier1.py         investingLive, ECB, Fed, BEA, FF & tier1-eurusd-filter-v2
│   │   ├── ctrader_demo.py          Containerized Demo quote tailer & bridge handshake
│   │   └── events.py                Source & filter contracts
│   └── trading/
│       ├── agent.py                 SinglePassTradingAgent (single-pass-agent-v4.1)
│       ├── portfolio.py             Target sizing & q-dependent risk budgets
│       ├── risk.py                  DeterministicHardRiskEngine & --kill-switch
│       ├── execution.py             SQLiteShadowLedger & model-free pip SL/TP/trailing
│       └── ctrader_cli_broker.py    ClosingDealId-verified CLI Demo execution (!Account.IsLive)
├── ctrader/EventLensCliBridge/      .NET 6 cBot (cTrader.Automate 1.0.21) + packages.lock.json
├── deploy/gcp/                      Ubuntu 24.04 setup, Docker compose, watchdog & systemd units
├── docs/
│   ├── blueprint-v2.md / _CN.md     Signed-off Commander Blueprint v2 (Stages 0-4)
│   ├── dashboard/                   Live bilingual PWA Web Dashboard
│   └── research/                    Preflight audits, overnight audits & historical FOMC studies
├── configs/ & notebooks/            Pinned replay manifests & diagnostic notebooks
└── tests/                           148 unit, protocol, integration & cloud-audit tests
```
</details>

### Documentation Index

| Document | Focus |
|---|---|
| **[Commander Blueprint v2 (EN / 中文)](docs/blueprint-v2.md)** | Nov 11–24 live Demo autopsy, Stage A/B roadmap, `Net-R` reward math, exit templates & Chart Lab spec |
| **[Architecture & Module Map](docs/architecture.md)** | Subsystem responsibilities and neuro-symbolic boundaries |
| **[Cloud 24×5 Deployment](docs/cloud-24x5-deployment.md)** | Ubuntu 24.04 `systemd` + Docker `EventLensCliBridge` provisioning and Demo acceptance gates |
| **[Observer & Live Quotes](docs/observer.md)** | Arrival-clock taxonomy, 4D stream health ([acceptance](docs/observer-acceptance.md)), and [quote cost policy](docs/live-quotes.md) |
| **[Memory & Reasoning Setup](docs/memory.md)** | Thesis lifecycle and [safe credential management](docs/reasoning.md) |
| **[Shadow Acceptance & Audits](docs/shadow-acceptance.md)** | Early live Shadow observations, [Nov 4 audit](docs/research/overnight-20261104-shadow-audit-and-prompt-draft.md), and [Nov 11 cloud preflight](docs/research/2026-11-11-cloud-preflight-audit.md) |
| **[Historical Roadmap](docs/roadmap.md)** | Original Stage 0–5 milestones linking to Blueprint v2 |

---

> [!IMPORTANT]
> **Research Boundary & Disclaimer:** EventLens is built strictly for quantitative systems research and educational demonstration. Real-money Live broker endpoints and accounts are hard-blocked in code (`!Account.IsLive`), `--kill-switch` is currently engaged on the cloud VM, and past Demo or offline observations do **not** guarantee future trading profitability.
