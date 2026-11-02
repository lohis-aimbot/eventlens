<p align="center">
  <img src="docs/assets/eventlens-banner.svg" alt="EventLens" width="100%" />
</p>

<p align="center"><strong>Event → Thesis Memory → Position</strong></p>

<p align="center">
  An LLM-powered intraday research agent for <strong>EUR/USD</strong>.<br />
  Interpret macro events. Maintain a thesis. Test decisions in Shadow mode.
</p>

<p align="center">
  <a href="https://github.com/lohis-aimbot/eventlens/actions/workflows/tests.yml"><img src="https://github.com/lohis-aimbot/eventlens/actions/workflows/tests.yml/badge.svg?branch=main" alt="CI checks" /></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/Python-3.11%2B-2563EB?logo=python&amp;logoColor=white" alt="Python 3.11 or later" /></a>
  <a href="#current-status"><img src="https://img.shields.io/badge/Mode-Shadow-0D9488" alt="Shadow research mode" /></a>
</p>

<p align="center">
  <a href="#current-status">Status</a> ·
  <a href="#how-it-works">Architecture</a> ·
  <a href="#quickstart">Quickstart</a> ·
  <a href="#evidence-and-limits">Evidence</a> ·
  <a href="#deployment">Deployment</a> ·
  <a href="#documentation">Docs</a>
</p>

## Current status

EventLens investigates whether event interpretation can support useful intraday decisions **after information delay and trading costs**. It uses existing LLM APIs; it does not train a language model.

| Status | What it means today |
|---|---|
| **Implemented** | News intake, persistent thesis memory, bounded portfolio proposals, deterministic risk, independent protective exits, and a local Shadow ledger |
| **Offline verified** | Automated checks for timing, recovery, quote quality, risk limits, Demo account locks, and protocol handling |
| **Observed live** | Bounded local news, model, and Demo-quote Shadow runs; selected records are summarized below |
| **Pending acceptance** | Multi-day unattended cloud operation and reproducible strategy evaluation after costs |
| **Unavailable** | Broker order mirroring and real-money trading. Demo quotes are supported; broker order submission is blocked |

> [!IMPORTANT]
> **Shadow fills are local simulations, not broker fills.** Model quality, passing tests, and individual profitable observations do not establish a profitable strategy. Demo mirroring remains disabled until position-aware closing, confirmed fills, and durable reconciliation are implemented and verified.

## How it works

<p align="center">
  <img src="docs/assets/eventlens-pipeline.svg" alt="Events feed an LLM and persistent thesis memory; target positions pass through hard risk into a Shadow ledger." width="100%" />
</p>

1. **Observe.** Capture macro news and Demo Bid/Ask quotes. Preserve publication, receipt, and availability times; missing consensus stays missing.
2. **Reason.** The LLM interprets an event, updates persistent thesis memory, and proposes a target position. Bounded reviews can revisit a thesis when its price or time conditions trigger.
3. **Govern.** Deterministic rules check exposure, quote freshness, spread, source health, and model-call budgets. Protective exits operate independently of model availability.
4. **Record.** The Shadow ledger simulates fills and costs. Events, thesis revisions, decisions, rejections, and fills remain traceable.

The LLM proposes; Python risk rules retain the final veto. Historical replay uses explicit data-resolution and timing assumptions and is kept separate from live observation.

## Quickstart

Requires **Python 3.11+**. Installation needs internet access. The two examples below run offline with no API keys:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

eventlens observe demo examples/synthetic-observer.json
eventlens research decision check examples/synthetic-position-proposal.json
```

<details>
<summary><strong>Optional: inspect live sources and upcoming macro releases</strong></summary>

These commands access public websites. Model calls and cTrader access require separate configuration.

```bash
# Upcoming EUR/USD macro windows, shown in UTC and Beijing time
eventlens observe schedule --hours 36

# One live capture across the configured Tier-1 sources
eventlens observe capture

# Inspect previously captured records locally
eventlens observe report
```

For offline calendar inspection, use `eventlens observe schedule --offline` with an existing observer database.

</details>

## Evidence and limits

| Evidence | Recorded result | What it establishes |
|---|---|---|
| **Offline checks · Nov 2, 2026** | 360 tests; Ruff and mypy checks | Verification of tested paths, including advancing-clock reviews and rejection of broker mirroring |
| **Live Shadow · Oct 23, 2026** | 1,786 quotes, 6 LLM cycles, 2 Shadow entries | Observed receipt-to-Shadow-fill times of 4.97 s and 14.15 s in one EUR/USD Flash PMI window |
| **Live Shadow · Oct 27, 2026** | 2,240 s observation, 3,517 accepted quotes, 10,761 protection checks | Bounded recovery and a recorded `flat_completed` Shadow drain |
| **Failure observation · Oct 29, 2026** | RSS cache `Age: 313s` and a quote-bridge outage | Real source and quote-availability failures; an HTTP response alone does not prove fresh information |
| **Historical FOMC study** | 8 episodes; 3/8 valid proposals, all flat | Schema, memory-continuity, and pause/resume checks; no evidence of trading alpha |

**Evidence boundaries:**

- Real captures remain in local, gitignored databases; they are summarized in the [acceptance report](docs/shadow-acceptance.md), not distributed as a public reproducibility dataset.
- The Oct 28 one-minute Yahoo-bar replay is [explicitly quarantined](docs/replay-errata-2026-10-28.md). It cannot establish live execution latency or second-level tradeability.
- Continuous operation and cloud deployment have code and offline checks. **A multi-day unattended forward run is still pending.**
- After-cost profitability, robustness across market regimes, and reliable broker execution remain unproven.

## Deployment

The repository includes an Ubuntu/systemd deployment bundle and a headless, Demo-only cTrader quote adapter. They support continuous Shadow research without a running desktop cTrader application.

**Deployment support is implemented; sustained cloud operation is not yet accepted.** The service runs Shadow accounting only. `--ctrader-demo-mirror-orders` is rejected before startup.

See the [cloud setup and operating guide](docs/cloud-24x5-deployment.md) for credentials, token storage, weekend handling, budgets, and shutdown behavior.

## Documentation

| Guide | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Module interfaces, data flow, and risk boundaries |
| [Observer and source timing](docs/observer.md) | Collection, publication clocks, and feed health |
| [Quotes and costs](docs/live-quotes.md) | Bid/Ask quality, commission provenance, and causal alignment |
| [Shadow acceptance](docs/shadow-acceptance.md) | Recorded observations, failures, and verification limits |
| [Roadmap](docs/roadmap.md) | Research directions and acceptance gates |
| [Web overview source](docs/index.html) | Bilingual HTML page; open locally. GitHub Pages is not configured |

<details>
<summary><strong>Repository layout and developer checks</strong></summary>

```text
eventlens/
├── src/eventlens/
│   ├── sources/           News and data adapters
│   ├── observer.py        Raw capture, revisions, source health
│   ├── ctrader_openapi.py Demo quote transport; broker execution blocked
│   ├── quotes.py          Quote storage, as-of alignment, quality checks
│   ├── sqlite_memory.py   Durable thesis memory and revisions
│   ├── runtime.py         Event loop, thesis reviews, independent protection
│   ├── trading/           LLM proposals, hard risk, Shadow accounting
│   └── research/          Historical replay and diagnostics
├── tests/                 Offline unit, integration, and protocol checks
├── configs/               Research assumption manifests
├── deploy/gcp/            Ubuntu/systemd deployment bundle
└── docs/                  Technical guides and vector assets
```

```bash
pytest -q
ruff check .
ruff format --check .
mypy src scripts
```

</details>
