# EventLens cTrader Bridges (`ctrader/`)

This directory contains the C# / `.NET` cBots and plugins used to connect EventLens to **Demo-only** cTrader accounts.

> **Hard Safety Boundary**: Every component in this directory enforces `!Account.IsLive` and refuses real-money Live accounts. Strategy sizing, evidence gates, cooldowns, and risk limits remain governed by EventLens's Python `DeterministicHardRiskEngine` (`src/eventlens/trading/risk.py`).
>
> **Offline & Cloud Build Verification**: During local macOS development, neither `.NET 8 SDK` nor Docker was required locally. On the Ubuntu 24.04 GCP VM (building on the [2026-11-11 cloud preflight audit](../docs/research/2026-11-11-cloud-preflight-audit.md)), the final `EventLensCliBridge.cs` (`TargetFramework=net6.0`, including `ClosingDealId` close-deal serialization, `position_closed_unpriced` fallback, `reconcile_snapshot.closed_deals`, and `Timer.Stop()` on safety stop) compiled cleanly (`0 errors / 0 warnings`, `.algo` SHA-256 `24d5b3996d783ccafa91109e3b7d137993897eda86128ee3`) via `mcr.microsoft.com/dotnet/sdk@sha256:1b955f179b02df4f0eb5fadedaa898cbe44d620a64aab52eec72c8e91d1355b5` (`SDK 8.0.428`, `BUILD_UID_GID=0:0`, `cTrader.Automate` locked to `[1.0.21]`), passed air-gapped (`--network none --user 1654:1654`) `metadata` inspection in `ghcr.io/spotware/ctrader-console@sha256:6a339e15ee895de270be4a4e293fdb3f2fcdbecfbbace460105703125742e5ad` (`5.10.6.0`), and passed live Pepperstone Demo `demo-reconcile` (`clean: true`) and bounded `1,000 EUR` `demo-smoke` (`passed: true`). Multi-day `24×5` forward stability remains under evaluation.

## Components

| Path | Target Runtime | Purpose | Order Execution |
|---|---|---|---|
| `EventLensCliBridge/` (`EventLensCliBridge.csproj`, `EventLensCliBridge.cs`, `packages.lock.json`) | **Headless Linux Docker** (`TargetFramework=net6.0` `.algo` built via pinned `.NET 8 SDK`, executed inside pinned `ghcr.io/spotware/ctrader-console@sha256:6a339e15ee895de270be4a4e293fdb3f2fcdbecfbbace460105703125742e5ad`, `linux/amd64`) | 24×5 rotating `quotes-<session>.jsonl` stream (`RotationIntervalSeconds=43200`), session-scoped `(bridge_session_id, event_seq)` `reconcile_snapshot` writer (including `closed_deals`), and opt-in `order-intents.jsonl` → `execution-events.jsonl` Demo execution daemon | **Opt-in Demo only** (`AllowDemoOrders=true` + Python `--ctrader-cli-demo-orders --confirm-demo-execution`); strictly locked to `!Account.IsLive`, `ExpectedDemoAccountNumber`, and `ExpectedAccountCurrency=USD` |
| `EventLensQuoteBridge.cs` | Local cTrader Desktop (`macOS` / `Windows`) | Bounded `10–86400s` local `EURUSD` quote bridge writing `quotes-<session>.jsonl` | Read-only (`AccessRights.FullAccess` for local directory writes only; zero trading calls) |
| `EventLensQuoteBridgePlugin.cs` | Local cTrader Desktop Plugin | Local desktop plugin variant for interactive quote bridge inspection | Read-only |
| `EventLensTickProbe.cs` | Local cTrader Desktop | Diagnostic symbol/tick/commission specification probe | Read-only |

---

## Building `EventLensCliBridge.algo` (`net6.0`) on Linux (`x86_64`)

No host `.NET` SDK installation is needed on Ubuntu 24.04 LTS. `deploy/gcp/build_ctrader_algo.sh` compiles `<TargetFramework>net6.0</TargetFramework>` inside **Microsoft's official `.NET 8 SDK` container (`${DOTNET_SDK_IMAGE:-mcr.microsoft.com/dotnet/sdk@sha256:1b955f179b02df4f0eb5fadedaa898cbe44d620a64aab52eec72c8e91d1355b5}`, `linux/amd64`; cloud-verified `SDK 8.0.428`)** with `BUILD_UID_GID=0:0` by default (deriving an unmapped host `eventlens` UID failed NuGet migration mutex initialization on Ubuntu 24.04 and is not accepted), `/src` mounted read-only (`:ro`), and dedicated `DOTNET_CLI_HOME`, `NUGET_PACKAGES`, `NUGET_HTTP_CACHE_PATH`, and `XDG_*` directories under `/tmp/build-work` (**without redefining `HOME`**), `cTrader.Automate` pinned explicitly to `[1.0.21]` (`packages.lock.json` `--locked-mode`, `AlgoBuild=true`, `AlgoPublish=false`, `Deterministic=true`), followed by an air-gapped (`--network none`, `--user 1654:1654` by default) `ctrader-console metadata /algo/EventLensCliBridge.algo` parameter check and host `chown eventlens:eventlens`:

```bash
sudo bash /opt/eventlens/deploy/gcp/build_ctrader_algo.sh
# Produces: /var/lib/eventlens/algo/EventLensCliBridge.algo, packages.lock.json, and EventLensCliBridge.metadata.txt
```

---

## `cTrader CLI` Capability & Verification Matrix

| Capability / Constraint | Status | Verification Basis & Operational Notes |
|---|---|---|
| `net6.0` `.algo` build via pinned `.NET 8 SDK` (`sha256:1b955f17...`, `SDK 8.0.428`, `BUILD_UID_GID=0:0`, `cTrader.Automate` `[1.0.21]`, `packages.lock.json`, air-gapped `metadata`) | **Cloud-Verified (`云端锁包编译与断网 metadata 已核验`)** | `cTrader.Automate 1.0.21` packager rejects `net8.0` (`CT0004`); `.csproj` targets `net6.0`, defaults `BUILD_UID_GID=0:0` (arbitrary host `eventlens` UID is not accepted), never overrides `HOME`, and runs `--network none --user 1654:1654` `ctrader-console metadata` |
| Official runtime image `ghcr.io/spotware/ctrader-console@sha256:6a339e15...` (`cTrader CLI 5.10.6.0`: env isolation & `--exit-on-stop`) | **Cloud Root Runtime Verified — Non-Root Account Runtime Unverified (`云端默认 root 运行已测 / 非 root 真实账户未验收`)** | Compose omits `env_file:` so `DEEPSEEK_API_KEY` never enters the container, enables `--exit-on-stop` + `init: true` + `restart: on-failure:5` for clean exit on safety `Stop()`, and keeps `/etc/eventlens/ctrader-cli.pwd` `root:root` `0600` (zero ACLs) |
| Session-scoped `(bridge_session_id, event_seq)`, fresh snapshot gate, & conservative cross-day `demo_daily_state` baseline | **Implemented & Offline-Tested (`协议与离线单测已验证`)** | `_execEventSeq` starts at `0` on every C# and `SimulatedCTraderCliDaemon` start; cross-day outages preserve `last_valid_equity_usd` / provisional baselines (`daily_baseline_unconfirmed` / `protective_daily_loss_limit`) |
| Mandatory UTC intent expiry (`expires_at`), approved target alignment (`side` + `approved_quantity_eur`), & independent protective close TTL | **Implemented & Offline-Tested (`离线单测已验证`)** | Intents written while `AllowDemoOrders=false` or past `expires_at` are immediately rejected/expired without replay; protective `close_position` uses its own fresh TTL |
| Partial-line-safe JSONL offsets, SQLite durable outbox, two-stage `intent-journal.jsonl`, & `unknown_execution` manual unlock | **Implemented & Offline-Tested (`离线单测已验证`)** | Offsets advance strictly up to `\n`; ambiguous pre/post-broker crashes enter `unknown_execution` + `manual_unlock_required` (`eventlens-shadow demo-unlock --note ...`) and never auto-retry `market_order` |
| Unique `ClosingDealId` close accounting (`HistoricalTrade`), `position_closed_unpriced` guard, & bounded `eventlens-shadow demo-smoke` | **Offline & Cloud Smoke Verified (`离线单测与云端 Demo 冒烟已验证`)** | Close fills/PnL/fees derive strictly from `closing_deal_id` deal evidence (deduplicated across `close_event` and `reconcile_snapshot.closed_deals`, and race-guarded against pre-close snapshots); missing/delayed `History` records `position_closed_unpriced` without substituting `Bid`/`Ask`/`entry_price`/`0` PnL |
| Live Spotware `cTrader CLI` container startup, cTID login, real Pepperstone Demo `demo-smoke`, and multi-day reconnects | **Smoke Verified — Multi-Day Window In Progress (`单次冒烟已通过 / 多日连续运行观测中`)** | Phase 1 `demo-reconcile` (`clean: true`) and Phase 2 bounded `1,000 EUR` `demo-smoke` (`passed: true`, SL attached, `flat_after_close: true`) passed on Ubuntu 24.04 (`x86_64`); multi-day `24×5` forward evaluation is underway |
