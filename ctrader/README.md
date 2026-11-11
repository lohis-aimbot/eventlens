# EventLens cTrader Bridges (`ctrader/`)

This directory contains the C# / `.NET` cBots and plugins used to connect EventLens to **Demo-only** cTrader accounts.

> **Hard Safety Boundary**: Every component in this directory enforces `!Account.IsLive` and refuses real-money Live accounts. Strategy sizing, evidence gates, cooldowns, and risk limits remain governed by EventLens's Python `DeterministicHardRiskEngine` (`src/eventlens/trading/risk.py`).
>
> **Offline Build Honesty**: During local macOS audit, neither `.NET 8 SDK` nor Docker was installed/executed locally. Cloud r1/r2 builds verified `CS0104` (`System.IO.File` alias) and `CT0111` (`[Parameter] string ExpectedDemoAccountNumber` parsed strictly via `long.TryParse` with `NumberStyles.None`), and identified `CT0004: Target framework "net8.0" is not supported` in Spotware's `cTrader.Automate 1.0.21` packager (NuGet's web "computed compatible" badge does not imply `.algo` packager support). `EventLensCliBridge.csproj` now targets Spotware-supported **`<TargetFramework>net6.0</TargetFramework>`** (compiled inside `mcr.microsoft.com/dotnet/sdk:8.0` with `cTrader.Automate` pinned to `[1.0.21]`, `RestorePackagesWithLockFile=true`, `AlgoBuild=true`, `AlgoPublish=false`), **`net8.0` cBot target was NOT built successfully, and `net6.0` `EventLensCliBridge.algo` + `packages.lock.json` + air-gapped `metadata` verification await cloud re-compilation (`待云端复编译产出 net6.0 .algo、锁文件与断网 metadata 核验`)**.

## Components

| Path | Target Runtime | Purpose | Order Execution |
|---|---|---|---|
| `EventLensCliBridge/` (`EventLensCliBridge.csproj`, `EventLensCliBridge.cs`) | **Headless Linux Docker** (`TargetFramework=net6.0` `.algo` built via `.NET 8 SDK`, executed inside `ghcr.io/spotware/ctrader-console:latest`, `linux/amd64`) | 24×5 rotating `quotes-<session>.jsonl` stream (`RotationIntervalSeconds=43200`), session-scoped `(bridge_session_id, event_seq)` `reconcile_snapshot` writer, and opt-in `order-intents.jsonl` → `execution-events.jsonl` Demo execution daemon | **Opt-in Demo only** (`AllowDemoOrders=true` + Python `--ctrader-cli-demo-orders --confirm-demo-execution`); strictly locked to `!Account.IsLive`, `ExpectedDemoAccountNumber`, and `ExpectedAccountCurrency=USD` |
| `EventLensQuoteBridge.cs` | Local cTrader Desktop (`macOS` / `Windows`) | Bounded `10–86400s` local `EURUSD` quote bridge writing `quotes-<session>.jsonl` | Read-only (`AccessRights.FullAccess` for local directory writes only; zero trading calls) |
| `EventLensQuoteBridgePlugin.cs` | Local cTrader Desktop Plugin | Local desktop plugin variant for interactive quote bridge inspection | Read-only |
| `EventLensTickProbe.cs` | Local cTrader Desktop | Diagnostic symbol/tick/commission specification probe | Read-only |

---

## Building `EventLensCliBridge.algo` (`net6.0`) on Linux (`x86_64`)

No host `.NET` SDK installation is needed on Ubuntu 24.04 LTS. `deploy/gcp/build_ctrader_algo.sh` compiles `<TargetFramework>net6.0</TargetFramework>` inside **Microsoft's official `.NET 8 SDK` container (`mcr.microsoft.com/dotnet/sdk:8.0`, `linux/amd64`; cloud-verified `SDK 8.0.428`)** as a non-root UID using dedicated `DOTNET_CLI_HOME`, `NUGET_PACKAGES`, `NUGET_HTTP_CACHE_PATH`, and `XDG_*` directories under `/tmp/build-work` (**without redefining `HOME`**) so `/src` stays read-only (`:ro`), with `cTrader.Automate` pinned explicitly to `[1.0.21]`, `RestorePackagesWithLockFile=true`, `AlgoBuild=true`, `AlgoPublish=false`, and `Deterministic=true`, followed by an air-gapped (`--network none`) `ctrader-console metadata /algo/EventLensCliBridge.algo` parameter check:

```bash
sudo bash /opt/eventlens/deploy/gcp/build_ctrader_algo.sh
# Produces: /var/lib/eventlens/algo/EventLensCliBridge.algo, packages.lock.json, and EventLensCliBridge.metadata.txt
```

---

## `cTrader CLI` Capability & Verification Matrix

| Capability / Constraint | Status | Verification Basis & Operational Notes |
|---|---|---|
| `net6.0` `.algo` build via `mcr.microsoft.com/dotnet/sdk:8.0` (`SDK 8.0.428`, `cTrader.Automate` `[1.0.21]`, `AlgoPublish=false`, `packages.lock.json`, air-gapped `metadata`) | **Script Prepared — Uncompiled Locally (`本地未编译 / 待云端 net6.0 复编译、锁包与断网 metadata 核验`)** | `cTrader.Automate 1.0.21` packager rejects `net8.0` (`CT0004`); `.csproj` targets `net6.0`, isolates via `DOTNET_CLI_HOME` + `NUGET_PACKAGES` + `XDG_*` (never overrides `HOME`), and runs `--network none` `ctrader-console metadata` |
| Official container image `ghcr.io/spotware/ctrader-console:latest` (`cTrader CLI 5.10.6.0`: `Config.User == ""` vs. internal `app` `1654:1654`) | **Offline `--version` Verified — Real Account Non-Root Unverified (`断网 --version 已测 / 真实账户非 root 未验收`)** | Upstream `Config.User` is empty (`""`, defaults to container `root`); image contains user `app` (`1654:1654`, `/home/app` `0750`), and `--user 1654:1654` **without overriding `HOME`** passed offline `--version` on the GCP VM. However, real Demo account execution (`run ... --pwd-file` + `/bridge` I/O) as non-root is **not** yet verified/accepted; default Compose leaves `user:` unset and `/etc/eventlens/ctrader-cli.pwd` stays `root:root` `0600` (zero ACLs) |
| Session-scoped `(bridge_session_id, event_seq)` & fresh snapshot gate (`max_snapshot_age_seconds`, `account_number`, `account_currency == "USD"`) | **Implemented & Offline-Tested (`协议与离线单测已验证`)** | `_execEventSeq` starts at `0` on every C# and `SimulatedCTraderCliDaemon` start; Python orders snapshots by append sequence and refuses new risk on missing/stale/mismatched snapshots |
| Mandatory UTC intent expiry (`expires_at`), approved target alignment (`side` + `approved_quantity_eur`), & independent protective close TTL | **Implemented & Offline-Tested (`离线单测已验证`)** | Intents written while `AllowDemoOrders=false` or past `expires_at` are immediately rejected/expired without replay; protective `close_position` uses its own fresh TTL |
| Partial-line-safe JSONL offsets, SQLite durable outbox, two-stage `intent-journal.jsonl`, & `unknown_execution` manual unlock | **Implemented & Offline-Tested (`离线单测已验证`)** | Offsets advance strictly up to `\n`; ambiguous pre/post-broker crashes enter `unknown_execution` + `manual_unlock_required` (`eventlens-shadow demo-unlock --note ...`) and never auto-retry `market_order` |
| Runtime → CLI Demo execution wiring (`CTraderCliDemoExecutionEngine`) & bounded `eventlens-shadow demo-smoke` | **Implemented & Offline-Tested (`离线单测已验证`)** | `PortfolioSnapshot` and `ShadowFill` reflect strictly broker-confirmed fills (`Position.Id`), never simulated Shadow ledger fills |
| Live Spotware `cTrader CLI` container startup, cTID login, real Pepperstone Demo fill/SL attachment, and multi-day reconnects | **Pending Cloud Demo Acceptance (`待云端 Demo 实测验收`)** | Must be executed on the Ubuntu `x86_64` VM following `docs/cloud-24x5-deployment.md` before claiming live cloud Demo execution acceptance |
