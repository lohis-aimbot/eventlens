# Continuous Cloud Deployment & Demo Acceptance Guide (GCP Ubuntu 24.04)

This guide covers deploying EventLens on an **Ubuntu 24.04 LTS (`x86_64` / `amd64`)** Linux VM for `24×5` unattended operation using **Spotware's official Docker `cTrader CLI` (`ghcr.io/spotware/ctrader-console@sha256:6a339e15ee895de270be4a4e293fdb3f2fcdbecfbbace460105703125742e5ad`)** as the primary headless bridge, with **opt-in position-ID-aware Demo account execution**. Native Python `cTrader Open API` quote streaming remains available as a quote-only alternative.

> **Epistemic Boundary**: Python runtime wiring (`CTraderCliDemoExecutionEngine`), `ClosingDealId`-backed close accounting (`position_closed_unpriced` when `History` is delayed), conservative cross-day `demo_daily_state` baseline guards, `systemd` / Docker templates, deterministic risk gates, durable outbox / crash-recovery state machines, and the `SimulatedCTraderCliDaemon` lifecycle suite are **implemented and offline-verified**. While a prior `net6.0` checkpoint compiled (`0 errors / 0 warnings`) and passed air-gapped `metadata` inspection on the GCP VM ([2026-11-11 cloud preflight audit](research/2026-11-11-cloud-preflight-audit.md)), **the current updated repository snapshot has NOT yet been re-compiled on the cloud VM (`当前更新快照待云端复编译与断网 metadata 核验`), and real Demo account login, `/bridge` IPC permissions, live Pepperstone Demo order fills, and multi-day unattended forward operation remain `pending_cloud_demo_verification`** until Section 5 is completed on the cloud VM.

---

## 1. Architecture & Safety Locks

### Primary Mode: Headless Docker `cTrader CLI` (`EventLensCliBridge`)
- **Two-Stage Container Separation (`deploy/gcp/build_ctrader_algo.sh` + `deploy/gcp/docker-compose.ctrader-cli.yml`)**:
  - **Build stage**: Compiles `EventLensCliBridge.algo` targeting **`<TargetFramework>net6.0</TargetFramework>`** (required by Spotware's `cTrader.Automate 1.0.21` packager, which rejects `net8.0` with `CT0004`) inside Microsoft's official `.NET 8 SDK` container (`${DOTNET_SDK_IMAGE:-mcr.microsoft.com/dotnet/sdk@sha256:1b955f179b02df4f0eb5fadedaa898cbe44d620a64aab52eec72c8e91d1355b5}`, `linux/amd64`; cloud-verified `SDK 8.0.428`) using `BUILD_UID_GID=0:0` by default with `/src` mounted `:ro` and dedicated `DOTNET_CLI_HOME`, `NUGET_PACKAGES`, `NUGET_HTTP_CACHE_PATH`, and `XDG_*` directories under `/tmp/build-work` (**without redefining `HOME`**; deriving an arbitrary unmapped host `eventlens` UID failed NuGet migration mutex initialization on Ubuntu 24.04 and is not accepted; `cTrader.Automate` pinned explicitly to `[1.0.21]`, `RestorePackagesWithLockFile=true`, `--locked-mode`, `AlgoBuild=true`, `AlgoPublish=false`, `Deterministic=true`), then runs an air-gapped (`--network none`, `--user 1654:1654` by default) `ctrader-console metadata /algo/EventLensCliBridge.algo` check and `chown`s exported artifacts (`EventLensCliBridge.algo`, `packages.lock.json`, `EventLensCliBridge.metadata.txt`) to `eventlens:eventlens`.
  - **Runtime stage**: Runs `EventLensCliBridge.algo` (`net6.0`) inside `${CTRADER_CONSOLE_IMAGE:-ghcr.io/spotware/ctrader-console@sha256:6a339e15ee895de270be4a4e293fdb3f2fcdbecfbbace460105703125742e5ad}` (`linux/amd64`; cloud-verified `cTrader CLI 5.10.6.0`, `--full-access --exit-on-stop`, `init: true`, `stop_grace_period: 30s`, `restart: on-failure:5`) with dedicated `DOTNET_CLI_HOME` / `NUGET_PACKAGES` / `XDG_*` directories under `/tmp` (**never overriding `HOME` and never attaching `env_file:`**, keeping `DEEPSEEK_API_KEY` and Open API secrets strictly out of the `ctrader-console` container environment while interpolating `CTRADER_*` CLI flags on the host via `docker compose --env-file /etc/eventlens/eventlens.env`). When `VerifyDemoSafetyOrStop` calls `Timer.Stop()` + `Stop()`, `--exit-on-stop` + `--abort-on-container-exit --exit-code-from ctrader-cli-bridge` + `Restart=on-failure` exit cleanly without hanging or infinite restart loops. Upstream `Config.User` is empty (`""`, defaults to container `root`); while `--user 1654:1654` without overriding `HOME` passed air-gapped `--version` and `metadata` checks on the GCP VM, **real-account execution (`run ... --pwd-file`) as non-root is not yet verified/accepted**, and `/etc/eventlens/ctrader-cli.pwd` remains `root:root` `0600` with zero ACL grants.
- **End-to-End Demo & Risk Governance Locks**:
  1. **C# Account & Currency Lock (`EventLensCliBridge.cs`)**: Parses `[Parameter] string ExpectedDemoAccountNumber` strictly via `long.TryParse` (`NumberStyles.None`, full 64-bit integer precision, never `double`; rejecting malformed strings and requiring `> 0` whenever `AllowDemoOrders=true`), and verifies `!Account.IsLive`, `SymbolName == "EURUSD"`, `(long)Account.Number`, and `ExpectedAccountCurrency` (`USD`) in `OnStart()` and before every order/close call; emits `safety_refusal`, stops `Timer`, and calls `Stop()` on violation.
  2. **Fresh Snapshot, Target-Binding, & Conservative Cross-Day Daily Baseline Gate (`CTraderCliDemoBrokerAdapter` + `CTraderCliDemoExecutionEngine`)**: Selects the latest `reconcile_snapshot` by append sequence across `(bridge_session_id, event_seq)` sessions; `submit_market_order` refuses new risk unless the snapshot is fresh (`<= max_snapshot_age_seconds`), `is_live == False`, `account_number` matches `expected_demo_account_number`, `account_currency == "USD"`, `allow_demo_orders == True`, `assessment.approved == True`, and `(side, volume_units)` strictly match `decision.target` and `assessment.approved_quantity_eur`. `demo_daily_state` persists conservative `last_valid_equity_usd` and provisional (`baseline_valid=0`) baselines across trading-day rollovers, quote/bridge outages, and restarts so post-outage equity drops can never reset the new day's baseline or erase outage losses (`daily_baseline_unconfirmed` / `protective_daily_loss_limit`).
  3. **Confirmed-Fill & Unique `ClosingDealId` Close Accounting (`CTraderCliDemoExecutionEngine`)**: When `--ctrader-cli-demo-orders` is enabled, `IntradayEventRuntime` and `ShadowRunner` wire directly into `CTraderCliDemoExecutionEngine` (never simulating Shadow fills in `SQLiteShadowLedger`). Entry positions require broker `Position.Id`; all closes (intent closes, staged partial closes, multi-deal closes, and autonomous broker SL/TP closes) require verified `HistoricalTrade` rows keyed by unique `closing_deal_id` (`ClosingDealId`, `ClosingTime`, `VolumeInUnits`, `ClosingPrice`, `GrossProfit`, `NetProfit`, `Commissions`, `Swap`), deduplicated across `close_position` `order_event` and `reconcile_snapshot.closed_deals`. Whenever `History` deal evidence is delayed or incomplete, the position is recorded as `position_closed_unpriced` (`pending_close_accounting=True`, `blocks_new_risk=True`) — **never fabricating a confirmed `ShadowFill` and never substituting `entry_price`, live `Bid`/`Ask`, default `1000` units, or `0` PnL**.
  4. **Mandatory UTC Intent Expiry (`expires_at`) & Independent Protective Close**: Every `order-intents.jsonl` frame carries a UTC `expires_at` deadline. Intents arriving while `AllowDemoOrders=false` or after `expires_at` are immediately rejected/expired so toggling `AllowDemoOrders` or restarting never replays stale entries. Protective exits (`close_position`) use their own independent TTL channel.
  5. **Partial-Line Safety, Durable Outbox, & `unknown_execution` Manual Unlock**: JSONL readers in both Python and C# advance offsets strictly up to `\n`-terminated lines. Intents are staged in SQLite outbox before file write, and C# records `in_flight` in `intent-journal.jsonl` before calling the broker. If a crash leaves broker execution ambiguous (`unknown_execution`), new risk is halted with `manual_unlock_required=True` (no automatic `market_order` retry and no fabricated fills) until the operator verifies the Demo account and runs `eventlens-shadow demo-unlock --note "..."`.
- **Ex-Ante Risk Budget & Broker SL Boundary (`G5`)**:
  `eventlens-shadow-cli.service` runs with `--max-risk-per-trade-pct 0.02`, sizing `EUR/USD` quantities `ROUND_DOWN` so `q * stop_distance + spread(q) + slippage(q) + commission(q) <= 2% equity` (`3,000 EUR` / `0.03 lots` on `USD 200` equity with an `11 pip` stop, instead of `4,000 EUR`). Both ex-ante sizing and broker-side stop-loss attachments are **pre-trade controls, not mathematical guarantees against market gaps, weekend jumps, or liquidity-drought slippage**.

### Alternative Quote-Only Mode: Native Python `cTrader Open API` (`src/eventlens/ctrader_openapi.py`)
- Connects directly to `demo.ctraderapi.com:5036` over TLS/TCP for Demo quotes only (`eventlens-shadow.service`). Legacy `--ctrader-demo-mirror-orders` remains hard-blocked (`CTraderDemoExecutionUnavailableError`).

---

## 2. `cTrader CLI` Capability & Verification Matrix

| Item | Status | Notes |
|---|---|---|
| `net6.0` `.algo` build via pinned `.NET 8 SDK` (`sha256:1b955f17...`, `SDK 8.0.428`, `BUILD_UID_GID=0:0`, `deploy/gcp/build_ctrader_algo.sh`) | **Cloud-Verified (`云端锁包编译与断网 metadata 已核验`)** | `cTrader.Automate 1.0.21` packager rejects `net8.0` (`CT0004`); `.csproj` targets `net6.0`, defaults `BUILD_UID_GID=0:0` (arbitrary host `eventlens` UID is not accepted), isolates via `DOTNET_CLI_HOME` + `NUGET_PACKAGES` + `XDG_*` (never overrides `HOME`) + `AlgoPublish=false`, and verifies `EventLensCliBridge.algo` + `packages.lock.json` + `--network none --user 1654:1654` `metadata` |
| `ghcr.io/spotware/ctrader-console@sha256:6a339e15...` (`cTrader CLI 5.10.6.0`) env isolation, `--exit-on-stop`, & non-root boundary | **Cloud Root Runtime Verified — Non-Root Account Runtime Unverified (`云端默认 root 运行已测 / 非 root 真实账户未验收`)** | Compose omits `env_file:` so `DEEPSEEK_API_KEY` never enters `ctrader-console`, sets `--exit-on-stop` + `init: true` + `restart: on-failure:5` (`--abort-on-container-exit` + `Restart=on-failure`) for clean safety-stop exits, and keeps `/etc/eventlens/ctrader-cli.pwd` `root:root` `0600` (zero ACLs) |
| `12h` bridge file rotation & Python `DirectoryTail` eviction (`>4` sessions) | **Offline-Verified** | Tested across `8+` consecutive rotated & crash-superseded files + runner restarts |
| Cross-restart `(bridge_session_id, event_seq)` ordering, snapshot gates, & conservative cross-day `demo_daily_state` baseline | **Offline-Verified** | Simulated daemon resets `_event_seq = 0` on restart matching C#; cross-day outages preserve `last_valid_equity_usd` / provisional baselines (`daily_baseline_unconfirmed` / `protective_daily_loss_limit`) |
| Unique `ClosingDealId` close accounting (`HistoricalTrade`), `position_closed_unpriced`, UTC `expires_at` TTL, & `unknown_execution` unlock | **Offline & Cloud Smoke Verified** | Covered in `tests/test_v0157_data_decision_runtime_and_cli_demo.py` (`377` total tests; delayed `History`, staged partial closes, multi-deal closes, snapshot-vs-`order_event` race guard, autonomous SL/TP, and restart idempotency) and live `demo-smoke` |
| Real GCP container login, live Pepperstone Demo `MarketOrder`/`ClosePosition` RTT, and `24×5` multi-day stability | **Smoke Verified — Multi-Day Window In Progress** | Phase 1 `demo-reconcile` (`clean: true`) and Phase 2 bounded `1,000 EUR` `demo-smoke` (`passed: true`, SL attached, `UsdPerOneLot` Razor metadata & `ClosingDealId` verified, `flat_after_close: true`) passed on Ubuntu 24.04 (`x86_64`); continuous `24×5` evaluation is active |

---

## 3. GCP VM Sizing & Billing Safety Notes

- **Recommended VM**: Compute Engine **`e2-small`** (2 shared vCPUs, 2 GB RAM, `x86_64`) or **`e2-medium`** (2 shared vCPUs, 4 GB RAM, `x86_64` — recommended when running Docker `.NET 8` `ctrader-console` + Python `eventlens-shadow` side-by-side), Ubuntu 24.04 LTS `x86_64`, 20 GB Balanced Persistent Disk, in `us-east4` or `europe-west2`.
- **Firewall**: Outbound TCP `443` (HTTPS news + DeepSeek + GHCR/MCR) and cTrader Demo endpoints; **zero inbound ports needed** beyond SSH.
- **Important GCP Billing Fact**: Standard Google Cloud **Budgets & alerts** send notifications (email / Pub/Sub) only and **do not automatically cap spend or stop Compute Engine VMs** (see https://docs.cloud.google.com/billing/docs/how-to/budgets and https://docs.cloud.google.com/billing/docs/how-to/budgets-spend-caps). If you want automated emergency VM shutdown on budget thresholds, wire Budget Pub/Sub notifications to a Cloud Run / Cloud Functions stopper webhook, and keep `--max-daily-llm-calls 80` enabled for DeepSeek API cost control.

---

## 4. Provisioning & Building on Ubuntu 24.04 (`x86_64`)

1. **Install Docker Engine + Compose plugin** and verify `uname -m` prints `x86_64`. Inspect the Spotware container's `run --help` flags on first pull:
   ```bash
   docker run --rm --platform linux/amd64 ghcr.io/spotware/ctrader-console:latest run --help
   ```
2. **Clone EventLens and run the idempotent host installer** (`setup_ubuntu.sh` locks `/etc/eventlens/ctrader-cli.pwd` to `root:root` mode `0600` with zero ACLs and configures `/var/lib/eventlens/bridge` default ACLs for `eventlens` only):
   ```bash
   git clone <YOUR_REPO_URL> ~/eventlens
   cd ~/eventlens
   sudo bash deploy/gcp/setup_ubuntu.sh
   ```
3. **Compile `EventLensCliBridge.algo` inside Microsoft's `.NET 8 SDK` container**:
   ```bash
   sudo bash /opt/eventlens/deploy/gcp/build_ctrader_algo.sh
   ```
4. **Configure `0600` credentials (never paste secrets into chat)**:
   ```bash
   sudo nano /etc/eventlens/eventlens.env
   # Set DEEPSEEK_API_KEY, CTRADER_CTID, CTRADER_DEMO_ACCOUNT_NUMBER, CTRADER_EXPECTED_ACCOUNT_CURRENCY=USD
   # Keep CTRADER_ALLOW_DEMO_ORDERS=false and EXTRA_SHADOW_ARGS= initially!

   sudo nano /etc/eventlens/ctrader-cli.pwd
   # Put ONLY your single-line cTID password in this file (root:root, mode 0600, no ACLs)
   ```

---

## 5. Step-by-Step Cloud Demo Acceptance Runbook

### Step 5.1 — Phase 1: Read-Only Quote Bridge & Reconciliation Preflight
Start the Docker `cTrader CLI` container in **read-only quote mode** (`CTRADER_ALLOW_DEMO_ORDERS=false`):

```bash
sudo systemctl enable --now eventlens-ctrader-cli
sudo docker logs -f eventlens-ctrader-cli
```

Confirm you see `EVENTLENS_CLI_BRIDGE|started|bridge_session=...|quote_session=...|symbol=EURUSD|demo_orders=disabled|expected_account=...|rotation_s=43200` and fresh `quotes-*.jsonl` files in `/var/lib/eventlens/bridge/`. Then run the read-only Demo reconciliation preflight:

```bash
sudo -u eventlens /opt/eventlens/.venv/bin/eventlens-shadow demo-reconcile \
  --quote-bridge-dir /var/lib/eventlens/bridge \
  --shadow-db /var/lib/eventlens/data/shadow-ledger.sqlite
```

Verify output shows `"blocks_new_risk": true` ONLY for `"bridge_demo_orders_disabled"` (because `CTRADER_ALLOW_DEMO_ORDERS=false`), `"account_currency": "USD"`, `"open_positions": []`, and `"manual_unlock_required": false`.

### Step 5.2 — Phase 2: Bounded Model-Free `demo-smoke` & Supervised Demo Execution (Opt-In)
Only after Phase 1 runs cleanly with zero unexpected positions on your Pepperstone Demo account:

1. Enable Demo orders on the bridge first (`CTRADER_ALLOW_DEMO_ORDERS=true` in `/etc/eventlens/eventlens.env`, keeping `eventlens-shadow-cli` stopped or `EXTRA_SHADOW_ARGS=` empty) and restart `eventlens-ctrader-cli`:
   ```bash
   sudo systemctl restart eventlens-ctrader-cli
   ```
2. Run the **bounded, model-free `demo-smoke` command** (`1,000 EUR` open → broker `Position.Id` + `sl_confirmed: true` → close by `Position.Id` → flat reconciliation, strictly gated by `DeterministicHardRiskEngine` with zero paid LLM calls):
   ```bash
   sudo -u eventlens /opt/eventlens/.venv/bin/eventlens-shadow demo-smoke \
     --quote-bridge-dir /var/lib/eventlens/bridge \
     --shadow-db /var/lib/eventlens/data/shadow-ledger.sqlite \
     --quotes-db /var/lib/eventlens/data/live-quotes.sqlite \
     --expected-demo-account-number <YOUR_DEMO_ACCOUNT_NUMBER> \
     --confirm-demo-execution \
     --volume-units 1000
   ```
   Verify `"passed": true`, `"sl_confirmed": true`, and `"flat_after_close": true`.
3. Once `demo-smoke` passes, opt in `eventlens-shadow-cli.service` by setting in `/etc/eventlens/eventlens.env`:
   ```dotenv
   CTRADER_ALLOW_DEMO_ORDERS=true
   EXTRA_SHADOW_ARGS=--ctrader-cli-demo-orders --confirm-demo-execution
   ```
   and restarting `sudo systemctl restart eventlens-shadow-cli`. (Note: `eventlens-shadow start --ctrader-cli-demo-orders` requires `--call-model` and refuses to start silently without a decision source.)
4. If an unexpected process/container crash ever occurs mid-order and leaves an intent in `unknown_execution` (`manual_unlock_required: true`), inspect the Demo account in cTrader UI + `demo-reconcile`, resolve any orphan Demo position if present, and unlock explicitly:
   ```bash
   sudo -u eventlens /opt/eventlens/.venv/bin/eventlens-shadow demo-unlock \
     --quote-bridge-dir /var/lib/eventlens/bridge \
     --shadow-db /var/lib/eventlens/data/shadow-ledger.sqlite \
     --note "Verified Pepperstone Demo flat after container restart"
   ```

### Step 5.3 — Rollback to Shadow-Only Mode
To return immediately to zero-order Shadow mode at any time:
1. Set `CTRADER_ALLOW_DEMO_ORDERS=false` and `EXTRA_SHADOW_ARGS=` in `/etc/eventlens/eventlens.env`.
2. Run `sudo systemctl restart eventlens-ctrader-cli eventlens-shadow-cli`.
3. Confirm via `demo-reconcile` or cTrader UI that `open_positions` is empty — disabling order submission never auto-closes pre-existing broker positions on its own.

### Step 5.4 — Cloud Demo Acceptance Criteria & Evidence Checklist
All items below must be recorded from the cloud `x86_64` VM before marking Cloud Demo execution accepted:

1. **Phase 0 — Deterministic `net6.0` `.algo`, Lockfile, & Air-Gapped `metadata` Build**:
   - `deploy/gcp/build_ctrader_algo.sh` exits `0` under `mcr.microsoft.com/dotnet/sdk:8.0` (`SDK 8.0.428`) targeting `<TargetFramework>net6.0</TargetFramework>` (`net8.0` is rejected by `cTrader.Automate 1.0.21` packager with `CT0004`), without redefining `HOME` (`DOTNET_CLI_HOME` / `NUGET_PACKAGES` / `XDG_*` isolated under `/tmp/build-work`, `AlgoBuild=true`, `AlgoPublish=false`).
   - Non-empty `/var/lib/eventlens/algo/EventLensCliBridge.algo`, `/var/lib/eventlens/algo/packages.lock.json` (`cTrader.Automate` locked to `1.0.21` under `net6.0`), and air-gapped (`--network none`) `ctrader-console metadata /algo/EventLensCliBridge.algo` output (`EventLensCliBridge.metadata.txt`, confirming `ExpectedDemoAccountNumber` and `BridgeDir`) are produced; archive `SHA-256` digests.
2. **Phase 1 — Read-Only Bridge & Snapshot Gate**:
   - `eventlens-ctrader-cli` starts with `AllowDemoOrders=false` and writes rotating `quotes-<session>.jsonl` plus `reconcile_snapshot` frames carrying `(bridge_session_id, event_seq)`, `is_live: false`, `account_number == ExpectedDemoAccountNumber`, and `account_currency == "USD"`.
   - `eventlens-shadow demo-reconcile` reports `"manual_unlock_required": false`, `"open_positions": []`, and `"blocks_new_risk": true` strictly due to `"bridge_demo_orders_disabled"`.
3. **Phase 2 — Bounded `demo-smoke` Order & Protection Lifecycle**:
   - With `CTRADER_ALLOW_DEMO_ORDERS=true` and explicit positive integer `--expected-demo-account-number`, `eventlens-shadow demo-smoke --confirm-demo-execution --volume-units 1000` exits `0` with `"passed": true`, `"sl_confirmed": true`, `"flat_after_close": true`, non-zero `broker_position_id`, and zero simulated `SQLiteShadowLedger` fills.
4. **Phase 3 — Crash / Restart & Rollback Hygiene**:
   - Restarting `eventlens-ctrader-cli` starts a new `bridge_session_id` with `event_seq = 1` while `demo-reconcile` selects the latest snapshot across sessions without sequence collision and does not replay expired `order-intents.jsonl` lines.
   - Returning to `CTRADER_ALLOW_DEMO_ORDERS=false` + `EXTRA_SHADOW_ARGS=` cleanly resumes Shadow-only operation.
5. **Non-Root Runtime Boundary Note (`1654:1654` `app`)**:
   - Passing `docker run --rm --network none --user 1654:1654 ghcr.io/spotware/ctrader-console:latest --version` (without overriding `HOME`, using `/home/app` `0750`) only proves CLI binary startup for `--version`.
   - Do **not** enable `--user 1654:1654` in production Compose or grant host secret ACLs unless a dedicated test first verifies cTID login, `.algo` execution, `/bridge` read/write coexistence with host `eventlens`, and tightly scoped secret delivery without exposing `/etc/eventlens/ctrader-cli.pwd` to unrelated host processes.
