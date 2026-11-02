# Cloud 24×5 Unattended Shadow Deployment (GCP Ubuntu + cTrader Open API Demo)

This guide covers deploying EventLens on an Ubuntu 24.04 LTS Linux VM (e.g. Google Cloud `e2-small`) so it runs **24×5 unattended** without requiring a local Mac/Windows cTrader desktop app or manual cBot restarts.

---

## 1. Architecture & Hard Safety Locks

- **Headless Native Python Streamer (`src/eventlens/ctrader_openapi.py`)**:
  Connects directly to Spotware's TLS/TCP JSON framing endpoint (`demo.ctraderapi.com:5036`) using zero extra third-party packages (`asyncio` + `ssl` + `struct`), verifies `EURUSD` symbol & Razor commission metadata (`30 USD` per `1M USD` volume), caches delta-only `bid`/`ask` spot updates, rotates `QuoteStore` sessions every `12h`, and persists refreshed OAuth2 tokens atomically with `0600` permissions.
- **Double Demo-Only Hard Lock**:
  1. **Endpoint Lock (`CTraderLiveEndpointForbiddenError`)**: `CTraderOpenApiConfig` rejects any host containing `"live"` (only `demo.ctraderapi.com` or local loopback test mocks are permitted).
  2. **Pre-Auth Account Lock (`CTraderLiveAccountForbiddenError`)**: Before sending `ProtoOAAccountAuthReq (2102)`, `CTraderOpenApiClient` queries `ProtoOAGetAccountListByAccessTokenReq (2149)` and verifies the target `ctidTraderAccountId` exists and explicitly reports `isLive == False`.
- **Risk Gate Integrity**:
  Shadow accounting (`SQLiteShadowLedger`) and `DeterministicHardRiskEngine` remain the sole authority for sizing, leverage, cooldowns, and protective exits. Optional Demo order mirroring (`--ctrader-demo-mirror-orders`, disabled by default) only mirrors already-approved Shadow fills downstream.
- **24×5 Continuous Safeguards**:
  - **FX Weekend Auto-Pause**: Automatically pauses news polling during FX weekend market closure (`America/New_York` Friday `17:00` → Sunday `17:00`, DST-aware) so weekend noise never wastes LLM calls or expires against closed markets.
  - **Daily LLM Budget Circuit Breaker (`--max-daily-llm-calls 80`)**: Caps paid DeepSeek invocations per UTC day while keeping `QuoteStore` ingestion and `ProtectiveExitPolicy` stop-loss/take-profit monitoring 100% active.
  - **Graceful `SIGTERM` Drain**: `systemctl stop` / `restart` triggers `ShadowRunner.stop()`, draining open shadow positions cleanly within `--exit-drain-timeout 60` before closing SQLite WAL files.

---

## 2. Step 1 — Obtain Spotware Open API Credentials (Once Approved)

Once Spotware emails approval for your application at https://openapi.ctrader.com/apps:

1. Open your approved app on https://openapi.ctrader.com/apps and click **View** / **Credentials** to note:
   - `Client ID`
   - `Client Secret`
2. Click **Playground** (or open the token generator for your app), sign in with your Spotware cTID, ensure your **Pepperstone Demo** account (`login 5298703`, `ctidTraderAccountId = 47589006`) is checked, and generate initial tokens:
   - `Access Token`
   - `Refresh Token`

> **Security Rule**: Never paste `Client Secret`, `Access Token`, `Refresh Token`, or `DEEPSEEK_API_KEY` into chat logs or commit them to Git.

---

## 3. Step 2 — Provision GCP VM & Run Setup Script

Recommended GCP Compute Engine spec:
- **Machine type**: `e2-small` (2 vCPU, 2 GB RAM)
- **Region**: `us-east4` (Northern Virginia — close to Equinix NY4 FX servers and US federal statistical releases) or `europe-west2` (London LD4)
- **Boot disk**: Ubuntu 24.04 LTS, 20 GB Balanced Persistent Disk
- **Firewall**: Outbound TCP `443` (HTTPS news feeds + DeepSeek API) and TCP `5036` (`demo.ctraderapi.com`); **zero inbound ports needed** (SSH only).

SSH into the VM, clone the repository, and run the idempotent installer:

```bash
git clone <YOUR_REPO_URL> ~/eventlens
cd ~/eventlens
sudo bash deploy/gcp/setup_ubuntu.sh
```

---

## 4. Step 3 — Configure `/etc/eventlens/eventlens.env` & Start `systemd`

Edit `/etc/eventlens/eventlens.env` (`CTRADER_ACCOUNT_ID=47589006` is already pre-filled for Pepperstone Demo `5298703`):

```bash
sudo nano /etc/eventlens/eventlens.env
```

Fill in:
- `DEEPSEEK_API_KEY`
- `CTRADER_CLIENT_ID`
- `CTRADER_CLIENT_SECRET`
- `CTRADER_ACCESS_TOKEN`
- `CTRADER_REFRESH_TOKEN`

Start and enable the service on boot:

```bash
sudo systemctl enable --now eventlens-shadow
sudo systemctl status eventlens-shadow
```

---

## 5. Step 4 — Everyday Inspection & Operations

- **Tail live runtime logs**:
  ```bash
  sudo journalctl -u eventlens-shadow -f
  ```
- **Check live runner & position status (strictly read-only, safe while running)**:
  ```bash
  sudo -u eventlens /opt/eventlens/.venv/bin/eventlens-shadow status \
    --shadow-db /var/lib/eventlens/data/shadow-ledger.sqlite
  ```
- **Generate full objective Shadow & quote quality report**:
  ```bash
  sudo -u eventlens /opt/eventlens/.venv/bin/eventlens-shadow report \
    --shadow-db /var/lib/eventlens/data/shadow-ledger.sqlite
  ```
- **Optional Demo Account Paper-Order Mirroring**:
  If you also want risk-approved Shadow fills mirrored onto Pepperstone Demo `5298703` so positions appear inside your cTrader mobile/desktop app, add `--ctrader-demo-mirror-orders` to `ExecStart` in `/etc/systemd/system/eventlens-shadow.service`, then run:
  ```bash
  sudo systemctl daemon-reload && sudo systemctl restart eventlens-shadow
  ```
