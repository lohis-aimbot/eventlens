#!/usr/bin/env bash
# Idempotent Ubuntu 24.04 LTS setup script for EventLens 24x5 Cloud Shadow Runtime.
# Usage (from cloned repo root on GCP VM):
#   sudo bash deploy/gcp/setup_ubuntu.sh
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "ERROR: Please run as root (sudo bash deploy/gcp/setup_ubuntu.sh)" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_SRC="$(cd "${SCRIPT_DIR}/../.." && pwd)"
APP_DIR="/opt/eventlens"
STATE_DIR="/var/lib/eventlens"
CONF_DIR="/etc/eventlens"
ENV_FILE="${CONF_DIR}/eventlens.env"
PWD_FILE="${CONF_DIR}/ctrader-cli.pwd"

echo "[1/6] Installing system packages..."
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  acl \
  ca-certificates \
  git \
  python3 \
  python3-pip \
  python3-venv \
  rsync \
  sqlite3

echo "[2/6] Ensuring dedicated 'eventlens' system user and directories..."
if ! id -u eventlens >/dev/null 2>&1; then
  useradd --system --home-dir "${STATE_DIR}" --shell /usr/sbin/nologin eventlens
fi

install -d -m 0755 -o eventlens -g eventlens "${APP_DIR}"
install -d -m 0750 -o eventlens -g eventlens "${STATE_DIR}"
install -d -m 0750 -o eventlens -g eventlens "${STATE_DIR}/data"
install -d -m 0755 -o eventlens -g eventlens "${STATE_DIR}/algo"
# Official `ghcr.io/spotware/ctrader-console:latest` has empty `Config.User` (defaults to container root;
# non-root runtime override is not verified). Setgid + default ACLs for `eventlens` ensure files created
# inside /bridge remain readable/writable by the host `eventlens` Python process without world/other access
# or unrelated UID grants.
install -d -m 2770 -o eventlens -g eventlens "${STATE_DIR}/bridge"
setfacl -b "${STATE_DIR}/bridge" || true
setfacl -m u:eventlens:rwX -m g:eventlens:rwX "${STATE_DIR}/bridge"
setfacl -d -m u:eventlens:rwX -m g:eventlens:rwX "${STATE_DIR}/bridge"
install -d -m 0750 -o root -g eventlens "${CONF_DIR}"

echo "[3/6] Syncing EventLens repository to ${APP_DIR}..."
rsync -a --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude 'data' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.mypy_cache' \
  --exclude '.ruff_cache' \
  "${REPO_SRC}/" "${APP_DIR}/"
chown -R eventlens:eventlens "${APP_DIR}"

echo "[4/6] Building Python virtual environment in ${APP_DIR}/.venv..."
sudo -u eventlens python3 -m venv "${APP_DIR}/.venv"
sudo -u eventlens "${APP_DIR}/.venv/bin/pip" install --upgrade pip wheel
sudo -u eventlens "${APP_DIR}/.venv/bin/pip" install -e "${APP_DIR}"

echo "[5/6] Preparing ${ENV_FILE} (mode 0600)..."
if [[ ! -f "${ENV_FILE}" ]]; then
  install -m 0600 -o eventlens -g eventlens \
    "${APP_DIR}/deploy/gcp/eventlens.env.example" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from template. Edit it with real credentials before starting."
else
  chown eventlens:eventlens "${ENV_FILE}"
  chmod 0600 "${ENV_FILE}"
  echo "Preserved existing ${ENV_FILE} (verified 0600 permissions)."
fi

if [[ ! -f "${PWD_FILE}" ]]; then
  install -m 0600 -o root -g root /dev/null "${PWD_FILE}"
  echo "replace_with_ctid_password" > "${PWD_FILE}"
  chmod 0600 "${PWD_FILE}"
  setfacl -b "${PWD_FILE}" || true
  echo "Created placeholder ${PWD_FILE} (root:root, mode 0600, no ACLs). Replace with single-line cTID password."
else
  chown root:root "${PWD_FILE}"
  chmod 0600 "${PWD_FILE}"
  setfacl -b "${PWD_FILE}" || true
  echo "Preserved existing ${PWD_FILE} (enforced root:root, mode 0600, stripped ACLs)."
fi

echo "[6/6] Installing systemd units..."
install -m 0644 "${APP_DIR}/deploy/gcp/eventlens-shadow.service" \
  /etc/systemd/system/eventlens-shadow.service
install -m 0644 "${APP_DIR}/deploy/gcp/eventlens-ctrader-cli.service" \
  /etc/systemd/system/eventlens-ctrader-cli.service
install -m 0644 "${APP_DIR}/deploy/gcp/eventlens-shadow-cli.service" \
  /etc/systemd/system/eventlens-shadow-cli.service
systemctl daemon-reload

cat <<EOF

Setup complete! Next steps (Docker cTrader CLI recommended mode):
  1. Ensure Docker Engine + Compose plugin are installed on this x86_64 VM, then build
     EventLensCliBridge.algo via official .NET 8 SDK container:
       sudo bash ${APP_DIR}/deploy/gcp/build_ctrader_algo.sh
  2. Edit credentials (never paste secrets into chat):
       sudo nano ${ENV_FILE}
       sudo nano ${PWD_FILE}
  3. Enable and start headless Docker cTrader CLI + EventLens 24x5 Shadow runtime:
       sudo systemctl enable --now eventlens-ctrader-cli
       sudo systemctl enable --now eventlens-shadow-cli
  4. Verify Demo bridge health and read-only reconciliation before any Demo order test:
       sudo -u eventlens ${APP_DIR}/.venv/bin/eventlens-shadow demo-reconcile --quote-bridge-dir ${STATE_DIR}/bridge --shadow-db ${STATE_DIR}/data/shadow-ledger.sqlite
       sudo -u eventlens ${APP_DIR}/.venv/bin/eventlens-shadow status --shadow-db ${STATE_DIR}/data/shadow-ledger.sqlite
  5. Optional supervised bounded Demo smoke test (after setting CTRADER_ALLOW_DEMO_ORDERS=true and restarting eventlens-ctrader-cli):
       sudo -u eventlens ${APP_DIR}/.venv/bin/eventlens-shadow demo-smoke --quote-bridge-dir ${STATE_DIR}/bridge --shadow-db ${STATE_DIR}/data/shadow-ledger.sqlite --expected-demo-account-number <DEMO_ACCOUNT_NUMBER> --confirm-demo-execution --volume-units 1000
EOF
