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

echo "[1/6] Installing system packages..."
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
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

echo "[6/6] Installing systemd unit eventlens-shadow.service..."
install -m 0644 "${APP_DIR}/deploy/gcp/eventlens-shadow.service" \
  /etc/systemd/system/eventlens-shadow.service
systemctl daemon-reload

cat <<EOF

Setup complete! Next steps:
  1. Edit credentials (never paste secrets into chat):
       sudo nano ${ENV_FILE}
  2. Enable and start 24x5 continuous service:
       sudo systemctl enable --now eventlens-shadow
  3. Watch live logs:
       sudo journalctl -u eventlens-shadow -f
  4. Check read-only status / report anytime:
       sudo -u eventlens ${APP_DIR}/.venv/bin/eventlens-shadow status --shadow-db ${STATE_DIR}/data/shadow-ledger.sqlite
EOF
