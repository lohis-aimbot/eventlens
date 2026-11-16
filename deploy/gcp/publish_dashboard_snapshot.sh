#!/usr/bin/env bash
# Export a strictly sanitized dashboard snapshot JSON and force-push it to
# the orphan `dashboard-live` branch so the mobile/web PWA stays live 24x7
# even when the GCP VM hibernates on weekends.
set -euo pipefail

VM_STATUS="${1:-running}"
if [[ "${VM_STATUS}" == "running" ]]; then
  if ! systemctl is-active --quiet eventlens-shadow-cli 2>/dev/null; then
    VM_STATUS="stopped"
  fi
fi

STATE_DIR="/var/lib/eventlens"
EXPORT_DIR="${STATE_DIR}/dashboard-export"
EXPORT_JSON="${EXPORT_DIR}/snapshot.json"
HASH_FILE="${EXPORT_DIR}/.last_hash"
REPO_DIR="${EXPORT_DIR}/repo"
SSH_KEY="${STATE_DIR}/.ssh/dashboard_deploy_ed25519"
PYTHON_CLI="/opt/eventlens/.venv/bin/eventlens-shadow"

if [[ ! -f "${SSH_KEY}" ]]; then
  echo "Deploy SSH key missing at ${SSH_KEY}; skipping dashboard push." >&2
  exit 0
fi

mkdir -p "${EXPORT_DIR}"

"${PYTHON_CLI}" export-dashboard \
  --shadow-db "${STATE_DIR}/data/shadow-ledger.sqlite" \
  --memory-db "${STATE_DIR}/data/shadow-memory.sqlite" \
  --quotes-db "${STATE_DIR}/data/live-quotes.sqlite" \
  --vm-status "${VM_STATUS}" \
  --output "${EXPORT_JSON}" >/dev/null

if [[ ! -s "${EXPORT_JSON}" ]]; then
  echo "Refused empty dashboard snapshot." >&2
  exit 1
fi

NEW_HASH="$(python3 - "${EXPORT_JSON}" <<'PY'
import hashlib, json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
data.pop("generated_at", None)
print(hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest())
PY
)"

SHOULD_PUSH="true"
if [[ -f "${HASH_FILE}" && "${VM_STATUS}" == "running" ]]; then
  OLD_HASH="$(cat "${HASH_FILE}" || true)"
  LAST_MTIME="$(stat -c %Y "${HASH_FILE}" 2>/dev/null || echo 0)"
  NOW_EPOCH="$(date +%s)"
  AGE_SECS=$(( NOW_EPOCH - LAST_MTIME ))
  if [[ "${OLD_HASH}" == "${NEW_HASH}" && "${AGE_SECS}" -lt 300 ]]; then
    SHOULD_PUSH="false"
  fi
fi

if [[ "${SHOULD_PUSH}" != "true" ]]; then
  exit 0
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  git init -b dashboard-live "${REPO_DIR}" >/dev/null 2>&1 || {
    git init "${REPO_DIR}" >/dev/null
    git -C "${REPO_DIR}" checkout -B dashboard-live >/dev/null
  }
  git -C "${REPO_DIR}" config user.name "EventLens Sync Bot"
  git -C "${REPO_DIR}" config user.email "sync-bot@users.noreply.github.com"
fi

cp "${EXPORT_JSON}" "${REPO_DIR}/snapshot.json"
git -C "${REPO_DIR}" add snapshot.json
if git -C "${REPO_DIR}" rev-parse --verify HEAD >/dev/null 2>&1; then
  git -C "${REPO_DIR}" commit --amend -m "Sync sanitized dashboard snapshot" --quiet
else
  git -C "${REPO_DIR}" commit -m "Sync sanitized dashboard snapshot" --quiet
fi

GIT_SSH_COMMAND="ssh -i ${SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new" \
  git -C "${REPO_DIR}" push --force git@github.com:lohis-aimbot/eventlens.git HEAD:dashboard-live >/dev/null 2>&1

echo "${NEW_HASH}" > "${HASH_FILE}"
