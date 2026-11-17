#!/usr/bin/env bash
set -euo pipefail

SERVICE="eventlens-ctrader-cli.service"
BRIDGE_DIR="/var/lib/eventlens/bridge"
PYTHON_BIN="/opt/eventlens/.venv/bin/python"

if ! systemctl is-active --quiet "${SERVICE}"; then
  echo "EVENTLENS_BRIDGE_WATCHDOG|status=skipped|reason=service_inactive"
  exit 0
fi

ACTIVE_TS="$(systemctl show -p ActiveEnterTimestamp --value "${SERVICE}" || true)"
ACTIVE_EPOCH=0
if [[ -n "${ACTIVE_TS}" && "${ACTIVE_TS}" != "n/a" ]]; then
  ACTIVE_EPOCH="$(date -u -d "${ACTIVE_TS}" +%s 2>/dev/null || echo 0)"
fi

CBOT_DEAD=0
if (( ACTIVE_EPOCH > 0 )); then
  if journalctl -u "${SERVICE}" --since "@${ACTIVE_EPOCH}" --no-pager -q 2>/dev/null | grep -Fq "process was unexpectedly terminated"; then
    CBOT_DEAD=1
  fi
fi

set +e
VERDICT_OUT="$("${PYTHON_BIN}" - "${ACTIVE_EPOCH}" "${CBOT_DEAD}" "${BRIDGE_DIR}" <<'PY'
import sys
from datetime import UTC, datetime
from pathlib import Path

from eventlens.trading.ctrader_cli_broker import evaluate_bridge_watchdog

active_epoch = int(sys.argv[1])
cbot_dead = bool(int(sys.argv[2]))
bridge_dir = Path(sys.argv[3])

now_utc = datetime.now(UTC)
active_dt = datetime.fromtimestamp(active_epoch, tz=UTC) if active_epoch > 0 else None

verdict = evaluate_bridge_watchdog(
    bridge_dir,
    now_utc=now_utc,
    service_active_since=active_dt,
    cbot_terminated_in_logs=cbot_dead,
)
rec_age = "none" if verdict.reconcile_age_seconds is None else f"{verdict.reconcile_age_seconds:.1f}"
q_age = "none" if verdict.quote_age_seconds is None else f"{verdict.quote_age_seconds:.1f}"
status = "restart" if verdict.should_restart else "ok"
print(
    f"EVENTLENS_BRIDGE_WATCHDOG|status={status}|reason={verdict.reason}"
    f"|reconcile_age_s={rec_age}|quote_age_s={q_age}"
)
sys.exit(10 if verdict.should_restart else 0)
PY
)"
PY_RC=$?
set -e

echo "${VERDICT_OUT}"
if (( PY_RC == 10 )); then
  echo "EVENTLENS_BRIDGE_WATCHDOG|action=restart_service|unit=${SERVICE}"
  systemctl restart "${SERVICE}"
elif (( PY_RC != 0 )); then
  exit "${PY_RC}"
fi
