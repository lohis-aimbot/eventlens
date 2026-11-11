#!/usr/bin/env bash
# Build EventLensCliBridge.algo (`<TargetFramework>net6.0</TargetFramework>`, as required by
# Spotware `cTrader.Automate 1.0.21` packager which rejects `net8.0` with CT0004) inside
# Microsoft's official .NET 8 SDK image (`mcr.microsoft.com/dotnet/sdk:8.0`, linux/amd64)
# as a non-root UID:GID using dedicated DOTNET_CLI_HOME / NUGET_PACKAGES / XDG_* directories
# under /tmp/build-work (without redefining HOME) and AlgoPublish=false, followed by an
# air-gapped (`--network none`) `ctrader-console metadata` inspection of the compiled `.algo`.
#
# NOTE: Not executed locally on the macOS audit host; run on the Ubuntu 24.04 x86_64 VM.
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/eventlens}"
ALGO_OUT_DIR="${ALGO_OUT_DIR:-/var/lib/eventlens/algo}"
DOTNET_SDK_IMAGE="${DOTNET_SDK_IMAGE:-mcr.microsoft.com/dotnet/sdk:8.0}"
CTRADER_CONSOLE_IMAGE="${CTRADER_CONSOLE_IMAGE:-ghcr.io/spotware/ctrader-console:latest}"
VERIFY_ALGO_METADATA="${VERIFY_ALGO_METADATA:-1}"

mkdir -p "${ALGO_OUT_DIR}"

if [[ -n "${BUILD_UID_GID:-}" ]]; then
  RUN_UID_GID="${BUILD_UID_GID}"
elif [[ "${EUID}" -eq 0 ]] && id -u eventlens >/dev/null 2>&1; then
  chown eventlens:eventlens "${ALGO_OUT_DIR}"
  RUN_UID_GID="$(id -u eventlens):$(id -g eventlens)"
else
  RUN_UID_GID="$(id -u):$(id -g)"
fi

docker run --rm \
  --platform linux/amd64 \
  -u "${RUN_UID_GID}" \
  -v "${APP_DIR}/ctrader/EventLensCliBridge:/src:ro" \
  -v "${ALGO_OUT_DIR}:/out" \
  "${DOTNET_SDK_IMAGE}" \
  bash -euo pipefail -c '
    PROJ_WORK_DIR=/tmp/build-work/EventLensCliBridge/EventLensCliBridge
    mkdir -p "${PROJ_WORK_DIR}" \
      /tmp/build-work/obj \
      /tmp/build-work/bin \
      /tmp/build-work/dotnet-cli-home \
      /tmp/build-work/nuget-packages \
      /tmp/build-work/nuget-http-cache \
      /tmp/build-work/nuget-plugins-cache \
      /tmp/build-work/xdg-config \
      /tmp/build-work/xdg-data \
      /tmp/build-work/xdg-cache
    cp -a /src/. "${PROJ_WORK_DIR}/"
    if [[ ! -f "${PROJ_WORK_DIR}/packages.lock.json" && -f /out/packages.lock.json ]] \
      && grep -Eq "(\"net6\.0\"|\.NETCoreApp,Version=v6\.0)" /out/packages.lock.json; then
      cp -f /out/packages.lock.json "${PROJ_WORK_DIR}/packages.lock.json"
    fi
    export DOTNET_CLI_HOME=/tmp/build-work/dotnet-cli-home
    export NUGET_PACKAGES=/tmp/build-work/nuget-packages
    export NUGET_HTTP_CACHE_PATH=/tmp/build-work/nuget-http-cache
    export NUGET_PLUGINS_CACHE_PATH=/tmp/build-work/nuget-plugins-cache
    export XDG_CONFIG_HOME=/tmp/build-work/xdg-config
    export XDG_DATA_HOME=/tmp/build-work/xdg-data
    export XDG_CACHE_HOME=/tmp/build-work/xdg-cache
    export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
    export DOTNET_CLI_TELEMETRY_OPTOUT=1

    RESTORE_ARGS=(
      "${PROJ_WORK_DIR}/EventLensCliBridge.csproj"
      "-p:BaseIntermediateOutputPath=/tmp/build-work/obj/"
      "-p:RestorePackagesWithLockFile=true"
      "-p:AlgoPublish=false"
    )
    if [[ -f "${PROJ_WORK_DIR}/packages.lock.json" ]]; then
      RESTORE_ARGS+=("--locked-mode")
    fi
    dotnet restore "${RESTORE_ARGS[@]}"

    LOCK_FILE="${PROJ_WORK_DIR}/packages.lock.json"
    if [[ ! -s "${LOCK_FILE}" ]]; then
      echo "ERROR: NuGet lockfile packages.lock.json was not produced" >&2
      exit 1
    fi
    grep -q "\"1.0.21\"" "${LOCK_FILE}"
    grep -Eq "(\"net6\.0\"|\.NETCoreApp,Version=v6\.0)" "${LOCK_FILE}"
    ! grep -Eq "(\"net8\.0\"|\.NETCoreApp,Version=v8\.0)" "${LOCK_FILE}"

    dotnet build "${PROJ_WORK_DIR}/EventLensCliBridge.csproj" \
      --no-restore \
      -c Release \
      -p:AlgoBuild=true \
      -p:AlgoPublish=false \
      -p:BaseIntermediateOutputPath=/tmp/build-work/obj/ \
      -p:BaseOutputPath=/tmp/build-work/bin/

    ALGO_FILE="$(find /tmp/build-work/bin -type f -name "EventLensCliBridge.algo" | head -n 1)"
    if [[ -z "${ALGO_FILE}" ]]; then
      echo "ERROR: EventLensCliBridge.algo was not produced under /tmp/build-work/bin" >&2
      ls -laR /tmp/build-work/bin >&2
      exit 1
    fi
    cp -f "${ALGO_FILE}" /out/EventLensCliBridge.algo
    cp -f "${LOCK_FILE}" /out/packages.lock.json
    chmod 0644 /out/EventLensCliBridge.algo /out/packages.lock.json
  '

test -s "${ALGO_OUT_DIR}/EventLensCliBridge.algo"
test -s "${ALGO_OUT_DIR}/packages.lock.json"

if [[ "${VERIFY_ALGO_METADATA}" == "1" ]]; then
  if ! docker image inspect "${CTRADER_CONSOLE_IMAGE}" >/dev/null 2>&1; then
    docker pull --platform linux/amd64 "${CTRADER_CONSOLE_IMAGE}"
  fi
  META_USER_ARGS=()
  if [[ -n "${CTRADER_METADATA_USER:-}" ]]; then
    META_USER_ARGS+=(--user "${CTRADER_METADATA_USER}")
  fi
  METADATA_OUT="$(
    docker run --rm \
      --platform linux/amd64 \
      --network none \
      ${META_USER_ARGS[@]+"${META_USER_ARGS[@]}"} \
      -e DOTNET_CLI_HOME=/tmp/ctrader-meta-dotnet \
      -e NUGET_PACKAGES=/tmp/ctrader-meta-nuget \
      -e XDG_CONFIG_HOME=/tmp/ctrader-meta-xdg-config \
      -e XDG_DATA_HOME=/tmp/ctrader-meta-xdg-data \
      -e DOTNET_CLI_TELEMETRY_OPTOUT=1 \
      -v "${ALGO_OUT_DIR}:/algo:ro" \
      "${CTRADER_CONSOLE_IMAGE}" \
      metadata /algo/EventLensCliBridge.algo
  )"
  printf '%s\n' "${METADATA_OUT}" > "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt"
  chmod 0644 "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt"
  grep -q "ExpectedDemoAccountNumber" "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt"
  grep -q "BridgeDir" "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt"
fi

if [[ "${EUID}" -eq 0 ]] && id -u eventlens >/dev/null 2>&1; then
  chown eventlens:eventlens "${ALGO_OUT_DIR}/EventLensCliBridge.algo" "${ALGO_OUT_DIR}/packages.lock.json"
  if [[ -f "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt" ]]; then
    chown eventlens:eventlens "${ALGO_OUT_DIR}/EventLensCliBridge.metadata.txt"
  fi
fi
ls -lh "${ALGO_OUT_DIR}/EventLensCliBridge.algo" "${ALGO_OUT_DIR}/packages.lock.json"
echo "Built ${ALGO_OUT_DIR}/EventLensCliBridge.algo (net6.0) and ${ALGO_OUT_DIR}/packages.lock.json successfully."
