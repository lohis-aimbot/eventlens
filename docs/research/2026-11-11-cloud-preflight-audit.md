# Cloud preflight audit — 2026-11-11

This is a bounded deployment checkpoint, not trading acceptance or profitability evidence.
No broker login, model API call, Demo order, live order, or EventLens service startup was performed by these cloud checks.

## Verified infrastructure

- Ubuntu 24.04, x86_64, `e2-medium`, 30 GB balanced persistent disk.
- Private VM, no external VM IP or attached service account. SSH ingress is limited to Google IAP (`35.235.240.0/20`, TCP 22). Outbound traffic uses Cloud NAT.
- Python 3.12.3; Docker 29.1.3; Compose 2.40.3; UTC; NTP synchronized.
- Estimated fixed infrastructure: USD 32.13/month, before NAT processing, egress and applicable tax. A USD 40 monthly gross-cost budget has alerts at 50/80/100%; alerts do not cap spending. User-authorized gross resource limit remains CNY 400/month, excluding model APIs.

## Independently reproduced checks

- Python snapshot before the subsequent ledger/risk revisions: `373 passed in 9.66s`; Ruff check and format check passed; mypy passed for 51 source files. These results must be rerun after further edits.
- The initial C# project failed real cloud compilation: `CS0104` (`File` ambiguity), `CT0111` (`Int64` cBot parameter), then `CT0004` (`net8.0` rejected by the cTrader packager).
- The corrected repository snapshot targeting `net6.0`, compiled with .NET SDK 8.0.428 and `cTrader.Automate` exactly `[1.0.21]`, produced a real `EventLensCliBridge.algo` through the repository build script using the explicit `BUILD_UID_GID=0:0` override.
- The same script inspected the artifact with the official runtime, `--network none`, and `--user 1654:1654`. Metadata inspection passed. Defaults include `AllowDemoOrders=false`, expected account number string `"0"`, and expected account currency `USD`.
- The default build UID derived from the host `eventlens` user failed before compilation in NuGet migration mutex initialization. Arbitrary non-root build UID support is **not accepted**. Non-root offline metadata/version checks do not prove authenticated broker runtime support.

Pinned images used in those checks:

```text
SDK: mcr.microsoft.com/dotnet/sdk@sha256:1b955f179b02df4f0eb5fadedaa898cbe44d620a64aab52eec72c8e91d1355b5
Runtime: ghcr.io/spotware/ctrader-console@sha256:6a339e15ee895de270be4a4e293fdb3f2fcdbecfbbace460105703125742e5ad
```

Verified snapshot and artifact SHA-256 values:

| Input / output | SHA-256 |
|---|---|
| `EventLensCliBridge.cs` | `ccd68d660fc097d7f0c36101b1c08f54919f99ece1a33f76b9d2bb66c7f6d98c` |
| `EventLensCliBridge.csproj` | `292b41a0988a6e6a496c81c260b88ee6e51eb77d90f884332d9abd2ce4a81f08` |
| `build_ctrader_algo.sh` | `6c526a8e0b0f1f740941350c303f8d748b1daf43009e6c81cd324cfdb8b1d989` |
| `.algo` (35,284 bytes) | `6b8447e3986b8c88be063cbfb0df7a2ece74c225bde8a57df23da4fc15a1b8be` |
| `packages.lock.json` (312 bytes) | `7e18786c88ab0bbda2c04691327c2d486682ffe78f213be5a89d5b940d88dd0a` |
| Metadata (1,436 bytes) | `29e1b5d9950123ec7200659d0f92be305ae588c74b8063e55abdb35e7e7aa872` |

The lockfile mirrored into the repository matches the cloud artifact byte-for-byte. This does not assert that subsequent source edits produce the same artifact; a changed snapshot requires a fresh build, locked restore, and metadata check.

## Open review findings at this checkpoint

These were reported to the implementation agent; they are not declared fixed here.

1. **P1 — Unconfirmed exit evidence presented as confirmed fills.** The broker-reconcile SL/TP branch synthesized entry-price exits, default quantities and zero realized PnL while marking them non-synthetic. The C# close path also substituted current quotes when History was absent and could reuse an earlier partial-close history record. Require uniquely identified broker deal evidence; record unresolved accounting explicitly and deduplicate close/reconcile paths.
2. **P1 — New Demo engine daily baseline can erase outage losses.** On a new trading day, the first recovered equity was used as baseline without the prior valid equity/provisional baseline guard. Preserve conservative cross-day state across outages/restarts and block new risk while the baseline is unconfirmed.
3. **Deployment follow-ups.** Verify the supported build UID configuration; avoid passing the entire model-secret environment file into the CLI container; pin image digests; verify clean CLI exit on a safety stop. Real account permissions and bridge IPC remain unverified.

## Release decision

Infrastructure and offline artifact feasibility are verified. The application is **not accepted for unattended Demo execution** at this checkpoint. Before starting any account-connected service, close the findings, rerun Python checks against the final tree, build the exact final C# snapshot with locked dependencies, inspect metadata offline, and separately authorize/configure Demo credentials through protected server files.
