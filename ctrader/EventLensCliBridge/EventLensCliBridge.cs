using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using cAlgo.API;
using File = System.IO.File;

namespace cAlgo.Robots
{
    /// <summary>
    /// Headless Linux/Docker cTrader CLI bridge for EventLens 24x5 EURUSD Demo quote streaming
    /// and opt-in position-ID-aware Demo execution &amp; reconciliation.
    ///
    /// Safety &amp; durability invariants:
    /// 1. Hard-locked to non-Live Demo accounts (!Account.IsLive), EURUSD, ExpectedDemoAccountNumber,
    ///    and ExpectedAccountCurrency ("USD") at startup, tick, timer, and order dispatch.
    /// 2. Emits (bridge_session_id, event_seq) on every execution/reconciliation frame so restarts
    ///    with _execEventSeq=0 never collide or reorder snapshots on the Python consumer.
    /// 3. Consumes order-intents.jsonl strictly up to complete '\n'-terminated lines (partial trailing
    ///    lines are never skipped across polls or restarts).
    /// 4. Enforces mandatory UTC expires_at TTL before any broker mutation; stale intents are rejected
    ///    as "expired" without calling the broker.
    /// 5. Journals "in_flight" BEFORE calling the broker and "terminal" AFTER the broker returns;
    ///    post-crash "in_flight" intents are reconciled against live Positions/History or marked
    ///    "unknown_execution" (never blindly retried).
    /// </summary>
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class EventLensCliBridge : Robot
    {
        [Parameter("Bridge directory", DefaultValue = "/bridge")]
        public string BridgeDir { get; set; } = "/bridge";

        [Parameter("Sample interval ms", DefaultValue = 200, MinValue = 100, MaxValue = 10000)]
        public int SampleIntervalMilliseconds { get; set; }

        [Parameter("Rotation interval seconds", DefaultValue = 43200, MinValue = 10, MaxValue = 86400)]
        public int RotationIntervalSeconds { get; set; }

        [Parameter("Allow Demo orders", DefaultValue = false)]
        public bool AllowDemoOrders { get; set; }

        [Parameter("Expected Demo account number (0=read-only any Demo)", DefaultValue = "0")]
        public string ExpectedDemoAccountNumber { get; set; } = "0";

        [Parameter("Expected account currency", DefaultValue = "USD")]
        public string ExpectedAccountCurrency { get; set; } = "USD";

        [Parameter("Reconcile interval seconds", DefaultValue = 15, MinValue = 2, MaxValue = 300)]
        public int ReconcileIntervalSeconds { get; set; }

        private StreamWriter? _quoteWriter;
        private StreamWriter? _execWriter;
        private Ticks? _ticks;
        private string _quoteSessionId = string.Empty;
        private string _bridgeSessionId = string.Empty;
        private string _bridgeStartedAt = string.Empty;
        private string _resolvedBridgeDir = string.Empty;
        private string _intentsPath = string.Empty;
        private string _journalPath = string.Empty;
        private long _intentsOffset;
        private readonly Dictionary<string, string> _intentJournalStages =
            new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _intentInFlightOrigUnits =
            new Dictionary<string, long>(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<long>> _intentInFlightPriorDealIds =
            new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        private readonly Stopwatch _sessionElapsed = new Stopwatch();
        private readonly Stopwatch _reconcileElapsed = new Stopwatch();
        private DateTime _lastTickTime = DateTime.MinValue;
        private DateTime _pendingTime;
        private double _pendingBid;
        private double _pendingAsk;
        private bool _hasPending;
        private bool _lastEmittedPositiveSpread;
        private double _lastEmittedBid;
        private double _lastEmittedAsk;
        private long _lastSample = -10000;
        private long _sequence;
        private long _rawTicks;
        private long _rawZeroSpreadTicks;
        private long _symbolMismatchTicks;
        private long _execEventSeq;
        private long _expectedDemoAccountNumber;
        private bool _safetyRefused;
        private bool _enabled;

        protected override void OnStart()
        {
            _resolvedBridgeDir = Path.GetFullPath(string.IsNullOrWhiteSpace(BridgeDir) ? "/bridge" : BridgeDir);
            Directory.CreateDirectory(_resolvedBridgeDir);

            _bridgeSessionId = Guid.NewGuid().ToString();
            _bridgeStartedAt = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
            _execEventSeq = 0;
            _safetyRefused = false;

            if (!VerifyDemoSafetyOrStop("startup"))
                return;

            _intentsPath = Path.Combine(_resolvedBridgeDir, "order-intents.jsonl");
            _journalPath = Path.Combine(_resolvedBridgeDir, "intent-journal.jsonl");
            var execPath = Path.Combine(_resolvedBridgeDir, "execution-events.jsonl");
            _execWriter = new StreamWriter(
                new FileStream(execPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite),
                new UTF8Encoding(false))
            {
                AutoFlush = true
            };

            LoadIntentJournal();

            _ticks = MarketData.GetTicks(SymbolName);
            if (_ticks.Count > 0)
                _lastTickTime = DateTime.SpecifyKind(_ticks.LastTick.Time, DateTimeKind.Utc);

            OpenNewQuoteSession();
            _enabled = true;
            _reconcileElapsed.Restart();
            EmitReconcileSnapshot("startup");
            Timer.Start(TimeSpan.FromMilliseconds(50));
            Print(
                "EVENTLENS_CLI_BRIDGE|started|bridge_session={0}|quote_session={1}|symbol={2}|demo_orders={3}|expected_account={4}|rotation_s={5}",
                _bridgeSessionId,
                _quoteSessionId,
                SymbolName,
                AllowDemoOrders ? "enabled" : "disabled",
                _expectedDemoAccountNumber,
                RotationIntervalSeconds);
        }

        private string AccountCurrencyName => Account.Asset?.Name ?? string.Empty;

        private static bool TryParseExpectedDemoAccountNumber(
            string? rawValue,
            bool allowDemoOrders,
            out long expectedAccountNumber,
            out string errorReason)
        {
            expectedAccountNumber = 0L;
            var trimmed = (rawValue ?? string.Empty).Trim();
            if (trimmed.Length == 0)
            {
                errorReason = "invalid_expected_demo_account_number";
                return false;
            }
            for (var i = 0; i < trimmed.Length; i++)
            {
                var ch = trimmed[i];
                if (ch < '0' || ch > '9')
                {
                    errorReason = "invalid_expected_demo_account_number";
                    return false;
                }
            }
            if (!long.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out expectedAccountNumber))
            {
                errorReason = "invalid_expected_demo_account_number";
                return false;
            }
            if (allowDemoOrders && expectedAccountNumber <= 0L)
            {
                errorReason = "demo_orders_require_explicit_expected_account_number";
                return false;
            }
            errorReason = string.Empty;
            return true;
        }

        private bool VerifyDemoSafetyOrStop(string stage)
        {
            var accountNum = (long)Account.Number;
            var expectedAccountValid = TryParseExpectedDemoAccountNumber(
                ExpectedDemoAccountNumber,
                AllowDemoOrders,
                out var expectedAccountNumber,
                out var expectedAccountError);
            var accountMismatch = expectedAccountValid &&
                                  expectedAccountNumber > 0L &&
                                  accountNum != expectedAccountNumber;
            var expectedCcy = string.IsNullOrWhiteSpace(ExpectedAccountCurrency) ? "USD" : ExpectedAccountCurrency.Trim();
            var currencyMismatch = !string.Equals(AccountCurrencyName, expectedCcy, StringComparison.OrdinalIgnoreCase);

            if (Account.IsLive || !expectedAccountValid || IsBacktesting || RunningMode != RunningMode.RealTime ||
                SymbolName != "EURUSD" || Symbol.PipSize <= 0 || accountMismatch || currencyMismatch)
            {
                var reason = Account.IsLive
                    ? "live_account_forbidden"
                    : !expectedAccountValid
                        ? expectedAccountError
                        : accountMismatch
                            ? "unexpected_demo_account_number"
                            : currencyMismatch
                                ? "unexpected_account_currency:" + AccountCurrencyName
                                : "requires_EURUSD_realtime_Demo";
                Print("EVENTLENS_CLI_BRIDGE|refused|stage={0}|reason={1}", stage, reason);
                TryWriteRefusalEvent(stage, reason);
                _safetyRefused = true;
                _enabled = false;
                Timer.Stop();
                Stop();
                return false;
            }
            _expectedDemoAccountNumber = expectedAccountNumber;
            return true;
        }

        private void TryWriteRefusalEvent(string stage, string reason)
        {
            try
            {
                var path = Path.Combine(_resolvedBridgeDir, "execution-events.jsonl");
                using var sw = new StreamWriter(
                    new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite),
                    new UTF8Encoding(false));
                sw.WriteLine(JsonSerializer.Serialize(new
                {
                    version = 1,
                    kind = "safety_refusal",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = Account.IsLive ? "live" : "demo",
                    is_live = Account.IsLive,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    stage,
                    reason
                }));
            }
            catch
            {
                // Best-effort safety refusal audit; Stop() still halts execution unconditionally.
            }
        }

        private void OpenNewQuoteSession()
        {
            CloseCurrentQuoteSession();
            _quoteSessionId = Guid.NewGuid().ToString();
            _sequence = 0;
            _rawTicks = 0;
            _rawZeroSpreadTicks = 0;
            _symbolMismatchTicks = 0;
            _lastSample = -10000;

            var quotePath = Path.Combine(_resolvedBridgeDir, "quotes-" + _quoteSessionId + ".jsonl");
            _quoteWriter = new StreamWriter(
                new FileStream(quotePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read),
                new UTF8Encoding(false))
            {
                AutoFlush = true
            };

            WriteQuoteLine(new
            {
                version = 1,
                kind = "start",
                session_id = _quoteSessionId,
                account_mode = "demo",
                running_mode = "realtime",
                symbol = SymbolName,
                started_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                sample_interval_ms = SampleIntervalMilliseconds,
                duration_seconds = RotationIntervalSeconds,
                digits = Symbol.Digits,
                pip_size = Symbol.PipSize.ToString("R", CultureInfo.InvariantCulture),
                commission = Symbol.Commission.ToString("R", CultureInfo.InvariantCulture),
                commission_type = Symbol.CommissionType.ToString()
            });
            _sessionElapsed.Restart();
        }

        private void CloseCurrentQuoteSession()
        {
            if (_quoteWriter == null)
                return;
            try
            {
                FlushPendingIfDue(true);
                WriteQuoteLine(new
                {
                    version = 1,
                    kind = "stop",
                    session_id = _quoteSessionId,
                    stopped_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    quotes = _sequence,
                    raw_ticks = _rawTicks,
                    raw_zero_spread_ticks = _rawZeroSpreadTicks,
                    symbol_mismatch_ticks = _symbolMismatchTicks,
                    throttled_ticks = _rawTicks - _sequence
                });
            }
            finally
            {
                _quoteWriter.Dispose();
                _quoteWriter = null;
            }
        }

        protected override void OnTick()
        {
            if (!_enabled) return;
            if (!VerifyDemoSafetyOrStop("on_tick")) return;
            if (_sessionElapsed.Elapsed.TotalSeconds >= RotationIntervalSeconds)
                OpenNewQuoteSession();
            if (_ticks == null || _ticks.Count == 0)
                return;

            var tick = _ticks.LastTick;
            if (tick.Bid <= 0 || tick.Ask < tick.Bid || double.IsNaN(tick.Bid) ||
                double.IsNaN(tick.Ask) || double.IsInfinity(tick.Bid) || double.IsInfinity(tick.Ask))
                return;

            var tickTime = DateTime.SpecifyKind(tick.Time, DateTimeKind.Utc);
            if (tickTime <= _lastTickTime)
                return;
            _lastTickTime = tickTime;
            _rawTicks++;
            if (tick.Ask == tick.Bid)
                _rawZeroSpreadTicks++;
            if (Math.Abs(Symbol.Bid - tick.Bid) > 1e-9 || Math.Abs(Symbol.Ask - tick.Ask) > 1e-9)
                _symbolMismatchTicks++;

            _pendingTime = tickTime;
            _pendingBid = tick.Bid;
            _pendingAsk = tick.Ask;
            _hasPending = true;
            FlushPendingIfDue();
        }

        protected override void OnTimer()
        {
            if (!_enabled) return;
            if (!VerifyDemoSafetyOrStop("on_timer")) return;

            if (_sessionElapsed.Elapsed.TotalSeconds >= RotationIntervalSeconds)
                OpenNewQuoteSession();

            FlushPendingIfDue();

            // Always poll order-intents.jsonl so disabled/expired intents are deterministically rejected
            // immediately instead of remaining unconsumed until a future restart.
            PollOrderIntents();

            if (_reconcileElapsed.Elapsed.TotalSeconds >= ReconcileIntervalSeconds)
            {
                EmitReconcileSnapshot("periodic");
                _reconcileElapsed.Restart();
            }
        }

        private void FlushPendingIfDue(bool force = false)
        {
            if (!_hasPending || _quoteWriter == null)
                return;
            var positiveSpread = _pendingAsk > _pendingBid;
            var spreadStateChanged = positiveSpread != _lastEmittedPositiveSpread;
            var priceChanged = Math.Abs(_pendingBid - _lastEmittedBid) > 1e-9 ||
                               Math.Abs(_pendingAsk - _lastEmittedAsk) > 1e-9;
            var effectiveInterval = Math.Min(SampleIntervalMilliseconds, 200);
            if (!force && !priceChanged && !positiveSpread && !spreadStateChanged &&
                _sessionElapsed.ElapsedMilliseconds - _lastSample < effectiveInterval)
                return;

            var now = DateTime.UtcNow;
            _hasPending = false;
            if (_pendingTime > now || (now - _pendingTime).TotalSeconds > 1.5)
                return;

            _lastSample = _sessionElapsed.ElapsedMilliseconds;
            _lastEmittedPositiveSpread = positiveSpread;
            _lastEmittedBid = _pendingBid;
            _lastEmittedAsk = _pendingAsk;

            WriteQuoteLine(new
            {
                version = 1,
                kind = "quote",
                session_id = _quoteSessionId,
                sequence = ++_sequence,
                symbol = SymbolName,
                source_at = _pendingTime.ToString("o", CultureInfo.InvariantCulture),
                bridge_at = now.ToString("o", CultureInfo.InvariantCulture),
                bid = _pendingBid.ToString("R", CultureInfo.InvariantCulture),
                ask = _pendingAsk.ToString("R", CultureInfo.InvariantCulture)
            });
        }

        private void LoadIntentJournal()
        {
            if (!System.IO.File.Exists(_journalPath))
                return;
            using var fs = new FileStream(_journalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var len = fs.Length;
            if (len <= 0)
                return;
            var raw = new byte[len];
            var read = fs.Read(raw, 0, (int)len);
            if (read <= 0)
                return;
            int lastNl = Array.LastIndexOf(raw, (byte)'\n', read - 1, read);
            if (lastNl < 0)
                return;
            var text = Encoding.UTF8.GetString(raw, 0, lastNl + 1);
            foreach (var line in text.Split('\n'))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    var root = doc.RootElement;
                    var cid = root.TryGetProperty("client_intent_id", out var idEl) ? idEl.GetString() : null;
                    var stage = root.TryGetProperty("stage", out var stEl) ? stEl.GetString() : null;
                    var status = root.TryGetProperty("status", out var sEl) ? sEl.GetString() : null;
                    if (string.IsNullOrEmpty(cid) || string.IsNullOrEmpty(stage))
                        continue;
                    if (stage == "terminal")
                    {
                        _intentJournalStages[cid] = "terminal:" + (status ?? "done");
                    }
                    else
                    {
                        if (!_intentJournalStages.TryGetValue(cid, out var existing) ||
                            !existing.StartsWith("terminal:", StringComparison.Ordinal))
                        {
                            _intentJournalStages[cid] = "in_flight";
                        }
                        if (root.TryGetProperty("orig_units", out var ouEl) && ouEl.TryGetInt64(out var ouVal) && ouVal > 0L)
                        {
                            _intentInFlightOrigUnits[cid] = ouVal;
                        }
                        if (root.TryGetProperty("prior_closing_deal_ids", out var pdEl) && pdEl.ValueKind == JsonValueKind.Array)
                        {
                            var dealSet = new HashSet<long>();
                            foreach (var item in pdEl.EnumerateArray())
                            {
                                if (item.TryGetInt64(out var dealId) && dealId > 0L)
                                    dealSet.Add(dealId);
                            }
                            _intentInFlightPriorDealIds[cid] = dealSet;
                        }
                    }
                }
                catch
                {
                    // Ignore malformed line; live broker label inspection still guards execution.
                }
            }
        }

        private void RecordIntentJournal(string clientIntentId, string stage, string status)
        {
            _intentJournalStages[clientIntentId] = stage == "terminal" ? "terminal:" + status : "in_flight";
            using var fs = new FileStream(_journalPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            using var sw = new StreamWriter(fs, new UTF8Encoding(false));
            sw.WriteLine(JsonSerializer.Serialize(new
            {
                client_intent_id = clientIntentId,
                stage,
                status,
                recorded_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)
            }));
            sw.Flush();
            fs.Flush(true);
        }

        private void RecordCloseInFlightJournal(
            string clientIntentId,
            long origUnits,
            HashSet<long> priorClosingDealIds)
        {
            _intentJournalStages[clientIntentId] = "in_flight";
            _intentInFlightOrigUnits[clientIntentId] = origUnits;
            _intentInFlightPriorDealIds[clientIntentId] = priorClosingDealIds;
            using var fs = new FileStream(_journalPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            using var sw = new StreamWriter(fs, new UTF8Encoding(false));
            sw.WriteLine(JsonSerializer.Serialize(new
            {
                client_intent_id = clientIntentId,
                stage = "in_flight",
                status = "in_flight",
                orig_units = origUnits,
                prior_closing_deal_ids = priorClosingDealIds.OrderBy(id => id).ToArray(),
                recorded_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)
            }));
            sw.Flush();
            fs.Flush(true);
        }

        private static object SerializeHistoricalTrade(HistoricalTrade h)
        {
            return new
            {
                closing_deal_id = (long)h.ClosingDealId,
                broker_position_id = (long)h.PositionId,
                symbol = h.SymbolName ?? string.Empty,
                position_side = h.TradeType == TradeType.Buy ? "buy" : "sell",
                volume_units = (long)Math.Round(h.VolumeInUnits),
                entry_price = h.EntryPrice.ToString("R", CultureInfo.InvariantCulture),
                closing_price = h.ClosingPrice.ToString("R", CultureInfo.InvariantCulture),
                gross_profit_usd = h.GrossProfit.ToString("R", CultureInfo.InvariantCulture),
                net_profit_usd = h.NetProfit.ToString("R", CultureInfo.InvariantCulture),
                commissions_usd = h.Commissions.ToString("R", CultureInfo.InvariantCulture),
                swap_usd = h.Swap.ToString("R", CultureInfo.InvariantCulture),
                label = h.Label ?? string.Empty,
                entry_time = DateTime.SpecifyKind(h.EntryTime, DateTimeKind.Utc)
                    .ToString("o", CultureInfo.InvariantCulture),
                closing_time = DateTime.SpecifyKind(h.ClosingTime, DateTimeKind.Utc)
                    .ToString("o", CultureInfo.InvariantCulture)
            };
        }

        private void EmitClosePositionOutcome(
            string clientIntentId,
            long posId,
            long requestedUnits,
            long origUnits,
            long remainingUnits,
            HistoricalTrade[] newDeals,
            bool recoveredAfterCrash)
        {
            var dealUnits = newDeals.Sum(d => (long)Math.Round(d.VolumeInUnits));
            var actualClosedUnits = origUnits > 0L
                ? Math.Max(0L, origUnits - remainingUnits)
                : dealUnits;
            var unpricedUnits = Math.Max(0L, actualClosedUnits - dealUnits);
            var serializedDeals = newDeals.Select(SerializeHistoricalTrade).ToArray();

            if (newDeals.Length > 0 && dealUnits > 0L && unpricedUnits == 0L)
            {
                var weightedCloseSum = newDeals.Sum(d => d.VolumeInUnits * d.ClosingPrice);
                var totalDealVol = newDeals.Sum(d => d.VolumeInUnits);
                var vwapClose = totalDealVol > 0.0 ? weightedCloseSum / totalDealVol : newDeals[0].ClosingPrice;
                var grossProfit = newDeals.Sum(d => d.GrossProfit);
                var netProfit = newDeals.Sum(d => d.NetProfit);
                var commissions = newDeals.Sum(d => d.Commissions);
                var swap = newDeals.Sum(d => d.Swap);
                var closeStatus = remainingUnits == 0L ? "filled" : "partial";

                RecordIntentJournal(clientIntentId, "terminal", closeStatus);
                WriteExecEvent(new
                {
                    version = 1,
                    kind = "order_event",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = "demo",
                    is_live = false,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    client_intent_id = clientIntentId,
                    intent_type = "close_position",
                    broker_position_id = posId,
                    requested_units = requestedUnits,
                    closed_units = actualClosedUnits,
                    priced_units = dealUnits,
                    unpriced_units = 0L,
                    filled_units = dealUnits,
                    remaining_units = remainingUnits,
                    status = closeStatus,
                    fill_price = vwapClose.ToString("R", CultureInfo.InvariantCulture),
                    realized_gross_pnl_usd = grossProfit.ToString("R", CultureInfo.InvariantCulture),
                    realized_net_pnl_usd = netProfit.ToString("R", CultureInfo.InvariantCulture),
                    commission_usd = commissions.ToString("R", CultureInfo.InvariantCulture),
                    swap_usd = swap.ToString("R", CultureInfo.InvariantCulture),
                    deals = serializedDeals,
                    recovered_after_crash = recoveredAfterCrash
                });
            }
            else
            {
                // Never substitute current Bid/Ask, entry_price, or 0 PnL when broker History deal rows
                // are missing or incomplete; record position_closed_unpriced for subsequent snapshot backfill.
                RecordIntentJournal(clientIntentId, "terminal", "position_closed_unpriced");
                WriteExecEvent(new
                {
                    version = 1,
                    kind = "order_event",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = "demo",
                    is_live = false,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    client_intent_id = clientIntentId,
                    intent_type = "close_position",
                    broker_position_id = posId,
                    requested_units = requestedUnits,
                    closed_units = actualClosedUnits,
                    priced_units = dealUnits,
                    unpriced_units = unpricedUnits,
                    filled_units = dealUnits,
                    remaining_units = remainingUnits,
                    status = "position_closed_unpriced",
                    fill_price = (string?)null,
                    realized_gross_pnl_usd = (string?)null,
                    realized_net_pnl_usd = (string?)null,
                    commission_usd = (string?)null,
                    swap_usd = (string?)null,
                    deals = serializedDeals,
                    error_code = "broker_history_deal_pending_reconcile",
                    recovered_after_crash = recoveredAfterCrash
                });
            }

            EmitReconcileSnapshot((recoveredAfterCrash ? "recovered_in_flight_close:" : "after_close:") + clientIntentId);
        }

        private void PollOrderIntents()
        {
            if (!System.IO.File.Exists(_intentsPath))
                return;
            using var fs = new FileStream(_intentsPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (fs.Length < _intentsOffset)
            {
                WriteOrderEvent(null, null, "rejected", "order_intents_file_truncated");
                _intentsOffset = fs.Length;
                return;
            }
            if (fs.Length == _intentsOffset)
                return;

            fs.Seek(_intentsOffset, SeekOrigin.Begin);
            var bufLen = (int)Math.Min(262144L, fs.Length - _intentsOffset);
            var buf = new byte[bufLen];
            var read = fs.Read(buf, 0, bufLen);
            if (read <= 0)
                return;

            // Advance _intentsOffset ONLY up to the last complete '\n'-terminated byte!
            int lastNewlineIdx = Array.LastIndexOf(buf, (byte)'\n', read - 1, read);
            if (lastNewlineIdx < 0)
                return;

            int completeBytes = lastNewlineIdx + 1;
            _intentsOffset += completeBytes;
            var chunk = Encoding.UTF8.GetString(buf, 0, completeBytes);
            foreach (var line in chunk.Split('\n'))
            {
                var raw = line.Trim();
                if (raw.Length > 0)
                    ProcessOrderIntent(raw);
            }
        }

        private void ProcessOrderIntent(string rawJson)
        {
            if (!VerifyDemoSafetyOrStop("order_dispatch"))
                return;

            JsonDocument doc;
            try
            {
                doc = JsonDocument.Parse(rawJson);
            }
            catch (Exception ex)
            {
                WriteOrderEvent(null, null, "rejected", "invalid_intent_json:" + ex.GetType().Name);
                return;
            }

            using (doc)
            {
                var root = doc.RootElement;
                var clientIntentId = root.TryGetProperty("client_intent_id", out var idEl)
                    ? idEl.GetString() ?? string.Empty
                    : string.Empty;
                if (string.IsNullOrWhiteSpace(clientIntentId))
                    return;

                var intentType = root.TryGetProperty("intent_type", out var itEl)
                    ? itEl.GetString() ?? string.Empty
                    : string.Empty;
                var label = "el:" + clientIntentId;

                if (_intentJournalStages.TryGetValue(clientIntentId, out var priorStage))
                {
                    if (priorStage.StartsWith("terminal:", StringComparison.Ordinal))
                    {
                        WriteOrderEvent(clientIntentId, intentType, "duplicate_ignored", "duplicate_client_intent_id");
                        return;
                    }
                    if (priorStage == "in_flight")
                    {
                        RecoverInFlightIntentAfterCrash(root, clientIntentId, intentType, label);
                        return;
                    }
                }

                if (intentType == "market_order" && Positions.FindAll(label, SymbolName).Length > 0)
                {
                    RecordIntentJournal(clientIntentId, "terminal", "duplicate_ignored");
                    WriteOrderEvent(clientIntentId, intentType, "duplicate_ignored", "duplicate_broker_label_exists");
                    return;
                }

                var symbol = root.TryGetProperty("symbol", out var symEl)
                    ? symEl.GetString() ?? string.Empty
                    : string.Empty;
                if (symbol != SymbolName)
                {
                    RecordIntentJournal(clientIntentId, "terminal", "rejected");
                    WriteOrderEvent(clientIntentId, intentType, "rejected", "symbol_mismatch");
                    return;
                }

                // Mandatory UTC expires_at TTL check on all intents before execution
                if (!TryValidateIntentExpiryUtc(root, out var expiryError))
                {
                    var termStatus = expiryError == "intent_expired_before_execution" ? "expired" : "rejected";
                    RecordIntentJournal(clientIntentId, "terminal", termStatus);
                    WriteOrderEvent(clientIntentId, intentType, termStatus, expiryError);
                    EmitReconcileSnapshot("after_intent_expired:" + clientIntentId);
                    return;
                }

                if (intentType == "reconcile")
                {
                    RecordIntentJournal(clientIntentId, "terminal", "filled");
                    EmitReconcileSnapshot("explicit_intent:" + clientIntentId);
                    return;
                }

                if (!AllowDemoOrders)
                {
                    RecordIntentJournal(clientIntentId, "terminal", "rejected");
                    WriteOrderEvent(clientIntentId, intentType, "rejected", "bridge_demo_orders_disabled");
                    EmitReconcileSnapshot("demo_orders_disabled:" + clientIntentId);
                    return;
                }

                if (intentType == "market_order")
                {
                    ExecuteMarketIntent(root, clientIntentId, label);
                }
                else if (intentType == "close_position")
                {
                    ExecuteCloseIntent(root, clientIntentId);
                }
                else if (intentType == "modify_protection")
                {
                    ExecuteModifyProtectionIntent(root, clientIntentId);
                }
                else if (intentType == "cancel_pending_order")
                {
                    ExecuteCancelPendingIntent(root, clientIntentId);
                }
                else
                {
                    RecordIntentJournal(clientIntentId, "terminal", "rejected");
                    WriteOrderEvent(clientIntentId, intentType, "rejected", "unsupported_intent_type");
                }
            }
        }

        private static bool TryValidateIntentExpiryUtc(JsonElement root, out string errorCode)
        {
            errorCode = string.Empty;
            if (!root.TryGetProperty("expires_at", out var expEl) || expEl.ValueKind != JsonValueKind.String)
            {
                errorCode = "missing_intent_expires_at";
                return false;
            }
            var rawExp = expEl.GetString();
            if (string.IsNullOrWhiteSpace(rawExp) ||
                !DateTimeOffset.TryParse(
                    rawExp,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var expDto))
            {
                errorCode = "invalid_intent_expires_at";
                return false;
            }
            if (DateTimeOffset.UtcNow >= expDto)
            {
                errorCode = "intent_expired_before_execution";
                return false;
            }
            return true;
        }

        private void RecoverInFlightIntentAfterCrash(
            JsonElement root,
            string clientIntentId,
            string intentType,
            string label)
        {
            // Never blindly re-execute an in-flight intent after a crash.
            if (intentType == "market_order")
            {
                var openMatch = Positions.FirstOrDefault(p => p.SymbolName == SymbolName && p.Label == label);
                if (openMatch != null)
                {
                    var units = (long)Math.Round(openMatch.VolumeInUnits);
                    RecordIntentJournal(clientIntentId, "terminal", "filled");
                    WriteExecEvent(new
                    {
                        version = 1,
                        kind = "order_event",
                        bridge_session_id = _bridgeSessionId,
                        bridge_started_at = _bridgeStartedAt,
                        event_seq = ++_execEventSeq,
                        emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                        account_mode = "demo",
                        is_live = false,
                        account_number = (long)Account.Number,
                        account_currency = AccountCurrencyName,
                        client_intent_id = clientIntentId,
                        intent_type = "market_order",
                        side = openMatch.TradeType == TradeType.Buy ? "buy" : "sell",
                        status = "filled",
                        broker_position_id = (long)openMatch.Id,
                        requested_units = units,
                        filled_units = units,
                        fill_price = openMatch.EntryPrice.ToString("R", CultureInfo.InvariantCulture),
                        stop_loss = openMatch.StopLoss?.ToString("R", CultureInfo.InvariantCulture),
                        take_profit = openMatch.TakeProfit?.ToString("R", CultureInfo.InvariantCulture),
                        sl_confirmed = openMatch.StopLoss.HasValue,
                        tp_confirmed = openMatch.TakeProfit.HasValue,
                        commission_usd = openMatch.Commissions.ToString("R", CultureInfo.InvariantCulture),
                        recovered_after_crash = true
                    });
                    EmitReconcileSnapshot("recovered_in_flight_fill:" + clientIntentId);
                    return;
                }
            }
            else if (intentType == "close_position")
            {
                var posId = root.TryGetProperty("broker_position_id", out var pEl) && pEl.TryGetInt64(out var pVal)
                    ? pVal
                    : 0L;
                var reqCloseUnits = root.TryGetProperty("volume_units", out var uEl) &&
                                    uEl.ValueKind == JsonValueKind.Number && uEl.TryGetInt64(out var uVal)
                    ? uVal
                    : 0L;
                if (posId > 0L)
                {
                    var stillOpen = Positions.FirstOrDefault(p => p.Id == posId && p.SymbolName == SymbolName);
                    var remainingUnits = stillOpen == null ? 0L : (long)Math.Round(stillOpen.VolumeInUnits);
                    _intentInFlightOrigUnits.TryGetValue(clientIntentId, out var origUnits);
                    if (!_intentInFlightPriorDealIds.TryGetValue(clientIntentId, out var priorDeals))
                    {
                        priorDeals = new HashSet<long>();
                    }
                    var newDeals = History
                        .Where(h => h.PositionId == posId && !priorDeals.Contains((long)h.ClosingDealId))
                        .OrderBy(h => h.ClosingTime)
                        .ThenBy(h => h.ClosingDealId)
                        .ToArray();

                    if ((origUnits > 0L && remainingUnits < origUnits) ||
                        (origUnits == 0L && stillOpen == null && newDeals.Length > 0))
                    {
                        var effReq = reqCloseUnits > 0L
                            ? reqCloseUnits
                            : (origUnits > 0L ? origUnits - remainingUnits : newDeals.Sum(d => (long)Math.Round(d.VolumeInUnits)));
                        EmitClosePositionOutcome(
                            clientIntentId,
                            posId,
                            effReq,
                            origUnits,
                            remainingUnits,
                            newDeals,
                            recoveredAfterCrash: true);
                        return;
                    }
                }
            }

            RecordIntentJournal(clientIntentId, "terminal", "unknown_execution");
            WriteOrderEvent(
                clientIntentId,
                intentType,
                "unknown_execution",
                "bridge_crash_in_flight_outcome_ambiguous");
            EmitReconcileSnapshot("unknown_in_flight:" + clientIntentId);
        }

        private void ExecuteMarketIntent(JsonElement root, string clientIntentId, string label)
        {
            var side = root.TryGetProperty("side", out var sEl) ? sEl.GetString() : null;
            var units = root.TryGetProperty("volume_units", out var uEl) && uEl.TryGetInt64(out var uVal) ? uVal : 0L;
            double? slPips = root.TryGetProperty("stop_loss_pips", out var slEl) && slEl.ValueKind == JsonValueKind.Number
                ? slEl.GetDouble()
                : null;
            double? tpPips = root.TryGetProperty("take_profit_pips", out var tpEl) && tpEl.ValueKind == JsonValueKind.Number
                ? tpEl.GetDouble()
                : null;

            if ((side != "buy" && side != "sell") || units <= 0 || units % 1000 != 0)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteOrderEvent(clientIntentId, "market_order", "rejected", "invalid_side_or_volume");
                return;
            }

            RecordIntentJournal(clientIntentId, "in_flight", "in_flight");
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = "market_order",
                side,
                requested_units = units,
                filled_units = 0L,
                status = "accepted",
                broker_accept_only = true
            });

            var tradeType = side == "buy" ? TradeType.Buy : TradeType.Sell;
            var res = ExecuteMarketOrder(tradeType, SymbolName, units, label, slPips, tpPips);
            if (!res.IsSuccessful || res.Position == null)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteExecEvent(new
                {
                    version = 1,
                    kind = "order_event",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = "demo",
                    is_live = false,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    client_intent_id = clientIntentId,
                    intent_type = "market_order",
                    side,
                    requested_units = units,
                    filled_units = 0L,
                    status = "rejected",
                    error_code = res.Error?.ToString() ?? "broker_execution_failed"
                });
                EmitReconcileSnapshot("after_reject:" + clientIntentId);
                return;
            }

            var pos = res.Position;
            var filledUnits = (long)Math.Round(pos.VolumeInUnits);
            var slConfirmed = !slPips.HasValue || pos.StopLoss.HasValue;
            var tpConfirmed = !tpPips.HasValue || pos.TakeProfit.HasValue;
            var status = filledUnits < units ? "partial" : "filled";

            RecordIntentJournal(clientIntentId, "terminal", status);
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = "market_order",
                side,
                status,
                broker_position_id = (long)pos.Id,
                requested_units = units,
                filled_units = filledUnits,
                fill_price = pos.EntryPrice.ToString("R", CultureInfo.InvariantCulture),
                stop_loss = pos.StopLoss?.ToString("R", CultureInfo.InvariantCulture),
                take_profit = pos.TakeProfit?.ToString("R", CultureInfo.InvariantCulture),
                sl_confirmed = slConfirmed,
                tp_confirmed = tpConfirmed,
                commission_usd = pos.Commissions.ToString("R", CultureInfo.InvariantCulture)
            });
            EmitReconcileSnapshot("after_fill:" + clientIntentId);
        }

        private void ExecuteCloseIntent(JsonElement root, string clientIntentId)
        {
            var posId = root.TryGetProperty("broker_position_id", out var pEl) && pEl.TryGetInt64(out var pVal)
                ? pVal
                : 0L;
            var closeUnits = root.TryGetProperty("volume_units", out var uEl) &&
                             uEl.ValueKind == JsonValueKind.Number && uEl.TryGetInt64(out var uVal)
                ? uVal
                : 0L;

            // Strictly close by explicit Position.Id; NEVER open an opposite hedging market order.
            if (posId <= 0)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteOrderEvent(clientIntentId, "close_position", "rejected", "missing_broker_position_id");
                return;
            }

            var pos = Positions.FirstOrDefault(p => p.Id == posId && p.SymbolName == SymbolName);
            if (pos == null)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteExecEvent(new
                {
                    version = 1,
                    kind = "order_event",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = "demo",
                    is_live = false,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    client_intent_id = clientIntentId,
                    intent_type = "close_position",
                    broker_position_id = posId,
                    status = "rejected",
                    error_code = "position_not_found_or_already_closed"
                });
                EmitReconcileSnapshot("close_missing_pos:" + clientIntentId);
                return;
            }

            var origUnits = (long)Math.Round(pos.VolumeInUnits);
            var targetCloseUnits = closeUnits > 0 && closeUnits < origUnits ? closeUnits : origUnits;
            var priorClosingDealIds = new HashSet<long>(
                History.Where(h => h.PositionId == posId).Select(h => (long)h.ClosingDealId));

            RecordCloseInFlightJournal(clientIntentId, origUnits, priorClosingDealIds);
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = "close_position",
                broker_position_id = posId,
                requested_units = targetCloseUnits,
                filled_units = 0L,
                status = "accepted",
                broker_accept_only = true
            });

            var res = targetCloseUnits < origUnits
                ? ClosePosition(pos, targetCloseUnits)
                : ClosePosition(pos);

            if (!res.IsSuccessful)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteExecEvent(new
                {
                    version = 1,
                    kind = "order_event",
                    bridge_session_id = _bridgeSessionId,
                    bridge_started_at = _bridgeStartedAt,
                    event_seq = ++_execEventSeq,
                    emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    account_mode = "demo",
                    is_live = false,
                    account_number = (long)Account.Number,
                    account_currency = AccountCurrencyName,
                    client_intent_id = clientIntentId,
                    intent_type = "close_position",
                    broker_position_id = posId,
                    requested_units = targetCloseUnits,
                    filled_units = 0L,
                    status = "rejected",
                    error_code = res.Error?.ToString() ?? "close_position_failed"
                });
                EmitReconcileSnapshot("after_close_reject:" + clientIntentId);
                return;
            }

            var stillOpen = Positions.FirstOrDefault(p => p.Id == posId && p.SymbolName == SymbolName);
            var remainingUnits = stillOpen == null ? 0L : (long)Math.Round(stillOpen.VolumeInUnits);
            var newDeals = History
                .Where(h => h.PositionId == posId && !priorClosingDealIds.Contains((long)h.ClosingDealId))
                .OrderBy(h => h.ClosingTime)
                .ThenBy(h => h.ClosingDealId)
                .ToArray();

            EmitClosePositionOutcome(
                clientIntentId,
                posId,
                targetCloseUnits,
                origUnits,
                remainingUnits,
                newDeals,
                recoveredAfterCrash: false);
        }

        private void ExecuteModifyProtectionIntent(JsonElement root, string clientIntentId)
        {
            var posId = root.TryGetProperty("broker_position_id", out var pEl) && pEl.TryGetInt64(out var pVal)
                ? pVal
                : 0L;
            double? slPrice = root.TryGetProperty("stop_loss_price", out var slEl) && slEl.ValueKind == JsonValueKind.Number
                ? slEl.GetDouble()
                : null;
            double? tpPrice = root.TryGetProperty("take_profit_price", out var tpEl) && tpEl.ValueKind == JsonValueKind.Number
                ? tpEl.GetDouble()
                : null;

            var pos = Positions.FirstOrDefault(p => p.Id == posId && p.SymbolName == SymbolName);
            if (pos == null)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteOrderEvent(clientIntentId, "modify_protection", "rejected", "position_not_found");
                return;
            }

            RecordIntentJournal(clientIntentId, "in_flight", "in_flight");
            var res = ModifyPosition(pos, slPrice, tpPrice, ProtectionType.Absolute);
            var modStatus = res.IsSuccessful ? "filled" : "rejected";
            RecordIntentJournal(clientIntentId, "terminal", modStatus);
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = "modify_protection",
                broker_position_id = posId,
                status = modStatus,
                stop_loss = res.Position?.StopLoss?.ToString("R", CultureInfo.InvariantCulture),
                take_profit = res.Position?.TakeProfit?.ToString("R", CultureInfo.InvariantCulture),
                sl_confirmed = !slPrice.HasValue || (res.Position?.StopLoss.HasValue ?? false),
                tp_confirmed = !tpPrice.HasValue || (res.Position?.TakeProfit.HasValue ?? false),
                error_code = res.IsSuccessful ? null : (res.Error?.ToString() ?? "modify_failed")
            });
            EmitReconcileSnapshot("after_modify:" + clientIntentId);
        }

        private void ExecuteCancelPendingIntent(JsonElement root, string clientIntentId)
        {
            var ordId = root.TryGetProperty("broker_order_id", out var oEl) && oEl.TryGetInt64(out var oVal)
                ? oVal
                : 0L;
            var ord = PendingOrders.FirstOrDefault(o => o.Id == ordId && o.SymbolName == SymbolName);
            if (ord == null)
            {
                RecordIntentJournal(clientIntentId, "terminal", "rejected");
                WriteOrderEvent(clientIntentId, "cancel_pending_order", "rejected", "pending_order_not_found");
                return;
            }

            RecordIntentJournal(clientIntentId, "in_flight", "in_flight");
            var res = CancelPendingOrder(ord);
            var canStatus = res.IsSuccessful ? "cancelled" : "rejected";
            RecordIntentJournal(clientIntentId, "terminal", canStatus);
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = "cancel_pending_order",
                broker_order_id = ordId,
                status = canStatus,
                error_code = res.IsSuccessful ? null : (res.Error?.ToString() ?? "cancel_failed")
            });
            EmitReconcileSnapshot("after_cancel:" + clientIntentId);
        }

        private void WriteOrderEvent(
            string? clientIntentId,
            string? intentType,
            string status,
            string? errorCode)
        {
            WriteExecEvent(new
            {
                version = 1,
                kind = "order_event",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                client_intent_id = clientIntentId,
                intent_type = intentType,
                status,
                error_code = errorCode
            });
        }

        private void EmitReconcileSnapshot(string trigger)
        {
            if (_execWriter == null)
                return;
            var openPositions = Positions
                .Where(p => p.SymbolName == SymbolName)
                .Select(p => new
                {
                    broker_position_id = (long)p.Id,
                    symbol = p.SymbolName,
                    side = p.TradeType == TradeType.Buy ? "buy" : "sell",
                    volume_units = (long)Math.Round(p.VolumeInUnits),
                    entry_price = p.EntryPrice.ToString("R", CultureInfo.InvariantCulture),
                    stop_loss = p.StopLoss?.ToString("R", CultureInfo.InvariantCulture),
                    take_profit = p.TakeProfit?.ToString("R", CultureInfo.InvariantCulture),
                    sl_confirmed = p.StopLoss.HasValue,
                    tp_confirmed = p.TakeProfit.HasValue,
                    gross_profit_usd = p.GrossProfit.ToString("R", CultureInfo.InvariantCulture),
                    net_profit_usd = p.NetProfit.ToString("R", CultureInfo.InvariantCulture),
                    commissions_usd = p.Commissions.ToString("R", CultureInfo.InvariantCulture),
                    swap_usd = p.Swap.ToString("R", CultureInfo.InvariantCulture),
                    label = p.Label ?? string.Empty,
                    entry_time = DateTime.SpecifyKind(p.EntryTime, DateTimeKind.Utc)
                        .ToString("o", CultureInfo.InvariantCulture)
                })
                .ToArray();

            var pendingOrders = PendingOrders
                .Where(o => o.SymbolName == SymbolName)
                .Select(o => new
                {
                    broker_order_id = (long)o.Id,
                    symbol = o.SymbolName,
                    order_type = o.OrderType.ToString(),
                    side = o.TradeType == TradeType.Buy ? "buy" : "sell",
                    volume_units = (long)Math.Round(o.VolumeInUnits),
                    target_price = o.TargetPrice.ToString("R", CultureInfo.InvariantCulture),
                    label = o.Label ?? string.Empty
                })
                .ToArray();

            var closedDeals = History
                .Where(h => h.SymbolName == SymbolName)
                .OrderByDescending(h => h.ClosingTime)
                .ThenByDescending(h => h.ClosingDealId)
                .Take(200)
                .OrderBy(h => h.ClosingTime)
                .ThenBy(h => h.ClosingDealId)
                .Select(SerializeHistoricalTrade)
                .ToArray();

            WriteExecEvent(new
            {
                version = 1,
                kind = "reconcile_snapshot",
                bridge_session_id = _bridgeSessionId,
                bridge_started_at = _bridgeStartedAt,
                event_seq = ++_execEventSeq,
                emitted_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                trigger,
                account_mode = "demo",
                is_live = false,
                account_number = (long)Account.Number,
                account_currency = AccountCurrencyName,
                allow_demo_orders = AllowDemoOrders,
                symbol = SymbolName,
                balance_usd = Account.Balance.ToString("R", CultureInfo.InvariantCulture),
                equity_usd = Account.Equity.ToString("R", CultureInfo.InvariantCulture),
                margin_used_usd = Account.Margin.ToString("R", CultureInfo.InvariantCulture),
                free_margin_usd = Account.FreeMargin.ToString("R", CultureInfo.InvariantCulture),
                open_positions = openPositions,
                pending_orders = pendingOrders,
                closed_deals = closedDeals
            });
        }

        protected override void OnStop()
        {
            _enabled = false;
            Timer.Stop();
            try
            {
                CloseCurrentQuoteSession();
                if (!_safetyRefused && !Account.IsLive)
                    EmitReconcileSnapshot("on_stop");
            }
            finally
            {
                _execWriter?.Dispose();
                _execWriter = null;
            }
            Print("EVENTLENS_CLI_BRIDGE|stopped|bridge_session={0}|quotes={1}", _bridgeSessionId, _sequence);
        }

        private void WriteQuoteLine(object value)
        {
            _quoteWriter?.WriteLine(JsonSerializer.Serialize(value));
        }

        private void WriteExecEvent(object value)
        {
            _execWriter?.WriteLine(JsonSerializer.Serialize(value));
        }
    }
}
