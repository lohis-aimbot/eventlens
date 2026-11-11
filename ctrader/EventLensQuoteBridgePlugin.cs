using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Plugins
{
    // Local Demo EURUSD quotes only, controllable via cTrader Desktop Local MCP (startPlugin/stopPlugin).
    // Strictly AccessRights.None, Demo-only hard check, zero order APIs or credentials.
    [Plugin(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class EventLensQuoteBridgePlugin : Plugin
    {
        private const string TargetSymbolName = "EURUSD";
        private const int DefaultDurationSeconds = 86400;
        private const int DefaultSampleIntervalMilliseconds = 200;
        private const double RotationIntervalSeconds = 43200.0; // Rotate every 12h inside 86400s QuoteStore bound

        private StreamWriter _writer;
        private Symbol _symbol;
        private Ticks _ticks;
        private string _session;
        private readonly Stopwatch _elapsed = new Stopwatch();
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
        private bool _enabled;

        protected override void OnStart()
        {
            if (Account.IsLive)
            {
                Print("EVENTLENS_QUOTES_PLUGIN|refused|requires=Demo_account|orders=none");
                return;
            }
            _symbol = Symbols.GetSymbol(TargetSymbolName);
            if (_symbol == null || _symbol.PipSize <= 0)
            {
                Print("EVENTLENS_QUOTES_PLUGIN|refused|requires=EURUSD_symbol|orders=none");
                return;
            }
            _ticks = MarketData.GetTicks(TargetSymbolName);
            if (_ticks.Count > 0)
                _lastTickTime = DateTime.SpecifyKind(_ticks.LastTick.Time, DateTimeKind.Utc);

            StartNewSession();
            _enabled = true;
            Account.Switched += OnAccountSwitched;
            _ticks.Tick += OnTicksTick;
            _symbol.Tick += OnSymbolTick;
            Timer.Start(TimeSpan.FromMilliseconds(50));
        }

        private void StartNewSession()
        {
            _session = Guid.NewGuid().ToString();
            _sequence = 0;
            _rawTicks = 0;
            _rawZeroSpreadTicks = 0;
            _symbolMismatchTicks = 0;
            _hasPending = false;
            _lastEmittedPositiveSpread = false;
            _lastEmittedBid = 0.0;
            _lastEmittedAsk = 0.0;
            _lastSample = -10000;
            _elapsed.Restart();

            _writer = new StreamWriter(
                new FileStream("quotes-" + _session + ".jsonl", FileMode.CreateNew, FileAccess.Write, FileShare.Read),
                new UTF8Encoding(false)
            );
            _writer.AutoFlush = true;
            Write(new
            {
                version = 1,
                kind = "start",
                session_id = _session,
                account_mode = "demo",
                running_mode = "realtime",
                symbol = TargetSymbolName,
                started_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                sample_interval_ms = DefaultSampleIntervalMilliseconds,
                duration_seconds = DefaultDurationSeconds,
                digits = _symbol.Digits,
                pip_size = _symbol.PipSize.ToString("R", CultureInfo.InvariantCulture),
                commission = _symbol.Commission.ToString("R", CultureInfo.InvariantCulture),
                commission_type = _symbol.CommissionType.ToString()
            });
            Print("EVENTLENS_QUOTES_PLUGIN|started|session={0}|symbol={1}|orders=none|duration_seconds={2}",
                _session, TargetSymbolName, DefaultDurationSeconds);
        }

        private void OnAccountSwitched(AccountSwitchedEventArgs args)
        {
            if (Account.IsLive)
                AbortOnLiveSwitch();
        }

        private void OnTicksTick(TicksTickEventArgs args)
        {
            if (!_enabled) return;
            if (Account.IsLive)
            {
                AbortOnLiveSwitch();
                return;
            }
            IngestLatestTick();
        }

        private void OnSymbolTick(SymbolTickEventArgs args)
        {
            if (!_enabled) return;
            if (Account.IsLive)
            {
                AbortOnLiveSwitch();
                return;
            }
            if (_ticks != null && _ticks.Count > 0)
            {
                IngestLatestTick();
                return;
            }
            IngestQuoteValues(DateTime.SpecifyKind(Server.TimeInUtc, DateTimeKind.Utc), args.Bid, args.Ask);
        }

        protected override void OnTimer()
        {
            if (!_enabled) return;
            if (Account.IsLive)
            {
                AbortOnLiveSwitch();
                return;
            }
            if (_elapsed.Elapsed.TotalSeconds >= RotationIntervalSeconds)
            {
                CloseActiveSession();
                StartNewSession();
                return;
            }
            IngestLatestTick();
            FlushPendingIfDue();
        }

        private void IngestLatestTick()
        {
            if (_ticks == null || _ticks.Count == 0)
                return;
            var tick = _ticks.LastTick;
            IngestQuoteValues(DateTime.SpecifyKind(tick.Time, DateTimeKind.Utc), tick.Bid, tick.Ask);
        }

        private void IngestQuoteValues(DateTime tickTime, double bid, double ask)
        {
            if (bid <= 0 || ask < bid || double.IsNaN(bid) ||
                double.IsNaN(ask) || double.IsInfinity(bid) || double.IsInfinity(ask))
                return;
            if (tickTime <= _lastTickTime)
                return;
            _lastTickTime = tickTime;
            _rawTicks++;
            if (ask == bid)
                _rawZeroSpreadTicks++;
            if (Math.Abs(_symbol.Bid - bid) > 1e-9 || Math.Abs(_symbol.Ask - ask) > 1e-9)
                _symbolMismatchTicks++;
            _pendingTime = tickTime;
            _pendingBid = bid;
            _pendingAsk = ask;
            _hasPending = true;
            FlushPendingIfDue();
        }

        private void FlushPendingIfDue(bool force = false)
        {
            if (!_hasPending || _writer == null)
                return;
            var positiveSpread = _pendingAsk > _pendingBid;
            var spreadStateChanged = positiveSpread != _lastEmittedPositiveSpread;
            var priceChanged = Math.Abs(_pendingBid - _lastEmittedBid) > 1e-9 ||
                               Math.Abs(_pendingAsk - _lastEmittedAsk) > 1e-9;
            if (!force && !priceChanged && !positiveSpread && !spreadStateChanged &&
                _elapsed.ElapsedMilliseconds - _lastSample < DefaultSampleIntervalMilliseconds)
                return;
            var now = DateTime.UtcNow;
            _hasPending = false;
            if (_pendingTime > now || (now - _pendingTime).TotalSeconds > 1.5)
                return;
            _lastSample = _elapsed.ElapsedMilliseconds;
            _lastEmittedPositiveSpread = positiveSpread;
            _lastEmittedBid = _pendingBid;
            _lastEmittedAsk = _pendingAsk;
            Write(new
            {
                version = 1,
                kind = "quote",
                session_id = _session,
                sequence = ++_sequence,
                symbol = TargetSymbolName,
                source_at = _pendingTime.ToString("o", CultureInfo.InvariantCulture),
                bridge_at = now.ToString("o", CultureInfo.InvariantCulture),
                bid = _pendingBid.ToString("R", CultureInfo.InvariantCulture),
                ask = _pendingAsk.ToString("R", CultureInfo.InvariantCulture)
            });
        }

        private void UnsubscribeHandlers()
        {
            Account.Switched -= OnAccountSwitched;
            if (_ticks != null)
                _ticks.Tick -= OnTicksTick;
            if (_symbol != null)
                _symbol.Tick -= OnSymbolTick;
        }

        private void AbortOnLiveSwitch()
        {
            _enabled = false;
            UnsubscribeHandlers();
            CloseActiveSession();
            Print("EVENTLENS_QUOTES_PLUGIN|aborted_on_live_account|orders=none");
        }

        private void CloseActiveSession()
        {
            if (_writer == null) return;
            try
            {
                FlushPendingIfDue(true);
                Write(new
                {
                    version = 1,
                    kind = "stop",
                    session_id = _session,
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
                _writer.Dispose();
                _writer = null;
            }
            Print("EVENTLENS_QUOTES_PLUGIN|stopped|quotes={0}|raw_ticks={1}|raw_zero_spread={2}|orders=none",
                _sequence, _rawTicks, _rawZeroSpreadTicks);
        }

        protected override void OnStop()
        {
            _enabled = false;
            UnsubscribeHandlers();
            CloseActiveSession();
        }

        private void Write(object value)
        {
            _writer.WriteLine(JsonSerializer.Serialize(value));
        }
    }
}
