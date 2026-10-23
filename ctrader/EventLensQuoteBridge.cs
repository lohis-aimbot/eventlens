using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using cAlgo.API;

namespace cAlgo.Robots
{
    // Local Demo quotes only. No order API, HTTP client, account ID or credentials.
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class EventLensQuoteBridge : Robot
    {
        [Parameter("Duration seconds", DefaultValue = 300, MinValue = 10, MaxValue = 600)]
        public int DurationSeconds { get; set; }

        [Parameter("Sample interval ms", DefaultValue = 200, MinValue = 100, MaxValue = 10000)]
        public int SampleIntervalMilliseconds { get; set; }

        private StreamWriter _writer;
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
            if (Account.IsLive || IsBacktesting || RunningMode != RunningMode.RealTime ||
                SymbolName != "EURUSD" || Symbol.PipSize <= 0)
            {
                Print("EVENTLENS_QUOTES|refused|requires=EURUSD_realtime_Demo|orders=none");
                Stop();
                return;
            }
            _session = Guid.NewGuid().ToString();
            _ticks = MarketData.GetTicks(SymbolName);
            if (_ticks.Count > 0)
                _lastTickTime = DateTime.SpecifyKind(_ticks.LastTick.Time, DateTimeKind.Utc);
            // Relative path is restricted to this cBot's sandbox by AccessRights.None.
            // Each run creates a new file and never truncates an earlier run.
            _writer = new StreamWriter(new FileStream("quotes-" + _session + ".jsonl",
                FileMode.CreateNew, FileAccess.Write, FileShare.Read), new UTF8Encoding(false));
            _writer.AutoFlush = true;
            Write(new { version = 1, kind = "start", session_id = _session,
                account_mode = "demo", running_mode = "realtime", symbol = SymbolName,
                started_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                sample_interval_ms = SampleIntervalMilliseconds, duration_seconds = DurationSeconds,
                digits = Symbol.Digits,
                pip_size = Symbol.PipSize.ToString("R", CultureInfo.InvariantCulture),
                commission = Symbol.Commission.ToString("R", CultureInfo.InvariantCulture),
                commission_type = Symbol.CommissionType.ToString() });
            _enabled = true;
            _elapsed.Start();
            Timer.Start(TimeSpan.FromMilliseconds(50));
            Print("EVENTLENS_QUOTES|started|session={0}|symbol={1}|orders=none|duration_seconds={2}",
                _session, SymbolName, DurationSeconds);
        }

        protected override void OnTick()
        {
            if (!_enabled) return;
            if (Account.IsLive || IsBacktesting || RunningMode != RunningMode.RealTime ||
                _elapsed.Elapsed.TotalSeconds >= DurationSeconds)
            {
                Stop();
                return;
            }
            if (_ticks.Count == 0)
                return;
            var tick = _ticks.LastTick;
            if (tick.Bid <= 0 || tick.Ask < tick.Bid || double.IsNaN(tick.Bid) ||
                double.IsNaN(tick.Ask) || double.IsInfinity(tick.Bid) || double.IsInfinity(tick.Ask))
                return;
            var tickTime = DateTime.SpecifyKind(tick.Time, DateTimeKind.Utc);
            // Ignore pre-start cached history or duplicate callbacks for the same LastTick.
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
            if (_elapsed.Elapsed.TotalSeconds >= DurationSeconds)
            {
                Stop();
                return;
            }
            FlushPendingIfDue();
        }

        private void FlushPendingIfDue(bool force = false)
        {
            if (!_hasPending)
                return;
            var positiveSpread = _pendingAsk > _pendingBid;
            var spreadStateChanged = positiveSpread != _lastEmittedPositiveSpread;
            var priceChanged = Math.Abs(_pendingBid - _lastEmittedBid) > 1e-9 ||
                               Math.Abs(_pendingAsk - _lastEmittedAsk) > 1e-9;
            var effectiveInterval = Math.Min(SampleIntervalMilliseconds, 200);
            if (!force && !priceChanged && !positiveSpread && !spreadStateChanged &&
                _elapsed.ElapsedMilliseconds - _lastSample < effectiveInterval)
                return;
            var now = DateTime.UtcNow;
            _hasPending = false;
            if (_pendingTime > now || (now - _pendingTime).TotalSeconds > 1.5)
                return;
            _lastSample = _elapsed.ElapsedMilliseconds;
            _lastEmittedPositiveSpread = positiveSpread;
            _lastEmittedBid = _pendingBid;
            _lastEmittedAsk = _pendingAsk;
            // Tick.Time follows the robot's UTC timezone; it is not a receipt clock.
            Write(new { version = 1, kind = "quote", session_id = _session, sequence = ++_sequence,
                symbol = SymbolName, source_at = _pendingTime.ToString("o", CultureInfo.InvariantCulture),
                bridge_at = now.ToString("o", CultureInfo.InvariantCulture),
                bid = _pendingBid.ToString("R", CultureInfo.InvariantCulture),
                ask = _pendingAsk.ToString("R", CultureInfo.InvariantCulture) });
        }

        protected override void OnStop()
        {
            _enabled = false;
            if (_writer == null) return;
            try
            {
                FlushPendingIfDue(true);
                Write(new { version = 1, kind = "stop", session_id = _session,
                    stopped_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    quotes = _sequence, raw_ticks = _rawTicks,
                    raw_zero_spread_ticks = _rawZeroSpreadTicks,
                    symbol_mismatch_ticks = _symbolMismatchTicks,
                    throttled_ticks = _rawTicks - _sequence });
            }
            finally { _writer.Dispose(); _writer = null; }
            Print("EVENTLENS_QUOTES|stopped|quotes={0}|raw_ticks={1}|raw_zero_spread={2}|orders=none",
                _sequence, _rawTicks, _rawZeroSpreadTicks);
        }

        private void Write(object value) { _writer.WriteLine(JsonSerializer.Serialize(value)); }
    }
}
