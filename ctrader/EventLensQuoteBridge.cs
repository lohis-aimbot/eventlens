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
        [Parameter("Duration seconds", DefaultValue = 120, MinValue = 10, MaxValue = 600)]
        public int DurationSeconds { get; set; }

        [Parameter("Sample interval ms", DefaultValue = 1000, MinValue = 1000, MaxValue = 10000)]
        public int SampleIntervalMilliseconds { get; set; }

        private StreamWriter _writer;
        private Ticks _ticks;
        private string _session;
        private readonly Stopwatch _elapsed = new Stopwatch();
        private long _lastSample = -10000;
        private long _sequence;
        private bool _enabled;

        protected override void OnStart()
        {
            if (Account.IsLive || IsBacktesting || RunningMode != RunningMode.RealTime || SymbolName != "EURUSD")
            {
                Print("EVENTLENS_QUOTES|refused|requires=EURUSD_realtime_Demo|orders=none");
                Stop();
                return;
            }
            _session = Guid.NewGuid().ToString();
            _ticks = MarketData.GetTicks(SymbolName);
            // Relative path is restricted to this cBot's sandbox by AccessRights.None.
            // Each run creates a new file and never truncates an earlier run.
            _writer = new StreamWriter(new FileStream("quotes-" + _session + ".jsonl",
                FileMode.CreateNew, FileAccess.Write, FileShare.Read), new UTF8Encoding(false));
            _writer.AutoFlush = true;
            Write(new { version = 1, kind = "start", session_id = _session,
                account_mode = "demo", running_mode = "realtime", symbol = SymbolName,
                started_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                sample_interval_ms = SampleIntervalMilliseconds, duration_seconds = DurationSeconds });
            _enabled = true;
            _elapsed.Start();
            Timer.Start(1);
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
            if (_elapsed.ElapsedMilliseconds - _lastSample < SampleIntervalMilliseconds || _ticks.Count == 0)
                return;
            var tick = _ticks.LastTick;
            if (tick.Bid <= 0 || tick.Ask < tick.Bid || double.IsNaN(tick.Bid) ||
                double.IsNaN(tick.Ask) || double.IsInfinity(tick.Bid) || double.IsInfinity(tick.Ask))
                return;
            _lastSample = _elapsed.ElapsedMilliseconds;
            // Tick.Time follows the robot's UTC timezone; it is not a receipt clock.
            Write(new { version = 1, kind = "quote", session_id = _session, sequence = ++_sequence,
                symbol = SymbolName, source_at = DateTime.SpecifyKind(tick.Time, DateTimeKind.Utc)
                    .ToString("o", CultureInfo.InvariantCulture),
                bridge_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                bid = tick.Bid.ToString("R", CultureInfo.InvariantCulture),
                ask = tick.Ask.ToString("R", CultureInfo.InvariantCulture) });
        }

        protected override void OnTimer()
        {
            if (_enabled && _elapsed.Elapsed.TotalSeconds >= DurationSeconds) Stop();
        }

        protected override void OnStop()
        {
            _enabled = false;
            if (_writer == null) return;
            try
            {
                Write(new { version = 1, kind = "stop", session_id = _session,
                    stopped_at = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                    quotes = _sequence });
            }
            finally { _writer.Dispose(); _writer = null; }
            Print("EVENTLENS_QUOTES|stopped|quotes={0}|orders=none", _sequence);
        }

        private void Write(object value) { _writer.WriteLine(JsonSerializer.Serialize(value)); }
    }
}
