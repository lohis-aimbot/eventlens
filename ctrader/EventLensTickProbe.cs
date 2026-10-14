using System;
using System.Globalization;
using cAlgo.API;

namespace cAlgo.Robots
{
    // Read-only feasibility probe. Run only in cTrader's Backtesting tab.
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class EventLensTickProbe : Robot
    {
        // One first replay snapshot at/after each scheduled offset. These are
        // market-data checkpoints, not executable fills or measured feed latency.
        private static readonly int[] CheckpointOffsetsSeconds =
            { 1, 5, 30, 60, 61, 65, 90, 120, 301, 305, 330, 360, 1801, 1805, 1830, 1860 };

        [Parameter("Focus UTC", DefaultValue = "2025-06-18T18:00:00Z")]
        public string FocusUtcText { get; set; }

        [Parameter("Sample interval ms (0 = off)", DefaultValue = 0, MinValue = 0)]
        public int SampleIntervalMilliseconds { get; set; }

        private DateTime _focusUtc;
        private readonly DateTime[] _checkpointTimes = new DateTime[CheckpointOffsetsSeconds.Length];
        private readonly double[] _checkpointBids = new double[CheckpointOffsetsSeconds.Length];
        private readonly double[] _checkpointAsks = new double[CheckpointOffsetsSeconds.Length];
        private readonly bool[] _checkpointSeen = new bool[CheckpointOffsetsSeconds.Length];
        private long _count;
        private long _invalidQuotes;
        private long _crossedQuotes;
        private long _backwardTimes;
        private long _maxGapMilliseconds;
        private DateTime _firstTime;
        private DateTime _lastTime;
        private double _minSpreadPips = double.PositiveInfinity;
        private double _maxSpreadPips = double.NegativeInfinity;
        private long _focusWindowTicks;
        private DateTime _lastBeforeFocus;
        private DateTime _firstAtOrAfterFocus;
        private double _bidBeforeFocus;
        private double _askBeforeFocus;
        private double _bidAtOrAfterFocus;
        private double _askAtOrAfterFocus;
        private bool _hasBeforeFocus;
        private bool _hasAtOrAfterFocus;
        private long _lastSampleBucket = -1;
        private long _windowSamples;
        private long _validWindowTicks;
        private long _zeroSpreadWindowTicks;
        private long _windowMaxGapMilliseconds;
        private DateTime _windowFirstTime;
        private DateTime _windowLastTime;

        protected override void OnStart()
        {
            if (!IsBacktesting)
            {
                Print("EVENTLENS_PROBE|refused|reason=backtesting_required");
                Stop();
                return;
            }

            if (Account.IsLive)
            {
                Print("EVENTLENS_PROBE|refused|reason=demo_account_required");
                Stop();
                return;
            }

            if (!DateTime.TryParseExact(FocusUtcText, "yyyy-MM-ddTHH:mm:ssZ",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out _focusUtc))
            {
                Print("EVENTLENS_PROBE|refused|reason=invalid_focus_utc");
                Stop();
                return;
            }

            Print("EVENTLENS_PROBE|start|symbol={0}|mode=demo_backtest_only|focus_utc={1}|orders=none|sample_interval_ms={2}",
                SymbolName, _focusUtc.ToString("o", CultureInfo.InvariantCulture), SampleIntervalMilliseconds);
            // Symbol metadata observed in Demo backtesting, not a confirmed fill
            // charge or proof of the fee schedule on the historical event date.
            Print("EVENTLENS_PROBE|symbol_cost|symbol={0}|commission={1}|commission_type={2}|min_commission={3}|min_commission_type={4}|min_commission_asset={5}|lot_size={6}|pip_size={7}|digits={8}|basis=demo_backtest_symbol_metadata",
                SymbolName,
                Symbol.Commission.ToString("R", CultureInfo.InvariantCulture), Symbol.CommissionType,
                Symbol.MinCommission.ToString("R", CultureInfo.InvariantCulture), Symbol.MinCommissionType,
                Symbol.MinCommissionAsset, Symbol.LotSize.ToString("R", CultureInfo.InvariantCulture),
                Symbol.PipSize.ToString("R", CultureInfo.InvariantCulture), Symbol.Digits);
        }

        protected override void OnTick()
        {
            var timestamp = Server.TimeInUtc;
            var bid = Symbol.Bid;
            var ask = Symbol.Ask;

            if (_count == 0)
            {
                _firstTime = timestamp;
            }
            else
            {
                var gap = (long)(timestamp - _lastTime).TotalMilliseconds;
                if (gap < 0)
                    _backwardTimes++;
                else if (gap > _maxGapMilliseconds)
                    _maxGapMilliseconds = gap;
            }

            _lastTime = timestamp;
            _count++;

            if (bid <= 0 || ask <= 0 || Symbol.PipSize <= 0 ||
                double.IsNaN(bid) || double.IsNaN(ask) ||
                double.IsInfinity(bid) || double.IsInfinity(ask))
            {
                _invalidQuotes++;
                return;
            }

            if (ask < bid)
            {
                _crossedQuotes++;
                return;
            }

            var spreadPips = (ask - bid) / Symbol.PipSize;
            _minSpreadPips = Math.Min(_minSpreadPips, spreadPips);
            _maxSpreadPips = Math.Max(_maxSpreadPips, spreadPips);

            if (timestamp >= _focusUtc.AddMinutes(-1) && timestamp < _focusUtc.AddMinutes(1))
                _focusWindowTicks++;

            var windowStart = _focusUtc.AddMinutes(-2);
            if (timestamp >= windowStart && timestamp <= _focusUtc.AddMinutes(35))
            {
                if (_validWindowTicks == 0)
                    _windowFirstTime = timestamp;
                else
                    _windowMaxGapMilliseconds = Math.Max(_windowMaxGapMilliseconds,
                        (long)(timestamp - _windowLastTime).TotalMilliseconds);
                _windowLastTime = timestamp;
                _validWindowTicks++;
                if (ask == bid)
                    _zeroSpreadWindowTicks++;
            }
            if (SampleIntervalMilliseconds > 0 && timestamp >= windowStart && timestamp <= _focusUtc.AddMinutes(35))
            {
                var bucket = (long)(timestamp - windowStart).TotalMilliseconds / SampleIntervalMilliseconds;
                if (bucket > _lastSampleBucket)
                {
                    _lastSampleBucket = bucket;
                    _windowSamples++;
                    // First valid observed snapshot in the bucket, never invented
                    // quotes for empty buckets or independent Bid/Ask side times.
                    Print("EVENTLENS_PROBE|quote|time_utc={0}|bid={1}|ask={2}",
                        timestamp.ToString("o", CultureInfo.InvariantCulture),
                        bid.ToString("R", CultureInfo.InvariantCulture),
                        ask.ToString("R", CultureInfo.InvariantCulture));
                }
            }

            if (timestamp < _focusUtc)
            {
                _hasBeforeFocus = true;
                _lastBeforeFocus = timestamp;
                _bidBeforeFocus = bid;
                _askBeforeFocus = ask;
            }
            else if (!_hasAtOrAfterFocus)
            {
                _hasAtOrAfterFocus = true;
                _firstAtOrAfterFocus = timestamp;
                _bidAtOrAfterFocus = bid;
                _askAtOrAfterFocus = ask;
            }

            for (var i = 0; i < CheckpointOffsetsSeconds.Length; i++)
            {
                if (!_checkpointSeen[i] && timestamp >= _focusUtc.AddSeconds(CheckpointOffsetsSeconds[i]))
                {
                    _checkpointSeen[i] = true;
                    _checkpointTimes[i] = timestamp;
                    _checkpointBids[i] = bid;
                    _checkpointAsks[i] = ask;
                }
            }

            if (_count <= 3)
                Print("EVENTLENS_PROBE|sample|time_utc={0}|bid={1}|ask={2}",
                    timestamp.ToString("o", CultureInfo.InvariantCulture),
                    bid.ToString("R", CultureInfo.InvariantCulture),
                    ask.ToString("R", CultureInfo.InvariantCulture));
        }

        protected override void OnStop()
        {
            Print("EVENTLENS_PROBE|window_quality|valid_ticks={0}|zero_spread_ticks={1}|max_tick_gap_ms={2}|first_utc={3}|last_utc={4}",
                _validWindowTicks, _zeroSpreadWindowTicks, _windowMaxGapMilliseconds,
                _validWindowTicks > 0 ? _windowFirstTime.ToString("o", CultureInfo.InvariantCulture) : "none",
                _validWindowTicks > 0 ? _windowLastTime.ToString("o", CultureInfo.InvariantCulture) : "none");
            Print("EVENTLENS_PROBE|summary|symbol={0}|ticks={1}|first_utc={2}|last_utc={3}|max_gap_ms={4}|invalid_quotes={5}|crossed_quotes={6}|backward_times={7}|min_spread_pips={8}|max_spread_pips={9}|window_samples={10}",
                SymbolName,
                _count,
                _count > 0 ? _firstTime.ToString("o", CultureInfo.InvariantCulture) : "none",
                _count > 0 ? _lastTime.ToString("o", CultureInfo.InvariantCulture) : "none",
                _maxGapMilliseconds,
                _invalidQuotes,
                _crossedQuotes,
                _backwardTimes,
                _count > _invalidQuotes ? _minSpreadPips.ToString("R", CultureInfo.InvariantCulture) : "none",
                _count > _invalidQuotes ? _maxSpreadPips.ToString("R", CultureInfo.InvariantCulture) : "none",
                _windowSamples);

            Print("EVENTLENS_PROBE|focus|focus_utc={0}|window_2m_ticks={1}|last_before_utc={2}|bid_before={3}|ask_before={4}|first_at_or_after_utc={5}|bid_after={6}|ask_after={7}|after_lag_ms={8}",
                _focusUtc.ToString("o", CultureInfo.InvariantCulture),
                _focusWindowTicks,
                _hasBeforeFocus ? _lastBeforeFocus.ToString("o", CultureInfo.InvariantCulture) : "none",
                _hasBeforeFocus ? _bidBeforeFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasBeforeFocus ? _askBeforeFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _firstAtOrAfterFocus.ToString("o", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _bidAtOrAfterFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _askAtOrAfterFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? (long)(_firstAtOrAfterFocus - _focusUtc).TotalMilliseconds : -1);

            for (var i = 0; i < CheckpointOffsetsSeconds.Length; i++)
            {
                Print("EVENTLENS_PROBE|checkpoint|focus_utc={0}|offset_seconds={1}|quote_utc={2}|bid={3}|ask={4}",
                    _focusUtc.ToString("o", CultureInfo.InvariantCulture),
                    CheckpointOffsetsSeconds[i],
                    _checkpointSeen[i] ? _checkpointTimes[i].ToString("o", CultureInfo.InvariantCulture) : "none",
                    _checkpointSeen[i] ? _checkpointBids[i].ToString("R", CultureInfo.InvariantCulture) : "none",
                    _checkpointSeen[i] ? _checkpointAsks[i].ToString("R", CultureInfo.InvariantCulture) : "none");
            }
        }
    }
}
