using System;
using System.Globalization;
using cAlgo.API;

namespace cAlgo.Robots
{
    // Read-only feasibility probe. Run only in cTrader's Backtesting tab.
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class EventLensTickProbe : Robot
    {
        private static readonly DateTime FocusUtc = new DateTime(2025, 6, 18, 18, 0, 0, DateTimeKind.Utc);
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

        protected override void OnStart()
        {
            if (!IsBacktesting)
            {
                Print("EVENTLENS_PROBE|refused|reason=backtesting_required");
                Stop();
                return;
            }

            Print("EVENTLENS_PROBE|start|symbol={0}|mode=backtest_only|orders=none", SymbolName);
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
                _crossedQuotes++;

            var spreadPips = (ask - bid) / Symbol.PipSize;
            _minSpreadPips = Math.Min(_minSpreadPips, spreadPips);
            _maxSpreadPips = Math.Max(_maxSpreadPips, spreadPips);

            if (timestamp >= FocusUtc.AddMinutes(-1) && timestamp < FocusUtc.AddMinutes(1))
                _focusWindowTicks++;

            if (timestamp < FocusUtc)
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

            if (_count <= 3)
                Print("EVENTLENS_PROBE|sample|time_utc={0}|bid={1}|ask={2}",
                    timestamp.ToString("o", CultureInfo.InvariantCulture),
                    bid.ToString("R", CultureInfo.InvariantCulture),
                    ask.ToString("R", CultureInfo.InvariantCulture));
        }

        protected override void OnStop()
        {
            Print("EVENTLENS_PROBE|summary|symbol={0}|ticks={1}|first_utc={2}|last_utc={3}|max_gap_ms={4}|invalid_quotes={5}|crossed_quotes={6}|backward_times={7}|min_spread_pips={8}|max_spread_pips={9}",
                SymbolName,
                _count,
                _count > 0 ? _firstTime.ToString("o", CultureInfo.InvariantCulture) : "none",
                _count > 0 ? _lastTime.ToString("o", CultureInfo.InvariantCulture) : "none",
                _maxGapMilliseconds,
                _invalidQuotes,
                _crossedQuotes,
                _backwardTimes,
                _count > _invalidQuotes ? _minSpreadPips.ToString("R", CultureInfo.InvariantCulture) : "none",
                _count > _invalidQuotes ? _maxSpreadPips.ToString("R", CultureInfo.InvariantCulture) : "none");

            Print("EVENTLENS_PROBE|focus|focus_utc={0}|window_2m_ticks={1}|last_before_utc={2}|bid_before={3}|ask_before={4}|first_at_or_after_utc={5}|bid_after={6}|ask_after={7}|after_lag_ms={8}",
                FocusUtc.ToString("o", CultureInfo.InvariantCulture),
                _focusWindowTicks,
                _hasBeforeFocus ? _lastBeforeFocus.ToString("o", CultureInfo.InvariantCulture) : "none",
                _hasBeforeFocus ? _bidBeforeFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasBeforeFocus ? _askBeforeFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _firstAtOrAfterFocus.ToString("o", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _bidAtOrAfterFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? _askAtOrAfterFocus.ToString("R", CultureInfo.InvariantCulture) : "none",
                _hasAtOrAfterFocus ? (long)(_firstAtOrAfterFocus - FocusUtc).TotalMilliseconds : -1);
        }
    }
}
