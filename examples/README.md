# Synthetic fixture

These four messages and 82 quotes are invented for software testing. Dates are arbitrary and do not identify real FOMC meetings. They must not be cited as market observations.

Each episode contains an initial hawkish phrase and a dovish follow-up 20 minutes later. The replay opens a small EUR/USD short on the first quote strictly after parsing, then closes when the same episode's follow-up reverses its thesis. One price path declines and one rises; neither is a performance claim.

Times are UTC. Default parsing latency is two seconds. Quotes contain bid and ask, not just candles. Run `eventlens demo` from the repository root. The two episodes produce two closed trades; repeated runs do not insert identical events again.
