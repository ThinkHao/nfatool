# Formula and Settlement

## Source Formula

- EDC raw to Mbps:
  - `raw * 8 / 300 / base / base`
- NFA raw to Mbps:
  - `raw * 8 / 60 / base / base`

## Settlement Semantics

- `daily_95_avg`:
  - compute daily 95th for each day in window
  - average daily results by day count
- `range_95`:
  - compute 95th directly on full time-range points

## Guardrails

- Keep `seconds_per_point` source-bound (`300` for EDC, `60` for NFA).
- Keep conversion test cases for both source types.
- Keep inverse conversion helpers for trace/debug when needed.
