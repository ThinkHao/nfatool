# Settlement and Export

## Settlement Modes

- `daily_95_avg`: calculate each day 95th, then average by day count in window.
- `range_95`: calculate 95th directly on full window points.

## Export Modes

- Daily export: one row per day.
- Monthly aggregate: one row per month.
- Yearly aggregate: one row per year (recommended extension by grouping months).

## Recommended Export Schema

- Common fields:
  - source type, instance
  - object key (`edc_name` or NFA school/ipgroup fields)
  - settlement mode
  - `95th_percentile_raw`
  - `95th_percentile_mbps`
  - points count
  - period label (`date`/`month`/`year`)

## File Output

- Formats: CSV, XLSX.
- Safe filename template with placeholders:
  - `{source}`, `{instance}`, `{window}`, `{date}`, `{edc}`, `{province}`, `{cp}`.
- Sanitize filesystem-invalid chars before writing.

## Data Budget (Optional)

- Keep formula explicit and auditable.
- Example step chain:
  - `step1 = raw * mul / div`
  - `result = step1 / base / base`
- Persist debug summary for traceability.
