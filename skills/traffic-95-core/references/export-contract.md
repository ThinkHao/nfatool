# Export Contract

## Core Export Columns

- `source_type`
- `instance`
- selector fields (`edc_name` or NFA school/ipgroup dimensions)
- `settlement_mode`
- `95th_percentile_raw`
- `95th_percentile_mbps`
- `data_points`
- period label (`date` or `month` or `year`)

## Aggregate Levels

- `aggregate=none`: default settlement row(s)
- `aggregate=month`: one row per month
- `aggregate=year`: one row per year

## File Rules

- Support `csv` and `xlsx`.
- Filename template placeholders:
  - `{source}`, `{instance}`, `{window}`, `{date}`, `{edc}`, `{province}`, `{cp}`, `{direction}`
- Sanitize invalid filename characters before writing.
