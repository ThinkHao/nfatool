# Parameter Mapping

## Shared Task Fields

- `data_source_type`: `nfa|edc`
- `data_source_instance`
- `window_selector`: `custom|last_week|last_month|last_n_days`
- `window_params`
- `params`
- `export_formats`

## EDC Params

- `edc_name`
- `settlement_mode`: `daily_95_avg|range_95`
- `unit_base`: `1000|1024`
- `export_daily`: bool
- `monthly_aggregate`: bool
- optional budget params:
  - `data_budget_enabled`
  - `data_budget_mul`
  - `data_budget_div`

## NFA Params

- `province`, `cp`, `direction`
- `settlement_mode`
- `unit_base`
- `export_daily`
- `monthly_aggregate`
- optional:
  - `aggregate_all`
  - `combine_v4_v6`
  - `merge_key`
  - `school` / `exclude_school`

## Naming Template

- `output_filename_template` placeholders:
  - `{source}`, `{instance}`, `{window}`, `{date}`, `{edc}`, `{province}`, `{cp}`, `{direction}`
