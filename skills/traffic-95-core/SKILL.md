---
name: traffic-95-core
description: Execute standardized 95th percentile workflows for EDC and NFA with fixed conversion formulas, settlement modes (daily_95_avg/range_95), and daily-monthly-yearly exports. Use when users ask to fetch, compute, or export EDC/NFA settlement data without redesigning logic.
---

# Traffic 95 Core

Use this skill as a stable business kernel. Do not redesign formulas unless user explicitly changes business rules.

## Fixed Business Rules

- Source types: `edc`, `nfa`
- Conversion:
  - `EDC`: `mbps = raw * 8 / 300 / base / base`
  - `NFA`: `mbps = raw * 8 / 60 / base / base`
- Unit base: `1000` or `1024`
- Settlement modes:
  - `daily_95_avg`
  - `range_95`
- Export levels:
  - `daily`
  - `monthly`
  - `yearly`

## Unified Request Contract

When user asks for export, normalize request into:

- `source_type`: `edc|nfa`
- `instance`: data source instance name
- `selector`:
  - EDC: `edc_name`
  - NFA: `province`, `cp`, optional `direction`
- `window`: `custom|last_week|last_month|last_n_days`
- `settlement_mode`: `daily_95_avg|range_95`
- `aggregate`: `none|month|year`
- `formats`: `csv|xlsx`

## Execution Rules

1. Validate source/instance before compute.
2. Keep conversion by source type only; never share constants across adapters.
3. Include traceable fields in export: selector, settlement_mode, base, points, period label.
4. Prefer monthly/yearly aggregate outputs for reporting, daily for diagnostics.

## References

- Read [references/formula-and-settlement.md](references/formula-and-settlement.md).
- Read [references/export-contract.md](references/export-contract.md).
- Read [references/operation-playbook.md](references/operation-playbook.md).
