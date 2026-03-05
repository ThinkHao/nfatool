---
name: traffic-95-platform
description: Build or integrate a dual-source (NFA/EDC) 95th-percentile traffic platform with unified source adapters, conversion rules, settlement modes (daily_95_avg and range_95), artifact exports (daily/monthly/yearly), task scheduling, data source config governance, and release/update control. Use when implementing or migrating EDC/NFA compute workflows into another project.
---

# Traffic 95 Platform

Standardize and integrate NFA + EDC traffic computation into reusable modules.

## Use This Workflow

1. Define source adapter contracts first.
2. Freeze conversion and settlement formulas.
3. Implement export and aggregation modes.
4. Add data source config governance (secure storage, audit, rotation).
5. Add operational controls (scheduler, progress, release update/rollback).

## Implementation Order

1. Read [references/source-adapter-and-conversion.md](references/source-adapter-and-conversion.md).
2. Read [references/settlement-and-export.md](references/settlement-and-export.md).
3. Read [references/data-source-governance.md](references/data-source-governance.md).
4. Read [references/release-and-version-control.md](references/release-and-version-control.md).

## Non-Negotiable Rules

- Keep source-specific conversion isolated by adapter:
  - `NFA`: `raw * 8 / 60 / base / base`
  - `EDC`: `raw * 8 / 300 / base / base`
- Keep settlement output explicit:
  - `daily_95_avg`
  - `range_95`
- Keep aggregation explicit and selectable:
  - daily
  - monthly
  - yearly
- Keep data source configs managed in DB with encryption + audit trail.
- Keep release updates reversible with health-check rollback.

## Output Checklist For New Integrations

- NFA and EDC source adapters both pass connectivity checks.
- Conversion tests exist for NFA `/60` and EDC `/300`.
- Export supports at least CSV + monthly aggregation.
- Task runtime progress is visible (stage + percent).
- Data source admin supports create/edit/test/delete.
- Update path supports rollback on unhealthy restart.
