# Ops Mapping

## Scheduler

- One-off and periodic tasks are supported.
- Periodic supports `cron` / `interval` / project presets.
- Show progress in run records (stage + percent).

## Governance

- Data source configs are DB-backed and encrypted.
- Key rotation supports manual and automatic modes.
- Audit endpoint tracks config changes.

## Upgrade

- Prefer external script mode on Linux production.
- Keep rollback via health-check when restarted version is unhealthy.

## Recommended Ad-hoc Export Procedure

1. Build one-off task payload using mapped params.
2. Create task.
3. Trigger run.
4. Poll jobs list until success/failure.
5. Download artifact(s).
6. Optionally delete one-off task.
