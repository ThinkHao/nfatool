# Operation Playbook

## Default Procedure

1. Validate source instance connectivity.
2. Resolve time window.
3. Run compute by source adapter.
4. Apply settlement mode.
5. Apply aggregate mode (none/month/year).
6. Export artifacts and return download metadata.

## Failure Triage

- Connection errors: check instance config and SSH tunnel path.
- Abnormal values: verify source formula and unit base.
- Export failure: verify filename sanitization and writable path.

## Response Expectations

- Return stage/progress for long-running jobs.
- Return explicit failure message with actionable reason.
- Keep auditable metadata for each run.
