# Data Source Governance

## Storage and Security

- Persist data source instances in DB (not only env file).
- Encrypt config payload at rest.
- Protect sensitive fields (DB password, SSH password/key passphrase).

## Key Rotation

- Support manual rotation.
- Support auto-rotation policy (interval days).
- Keep audit records for rotate/create/update/delete.

## Admin APIs and UI

- Required capabilities:
  - list instances
  - create/update instance
  - delete instance
  - test connectivity
  - view audit logs
- Keep config panels collapsible to reduce UI clutter.

## Reliability Controls

- Validate config before save.
- Provide actionable connection errors.
- Keep retry knobs for unstable SSH networks.
