# Release and Version Control

## Release Metadata

- Keep runtime version in `APP_VERSION`.
- Check latest version from GitHub Releases.
- Match release asset by OS (`nfa95`/`nfa95.exe`).

## Safe Update Strategy

- Prefer external updater script in Linux production.
- Use staged release directories and atomic switch.
- Restart service and verify health endpoint.
- Auto rollback on health check failure.

## Update Execution Model

- App process triggers update.
- External runner performs download/switch/restart/rollback.
- Write runner logs for diagnosis.

## CI/CD Requirements

- Push to GitHub triggers build artifact generation.
- Release artifact naming must stay stable.
- Upgrade endpoint only consumes trusted release channel.

## Operational Checks

- Before update: check current version, target version, disk space.
- After update: check service status, API health, scheduler status.
- On failure: confirm rollback success and capture error log.
