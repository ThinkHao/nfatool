---
name: edc-onboarding
description: Onboard and validate EDC data source instances for the nfatool server project. Use when adding a new EDC instance, switching direct MySQL to MySQL-over-SSH, debugging EDC connection/auth failures, enabling wildcard matching behavior checks, or preparing batch-import-ready EDC task templates.
---

# EDC Onboarding

## Goal

Standardize EDC instance onboarding with repeatable checks for connectivity, auth, SQL fields, and scheduler readiness.  
Prefer UI-based instance management first, then fallback to `.env` only when bootstrapping.

## Workflow

1. Prepare instance config.
2. Test connection from UI (`数据源配置管理 -> 测试连接`).
3. Validate EDC query fields (`table`, `time_column`, `name_column`, `value_column`).
4. Create one-off EDC task for the previous month and run it.
5. Confirm artifact naming and budget summary output.
6. Enable periodic schedule only after one-off verification succeeds.

## Config Checklist

- Required DB fields: `host`, `port`, `user`, `password`, `db`.
- Required EDC SQL fields: `table`, `time_column`, `name_column`, `value_column`.
- Recommended: `exclude_like`, `wildcard_mode`, `daily_rank_index`.
- SSH mode fields: `ssh_enabled`, `ssh_host`, `ssh_port`, `ssh_user`, plus one auth method.

### SSH auth methods

- Password: `ssh_password`.
- Key file: `ssh_pkey` must be a private key file path.
- Agent/default key: `ssh_allow_agent=true`.
- Old SSH server compatibility: `ssh_legacy_rsa=true`.
- Transient retry tuning: `ssh_connect_retries`, `ssh_retry_delay_ms`.

## Failure Triage

When test or job fails, classify first:

1. `Could not establish session to SSH gateway`
- Check `ssh_host/ssh_port/ssh_user`.
- Check auth method and key path.
- If OpenSSH 6.x server, enable `ssh_legacy_rsa=true`.

2. `Authentication (publickey) failed`
- Key mismatch or algorithm mismatch.
- Avoid `id_rsa.pub` as `ssh_pkey`; use private key file.
- Try password auth to isolate auth-vs-network issues.

3. `Can't connect to MySQL server`
- SSH tunnel may be up but remote DB bind/port is wrong.
- Verify `ssh_remote_host` and `ssh_remote_port`.
- Verify DB user rights from jump host.

## Acceptance

- Connection test returns `ok=true`.
- One-off EDC run succeeds and produces artifact.
- Budget summary appears in task list (if enabled).
- Periodic task shows valid next run time.

## References

Use [references/edc-instance-template.md](references/edc-instance-template.md) for copy-ready config templates.

**Note:** Scripts may be executed without loading into context, but can still be read by Codex for patching or environment adjustments.

### references/
Documentation and reference material intended to be loaded into context to inform Codex's process and thinking.

**Examples from other skills:**
- Product management: `communication.md`, `context_building.md` - detailed workflow guides
- BigQuery: API reference documentation and query examples
- Finance: Schema documentation, company policies

**Appropriate for:** In-depth documentation, API references, database schemas, comprehensive guides, or any detailed information that Codex should reference while working.

### assets/
Files not intended to be loaded into context, but rather used within the output Codex produces.

**Examples from other skills:**
- Brand styling: PowerPoint template files (.pptx), logo files
- Frontend builder: HTML/React boilerplate project directories
- Typography: Font files (.ttf, .woff2)

**Appropriate for:** Templates, boilerplate code, document templates, images, icons, fonts, or any files meant to be copied or used in the final output.

---

**Not every skill requires all three types of resources.**
