---
name: traffic-95-nfatool-adapter
description: Map traffic-95-core workflows onto the nfatool project APIs, task schema, scheduler, and update pipeline. Use when executing EDC/NFA exports in this repository via existing endpoints and task models.
---

# Traffic 95 Nfatool Adapter

Use this adapter with `traffic-95-core`.

## Purpose

Translate unified request contract into this project's task/job APIs and params.

## Mapping Scope

- Data source types and instances
- Task payload model
- Window selectors and schedule types
- Settlement/export params
- Artifact download and batch download

## Required References

1. Read [references/api-mapping.md](references/api-mapping.md).
2. Read [references/param-mapping.md](references/param-mapping.md).
3. Read [references/ops-mapping.md](references/ops-mapping.md).

## Adapter Rules

- Do not modify source formulas in adapter.
- Keep EDC/NFA behavior aligned with core skill.
- Prefer creating tasks through API for reproducibility.
- For ad-hoc requests, use one-off task + immediate run + artifact fetch.
