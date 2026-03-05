# Source Adapter and Conversion

## Adapter Contract

- Input: `source_type`, `instance`, time window, task params.
- Output: normalized rows for settlement/export.
- Required source types: `nfa`, `edc`.

## Conversion Rules (Must Not Mix)

- NFA: `mbps = raw * 8 / 60 / base / base`
- EDC: `mbps = raw * 8 / 300 / base / base`
- Base: `1000` or `1024`.

## Matching Behavior

- EDC supports wildcard name matching:
  - `*` => SQL `%`
  - `?` => SQL `_`
- Support configurable `wildcard_mode` (`prefix`/`exact`) and `exclude_like`.

## EDC Connectivity

- Direct MySQL and MySQL-over-SSH.
- SSH auth methods:
  - password
  - private key file
  - ssh-agent/default key
- Optional old-server compatibility: legacy RSA mode.

## Validation Checklist

- Verify source instance can run `SELECT 1`.
- Verify query fields: table/time/name/value columns.
- Verify same raw sample under adapter gives expected Mbps.
- Add unit tests for NFA `/60` and EDC `/300`.
