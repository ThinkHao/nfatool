# Repository Guidelines

## Project Structure & Module Organization
This repository mixes data-processing scripts and a web service:

- `server/`: FastAPI application (`main.py`), config/security/db modules, and business logic in `server/services/`.
- `server/static/`: frontend assets (`index.html`, `app.js`, `styles.css`) served by FastAPI.
- `server/scripts/`: deployment helpers (for example `nfa95.service`, `useradd.sh`).
- Root `*.py` and `*.md`: standalone tools and usage notes (for example `calculate_95th_percentile.py`).
- `sql/`, `txt/`: SQL and text resources.
- `output/`, `server/storage/`, `server/logs/`: generated artifacts and runtime data.

## Build, Test, and Development Commands
- Install dependencies: `pip install -r server/requirements.txt`
- Run locally (dev reload): `uvicorn server.main:app --reload --port 8000`
- Open app/docs: `http://127.0.0.1:8000/` and `http://127.0.0.1:8000/docs`
- Build single-file executable (Windows): `powershell -ExecutionPolicy Bypass -File server/build.ps1 -Name nfa95`
- Build single-file executable (Linux/macOS): `bash server/build.sh nfa95`

Use `.env.example` as the baseline for runtime configuration.

## Coding Style & Naming Conventions
- Python: follow PEP 8 with 4-space indentation, `snake_case` for functions/variables, `PascalCase` for classes, and type hints where practical.
- Frontend JS/CSS/HTML in `server/static/`: keep existing lightweight style and naming; use clear method names (`loadTasksPage`, `normalizeCustomWindow`) and avoid single-letter identifiers except loop counters.
- Keep modules focused: API orchestration in `server/main.py`, domain logic in `server/services/`.

## Testing Guidelines
There is no committed automated test suite yet. Validate changes with:

- API smoke checks: `GET /api/health`, task CRUD, run triggering, and artifact download flow.
- UI checks: task list pagination, filters, create/edit/delete task, and export options.
- If adding tests, prefer `pytest` under `server/tests/` with filenames like `test_scheduler.py`.

## Commit & Pull Request Guidelines
- Follow the repository’s commit pattern: `feat: ...`, `fix: ...`, `add: ...`, `change: ...` (concise, imperative).
- Keep each commit scoped to one logical change.
- PRs should include: purpose, key changes, verification steps/commands, related issue/task, and UI screenshots when `server/static/` changes.
- Highlight config or data-impacting changes (`.env`, DB fields, export format) in the PR description.
