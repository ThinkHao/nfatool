from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


def cleanup_tasks_merge_key(db_path: str | Path, apply: bool = False) -> dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"db not found: {path}")

    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, params FROM tasks")
        rows = cur.fetchall()

        matched_ids: list[int] = []
        skipped_invalid_json = 0

        for task_id, params_json in rows:
            try:
                params = json.loads(params_json or "{}")
            except Exception:
                skipped_invalid_json += 1
                continue
            if not isinstance(params, dict):
                continue

            combine_v4_v6 = bool(params.get("combine_v4_v6", False))
            merge_key = str(params.get("merge_key") or "").strip()
            if (not combine_v4_v6) and merge_key:
                matched_ids.append(int(task_id))
                if apply:
                    params["merge_key"] = ""
                    cur.execute(
                        "UPDATE tasks SET params=? WHERE id=?",
                        (json.dumps(params, ensure_ascii=False), int(task_id)),
                    )

        if apply:
            conn.commit()

        return {
            "db_path": str(path),
            "scanned_count": len(rows),
            "matched_ids": matched_ids,
            "updated_count": len(matched_ids) if apply else 0,
            "skipped_invalid_json": skipped_invalid_json,
            "apply": bool(apply),
        }
    finally:
        conn.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Cleanup tasks.params.merge_key when combine_v4_v6 is false."
    )
    p.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parents[1] / "storage" / "app.db"),
        help="SQLite database path (default: server/storage/app.db)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Omit to run as dry-run.",
    )
    return p


def main() -> int:
    args = _build_arg_parser().parse_args()
    report = cleanup_tasks_merge_key(args.db_path, apply=bool(args.apply))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
