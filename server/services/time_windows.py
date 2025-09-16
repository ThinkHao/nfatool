from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Tuple


def resolve_time_window(selector: str, params: dict[str, Any] | None, tz_name: str) -> tuple[str, str, str]:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    if selector == "last_week":
        # previous natural week: Monday 00:00:00 to Sunday 23:59:59
        # Find current week's Monday
        weekday = now.weekday()  # Monday=0
        this_monday = datetime(now.year, now.month, now.day, tzinfo=tz) - timedelta(days=weekday)
        last_monday = this_monday - timedelta(days=7)
        last_sunday_end = this_monday - timedelta(seconds=1)
        start = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = last_sunday_end.replace(microsecond=0)
        window_label = f"{start:%Y%m%d}-{end:%Y%m%d}"
        return start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S'), window_label
    elif selector == "last_n_days":
        n = int((params or {}).get("n", 7))
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start = (end - timedelta(days=n-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        window_label = f"last{n}d-{end:%Y%m%d}"
        return start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S'), window_label
    elif selector == "custom":
        # Expect params has start_time and end_time
        if not params or not params.get("start_time") or not params.get("end_time"):
            raise ValueError("custom window requires start_time and end_time")
        start = params["start_time"]
        end = params["end_time"]
        window_label = f"{start.split(' ')[0]}-{end.split(' ')[0]}"
        return start, end, window_label
    else:
        raise ValueError(f"Unsupported window selector: {selector}")
