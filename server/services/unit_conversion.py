from __future__ import annotations


def raw_to_mbps(raw_value: float, unit_base: int, seconds_per_point: float) -> float:
    if seconds_per_point <= 0:
        raise ValueError("seconds_per_point must be > 0")
    return float(raw_value) * 8.0 / float(seconds_per_point) / float(unit_base) / float(unit_base)


def mbps_to_raw(mbps_value: float, unit_base: int, seconds_per_point: float) -> float:
    if seconds_per_point <= 0:
        raise ValueError("seconds_per_point must be > 0")
    return float(mbps_value) * float(seconds_per_point) * float(unit_base) * float(unit_base) / 8.0
