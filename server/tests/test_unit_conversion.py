from server.services.unit_conversion import mbps_to_raw, raw_to_mbps


def test_edc_conversion_uses_300_seconds() -> None:
    raw = 30_000_000.0
    mbps = raw_to_mbps(raw, unit_base=1000, seconds_per_point=300.0)
    assert abs(mbps - 0.8) < 1e-12
    raw_back = mbps_to_raw(mbps, unit_base=1000, seconds_per_point=300.0)
    assert abs(raw_back - raw) < 1e-6


def test_nfa_conversion_uses_60_seconds() -> None:
    raw = 60_000_000.0
    mbps = raw_to_mbps(raw, unit_base=1000, seconds_per_point=60.0)
    assert abs(mbps - 8.0) < 1e-12
    raw_back = mbps_to_raw(mbps, unit_base=1000, seconds_per_point=60.0)
    assert abs(raw_back - raw) < 1e-6
