import pytest

from server.services.compute95 import (
    EDC_MAX_NAMES,
    _edc_name_predicate,
    _parse_edc_names,
)


def test_single_prefix_matches_legacy_like() -> None:
    frag, args = _edc_name_predicate("edc_name", "SD-cs-bj-3495", "prefix")
    assert frag == "edc_name LIKE %s"
    assert args == ["SD-cs-bj-3495%"]


def test_single_exact_uses_equality() -> None:
    frag, args = _edc_name_predicate("edc_name", "SD-cs-bj-3495", "exact")
    assert frag == "edc_name = %s"
    assert args == ["SD-cs-bj-3495"]


def test_multi_exact_uses_in_clause() -> None:
    # The headline case: two exact names must become an IN, and must NOT match 3497.
    frag, args = _edc_name_predicate(
        "edc_name", "SD-cs-bj-3495, SD-cs-bj-3496", "exact"
    )
    assert frag == "edc_name IN (%s, %s)"
    assert args == ["SD-cs-bj-3495", "SD-cs-bj-3496"]
    assert "SD-cs-bj-3497" not in args


def test_multi_prefix_is_ored_like() -> None:
    frag, args = _edc_name_predicate("edc_name", "a-,b-", "prefix")
    assert frag == "(edc_name LIKE %s OR edc_name LIKE %s)"
    assert args == ["a-%", "b-%"]


def test_glob_token_overrides_mode() -> None:
    frag, args = _edc_name_predicate("edc_name", "SD-cs-bj-349*", "exact")
    assert frag == "edc_name LIKE %s"
    assert args == ["SD-cs-bj-349%"]


def test_mixed_exact_and_glob() -> None:
    frag, args = _edc_name_predicate("edc_name", "x,y*", "exact")
    assert frag == "(edc_name = %s OR edc_name LIKE %s)"
    assert args == ["x", "y%"]


def test_parse_trims_dedups_and_drops_blanks() -> None:
    assert _parse_edc_names("a, a\nb,\n  ,c ") == ["a", "b", "c"]


def test_empty_name_raises() -> None:
    with pytest.raises(ValueError):
        _edc_name_predicate("edc_name", "  ,\n ", "exact")


def test_too_many_names_raises() -> None:
    big = ",".join(f"n{i}" for i in range(EDC_MAX_NAMES + 1))
    with pytest.raises(ValueError):
        _edc_name_predicate("edc_name", big, "exact")
