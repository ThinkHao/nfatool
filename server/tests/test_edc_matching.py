from server.services.edc_matching import build_name_predicate, resolve_edc_match


class _Cursor:
    def __init__(self):
        self.sql = ""
        self.args = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, args=()):
        self.sql = sql
        self.args = args

    def fetchall(self):
        return [{"edc_name": "BJ-node-01"}, {"edc_name": "BJ-node-02"}]


class _Conn:
    def __init__(self):
        self.cursor_obj = _Cursor()

    def cursor(self):
        return self.cursor_obj


def test_build_name_predicate_supports_exact_and_prefix_tokens():
    fragment, args = build_name_predicate("edc_name", "BJ-node-01, BJ-node-02", "exact")
    assert fragment == "edc_name IN (%s, %s)"
    assert args == ["BJ-node-01", "BJ-node-02"]


def test_resolve_edc_match_returns_actual_objects_and_rule():
    conn = _Conn()
    items = resolve_edc_match(
        conn,
        {"table": "edc_data", "time_column": "create_time", "name_column": "edc_name"},
        "BJ-node-",
        "prefix",
        "2026-07-01 00:00:00",
        "2026-07-31 23:59:59",
        "%-backup",
    )
    assert [x["edc_name"] for x in items] == ["BJ-node-01", "BJ-node-02"]
    assert items[0]["matched_by"] == "BJ-node-"
    assert items[0]["match_operator"] == "LIKE"
    assert "DISTINCT edc_name" in conn.cursor_obj.sql
    assert conn.cursor_obj.args[-1] == "%-backup"
