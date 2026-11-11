"""Unit tests of the pure functions of test/harness.py (no engines needed): python -m pytest test/test_harness.py"""
import decimal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402
from harness import (Case, Outcome, XfailEntry, apply_xfail, config_slug, duck_type_to_spark, error_class_matches,  # noqa: E402
                     exact_mode, judge, matrix_combinations, names_status, normalize, results_equal, types_equal)


class T:
    """A stand-in for duckdb.DuckDBPyType."""

    def __init__(self, id, children=None):
        self.id = id
        self.children = children

    def __str__(self):
        return self.id


def R(status="ok", rows=None, error=None, **extra):
    return SimpleNamespace(status=status, rows=rows, error=error, extra=extra)


# ---------------------------------------------------------------- type mapping

@pytest.mark.parametrize("duck,spark", [
    (T("integer"), "int"), (T("bigint"), "bigint"), (T("varchar"), "string"), (T("blob"), "binary"),
    (T("boolean"), "boolean"), (T("null"), "void"), (T("float"), "float"), (T("double"), "double"),
    (T("date"), "date"), (T("timestamp"), "timestamp"), (T("timestamp with time zone"), "timestamp"),
    (T("interval"), "interval"), (T("tinyint"), "tinyint"), (T("smallint"), "smallint"),
    (T("decimal", [("precision", 10), ("scale", 2)]), "decimal(10,2)"),
    (T("list", [("child", T("integer"))]), "array<int>"),
    (T("list", [("child", T("list", [("child", T("varchar"))]))]), "array<array<string>>"),
    (T("map", [("key", T("integer")), ("value", T("varchar"))]), "map<int,string>"),
    (T("struct", [("a", T("integer")), ("b", T("varchar"))]), "struct<a:int,b:string>"),
    (T("hugeint"), "hugeint"),
])
def test_duck_type_to_spark(duck, spark):
    assert duck_type_to_spark(duck) == spark


def test_representation_limits_are_not_hidden():
    # DuckDB TIMESTAMP must not match timestamp_ntz, INTERVAL must not match an interval range
    assert not types_equal(["timestamp_ntz"], [duck_type_to_spark(T("timestamp"))])
    assert not types_equal(["interval day to second"], [duck_type_to_spark(T("interval"))])
    assert not types_equal(["interval year to month"], [duck_type_to_spark(T("interval"))])


def test_char_varchar_are_string():
    assert types_equal(["char(5)", "varchar(3)", "int"], ["string", "string", "int"])
    assert not types_equal(["int"], ["bigint"])
    assert not types_equal(["double"], ["decimal(2,1)"])
    assert not types_equal(["int", "int"], ["int"])


# ---------------------------------------------------------------- numbers

def test_normalize_and_tolerance():
    assert harness.values_equal(normalize(1.0), normalize(1.0 + 1e-12))
    assert harness.values_equal(normalize(1), normalize(decimal.Decimal("1.00")))


def test_exact_mode():
    zero, neg = normalize(0.0), normalize(-0.0)
    assert harness.values_equal(zero, neg)  # tolerance mode: -0.0 equals 0.0
    with exact_mode():
        assert not harness.values_equal(zero, neg)
        assert harness.values_equal(normalize(float("nan")), normalize(float("nan")))
        assert not harness.values_equal(normalize(1.0), normalize(1.0 + 1e-12))
        assert not harness.values_equal(normalize(1.0), normalize(1))        # float against integer
        assert harness.values_equal(normalize(decimal.Decimal("1.5")), normalize(decimal.Decimal("1.50")))
        assert not harness.values_equal(normalize(decimal.Decimal("1.5")), normalize(decimal.Decimal("1.5000001")))
        assert results_equal([[normalize(1.5)]], [[normalize(1.5)]], True)
    assert harness.values_equal(zero, neg)  # restored


# ---------------------------------------------------------------- directives

def test_matrix_and_slug():
    combos = matrix_combinations([("a.b", ["1", "2"]), ("c", ["x"])])
    assert combos == [(("a.b", "1"), ("c", "x")), (("a.b", "2"), ("c", "x"))]
    assert matrix_combinations([]) == [()]
    assert config_slug(()) == ""
    assert config_slug((("spark.sql.ansi.enabled", "true"),)) == "ansi.enabled=true"
    assert "/" not in config_slug((("spark.sql.session.timeZone", "America/Los_Angeles"),))


def test_load_cases_directives(tmp_path):
    f = tmp_path / "x.sql"
    f.write_text("-- @exact\n-- @matrix k=1,2\n\n-- @set a.b=1\n-- @set c.d=two words\n-- @xfail some-id because\nSELECT 1;\n\nSELECT 2;\n")
    d = harness.file_directives(f)
    assert d["exact"] and d["matrix"] == [("k", ["1", "2"])]
    cases = harness.load_cases([f])
    assert len(cases) == 2
    assert cases[0].sets == [("a.b", "1"), ("c.d", "two words")]
    assert cases[0].xfail == ("some-id", "because")
    assert cases[1].sets == [] and cases[1].xfail is None


# ---------------------------------------------------------------- judge

OK = {"ok": True, "rows": [[("num", decimal.Decimal(1))]], "types": ["int"], "names": ["a"]}


def test_judge_pass_and_rows_types_names():
    r = R(rows=[[("num", decimal.Decimal(1))]], spark_types=["int"], names=["a"])
    assert judge("SELECT 1 AS a", False, {}, OK, r).kind == "PASS"
    assert judge("SELECT 1 AS a", False, {}, OK, R(rows=[[("num", decimal.Decimal(2))]], spark_types=["int"],
                                                   names=["a"])).kind == "FAIL"
    # INT against BIGINT must differ even though the rows are the same
    assert judge("SELECT 1 AS a", False, {}, OK, R(rows=OK["rows"], spark_types=["bigint"], names=["a"])).kind == "FAIL"
    # an alias must match, a generated name is only a note
    assert judge("SELECT 1 AS a", False, {}, OK, R(rows=OK["rows"], spark_types=["int"], names=["b"])).kind == "FAIL"
    generated = dict(OK, names=["(1 + 1)"])
    out = judge("SELECT 1 + 1", False, {}, generated, R(rows=OK["rows"], spark_types=["int"], names=["1 + 1"]))
    assert out.kind == "PASS" and out.notes["names"] == "report"
    assert judge("SELECT 1, 2", False, {}, OK, R(rows=OK["rows"], spark_types=["int", "int"],
                                                  names=["a", "b"])).kind == "FAIL"


def test_judge_commands_and_nondeterministic():
    expected = {"ok": True, "rows": []}
    assert judge("CREATE TABLE t(a INT)", False, {}, expected, R(rows=[[("num", decimal.Decimal(0))]])).kind == "PASS"
    wrong_rows = R(rows=[[("num", decimal.Decimal(9))]], spark_types=["int"], names=["a"])
    assert judge("SELECT rand()", False, {"nondeterministic": ""}, OK, wrong_rows).kind == "PASS"


def test_judge_errors():
    err = {"ok": False, "error": "[DIVIDE_BY_ZERO] boom", "phase": "runtime", "error_class": "DIVIDE_BY_ZERO"}
    exec_error = R("error", error="Invalid Input Error: [DIVIDE_BY_ZERO] x", stage="exec", error_class="DIVIDE_BY_ZERO",
                   exc="InvalidInputException")
    assert judge("SELECT 1/0", False, {}, err, exec_error).kind == "PASS"
    bind_error = R("error", error="Binder Error: x", stage="bind", exc="BinderException")
    assert judge("SELECT 1/0", False, {}, err, bind_error, "spark").kind == "PHASE"
    assert judge("SELECT 1/0", False, {}, err, bind_error, "spark_strict").kind == "FAIL"
    assert judge("SELECT 1/0", False, {}, err, R(rows=[]), "spark").kind == "LENIENT"
    assert judge("SELECT 1/0", False, {}, err, R(rows=[]), "spark_strict").kind == "FAIL"
    # error class only gates strict, in types/ and settings/ files, for the listed classes
    wrong_class = R("error", error="Invalid Input Error: [CAST_OVERFLOW] x", stage="exec", error_class="CAST_OVERFLOW")
    assert judge("SELECT 1/0", False, {}, err, wrong_class, "spark_strict", "operators.sql").kind == "PASS"
    assert judge("SELECT 1/0", False, {}, err, wrong_class, "spark_strict", "types__c3.sql").kind == "FAIL"
    assert judge("SELECT 1/0", False, {}, err, wrong_class, "spark", "types__c3.sql").kind == "PASS"
    assert judge("SELECT 1", False, {}, err, R("crash", error="worker died")).kind == "FAIL"
    assert judge("SELECT 1", False, {}, err, R("error", error="INTERNAL Error: x", stage="exec")).kind == "FAIL"


def test_error_class_matching():
    assert error_class_matches("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", "DATATYPE_MISMATCH")
    assert not error_class_matches("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", "DATATYPE_MISMATCH.OTHER")
    assert error_class_matches("DIVIDE_BY_ZERO", "DIVIDE_BY_ZERO")
    assert not error_class_matches("DIVIDE_BY_ZERO", None)


def test_names_status():
    assert names_status(["a", "b"], ["a", "b"]) == ("ok", None)
    assert names_status(["a"], ["a", "b"])[0] == "count"
    assert names_status(["a"], ["A"], "SELECT 1 AS a")[0] == "gating"
    assert names_status(["a"], ["A"], "SELECT a FROM t")[0] == "gating"
    assert names_status(["a", "b"], ["a", "B"], "SELECT a, t.b FROM t")[0] == "gating"
    assert names_status(["count(1)"], ["count_star()"], "SELECT count(1)")[0] == "report"
    assert names_status(["1"], ["1"]) == ("ok", None)
    # Spark names CAST(x AS INT) `x`: generated, so not gating
    assert names_status(["x"], ["cast_value(x)"], "SELECT CAST(x AS INT) FROM VALUES (1) t(x)")[0] == "report"
    assert names_status(["col"], ["foo"], "SELECT explode(a) FROM t")[0] == "report"


# ---------------------------------------------------------------- xfail

def _case(file="types__c1.sql", index=1, sql="SELECT 1", xfail=None):
    c = Case(file, index, sql, {"xfail": xfail} if xfail else {})
    c.set_list = []
    return c


def test_xfail_marker_and_entries():
    fail, ok = Outcome("FAIL", "boom"), Outcome("PASS")
    out = apply_xfail(_case(xfail="types-ntz reason"), "", "spark", fail, [])
    assert out.kind == "XFAIL" and out.notes["xfail"] == "types-ntz"
    assert apply_xfail(_case(xfail="types-ntz reason"), "", "spark", ok, []).kind == "XPASS"
    entry = XfailEntry({"id": "x", "file": "types/c1.sql", "cases": [1, "5-6"], "dialects": ["spark"]})
    assert apply_xfail(_case(index=5), "", "spark", fail, [entry]).kind == "XFAIL"
    assert apply_xfail(_case(index=5), "", "spark_strict", fail, [entry]).kind == "FAIL"   # other dialect
    assert apply_xfail(_case(index=2), "", "spark", fail, [entry]).kind == "FAIL"           # not listed
    assert apply_xfail(_case(index=6), "", "spark", ok, [entry]).kind == "XPASS"
    by_sql = XfailEntry({"id": "y", "file": "types/*.sql", "sql": "TIMESTAMP_NTZ", "config": "ansi*"})
    assert apply_xfail(_case(sql="SELECT TIMESTAMP_NTZ '1'"), "ansi.enabled=true", "spark", fail, [by_sql]).kind == "XFAIL"
    assert apply_xfail(_case(sql="SELECT TIMESTAMP_NTZ '1'"), "", "spark", fail, [by_sql]).kind == "FAIL"
    assert entry.hits >= 2 and getattr(entry, "fails", 0) >= 1


def test_xfail_entry_config_semantics():
    """An entry with config = "" applies to the default configuration only; no config applies to every one; a glob matches slugs."""
    from harness import XfailEntry
    base = {"id": "x", "file": "a.sql"}
    assert XfailEntry({**base, "config": ""}).applies_to_file("a.sql", "")
    assert not XfailEntry({**base, "config": ""}).applies_to_file("a.sql", "ansi.enabled=true")
    assert XfailEntry(base).applies_to_file("a.sql", "ansi.enabled=true")
    assert XfailEntry({**base, "config": "ansi.enabled=true*"}).applies_to_file("a.sql", "ansi.enabled=true_storeAssignmentPolicy=ANSI")
    assert not XfailEntry({**base, "file": "b.sql"}).applies_to_file("a.sql", "")


def test_case_groups_cover_every_generated_file():
    """Every file under test/cases/types and test/cases/settings belongs to exactly one CI group."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import case_group
    groups = ["base", "settings", "corpus", "types-casts", "types-operators", "types-common", "types-args", "types-compose", "types-rest"]
    seen = [p for g in groups for p in case_group.files(g)]
    assert len(seen) == len(set(seen))
    cases = Path(__file__).resolve().parent / "cases"
    everything = {p for p in cases.rglob("*.sql")}
    assert everything - set(seen) == set(), sorted(everything - set(seen))


def test_error_class_gating_is_limited_to_types_and_settings_files():
    from harness import class_is_gated
    assert class_is_gated("DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE", "types__c3_arithmetic.sql")
    assert not class_is_gated("DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE", "operators.sql")


def test_spark_oracle_restarts_after_a_jvm_that_dies_with_the_statement():
    """A runtime error whose text cannot be read (the JVM is gone) or that names a fatal error below its first line
    keeps the statement's error and restarts the JVM before the next statement, without starting Spark here."""
    from engines import SparkOracle

    class Unreadable(Exception):
        def __str__(self):
            raise ConnectionRefusedError("[Errno 111] Connection refused")

    class Fatal(Exception):
        def __str__(self):
            return "An error occurred while calling o1.collectToPython.\njava.lang.OutOfMemoryError: Java heap space"

    for exc in (Unreadable(), Fatal()):
        oracle = SparkOracle.__new__(SparkOracle)
        restarts = []
        oracle._restart = lambda: restarts.append(1)
        oracle._alive = lambda: True

        def collect(exc=exc):
            raise exc

        oracle.spark = SimpleNamespace(sql=lambda sql: SimpleNamespace(collect=collect))
        r = oracle.run("SELECT 1")
        assert r.status == "error" and r.phase == "runtime" and "fatal" not in r.extra
        assert restarts == [1]
