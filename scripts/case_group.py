"""Print the case files of a CI group (plan: CI sharding), one per argument, for `make_golden.py` / `run_duckdb.py`.

usage: python scripts/case_group.py base|settings|types-casts|types-operators|types-common|types-rest
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CASES = ROOT / "test" / "cases"
TYPES = {
    "types-casts": ["c1_casts", "c2_string_rendering", "c9_literals", "c13_nested"],
    "types-operators": ["c3_arithmetic", "c4_comparison", "c8_decimal_precision", "c10_datetime_arithmetic"],
    "types-common": ["c5_common_type"],
    "types-args": ["c6_function_args"],
    "types-rest": ["c7_store_assignment", "c11_boolean_contexts", "c12_aggregates_windows", "c14_equality_ordering"],
}


def files(group):
    if group == "base":
        return sorted(list(CASES.glob("*.sql")) + list((CASES / "ansi").glob("*.sql")))
    if group == "corpus":
        return sorted((CASES / "spark_corpus").glob("*.sql"))
    if group == "settings":
        return sorted((CASES / "settings").glob("*.sql"))
    return [CASES / "types" / f"{n}.sql" for n in TYPES[group]]


if __name__ == "__main__":
    print(" ".join(str(p.relative_to(ROOT)) if False else str(p) for p in files(sys.argv[1])))
