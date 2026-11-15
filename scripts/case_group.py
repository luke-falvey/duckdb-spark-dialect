"""Print the case files of a CI group (plan: CI sharding), one per argument, for `make_golden.py` / `run_duckdb.py`.

usage: python scripts/case_group.py base|settings|types-casts|types-operators|types-common|types-rest
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CASES = ROOT / "test" / "cases"
TYPES = {
    "types-casts": ["c1_casts", "c2_string_rendering", "c9_literals", "c13_nested", "c16_formats", "c17_type_names"],
    "types-operators": ["c3_arithmetic", "c4_comparison", "c8_decimal_precision", "c10_datetime_arithmetic"],
    "types-common": ["c5_common_type"],
    "types-args": ["c6_function_args"],
    "types-compose": ["c15_composition", "c18_from_shapes"],
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


CONFIGS = {  # workstream D: the files each configuration reruns (plan section 8)
    "ansi": {"types-casts": ["c1_casts", "c2_string_rendering", "c9_literals", "c13_nested"],
             "types-operators": ["c3_arithmetic", "c4_comparison", "c8_decimal_precision", "c10_datetime_arithmetic"],
             "types-common": ["c5_common_type"],
             "types-rest": ["c7_store_assignment", "c11_boolean_contexts", "c12_aggregates_windows", "c14_equality_ordering"],
             "base": ["operators", "literals_types", "functions_examples", "queries", "ddl_dml"]},
    "tz": {"types-casts": ["c1_casts", "c2_string_rendering", "c16_formats"], "types-operators": ["c4_comparison", "c10_datetime_arithmetic"],
           "types-rest": ["c12_aggregates_windows"], "base": ["functions_examples"]},
    "apl": {"types-operators": ["c3_arithmetic", "c8_decimal_precision"]},
}


def config_files(config, group):
    out = []
    for name in CONFIGS[config].get(group, []):
        out.append(CASES / ("types" if name.startswith("c") and name[1].isdigit() else ".") / f"{name}.sql")
    return out


if __name__ == "__main__":
    # case_group.py GROUP            the files of a group
    # case_group.py GROUP --config X the files of the group that configuration X (ansi, tz, apl) reruns
    if len(sys.argv) > 2 and sys.argv[2] == "--config":
        print(" ".join(str(p) for p in config_files(sys.argv[3], sys.argv[1])))
    else:
        print(" ".join(str(p) for p in files(sys.argv[1])))
