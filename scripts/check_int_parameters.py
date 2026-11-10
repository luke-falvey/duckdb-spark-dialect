"""Compare the hand-collected INT_PARAMETERS table of src/spark_grammar.cpp with the functions derived from Spark's
answers (test/golden/int_parameters_derived.json, written by `test/fuzz/args_sweep.py --types --record`): a function
whose argument slot gives the same result for 4294967297L as for 1 wraps a BIGINT to INT in Spark (plan 7.4).

usage: python scripts/check_int_parameters.py
Report only: the slot numbers of the derived list count literal slots of the documentation example, not argument
positions, and arguments that are ignored for some values also match, so every difference is a lead to read, not a bug.
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
grammar = (ROOT / "src" / "spark_grammar.cpp").read_text()
block = grammar[grammar.index("INT_PARAMETERS = {"):]
block = block[:block.index("};")]
table = set(re.findall(r'\{"(\w+)", \{', block))
derived = set(json.loads((ROOT / "test" / "golden" / "int_parameters_derived.json").read_text()))
print(f"table: {len(table)} functions, derived from Spark's answers: {len(derived)}")
print("in the table but not derived:", ", ".join(sorted(table - derived)) or "-")
print("derived but not in the table:", ", ".join(sorted(derived - table)) or "-")
