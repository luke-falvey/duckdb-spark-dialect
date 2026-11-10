"""Consistency checks of test/settings/inventory.toml (run in CI): python test/settings/check_inventory.py

  * every key of test/spark35_sqlconf.json is classified, and no entry names a key Spark does not have;
  * every `semantic` key has a witness whose Spark result differs (conf_witnesses.json `differs`);
  * a `semantic` key has a case file that exists; `status` is one of the allowed values and `emulated` / `partial` only
    for semantic keys;
  * the generated C++ registry (src/spark_conf_registry.inc) is up to date with the inventory.
"""
import json
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
CLASSES = {"semantic", "command", "storage", "execution", "client", "static", "unobservable"}
STATUSES = {"emulated", "partial", "accepted", "rejected"}


def main():
    inventory = tomllib.loads((ROOT / "test" / "settings" / "inventory.toml").read_text())["keys"]
    confs = json.loads((ROOT / "test" / "spark35_sqlconf.json").read_text())
    witnesses = {}
    for w in json.loads((ROOT / "docs" / "plans" / "spark-parity" / "conf_witnesses.json").read_text()):
        witnesses[w["key"]] = witnesses.get(w["key"], False) or w["differs"]
    problems = []
    keys = {c["key"] for c in confs}
    for key in sorted(keys - set(inventory)):
        problems.append(f"{key}: missing from the inventory")
    for key in sorted(set(inventory) - keys):
        problems.append(f"{key}: not a Spark 3.5 setting")
    for key, entry in sorted(inventory.items()):
        cls = entry.get("class")
        if cls not in CLASSES:
            problems.append(f"{key}: unknown class {cls!r}")
            continue
        if entry.get("status") not in STATUSES:
            problems.append(f"{key}: unknown status {entry.get('status')!r}")
        if cls != "semantic" and entry.get("status") in ("emulated", "partial", "rejected"):
            problems.append(f"{key}: status {entry['status']} for class {cls}")
        if cls == "semantic":
            if not witnesses.get(key):
                problems.append(f"{key}: semantic key without a witness whose Spark result differs")
            cases = entry.get("cases")
            if not cases or not (ROOT / cases).exists():
                problems.append(f"{key}: case file {cases!r} does not exist")
            if entry.get("tier") not in (1, 2, 3):
                problems.append(f"{key}: semantic key without a tier")
            if entry.get("tier") in (1, 2) and entry.get("status") == "rejected" and not entry.get("note"):
                problems.append(f"{key}: tier {entry['tier']} key still rejected without a note (plan 9.2)")
        if cls == "unobservable" and not entry.get("note"):
            problems.append(f"{key}: unobservable key without a note saying what was tried")
    registry = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_conf_registry.py"), "--check"],
                              capture_output=True, text=True)
    if registry.returncode != 0:
        problems.append("src/spark_conf_registry.inc is out of date: run scripts/gen_conf_registry.py")
    for p in problems:
        print("INVENTORY:", p)
    print(f"{len(inventory)} keys checked, {len(problems)} problems")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
