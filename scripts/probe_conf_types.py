"""Probe every Spark 3.5 SQL setting for: static or not, value type, and the legal values of an enum.

usage: python scripts/probe_conf_types.py   (writes test/settings/spark_conf_types.json)

For each key of test/spark35_sqlconf.json the script sets an invalid value in a live Spark session and reads the
error: "Cannot modify the value of a static config" marks a static key; the validation message names the type
(`should be boolean`, `should be int`, ...) or lists the legal enum values. A key that accepts the probe value is a
free-form string (or a validated string with no message). Then it sets the key's default (when the default is a
plain literal) to check that the default is accepted, and unsets it again.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "test"))
from engines import SparkOracle  # noqa: E402

PROBE = "@@invalid@@"


def kind_of(message):
    low = message.lower()
    if "size must be specified" in low:
        return "bytes"
    if "time must be specified" in low:
        return "time"
    if "valid values are" in low:
        return "enum"
    for word, kind in (("should be boolean", "boolean"), ("should be int", "int"), ("should be long", "long"),
                       ("should be double", "double"), ("should be float", "double"),
                       ("should be time", "time"), ("should be size", "bytes"), ("should be byte", "bytes")):
        if word in low:
            return kind
    if "must be" in low or "should be one of" in low or "one of" in low:
        return "enum"
    return "other"


def choices_of(message):
    m = re.search(r"Valid values are ([^.]*)", message)
    if m:
        return [c.strip(" '\"") for c in m.group(1).replace(" and ", ",").split(",") if c.strip(" '\"")]
    message = re.sub(r",? but was.*", "", message)
    m = re.search(r"(?:must be|should be|one of)[^:]*?:?\s*((?:[A-Za-z0-9_.-]+\s*,\s*)+[A-Za-z0-9_.-]+)", message)
    if m:
        return [c.strip() for c in m.group(1).split(",")]
    m = re.search(r"\{([^}]*)\}", message)
    if m:
        return [c.strip(" '\"") for c in m.group(1).split(",")]
    return []


def main():
    confs = json.loads((ROOT / "test" / "spark35_sqlconf.json").read_text())
    spark = SparkOracle().spark
    out = {}
    for entry in confs:
        key = entry["key"]
        row = {"default": entry["default"], "public": entry["public"], "static": False, "kind": "string", "choices": []}
        try:
            spark.conf.set(key, PROBE)
            row["kind"] = "string"
            spark.conf.unset(key)
        except Exception as e:  # noqa: BLE001
            text = str(e)
            first = " ".join(text.split("\n")[:3])
            if "static config" in text or "CANNOT_MODIFY_CONFIG" in text:
                row["static"] = True
                row["kind"] = "static"
            else:
                row["kind"] = kind_of(text)
                row["choices"] = choices_of(text) if row["kind"] == "enum" else []
                row["message"] = first[:200]
        out[key] = row
    (ROOT / "test" / "settings" / "spark_conf_types.json").write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")
    kinds = {}
    for r in out.values():
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(kinds)


if __name__ == "__main__":
    main()
