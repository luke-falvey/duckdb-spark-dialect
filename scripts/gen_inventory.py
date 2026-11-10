"""Create or update test/settings/inventory.toml: one entry per Spark 3.5 SQL setting (plan section B1).

usage: python scripts/gen_inventory.py [--check]

Inputs: test/spark35_sqlconf.json (key, default, doc), test/settings/spark_conf_types.json (type, static, legal values;
scripts/probe_conf_types.py), docs/plans/spark-parity/conf_witnesses.json (witnesses with `differs`).
Existing entries keep their hand-written fields (`class`, `tier`, `status`, `note`) unless the key is new; the
classification of a new key follows RULES below. Fields of an entry:

  class    semantic | command | storage | execution | client | static | unobservable      (plan B1)
  tier     1 | 2 | 3                                (semantic only: plan 9.2)
  status   emulated | partial | accepted | rejected  (what the dialect does with `SET key = value`)
  kind     boolean | int | long | double | bytes | time | enum | string | static  (value validation)
  values   the legal values to test (enum choices, true / false, or the witnessed values)
  default  Spark's default
  cases    the case file of the key (semantic keys)
  note     why, for unobservable keys what was tried
"""
import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INVENTORY = ROOT / "test" / "settings" / "inventory.toml"

TIER1 = ["spark.sql.ansi.enabled", "spark.sql.session.timeZone", "spark.sql.storeAssignmentPolicy",
         "spark.sql.mapKeyDedupPolicy", "spark.sql.caseSensitive", "spark.sql.legacy.timeParserPolicy"]
TIER2 = ["spark.sql.legacy.sizeOfNull", "spark.sql.groupByOrdinal", "spark.sql.orderByOrdinal",
         "spark.sql.groupByAliases", "spark.sql.jsonGenerator.ignoreNullFields",
         "spark.sql.function.concatBinaryAsString", "spark.sql.function.eltOutputAsString",
         "spark.sql.legacy.statisticalAggregate", "spark.sql.legacy.negativeIndexInArrayInsert",
         "spark.sql.legacy.nullInEmptyListBehavior", "spark.sql.chunkBase64String.enabled",
         "spark.sql.decimalOperations.allowPrecisionLoss", "spark.sql.legacy.setopsPrecedence.enabled",
         "spark.sql.crossJoin.enabled", "spark.sql.parser.escapedStringLiterals",
         "spark.sql.parser.quotedRegexColumnNames", "spark.sql.ansi.doubleQuotedIdentifiers",
         "spark.sql.ansi.enforceReservedKeywords", "spark.sql.ansi.relationPrecedence",
         "spark.sql.variable.substitute", "spark.sql.lateralColumnAlias.enableImplicitResolution",
         "spark.sql.legacy.castComplexTypesToString.enabled", "spark.sql.legacy.followThreeValuedLogicInArrayExists",
         "spark.sql.legacy.allowParameterlessCount", "spark.sql.legacy.createHiveTableByDefault",
         "spark.sql.legacy.ctePrecedencePolicy", "spark.sql.charAsVarchar"]

# first prefix match wins; (prefix, class)
PREFIX_CLASS = [
    ("spark.sql.streaming.", "execution"), ("spark.sql.optimizer.", "execution"), ("spark.sql.adaptive.", "execution"),
    ("spark.sql.codegen.", "execution"), ("spark.sql.cbo.", "execution"), ("spark.sql.statistics.", "execution"),
    ("spark.sql.inMemoryColumnarStorage.", "execution"), ("spark.sql.execution.pyspark", "client"),
    ("spark.sql.execution.arrow", "client"), ("spark.sql.execution.pandas", "client"),
    ("spark.sql.execution.", "execution"),
    ("spark.sql.parquet.", "storage"), ("spark.sql.orc.", "storage"), ("spark.sql.avro.", "storage"),
    ("spark.sql.hive.", "storage"), ("spark.sql.files.", "storage"), ("spark.sql.csv.", "storage"),
    ("spark.sql.ui.", "client"), ("spark.sql.thriftserver.", "client"), ("spark.sql.thriftServer.", "client"),
    ("spark.sql.repl.", "client"), ("spark.sql.pyspark.", "client"), ("spark.sql.cli.", "client"),
    ("spark.sql.debug", "client"), ("spark.sql.planChangeLog.", "client"), ("spark.sql.redaction.", "client"),
    ("spark.sql.sortMergeJoinExec.", "execution"), ("spark.sql.cartesianProductExec.", "execution"),
    ("spark.sql.windowExec.", "execution"), ("spark.sql.sessionWindow.", "execution"),
    ("spark.sql.subexpressionElimination.", "execution"), ("spark.sql.bucketing.", "execution"),
    ("spark.sql.limit.", "execution"), ("spark.sql.udt.", "client"), ("spark.sql.analyzer.", "execution"),
    ("spark.sql.broadcast", "execution"), ("spark.sql.autoBroadcast", "execution"),
    ("spark.sql.sources.", "storage"),
]

# explicit classes (checked before the prefixes)
EXPLICIT = {
    # semantic keys with no witness yet, or classified by reading the doc
    "spark.sql.analyzer.failAmbiguousSelfJoin": "client", "spark.sql.pivotMaxValues": "client",
    "spark.sql.readSideCharPadding": "semantic", "spark.sql.legacy.noCharPaddingInPredicate": "semantic",
    "spark.sql.defaultColumn.allowedProviders": "storage", "spark.sql.legacy.useCurrentConfigsForView": "semantic",
    "spark.sql.legacy.v1IdentifierNoCatalog": "semantic", "spark.sql.legacy.inSubqueryNullability": "semantic",
    "spark.sql.legacy.doLooseUpcast": "semantic", "spark.sql.legacy.groupingIdWithAppendedUserGroupBy": "semantic",
    "spark.sql.legacy.allowTempViewCreationWithMultipleNameparts": "semantic",
    "spark.sql.legacy.json.allowEmptyString.enabled": "semantic", "spark.sql.defaultCatalog": "semantic",
    "spark.sql.tvf.allowMultipleTableArguments.enabled": "semantic",
    "spark.sql.jsonGenerator.writeNullIfWithDefaultValue": "storage", "spark.sql.json.enablePartialResults": "semantic",
    "spark.sql.columnNameOfCorruptRecord": "semantic",
    # command output only
    "spark.sql.legacy.useV1Command": "command", "spark.sql.legacy.keepCommandOutputSchema": "command",
    "spark.sql.error.messageFormat": "command", "spark.sql.maxMetadataStringLength": "command",
    "spark.sql.maxPlanStringLength": "command", "spark.sql.legacy.emptyCurrentDBInCli": "client",
    # client API
    "spark.sql.datetime.java8API.enabled": "client", "spark.sql.retainGroupColumns": "client",
    "spark.sql.selfJoinAutoResolveAmbiguity": "client", "spark.sql.legacy.allowUntypedScalaUDF": "client",
    "spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue": "client",
    "spark.sql.legacy.respectNullabilityInTextDatasetConversion": "client",
    "spark.sql.legacy.extraOptionsBehavior.enabled": "client", "spark.sql.legacy.pathOptionBehavior.enabled": "client",
    "spark.sql.legacy.dataFrameWriterV2IgnorePathOption": "client", "spark.sql.runSQLOnFiles": "storage",
    # storage
    "spark.sql.sources.partitionOverwriteMode": "semantic", "spark.sql.sources.default": "storage",
    "spark.sql.legacy.keepPartitionSpecAsStringLiteral": "semantic",
    "spark.sql.legacy.parseNullPartitionSpecAsStringLiteral": "semantic",
    "spark.sql.legacy.allowEmptySchemaWrite": "storage", "spark.sql.legacy.allowNonEmptyLocationInCTAS": "storage",
    "spark.sql.legacy.avro.allowIncompatibleSchema": "storage", "spark.sql.legacy.bucketedTableScan.outputOrdering": "execution",
    "spark.sql.legacy.csv.enableDateTimeParsingFallback": "storage", "spark.sql.legacy.json.enableDateTimeParsingFallback": "storage",
    "spark.sql.legacy.mssqlserver.numericMapping.enabled": "storage", "spark.sql.legacy.nullValueWrittenAsQuotedEmptyStringCsv": "storage",
    "spark.sql.legacy.parquet.nanosAsLong": "storage", "spark.sql.legacy.replaceDatabricksSparkAvro.enabled": "storage",
    "spark.sql.legacy.skipTypeValidationOnAlterPartition": "storage", "spark.sql.legacy.addSingleFileInAddFile": "storage",
    "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": "client",
    "spark.sql.maxConcurrentOutputFileWriters": "storage", "spark.sql.truncateTable.ignorePermissionAcl.enabled": "storage",
    "spark.sql.maven.additionalRemoteRepositories": "client", "spark.sql.catalog.spark_catalog": "storage",
    "spark.sql.defaultSizeInBytes": "execution", "spark.sql.maxSinglePartitionBytes": "execution",
    "spark.sql.leafNodeDefaultParallelism": "execution", "spark.sql.shuffle.partitions": "execution",
    "spark.sql.session.localRelationCacheThreshold": "execution", "spark.sql.addPartitionInBatch.size": "storage",
    "spark.sql.cache.serializer": "static", "spark.sql.constraintPropagation.enabled": "execution",
    "spark.sql.exchange.reuse": "execution", "spark.sql.inMemoryTableScanStatistics.enable": "execution",
    "spark.sql.join.preferSortMergeJoin": "execution", "spark.sql.json.filterPushdown.enabled": "execution",
    "spark.sql.objectHashAggregate.sortBased.fallbackThreshold": "execution", "spark.sql.optimizeNullAwareAntiJoin": "execution",
    "spark.sql.planChangeValidation": "execution", "spark.sql.requireAllClusterKeysForCoPartition": "execution",
    "spark.sql.requireAllClusterKeysForDistribution": "execution", "spark.sql.shuffledHashJoinFactor": "execution",
    "spark.sql.sort.enableRadixSort": "execution", "spark.sql.columnVector.offheap.enabled": "execution",
    "spark.sql.scriptTransformation.exitTimeoutInSeconds": "execution",
    "spark.sql.legacy.storeAnalyzedPlanForView": "semantic",
}

# settings whose witness exists but whose effect the Spark dialect cannot show (or does not need to): see note
UNOBSERVABLE = {
    "spark.sql.readSideCharPadding": "tried length(c) of a CHAR(5) column of a parquet table written by INSERT: padded on write, same with both values (the setting matters for files written by other engines)",
    "spark.sql.legacy.noCharPaddingInPredicate": "tried c = 'ab   ' on a CHAR(5) column: same result with both values in Spark 3.5.9",
    "spark.sql.legacy.v1IdentifierNoCatalog": "no catalog other than spark_catalog exists in the dialect",
    "spark.sql.tvf.allowMultipleTableArguments.enabled": "table-valued function arguments of type TABLE need user-defined table functions, which the dialect does not have",
    "spark.sql.defaultCatalog": "no catalog other than spark_catalog exists in the dialect",
    "spark.sql.legacy.useV1Command": "only changes which command implementation runs",
    "spark.sql.legacy.doLooseUpcast": "Dataset encoders only (no SQL statement differs)",
    "spark.sql.legacy.groupingIdWithAppendedUserGroupBy": "only the grouping_id() value when GROUP BY has extra columns after ROLLUP: tried grouping_id() with GROUP BY a, ROLLUP(b); same result with both values in Spark 3.5.9",
    "spark.sql.legacy.inSubqueryNullability": "nullability metadata only; result rows are equal",
    "spark.sql.legacy.respectNullabilityInTextDatasetConversion": "Dataset API",
    "spark.sql.legacy.json.allowEmptyString.enabled": "from_json of an empty string: same result with both values in Spark 3.5.9",
    "spark.sql.legacy.allowTempViewCreationWithMultipleNameparts": "same result with both values for the statements tried (CREATE TEMPORARY VIEW a.b)",
}

KIND_ALIASES = {"other": "string"}


def classify(key, kind, witnesses):
    if kind == "static":
        return "static"
    if key in EXPLICIT:
        return EXPLICIT[key]
    for prefix, cls in PREFIX_CLASS:
        if key.startswith(prefix):
            return cls
    if key.startswith("spark.sql.legacy.") or key in witnesses:
        return "semantic"
    return "semantic"


def short_name(key):
    return key[len("spark.sql."):].replace(".", "_")


def tier_of(key):
    return 1 if key in TIER1 else 2 if key in TIER2 else 3


def toml_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(v) for v in value) + "]"
    return json.dumps(str(value))


def main():
    confs = json.loads((ROOT / "test" / "spark35_sqlconf.json").read_text())
    types = json.loads((ROOT / "test" / "settings" / "spark_conf_types.json").read_text())
    witnesses = {}
    for w in json.loads((ROOT / "docs" / "plans" / "spark-parity" / "conf_witnesses.json").read_text()):
        entry = witnesses.setdefault(w["key"], {"differs": False, "values": []})
        entry["differs"] = entry["differs"] or w["differs"]
        entry["values"].append(w["value"])
    existing = {}
    if INVENTORY.exists():
        existing = tomllib.loads(INVENTORY.read_text()).get("keys", {})
    lines = ["# Classification of all Spark 3.5 SQL settings (docs/plans/spark-parity-testing.md, section B1).",
             "# Generated by scripts/gen_inventory.py; the fields class / tier / status / note may be edited by hand",
             "# (a hand-edited entry is kept). `status` is what the dialect does with `SET key = value` for a value",
             "# other than the default: emulated, partial (some values), accepted (stored, no effect: no result",
             "# can change) or rejected ([UNSUPPORTED_FEATURE.SET_CONF], plan 9.1).", ""]
    counts = {}
    for entry in confs:
        key = entry["key"]
        info = types[key]
        kind = KIND_ALIASES.get(info["kind"], info["kind"])
        old = existing.get(key, {})
        cls = old.get("class") or classify(key, info["kind"], witnesses)
        if key in UNOBSERVABLE and not old.get("class"):
            cls = "unobservable"
        values = old.get("values")
        if values is None:
            if kind == "boolean":
                values = ["true", "false"]
            elif kind == "enum":
                values = list(info["choices"])
            else:
                values = sorted(set(witnesses.get(key, {}).get("values", []))) or [info["default"]]
        row = {"class": cls, "kind": kind, "default": entry["default"], "values": values}
        if cls in ("semantic", "unobservable"):
            row["tier"] = old.get("tier") or tier_of(key)
        if cls == "semantic":
            row["status"] = old.get("status") or "rejected"
            row["cases"] = old.get("cases") or f"test/cases/settings/{short_name(key)}.sql"
        elif cls == "unobservable":
            row["status"] = old.get("status") or "accepted"
            row["cases"] = old.get("cases") or ""
        else:
            row["status"] = old.get("status") or "accepted"
        row["witness"] = key in witnesses
        row["differs"] = witnesses.get(key, {}).get("differs", False)
        note = old.get("note") or UNOBSERVABLE.get(key, "")
        row["note"] = note
        counts[cls] = counts.get(cls, 0) + 1
        lines.append(f'[keys."{key}"]')
        for field in ("class", "tier", "status", "kind", "default", "values", "cases", "witness", "differs", "note"):
            if field in row and not (field == "cases" and row[field] == ""):
                lines.append(f"{field} = {toml_value(row[field])}")
        lines.append("")
    text = "\n".join(lines)
    if "--check" in sys.argv:
        if not INVENTORY.exists() or INVENTORY.read_text() != text:
            print("inventory.toml is out of date: run scripts/gen_inventory.py")
            sys.exit(1)
        return
    INVENTORY.write_text(text)
    print(counts)


if __name__ == "__main__":
    main()
