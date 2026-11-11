#!/bin/bash
# usage: fullrun.sh OUTDIR  -- runs the 8 configuration x dialect runs (4 at a time), normal mode with test/xfail.toml
set -u
R=$1; rm -rf $R; mkdir -p $R/def $R/ansi $R/tz $R/apl
cd "$(dirname "$0")/../.."
ANSI=$(for g in base types-casts types-operators types-common types-rest; do python3 scripts/case_group.py $g --config ansi; done)
TZ=$(for g in base types-casts types-operators types-rest; do python3 scripts/case_group.py $g --config tz; done)
APL=$(python3 scripts/case_group.py types-operators --config apl)
python3 test/run_duckdb.py --results $R/def > $R/def_spark.log 2>&1 &
python3 test/run_duckdb.py --dialect spark_strict --results $R/def > $R/def_strict.log 2>&1 &
python3 test/run_duckdb.py --config spark.sql.ansi.enabled=true --results $R/ansi $ANSI > $R/ansi_spark.log 2>&1 &
python3 test/run_duckdb.py --dialect spark_strict --config spark.sql.ansi.enabled=true --results $R/ansi $ANSI > $R/ansi_strict.log 2>&1 &
wait
python3 test/run_duckdb.py --config spark.sql.session.timeZone=America/Los_Angeles --results $R/tz $TZ > $R/tz_spark.log 2>&1 &
python3 test/run_duckdb.py --dialect spark_strict --config spark.sql.session.timeZone=America/Los_Angeles --results $R/tz $TZ > $R/tz_strict.log 2>&1 &
python3 test/run_duckdb.py --config spark.sql.decimalOperations.allowPrecisionLoss=false --results $R/apl $APL > $R/apl_spark.log 2>&1 &
python3 test/run_duckdb.py --dialect spark_strict --config spark.sql.decimalOperations.allowPrecisionLoss=false --results $R/apl $APL > $R/apl_strict.log 2>&1 &
wait
for f in $R/*.log; do echo "$(basename $f .log): $(grep TOTAL $f | sed 's/  */ /g') stale=$(grep -c STALE $f)"; done
