#!/bin/bash
# usage: regen.sh RUNDIR  -- (RUNDIR from run_all_configs.sh) regenerate test/xfail.toml from a fullrun.sh directory (XFAIL blocks turned into FAIL)
set -eu
R=$1; C=$R/conv
cd "$(dirname "$0")/../.."
cp test/xfail.toml $R/xfail_before.toml
for c in def ansi tz apl; do mkdir -p $C/$c; for f in failures.txt failures_spark_strict.txt; do
  python3 scripts/dev/xfail_to_fail.py $R/$c/$f $C/$c/$f $R/xfail_before.toml; done; done
ALL=$(cd test/cases && find . -name "*.sql" | sed 's#^\./##' | sort)
ANSI=$(for g in base types-casts types-operators types-common types-rest; do python3 scripts/case_group.py $g --config ansi; done | tr ' ' '\n' | sed 's#.*/test/cases/##')
TZ=$(for g in base types-casts types-operators types-rest; do python3 scripts/case_group.py $g --config tz; done | tr ' ' '\n' | sed 's#.*/test/cases/##')
APL=$(python3 scripts/case_group.py types-operators --config apl | tr ' ' '\n' | sed 's#.*/test/cases/##')
python3 scripts/xfail_from_failures.py --write --results $C/def --files $ALL
python3 scripts/xfail_from_failures.py --write --results $C/ansi --config-slug ansi.enabled=true --files $ANSI
python3 scripts/xfail_from_failures.py --write --results $C/tz --config-slug session.timeZone=America-Los_Angeles --files $TZ
python3 scripts/xfail_from_failures.py --write --results $C/apl --config-slug decimalOperations.allowPrecisionLoss=false --files $APL
