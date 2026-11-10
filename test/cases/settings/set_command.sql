-- The SET / RESET command (plan section B2). The cases run in order in one session; Spark's answers are recorded.

-- @nondeterministic
SET;

-- @nondeterministic
SET -v;

SET spark.sql.shuffle.partitions;

SET spark.sql.mapKeyDedupPolicy;

SET spark.no.such.key;

SET my.custom.key;

SET spark.sql.shuffle.partitions=7;

SET spark.sql.shuffle.partitions;

SET spark.sql.shuffle.partitions = 8;

SET spark.sql.shuffle.partitions;

SET spark.sql.shuffle.partitions=`9`;

SET spark.sql.shuffle.partitions;

SET my.custom.key = a value with spaces;

SET my.custom.key;

SET my.custom.key = ;

SET my.custom.key;

SET my.custom.key = a=b;

SET my.custom.key;

SET my.custom.key = 'quoted';

SET my.custom.key;

SET my.custom.key=trailing   ;

SET my.custom.key;

SET spark.sql.ansi.enabled=maybe;

SET spark.sql.shuffle.partitions=text;

SET spark.sql.mapKeyDedupPolicy=NOT_A_POLICY;

SET spark.sql.broadcastTimeout=-1;

SET spark.sql.autoBroadcastJoinThreshold=10x;

SET spark.sql.caseSensitive=TRUE;

SET spark.sql.caseSensitive;

SET spark.sql.caseSensitive=false;

SET SPARK.SQL.ANSI.ENABLED=true;

SELECT CAST('a' AS INT);

SET SPARK.SQL.ANSI.ENABLED;

RESET SPARK.SQL.ANSI.ENABLED;

SET spark.sql.warehouse.dir=/tmp/x;

SET spark.sql.catalogImplementation=hive;

SET spark.task.cpus=2;

SET spark.sql.legacy.setCommandRejectsSparkCoreConfs=false;

SET spark.task.cpus=2;

SET spark.task.cpus;

SET spark.sql.legacy.setCommandRejectsSparkCoreConfs=true;

RESET spark.sql.shuffle.partitions;

SET spark.sql.shuffle.partitions;

RESET spark.sql.autoBroadcastJoinThreshold;

RESET spark.no.such.key;

RESET my.custom.key;

SET my.custom.key;

SET spark.sql.shuffle.partitions=11;

SET my.other.key=1;

RESET;

SET spark.sql.shuffle.partitions;

SET my.other.key;

RESET spark.sql.warehouse.dir;

SET spark.sql.ansi.enabled=true;

SET spark.sql.ansi.enabled;

SELECT CAST('a' AS INT);

SET spark.sql.ansi.enabled=false;

SELECT CAST('a' AS INT);

SET spark.sql.ansi.enabled=true;

RESET spark.sql.ansi.enabled;

SELECT CAST('a' AS INT);

SET spark.sql.ansi.enabled=true;

RESET;

SELECT CAST('a' AS INT);
