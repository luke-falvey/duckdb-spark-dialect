# Used with DuckDB's build: make EXTENSION_CONFIGS=<this file>
# Builds the loadable extension only (it is not statically linked into duckdb).
duckdb_extension_load(spark_dialect
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    EXTENSION_VERSION v0.1.0
)
