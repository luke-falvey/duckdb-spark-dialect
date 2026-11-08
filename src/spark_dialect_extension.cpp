#include "spark_dialect.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

SparkDialect::SparkDialect(bool strict_p)
    : DialectExtension(strict_p ? "spark_strict" : "spark", DialectCompatibilityMode::SPARK), strict(strict_p) {
}

unique_ptr<Tokenizer> SparkDialect::CreateTokenizer(CreateTokenizerInput &input) {
	return make_uniq<SparkTokenizer>(input.keyword_helper, strict);
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Spark SQL (3.5) dialect: SET current_dialect = 'spark' or 'spark_strict'");
	// Spark functions live in their own schema so the default DuckDB dialect is unaffected
	loader.CreateSchema(Identifier(SPARK_SCHEMA));
	loader.UseDefaultSchema(Identifier(SPARK_SCHEMA));
	RegisterSparkFunctions(loader);
	RegisterSparkAggregates(loader);
	RegisterSparkFormatString(loader);
	RegisterSparkTypeRules(loader);
	RegisterSparkLibrary(loader);
	RegisterSparkDatetime(loader);
	RegisterSparkHashes(loader);
	RegisterSparkFormats(loader);
	RegisterSparkExtras(loader);
	RegisterSparkIntervals(loader);
	RegisterSparkCasts(loader);
	RegisterSparkMacros(loader);
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	// SET spark.sql.ansi.enabled = true/false (rewritten to this setting by the tokenizer)
	config.AddExtensionOption(Identifier("spark_ansi_enabled"), "Spark ANSI mode: runtime errors for overflow, invalid "
	                                                            "casts, division by zero and invalid array indexes",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	DialectExtension::Register(config, make_shared_ptr<SparkDialect>(false));
	DialectExtension::Register(config, make_shared_ptr<SparkDialect>(true));
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(spark_dialect, loader) {
	duckdb::LoadInternal(loader);
}
}
