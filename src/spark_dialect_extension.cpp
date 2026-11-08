#include "spark_dialect.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

SparkDialect::SparkDialect() : DialectExtension("spark", DialectCompatibilityMode::SPARK) {
}

unique_ptr<Tokenizer> SparkDialect::CreateTokenizer(CreateTokenizerInput &input) {
	return make_uniq<SparkTokenizer>(input.keyword_helper);
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Spark SQL (3.5) dialect: SET current_dialect = 'spark'");
	// Spark functions live in their own schema so the default DuckDB dialect is unaffected
	loader.CreateSchema(Identifier(SPARK_SCHEMA));
	loader.UseDefaultSchema(Identifier(SPARK_SCHEMA));
	RegisterSparkFunctions(loader);
	RegisterSparkLibrary(loader);
	RegisterSparkDatetime(loader);
	RegisterSparkHashes(loader);
	RegisterSparkFormats(loader);
	RegisterSparkExtras(loader);
	RegisterSparkIntervals(loader);
	RegisterSparkCasts(loader);
	RegisterSparkMacros(loader);
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	DialectExtension::Register(config, make_shared_ptr<SparkDialect>());
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(spark_dialect, loader) {
	duckdb::LoadInternal(loader);
}
}
