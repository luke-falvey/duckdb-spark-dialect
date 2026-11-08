//===----------------------------------------------------------------------===//
// spark_dialect: Spark SQL (3.5) dialect for DuckDB
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Name of the catalog schema that holds the Spark function library.
static constexpr const char *SPARK_SCHEMA = "spark";

//! Tokenizer that applies Spark's lexical rules, then rewrites the token stream into tokens the
//! DuckDB-based grammar understands (see spark_tokenizer.cpp).
class SparkTokenizer final : public Tokenizer {
public:
	SparkTokenizer(const PEGKeywordHelper &keyword_helper, bool strict_p)
	    : Tokenizer(keyword_helper), strict(strict_p) {
	}
	bool TokenizeInput(TokenizerBehavior &behavior) const override;

protected:
	bool IsQuotedIdentifierDelimiter(char c) const override {
		return c == '`';
	}

	//! `spark_strict`: reject constructs Spark does not have
	bool strict;
};

//! Rewrite the tokens of a whole query (all statements) in place.
void SparkRewriteTokens(vector<MatcherToken> &tokens, bool strict);

class SparkDialect final : public DialectExtension {
public:
	//! `strict` registers `spark_strict`, which rejects SQL that Spark 3.5 rejects; `spark` is lenient
	explicit SparkDialect(bool strict);
	void ApplyGrammarChanges(GrammarChangesInput &input) override;
	unique_ptr<Tokenizer> CreateTokenizer(CreateTokenizerInput &input) override;

private:
	bool strict;
};

//! Spark functions can raise errors (invalid input, malformed data), which DuckDB requires to be declared
inline void RegisterFallible(ExtensionLoader &loader, ScalarFunction function) {
	function.SetFallible();
	loader.RegisterFunction(std::move(function));
}

//! Register the Spark function library in the `spark` schema.
void RegisterSparkFunctions(ExtensionLoader &loader);
void RegisterSparkMacros(ExtensionLoader &loader);
void RegisterSparkAggregates(ExtensionLoader &loader);
//! spark.sql.ansi.enabled (the `spark_ansi_enabled` setting): overflow, invalid casts, division by zero and invalid
//! array indexes are errors instead of NULL / wrap-around
bool SparkAnsiEnabled(ClientContext &context);
void RegisterSparkFormatString(ExtensionLoader &loader);
void RegisterSparkTypeRules(ExtensionLoader &loader);
void RegisterSparkLibrary(ExtensionLoader &loader);
void RegisterSparkDatetime(ExtensionLoader &loader);
void RegisterSparkHashes(ExtensionLoader &loader);
void RegisterSparkFormats(ExtensionLoader &loader);
void RegisterSparkExtras(ExtensionLoader &loader);
void RegisterSparkIntervals(ExtensionLoader &loader);
void RegisterSparkCasts(ExtensionLoader &loader);

//! Functions in the `spark` schema. The parser routes calls to these names into that schema.
bool IsSparkOverride(const string &lower_name);
void AddSparkOverride(const string &lower_name);
//! Spark 3.5 built-in function names (spark_function_names.hpp)
bool IsSparkFunction(const string &lower_name);
//! Bind an ad-hoc kernel (a ScalarFunction that is not in the catalog) so that plans using it can be copied
unique_ptr<Expression> BindSparkKernel(ClientContext &context, ScalarFunction function,
                                       vector<unique_ptr<Expression>> args);

} // namespace duckdb
