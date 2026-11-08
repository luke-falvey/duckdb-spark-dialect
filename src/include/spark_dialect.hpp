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
	explicit SparkTokenizer(const PEGKeywordHelper &keyword_helper) : Tokenizer(keyword_helper) {
	}
	bool TokenizeInput(TokenizerBehavior &behavior) const override;

protected:
	bool IsQuotedIdentifierDelimiter(char c) const override {
		return c == '`';
	}
};

//! Rewrite the tokens of a whole query (all statements) in place.
void SparkRewriteTokens(vector<MatcherToken> &tokens);

class SparkDialect final : public DialectExtension {
public:
	SparkDialect();
	void ApplyGrammarChanges(GrammarChangesInput &input) override;
	unique_ptr<Tokenizer> CreateTokenizer(CreateTokenizerInput &input) override;
};

//! Spark functions can raise errors (invalid input, malformed data), which DuckDB requires to be declared
inline void RegisterFallible(ExtensionLoader &loader, ScalarFunction function) {
	function.SetFallible();
	loader.RegisterFunction(std::move(function));
}

//! Register the Spark function library in the `spark` schema.
void RegisterSparkFunctions(ExtensionLoader &loader);
void RegisterSparkMacros(ExtensionLoader &loader);
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

} // namespace duckdb
