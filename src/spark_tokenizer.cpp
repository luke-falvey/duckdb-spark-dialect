//===----------------------------------------------------------------------===//
// Spark lexical rules and token-stream rewrites.
//
// 1. A pre-scan applies Spark's string rules ('...' and "..." strings, backslash escapes, r'raw'
//    strings, adjacent literals concatenate). Each Spark string is masked to a same-length plain
//    '...' string so DuckDB's tokenizer sees a simple literal at the same offsets; afterwards the
//    token text is replaced by the canonical DuckDB literal of the Spark value.
// 2. Token rewrites handle Spark syntax that is pure sugar over DuckDB syntax: typed numeric
//    literals (10L, 1.5BD), '!' for NOT, SORT/CLUSTER/DISTRIBUTE BY, LEFT SEMI/ANTI, MINUS,
//    WITH ROLLUP/CUBE, LATERAL VIEW, table options (USING, TBLPROPERTIES, ...), INSERT OVERWRITE,
//    CACHE/UNCACHE/REFRESH, Spark SET commands, ALTER TABLE ADD COLUMNS, ...
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/matcher_token.hpp"

namespace duckdb {

namespace {

bool IsIdentChar(char c) {
	return StringUtil::CharacterIsAlphaNumeric(c) || c == '_';
}

void AppendUTF8(string &out, uint32_t cp) {
	if (cp < 0x80) {
		out += char(cp);
	} else if (cp < 0x800) {
		out += char(0xC0 | (cp >> 6));
		out += char(0x80 | (cp & 0x3F));
	} else if (cp < 0x10000) {
		out += char(0xE0 | (cp >> 12));
		out += char(0x80 | ((cp >> 6) & 0x3F));
		out += char(0x80 | (cp & 0x3F));
	} else {
		out += char(0xF0 | (cp >> 18));
		out += char(0x80 | ((cp >> 12) & 0x3F));
		out += char(0x80 | ((cp >> 6) & 0x3F));
		out += char(0x80 | (cp & 0x3F));
	}
}

bool IsHex(char c) {
	return StringUtil::CharacterIsHex(c);
}

//! Spark's ParserUtils.unescapeSQLString (escapedStringLiterals = false)
string SparkUnescape(const string &s) {
	string out;
	idx_t n = s.size();
	for (idx_t i = 0; i < n; i++) {
		char c = s[i];
		if (c != '\\' || i + 1 >= n) {
			out += c;
			continue;
		}
		char next = s[i + 1];
		if (next == 'u' && i + 5 < n && IsHex(s[i + 2]) && IsHex(s[i + 3]) && IsHex(s[i + 4]) &&
		    IsHex(s[i + 5])) {
			uint32_t cp = std::stoul(s.substr(i + 2, 4), nullptr, 16);
			AppendUTF8(out, cp);
			i += 5;
			continue;
		}
		if (i + 3 < n && next >= '0' && next <= '3' && s[i + 2] >= '0' && s[i + 2] <= '7' && s[i + 3] >= '0' &&
		    s[i + 3] <= '7') {
			out += char((next - '0') * 64 + (s[i + 2] - '0') * 8 + (s[i + 3] - '0'));
			i += 3;
			continue;
		}
		switch (next) {
		case '0':
			out += '\0';
			break;
		case 'b':
			out += '\b';
			break;
		case 'n':
			out += '\n';
			break;
		case 'r':
			out += '\r';
			break;
		case 't':
			out += '\t';
			break;
		case 'Z':
			out += '\x1A';
			break;
		case '%':
			out += "\\%";
			break;
		case '_':
			out += "\\_";
			break;
		default:
			out += next;
			break;
		}
		i++;
	}
	return out;
}

string DuckStringLiteral(const string &value) {
	return "'" + StringUtil::Replace(value, "'", "''") + "'";
}

struct SparkLexResult {
	string masked;
	//! offset of a masked literal -> its Spark value
	unordered_map<idx_t, string> strings;
};

SparkLexResult PreScan(const string &sql) {
	SparkLexResult result;
	result.masked = sql;
	idx_t n = sql.size();
	idx_t i = 0;
	idx_t last_string_start = DConstants::INVALID_INDEX;
	idx_t last_string_end = DConstants::INVALID_INDEX;
	while (i < n) {
		char c = sql[i];
		if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
			while (i < n && sql[i] != '\n') {
				i++;
			}
			continue;
		}
		if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
			idx_t depth = 0;
			while (i < n) {
				if (sql[i] == '/' && i + 1 < n && sql[i + 1] == '*') {
					depth++;
					i += 2;
				} else if (sql[i] == '*' && i + 1 < n && sql[i + 1] == '/') {
					depth--;
					i += 2;
					if (depth == 0) {
						break;
					}
				} else {
					i++;
				}
			}
			continue;
		}
		if (c == '`') {
			i++;
			while (i < n) {
				if (sql[i] == '`') {
					if (i + 1 < n && sql[i + 1] == '`') {
						i += 2;
						continue;
					}
					break;
				}
				i++;
			}
			i++;
			continue;
		}
		if (c == '\'' || c == '"') {
			bool prefix_ok = i >= 1 && (i < 2 || !IsIdentChar(sql[i - 2]));
			bool raw = prefix_ok && (sql[i - 1] == 'r' || sql[i - 1] == 'R');
			bool hex = prefix_ok && c == '\'' && (sql[i - 1] == 'x' || sql[i - 1] == 'X');
			idx_t j = i + 1;
			bool terminated = false;
			while (j < n) {
				if (!raw && !hex && sql[j] == '\\') {
					j += 2;
					continue;
				}
				if (sql[j] == c) {
					terminated = true;
					break;
				}
				j++;
			}
			if (!terminated) {
				// leave it to the DuckDB tokenizer to report
				break;
			}
			if (!hex) {
				idx_t start = raw ? i - 1 : i;
				auto content = sql.substr(i + 1, j - i - 1);
				auto value = raw ? content : SparkUnescape(content);
				if (last_string_end != DConstants::INVALID_INDEX && last_string_end + 1 == start) {
					// directly adjacent to the previous literal: extend that one so the DuckDB tokenizer does not
					// read the quotes in between as an escaped quote
					result.strings[last_string_start] += value;
					for (idx_t k = last_string_end; k < j; k++) {
						result.masked[k] = 'x';
					}
					result.masked[j] = '\'';
				} else {
					result.strings[start] = value;
					result.masked[start] = '\'';
					for (idx_t k = start + 1; k < j; k++) {
						result.masked[k] = 'x';
					}
					result.masked[j] = '\'';
					last_string_start = start;
				}
				last_string_end = j;
			}
			i = j + 1;
			continue;
		}
		i++;
	}
	return result;
}

//! Tokenizes against the masked SQL while every token callback goes to the caller's behavior.
class ForwardingBehavior final : public TokenizerBehavior {
public:
	ForwardingBehavior(const string &masked, TokenizerBehavior &inner_p)
	    : TokenizerBehavior(masked, inner_p.tokens), inner(inner_p) {
	}
	void PushToken(idx_t start, idx_t end, TokenType type, bool unterminated) override {
		inner.PushToken(start, end, type, unterminated);
	}
	void OnStatementEnd(idx_t pos) override {
		inner.OnStatementEnd(pos);
	}
	void OnLastToken(const Tokenizer &tokenizer, TokenizeState state, string last_word, idx_t last_pos) override {
		inner.OnLastToken(tokenizer, state, inner.sql.substr(last_pos), last_pos);
	}
	TokenType GetTerminator() const override {
		return inner.GetTerminator();
	}

private:
	TokenizerBehavior &inner;
};

} // namespace

bool SparkTokenizer::TokenizeInput(TokenizerBehavior &behavior) const {
	auto lex = PreScan(behavior.sql);
	ForwardingBehavior forwarding(lex.masked, behavior);
	auto result = Tokenizer::TokenizeInput(forwarding);
	if (strict) {
		// Spark has neither E'...' escape strings nor $$...$$ dollar quoting (checked before Spark strings are rewritten)
		for (auto &token : behavior.tokens) {
			if (token.type != TokenType::STRING_LITERAL || token.text.empty() || token.text[0] == '\'') {
				continue;
			}
			if (token.text[0] == '$') {
				throw ParserException("Spark SQL does not support dollar-quoted strings");
			}
			if (token.text.size() > 1 && (token.text[0] == 'E' || token.text[0] == 'e') && token.text[1] == '\'') {
				throw ParserException("Spark SQL does not support E'...' escape strings");
			}
		}
	}
	for (auto &token : behavior.tokens) {
		if (token.type != TokenType::STRING_LITERAL) {
			continue;
		}
		auto entry = lex.strings.find(token.offset);
		if (entry == lex.strings.end()) {
			continue;
		}
		auto length = token.length;
		token = MatcherToken(DuckStringLiteral(entry->second), token.offset, TokenType::STRING_LITERAL);
		token.length = length;
	}
	if (getenv("SPARK_DIALECT_DEBUG_TOKENS")) {
		for (auto &token : behavior.tokens) {
			fprintf(stderr, "token type=%d offset=%llu length=%llu text=[%s]\n", int(token.type),
			        (unsigned long long)token.offset, (unsigned long long)token.length, token.text.c_str());
		}
	}
	SparkRewriteTokens(behavior.tokens, strict);
	if (getenv("SPARK_DIALECT_DEBUG_TOKENS")) {
		string joined;
		for (auto &token : behavior.tokens) {
			joined += token.text + " ";
		}
		fprintf(stderr, "rewritten: %s\n", joined.c_str());
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Token rewrites
//===----------------------------------------------------------------------===//
namespace {

using Tokens = vector<MatcherToken>;

//! An unquoted word: a keyword or a plain identifier
bool IsWord(const MatcherToken &t) {
	if (t.type == TokenType::KEYWORD) {
		return true;
	}
	return t.type == TokenType::IDENTIFIER && !t.text.empty() && t.text[0] != '"' && t.text[0] != '`';
}

bool IsWord(const MatcherToken &t, const char *word) {
	return IsWord(t) && StringUtil::CIEquals(t.text, word);
}

bool IsOp(const MatcherToken &t, const char *op) {
	// note: DuckDB's tokenizer types a lone '.' as a number
	return (t.type == TokenType::OPERATOR || t.type == TokenType::TERMINATOR ||
	        (t.type == TokenType::NUMBER_LITERAL && t.text == ".")) &&
	       t.text == op;
}

bool IsEnd(const MatcherToken &t) {
	return t.type == TokenType::TERMINATOR || t.type == TokenType::END_OF_INPUT ||
	       t.type == TokenType::END_OF_INPUT_AUTOCOMPLETE;
}

bool WordAt(const Tokens &ts, idx_t i, const char *word) {
	return i < ts.size() && IsWord(ts[i], word);
}

bool OpAt(const Tokens &ts, idx_t i, const char *op) {
	return i < ts.size() && IsOp(ts[i], op);
}

//! Is the '-' at position i a unary minus (no operand before it)?
bool UnaryMinusAt(const Tokens &ts, idx_t i) {
	if (i == 0) {
		return true;
	}
	auto &prev = ts[i - 1];
	if (prev.type == TokenType::OPERATOR || prev.type == TokenType::TERMINATOR) {
		return prev.text != ")" && prev.text != "]";
	}
	if (prev.type == TokenType::KEYWORD) {
		auto upper = StringUtil::Upper(prev.text);
		return upper != "NULL" && upper != "TRUE" && upper != "FALSE" && upper != "END";
	}
	return false; // a number, string or identifier: binary minus
}

//! Tokens for a snippet of DuckDB SQL, all placed at the given offset.
Tokens Lex(const string &snippet, idx_t offset) {
	// split on spaces; quoted strings in snippets never contain spaces we care about
	Tokens result;
	idx_t i = 0;
	while (i < snippet.size()) {
		if (snippet[i] == ' ') {
			i++;
			continue;
		}
		idx_t j = i;
		TokenType type;
		if (snippet[i] == '\'') {
			j++;
			while (j < snippet.size()) {
				if (snippet[j] == '\'' && (j + 1 >= snippet.size() || snippet[j + 1] != '\'')) {
					break;
				}
				if (snippet[j] == '\'') {
					j++;
				}
				j++;
			}
			j++;
			type = TokenType::STRING_LITERAL;
		} else if (snippet[i] == '"') {
			j++;
			while (j < snippet.size() && snippet[j] != '"') {
				j++;
			}
			j++;
			type = TokenType::IDENTIFIER;
		} else if (StringUtil::CharacterIsDigit(snippet[i])) {
			while (j < snippet.size() && (StringUtil::CharacterIsDigit(snippet[j]) || snippet[j] == '.')) {
				j++;
			}
			type = TokenType::NUMBER_LITERAL;
		} else if (IsIdentChar(snippet[i])) {
			while (j < snippet.size() && IsIdentChar(snippet[j])) {
				j++;
			}
			// snippets spell keywords in upper case
			auto word = snippet.substr(i, j - i);
			type = word == StringUtil::Upper(word) ? TokenType::KEYWORD : TokenType::IDENTIFIER;
		} else if (snippet[i] == ';') {
			j++;
			type = TokenType::TERMINATOR;
		} else {
			// single character operator, or '::'
			j++;
			// two-character operators
			static const char *pairs[] = {"::", ":=", "<=", ">=", "<>", "!=", "==", "->", "||"};
			for (auto pair : pairs) {
				if (j < snippet.size() && snippet[i] == pair[0] && snippet[j] == pair[1]) {
					j++;
					break;
				}
			}
			type = TokenType::OPERATOR;
		}
		MatcherToken token(snippet.substr(i, j - i), offset, type);
		token.length = 0;
		result.push_back(std::move(token));
		i = j;
	}
	return result;
}

void Replace(Tokens &ts, idx_t start, idx_t end, Tokens replacement) {
	ts.erase(ts.begin() + NumericCast<int64_t>(start), ts.begin() + NumericCast<int64_t>(end));
	ts.insert(ts.begin() + NumericCast<int64_t>(start), replacement.begin(), replacement.end());
}

void Erase(Tokens &ts, idx_t start, idx_t end) {
	ts.erase(ts.begin() + NumericCast<int64_t>(start), ts.begin() + NumericCast<int64_t>(end));
}

//! Index just past the parenthesised group that starts at ts[i] == '('
idx_t SkipParens(const Tokens &ts, idx_t i) {
	if (!OpAt(ts, i, "(")) {
		return i;
	}
	int64_t depth = 0;
	for (; i < ts.size(); i++) {
		if (IsOp(ts[i], "(")) {
			depth++;
		} else if (IsOp(ts[i], ")")) {
			depth--;
			if (depth == 0) {
				return i + 1;
			}
		} else if (IsEnd(ts[i])) {
			return i;
		}
	}
	return i;
}

string Join(const Tokens &ts, idx_t start, idx_t end, const char *sep = "") {
	string result;
	for (idx_t i = start; i < end; i++) {
		if (i > start) {
			result += sep;
		}
		result += ts[i].text;
	}
	return result;
}

//! End of the clause starting at `i`: the first token at the same nesting depth that starts another clause.
idx_t ClauseEnd(const Tokens &ts, idx_t i, const vector<const char *> &stop_words) {
	int64_t depth = 0;
	for (; i < ts.size(); i++) {
		auto &t = ts[i];
		if (IsEnd(t)) {
			return i;
		}
		if (IsOp(t, "(")) {
			depth++;
			continue;
		}
		if (IsOp(t, ")")) {
			if (depth == 0) {
				return i;
			}
			depth--;
			continue;
		}
		if (depth == 0 && IsWord(t) && !(i + 1 < ts.size() && IsOp(ts[i + 1], "("))) {
			// a word followed by '(' is a function call (e.g. window(...)), not a clause
			for (auto word : stop_words) {
				if (StringUtil::CIEquals(t.text, word)) {
					return i;
				}
			}
		}
	}
	return i;
}

const vector<const char *> QUERY_CLAUSES = {"SORT",   "LIMIT",     "OFFSET", "ORDER",  "CLUSTER", "DISTRIBUTE",
                                            "WINDOW", "UNION",     "EXCEPT", "MINUS",  "HAVING",  "QUALIFY",
                                            "INTERSECT", "WHERE",  "GROUP",  "LATERAL"};

//===----------------------------------------------------------------------===//
// Literal rewrites
//===----------------------------------------------------------------------===//
string DecimalTypeForLiteral(const string &number) {
	// Spark: precision = number of digits, scale = digits after the point
	idx_t digits = 0, scale = 0;
	bool after_point = false;
	bool leading = true;
	for (auto c : number) {
		if (c == '.') {
			after_point = true;
			continue;
		}
		if (!StringUtil::CharacterIsDigit(c)) {
			continue;
		}
		if (leading && c == '0' && !after_point) {
			continue;
		}
		leading = false;
		digits++;
		if (after_point) {
			scale++;
		}
	}
	auto precision = MaxValue<idx_t>(MaxValue<idx_t>(digits, scale), 1);
	return "DECIMAL(" + to_string(precision) + "," + to_string(scale) + ")";
}

bool IsSparkOperator(const string &op) {
	static const unordered_set<string> ops = {"<=>", "<=", ">=", "<>", "!=", "==", "||", "->", "=>", "::", "&&"};
	return ops.count(op) > 0;
}

void RewriteLiterals(Tokens &ts) {
	for (idx_t i = 0; i < ts.size(); i++) {
		auto &t = ts[i];
		if (t.type == TokenType::OPERATOR && !t.text.empty() &&
		    t.text.find_first_not_of('!') == string::npos) {
			// '!' is NOT in Spark
			auto count = t.text.size();
			auto offset = t.offset;
			Tokens nots;
			for (idx_t k = 0; k < count; k++) {
				nots.emplace_back("NOT", offset, TokenType::KEYWORD);
			}
			Replace(ts, i, i + 1, nots);
			i += count - 1;
			continue;
		}
		if (t.type == TokenType::OPERATOR && t.text.size() > 1 && !IsSparkOperator(t.text) &&
		    t.text.find_first_not_of('>') != string::npos) {
			// e.g. '~-' in ~-1: Spark has no such operator, so it is two operators
			auto text = t.text;
			auto offset = t.offset;
			Tokens singles;
			for (idx_t k = 0; k < text.size(); k++) {
				singles.emplace_back(string(1, text[k]), offset + k, TokenType::OPERATOR);
			}
			Replace(ts, i, i + 1, singles);
			i--;
			continue;
		}
		if (t.type == TokenType::OPERATOR && t.text.size() > 1 && t.text.find_first_not_of('>') == string::npos) {
			// ARRAY<ARRAY<INT>>: '>>' closes two type parameter lists
			auto count = t.text.size();
			auto offset = t.offset;
			Tokens gts;
			for (idx_t k = 0; k < count; k++) {
				gts.emplace_back(">", offset + k, TokenType::OPERATOR);
			}
			Replace(ts, i, i + 1, gts);
			i += count - 1;
			continue;
		}
		if (t.type != TokenType::NUMBER_LITERAL || t.text.find_first_of("0123456789") == string::npos) {
			// note: a lone '.' is tokenized as a number
			continue;
		}
		string number = t.text;
		string suffix;
		idx_t consumed = 1;
		// suffix glued to the number by the tokenizer, or an adjacent word token
		idx_t letters = number.size();
		while (letters > 0 && StringUtil::CharacterIsAlpha(number[letters - 1])) {
			letters--;
		}
		if (letters < number.size() && letters > 0) {
			suffix = number.substr(letters);
			number = number.substr(0, letters);
		} else if (i + 1 < ts.size() && IsWord(ts[i + 1]) && ts[i + 1].offset == t.offset + t.length) {
			suffix = ts[i + 1].text;
			consumed = 2;
		}
		auto upper = StringUtil::Upper(suffix);
		string type;
		if (upper == "L") {
			type = "BIGINT";
		} else if (upper == "S") {
			type = "SMALLINT";
		} else if (upper == "Y") {
			type = "TINYINT";
		} else if (upper == "D") {
			type = "DOUBLE";
		} else if (upper == "F") {
			type = "FLOAT";
		} else if (upper == "BD") {
			type = DecimalTypeForLiteral(number);
		} else if (!suffix.empty() && consumed == 1) {
			// e.g. 1E3 is a single number token - leave alone
			continue;
		} else {
			// plain integer literal larger than BIGINT: Spark makes it a DECIMAL(n, 0)
			bool all_digits = number.find_first_not_of("0123456789") == string::npos;
			if (!all_digits || number.size() < 19) {
				continue;
			}
			auto stripped = number.substr(MinValue(number.find_first_not_of('0'), number.size() - 1));
			if (stripped.size() > 38) {
				// Spark has no integer type wider than DECIMAL(38, 0); DuckDB would make it a BIGNUM
				throw ParserException("[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision %llu exceeds max "
				                      "precision 38.",
				                      (unsigned long long)stripped.size());
			}
			if (stripped.size() < 19 || (stripped.size() == 19 && stripped <= "9223372036854775807")) {
				continue;
			}
			type = "DECIMAL(" + to_string(stripped.size()) + ",0)";
			consumed = 1;
		}
		auto offset = t.offset;
		bool as_string = upper == "BD" || StringUtil::StartsWith(type, "DECIMAL");
		// -128Y, -32768S and -9223372036854775808L: Spark's grammar reads the minus as part of the literal, and
		// the positive magnitude alone would overflow the type
		bool minimum = (upper == "Y" && number == "128") || (upper == "S" && number == "32768") ||
		               (upper == "L" && number == "9223372036854775808");
		if (minimum && i > 0 && IsOp(ts[i - 1], "-") && UnaryMinusAt(ts, i - 1)) {
			number = "-" + number;
			as_string = true; // a number token cannot carry the sign
			Erase(ts, i - 1, i);
			i--;
		}
		if (upper == "L" || upper == "S" || upper == "Y") {
			// Spark rejects integral literals outside the type's range at parse time
			auto digits = number[0] == '-' ? number.substr(1) : number;
			auto stripped = digits.substr(MinValue(digits.find_first_not_of('0'), digits.size() - 1));
			string max = upper == "L" ? (number[0] == '-' ? "9223372036854775808" : "9223372036854775807")
			                          : upper == "S" ? (number[0] == '-' ? "32768" : "32767")
			                                         : (number[0] == '-' ? "128" : "127");
			bool integral = !digits.empty() && digits.find_first_not_of("0123456789") == string::npos;
			if (integral && (stripped.size() > max.size() || (stripped.size() == max.size() && stripped > max))) {
				throw ParserException("Numeric literal %s%s does not fit in range for type %s", number, suffix,
				                      StringUtil::Lower(type));
			}
		}
		Tokens replacement = Lex("CAST (", offset);
		replacement.emplace_back(as_string ? DuckStringLiteral(number) : number, offset,
		                         as_string ? TokenType::STRING_LITERAL : TokenType::NUMBER_LITERAL);
		auto tail = Lex("AS " + type + " )", offset);
		replacement.insert(replacement.end(), tail.begin(), tail.end());
		// keep the number token itself so error messages still point at it
		Replace(ts, i, i + consumed, replacement);
		i += replacement.size() - 1;
	}
	// adjacent string literals concatenate
	for (idx_t i = 0; i + 1 < ts.size(); i++) {
		auto &a = ts[i];
		auto &b = ts[i + 1];
		if (a.type == TokenType::STRING_LITERAL && b.type == TokenType::STRING_LITERAL && !a.text.empty() &&
		    !b.text.empty() && a.text[0] == '\'' && b.text[0] == '\'') {
			auto merged = a.text.substr(0, a.text.size() - 1) + b.text.substr(1);
			auto offset = a.offset;
			auto length = b.offset + b.length - a.offset;
			ts[i] = MatcherToken(merged, offset, TokenType::STRING_LITERAL);
			ts[i].length = length;
			Erase(ts, i + 1, i + 2);
			i--;
		}
	}
}

//! INTERVAL 'str' FROM_UNIT TO TO_UNIT -> a DuckDB interval string, e.g. '1 2:03:04' DAY TO SECOND
bool ConvertIntervalString(const string &text, const string &from, const string &to, bool negate, string &out) {
	static const vector<string> year_units = {"YEAR", "MONTH"};
	static const vector<string> day_units = {"DAY", "HOUR", "MINUTE", "SECOND"};
	const vector<string> *units = nullptr;
	for (auto list : {&year_units, &day_units}) {
		if (std::find(list->begin(), list->end(), from) != list->end() &&
		    std::find(list->begin(), list->end(), to) != list->end()) {
			units = list;
		}
	}
	if (!units) {
		return false;
	}
	auto begin = std::find(units->begin(), units->end(), from) - units->begin();
	auto end = std::find(units->begin(), units->end(), to) - units->begin();
	if (begin > end) {
		return false;
	}
	string v = text;
	StringUtil::Trim(v);
	bool negative = negate;
	if (!v.empty() && (v[0] == '-' || v[0] == '+')) {
		negative = negative != (v[0] == '-');
		v = v.substr(1);
	}
	vector<string> parts;
	string current;
	for (auto c : v) {
		if (StringUtil::CharacterIsDigit(c) || (c == '.' && units == &day_units)) {
			current += c;
		} else if (!current.empty()) {
			parts.push_back(current);
			current.clear();
		}
	}
	if (!current.empty()) {
		parts.push_back(current);
	}
	if (int64_t(parts.size()) != end - begin + 1) {
		return false;
	}
	for (idx_t i = 0; i < parts.size(); i++) {
		if (!out.empty()) {
			out += " ";
		}
		out += (negative ? "-" : "") + parts[i] + " " + StringUtil::Lower((*units)[begin + i]) + "s";
	}
	return true;
}

//===----------------------------------------------------------------------===//
// Query clause rewrites
//===----------------------------------------------------------------------===//
bool IsGenerator(const string &name) {
	auto lower = StringUtil::Lower(name);
	return lower == "explode" || lower == "explode_outer" || lower == "posexplode" || lower == "posexplode_outer" ||
	       lower == "inline" || lower == "inline_outer" || lower == "stack" || lower == "json_tuple";
}

void RewriteQueryClauses(Tokens &ts) {
	for (idx_t i = 0; i < ts.size(); i++) {
		// INTERVAL 5 DAYS 3 HOURS ... / INTERVAL -'100' YEAR -> INTERVAL '5 days 3 hours'
		if (WordAt(ts, i, "INTERVAL")) {
			idx_t j = i + 1;
			string text;
			idx_t units = 0;
			while (j + 1 < ts.size()) {
				bool negative = false;
				idx_t k = j;
				if (OpAt(ts, k, "-")) {
					negative = true;
					k++;
				}
				if (k + 1 >= ts.size() || !IsWord(ts[k + 1])) {
					break;
				}
				string number;
				if (ts[k].type == TokenType::NUMBER_LITERAL) {
					number = ts[k].text;
				} else if (ts[k].type == TokenType::STRING_LITERAL && ts[k].text.size() >= 2) {
					number = ts[k].text.substr(1, ts[k].text.size() - 2);
					StringUtil::Trim(number);
					if (number.empty() || number.find_first_not_of("+-0123456789.") != string::npos) {
						break;
					}
				} else {
					break;
				}
				auto unit = StringUtil::Lower(ts[k + 1].text);
				static const unordered_set<string> known = {
				    "year", "years", "month", "months", "week", "weeks", "day", "days", "hour", "hours", "minute",
				    "minutes", "second", "seconds", "millisecond", "milliseconds", "microsecond", "microseconds"};
				if (!known.count(unit) || WordAt(ts, k + 2, "TO")) {
					break;
				}
				if (negative && !number.empty() && number[0] == '-') {
					number = number.substr(1);
				} else if (negative) {
					number = "-" + number;
				}
				text += (text.empty() ? "" : " ") + number + " " + unit;
				units++;
				j = k + 2;
			}
			if (units > 1 || (units == 1 && OpAt(ts, i + 1, "-"))) {
				Tokens replacement;
				replacement.emplace_back("INTERVAL", ts[i].offset, TokenType::KEYWORD);
				replacement.emplace_back(DuckStringLiteral(text), ts[i].offset, TokenType::STRING_LITERAL);
				Replace(ts, i, j, replacement);
				continue;
			}
		}
		// INTERVAL [-]'str' UNIT TO UNIT
		if (WordAt(ts, i, "INTERVAL")) {
			idx_t j = i + 1;
			bool negate = false;
			if (OpAt(ts, j, "-")) {
				negate = true;
				j++;
			}
			if (j + 3 < ts.size() && ts[j].type == TokenType::STRING_LITERAL && IsWord(ts[j + 1]) &&
			    WordAt(ts, j + 2, "TO") && IsWord(ts[j + 3])) {
				auto text = ts[j].text.substr(1, ts[j].text.size() - 2);
				string converted;
				if (ConvertIntervalString(text, StringUtil::Upper(ts[j + 1].text), StringUtil::Upper(ts[j + 3].text),
				                          negate, converted)) {
					Tokens replacement;
					replacement.emplace_back("INTERVAL", ts[i].offset, TokenType::KEYWORD);
					replacement.emplace_back(DuckStringLiteral(converted), ts[j].offset, TokenType::STRING_LITERAL);
					Replace(ts, i, j + 4, replacement);
					continue;
				}
			}
		}
		// lambda parameters named like keywords: (left, right) -> ... (array_sort comparators)
		if (OpAt(ts, i, "(") && i + 5 < ts.size() && IsWord(ts[i + 1]) && OpAt(ts, i + 2, ",") && IsWord(ts[i + 3]) &&
		    OpAt(ts, i + 4, ")") && OpAt(ts, i + 5, "->") &&
		    (IsWord(ts[i + 1], "LEFT") || IsWord(ts[i + 1], "RIGHT") || IsWord(ts[i + 3], "LEFT") ||
		     IsWord(ts[i + 3], "RIGHT"))) {
			auto a = StringUtil::Lower(ts[i + 1].text), b = StringUtil::Lower(ts[i + 3].text);
			auto end = ClauseEnd(ts, i + 6, {});
			for (idx_t k = i + 1; k < end; k++) {
				if (IsWord(ts[k]) && !OpAt(ts, k + 1, "(") &&
				    (StringUtil::CIEquals(ts[k].text, a) || StringUtil::CIEquals(ts[k].text, b))) {
					ts[k] = MatcherToken("__lambda_" + StringUtil::Lower(ts[k].text), ts[k].offset, TokenType::IDENTIFIER);
				}
			}
		}
		// X 'hex' with a space: a binary literal (an odd number of digits gets a leading 0)
		if (i + 1 < ts.size() && IsWord(ts[i], "X") && ts[i + 1].type == TokenType::STRING_LITERAL &&
		    ts[i + 1].text.size() >= 2 && ts[i + 1].text[0] == '\'') {
			auto hex = ts[i + 1].text.substr(1, ts[i + 1].text.size() - 2);
			if (hex.find_first_not_of("0123456789abcdefABCDEF") == string::npos) {
				if (hex.size() % 2 == 1) {
					hex = "0" + hex;
				}
				auto offset = ts[i].offset;
				Tokens replacement = Lex("unhex (", offset);
				replacement.emplace_back(DuckStringLiteral(hex), offset, TokenType::STRING_LITERAL);
				replacement.emplace_back(")", offset, TokenType::OPERATOR);
				Replace(ts, i, i + 2, replacement);
				continue;
			}
		}
		// FROM range(...) / JOIN range(...): Spark's range has a column named id
		if ((WordAt(ts, i, "FROM") || WordAt(ts, i, "JOIN")) && WordAt(ts, i + 1, "range") && OpAt(ts, i + 2, "(")) {
			auto offset = ts[i + 1].offset;
			auto close = SkipParens(ts, i + 2);
			idx_t args = 0, depth = 0;
			for (idx_t k = i + 3; k + 1 < close; k++) {
				if (OpAt(ts, k, "(")) {
					depth++;
				} else if (OpAt(ts, k, ")")) {
					depth--;
				} else if (depth == 0 && OpAt(ts, k, ",")) {
					args++;
				}
			}
			args = close > i + 4 ? args + 1 : 0;
			if (args >= 1 && args <= 4) {
				Replace(ts, i + 1, i + 2, Lex("system . spark . spark_range_" + to_string(args), offset));
			}
			continue;
		}
		// FROM sql_keywords()
		if (WordAt(ts, i, "FROM") && WordAt(ts, i + 1, "sql_keywords") && OpAt(ts, i + 2, "(")) {
			auto offset = ts[i + 1].offset;
			Replace(ts, i + 1, i + 2, Lex("system . spark . sql_keywords", offset));
			continue;
		}
		// FROM generator(...)  ->  FROM (SELECT generator(...)) AS __generator
		if (WordAt(ts, i, "FROM") && i + 2 < ts.size() && IsWord(ts[i + 1]) && IsGenerator(ts[i + 1].text) &&
		    OpAt(ts, i + 2, "(")) {
			auto end = SkipParens(ts, i + 2);
			auto offset = ts[i + 1].offset;
			Tokens replacement = Lex("( SELECT", offset);
			replacement.insert(replacement.end(), ts.begin() + NumericCast<int64_t>(i + 1), ts.begin() + NumericCast<int64_t>(end));
			auto tail = Lex(") AS __generator", offset);
			replacement.insert(replacement.end(), tail.begin(), tail.end());
			Replace(ts, i + 1, end, replacement);
			continue;
		}
		// FROM VALUES (...), (...) without an alias: Spark names the columns col1, col2, ...
		if (WordAt(ts, i, "FROM") && WordAt(ts, i + 1, "VALUES") && OpAt(ts, i + 2, "(")) {
			idx_t first_end = SkipParens(ts, i + 2);
			idx_t columns = 1;
			int64_t depth = 0;
			for (idx_t k = i + 3; k + 1 < first_end; k++) {
				if (IsOp(ts[k], "(")) {
					depth++;
				} else if (IsOp(ts[k], ")")) {
					depth--;
				} else if (depth == 0 && IsOp(ts[k], ",")) {
					columns++;
				}
			}
			idx_t end = first_end;
			while (OpAt(ts, end, ",") && OpAt(ts, end + 1, "(")) {
				end = SkipParens(ts, end + 1);
			}
			bool has_alias = end < ts.size() && (WordAt(ts, end, "AS") || ts[end].type == TokenType::IDENTIFIER);
			if (end < ts.size() && IsWord(ts[end]) && !has_alias) {
				// an unquoted word that is not a clause keyword is a table alias
				has_alias = true;
				for (auto w : QUERY_CLAUSES) {
					if (IsWord(ts[end], w)) {
						has_alias = false;
					}
				}
				for (auto w : {"JOIN", "INNER", "CROSS", "LEFT", "RIGHT", "FULL", "NATURAL", "ON", "USING", "PIVOT",
				               "UNPIVOT"}) {
					if (IsWord(ts[end], w)) {
						has_alias = false;
					}
				}
			}
			if (!has_alias) {
				string alias = "AS __values (";
				for (idx_t c = 0; c < columns; c++) {
					alias += (c > 0 ? " , col" : " col") + to_string(c + 1);
				}
				alias += " )";
				auto tokens = Lex(alias, end < ts.size() ? ts[end].offset : 0);
				ts.insert(ts.begin() + NumericCast<int64_t>(end), tokens.begin(), tokens.end());
			}
			continue;
		}
		// LEFT SEMI / LEFT ANTI
		if (WordAt(ts, i, "LEFT") && (WordAt(ts, i + 1, "SEMI") || WordAt(ts, i + 1, "ANTI"))) {
			Erase(ts, i, i + 1);
			continue;
		}
		// MINUS is EXCEPT
		if (WordAt(ts, i, "MINUS") && i > 0 &&
		    (WordAt(ts, i + 1, "SELECT") || OpAt(ts, i + 1, "(") || WordAt(ts, i + 1, "ALL") ||
		     WordAt(ts, i + 1, "DISTINCT") || WordAt(ts, i + 1, "FROM") || WordAt(ts, i + 1, "VALUES") ||
		     WordAt(ts, i + 1, "TABLE") || WordAt(ts, i + 1, "WITH"))) {
			ts[i] = MatcherToken("EXCEPT", ts[i].offset, TokenType::KEYWORD);
			continue;
		}
		// SORT BY / CLUSTER BY -> ORDER BY ; DISTRIBUTE BY x -> (nothing)
		if ((WordAt(ts, i, "SORT") || WordAt(ts, i, "CLUSTER")) && WordAt(ts, i + 1, "BY")) {
			ts[i] = MatcherToken("ORDER", ts[i].offset, TokenType::KEYWORD);
			continue;
		}
		if (WordAt(ts, i, "DISTRIBUTE") && WordAt(ts, i + 1, "BY")) {
			auto end = ClauseEnd(ts, i + 2, QUERY_CLAUSES);
			Erase(ts, i, end);
			i--;
			continue;
		}
		// GROUP BY a, b WITH ROLLUP|CUBE  ->  GROUP BY ROLLUP (a, b)
		// GROUP BY a GROUPING SETS (...)   ->  GROUP BY GROUPING SETS (...)
		if (WordAt(ts, i, "GROUP") && WordAt(ts, i + 1, "BY")) {
			auto list_start = i + 2;
			auto stops = QUERY_CLAUSES;
			stops.push_back("WITH");
			stops.push_back("GROUPING");
			auto end = ClauseEnd(ts, list_start, stops);
			if (WordAt(ts, end, "WITH") && (WordAt(ts, end + 1, "ROLLUP") || WordAt(ts, end + 1, "CUBE"))) {
				auto kind = StringUtil::Upper(ts[end + 1].text);
				auto offset = ts[end].offset;
				Erase(ts, end, end + 2);
				ts.insert(ts.begin() + NumericCast<int64_t>(end), MatcherToken(")", offset, TokenType::OPERATOR));
				auto open = Lex(kind + " (", ts[list_start].offset);
				ts.insert(ts.begin() + NumericCast<int64_t>(list_start), open.begin(), open.end());
			} else if (WordAt(ts, end, "GROUPING") && WordAt(ts, end + 1, "SETS") && end > list_start) {
				Erase(ts, list_start, end);
			}
			continue;
		}
		// TABLESAMPLE (BUCKET x OUT OF y) -> TABLESAMPLE (x/y*100 PERCENT)
		if (WordAt(ts, i, "TABLESAMPLE") && OpAt(ts, i + 1, "(") && WordAt(ts, i + 2, "BUCKET") &&
		    i + 7 < ts.size() && WordAt(ts, i + 4, "OUT") && WordAt(ts, i + 5, "OF")) {
			double x = std::stod(ts[i + 3].text);
			double y = std::stod(ts[i + 6].text);
			auto pct = to_string(x * 100.0 / y);
			Replace(ts, i + 2, i + 7, Lex(pct + " PERCENT", ts[i + 2].offset));
			continue;
		}
		// LATERAL VIEW [OUTER] gen(args) tbl [AS] c1, c2  ->  CROSS JOIN LATERAL (SELECT gen(args)) tbl(c1, c2)
		if (WordAt(ts, i, "LATERAL") && WordAt(ts, i + 1, "VIEW")) {
			auto offset = ts[i].offset;
			idx_t j = i + 2;
			bool outer = false;
			if (WordAt(ts, j, "OUTER")) {
				outer = true;
				j++;
			}
			idx_t name_start = j;
			while (j + 1 < ts.size() && IsWord(ts[j]) && OpAt(ts, j + 1, ".")) {
				j += 2;
			}
			if (j >= ts.size() || !IsWord(ts[j]) || !OpAt(ts, j + 1, "(")) {
				continue;
			}
			string generator = ts[j].text;
			idx_t call_end = SkipParens(ts, j + 1);
			Tokens call(ts.begin() + NumericCast<int64_t>(name_start), ts.begin() + NumericCast<int64_t>(call_end));
			if (outer && !StringUtil::EndsWith(StringUtil::Lower(generator), "_outer")) {
				call[j - name_start] = MatcherToken(generator + "_outer", ts[j].offset, TokenType::KEYWORD);
			}
			idx_t k = call_end;
			Tokens table_alias;
			vector<const char *> stop_words = QUERY_CLAUSES;
			for (auto w : {"JOIN", "INNER", "CROSS", "LEFT", "RIGHT", "FULL", "NATURAL", "SEMI", "ANTI", "ON",
			               "USING", "PIVOT", "UNPIVOT", "AS"}) {
				stop_words.push_back(w);
			}
			auto is_stop = [&](idx_t idx) {
				if (idx >= ts.size() || IsEnd(ts[idx]) || (!IsWord(ts[idx]) && ts[idx].type != TokenType::IDENTIFIER)) {
					return true;
				}
				for (auto w : stop_words) {
					if (IsWord(ts[idx], w)) {
						return true;
					}
				}
				return false;
			};
			if (!is_stop(k)) {
				table_alias.push_back(ts[k]);
				k++;
			}
			Tokens columns;
			bool has_as = WordAt(ts, k, "AS");
			if (has_as || !is_stop(k)) {
				if (has_as) {
					k++;
				}
				while (k < ts.size() && (IsWord(ts[k]) || ts[k].type == TokenType::IDENTIFIER)) {
					if (!columns.empty()) {
						columns.emplace_back(",", offset, TokenType::OPERATOR);
					}
					columns.push_back(ts[k]);
					k++;
					if (OpAt(ts, k, ",") && k + 1 < ts.size() &&
					    (IsWord(ts[k + 1]) || ts[k + 1].type == TokenType::IDENTIFIER) && !is_stop(k + 1)) {
						k++;
						continue;
					}
					break;
				}
			}
			Tokens replacement = Lex("CROSS JOIN LATERAL ( SELECT", offset);
			replacement.insert(replacement.end(), call.begin(), call.end());
			replacement.emplace_back(")", offset, TokenType::OPERATOR);
			replacement.emplace_back("AS", offset, TokenType::KEYWORD);
			if (table_alias.empty()) {
				table_alias.emplace_back("__lateral_view", offset, TokenType::KEYWORD);
			}
			replacement.insert(replacement.end(), table_alias.begin(), table_alias.end());
			if (!columns.empty()) {
				replacement.emplace_back("(", offset, TokenType::OPERATOR);
				replacement.insert(replacement.end(), columns.begin(), columns.end());
				replacement.emplace_back(")", offset, TokenType::OPERATOR);
			}
			Replace(ts, i, k, replacement);
			i += replacement.size() - 1;
			continue;
		}
	}
}

//===----------------------------------------------------------------------===//
// Statement rewrites. `ts` holds one statement (without terminator).
//===----------------------------------------------------------------------===//

//! Removes Spark table/view/database options that have no DuckDB meaning.
void StripTableOptions(Tokens &ts, idx_t start) {
	int64_t depth = 0;
	for (idx_t i = start; i < ts.size();) {
		auto &t = ts[i];
		if (IsOp(t, "(")) {
			depth++;
			i++;
			continue;
		}
		if (IsOp(t, ")")) {
			depth--;
			i++;
			continue;
		}
		if (depth == 0 && IsWord(t, "AS")) {
			// the query of CREATE ... AS SELECT starts here
			return;
		}
		if (depth <= 1 && IsWord(t, "COMMENT") && i + 1 < ts.size() && ts[i + 1].type == TokenType::STRING_LITERAL) {
			Erase(ts, i, i + 2);
			continue;
		}
		if (depth != 0) {
			i++;
			continue;
		}
		if ((IsWord(t, "USING") || IsWord(t, "STORED")) && i + 1 < ts.size()) {
			auto skip = IsWord(t, "STORED") ? 3 : 2;
			Erase(ts, i, MinValue<idx_t>(i + skip, ts.size()));
			continue;
		}
		if (IsWord(t, "LOCATION") && i + 1 < ts.size()) {
			Erase(ts, i, i + 2);
			continue;
		}
		if ((IsWord(t, "PARTITIONED") || IsWord(t, "SORTED")) && WordAt(ts, i + 1, "BY")) {
			Erase(ts, i, SkipParens(ts, i + 2));
			continue;
		}
		if (IsWord(t, "CLUSTERED") && WordAt(ts, i + 1, "BY")) {
			auto end = SkipParens(ts, i + 2);
			if (WordAt(ts, end, "SORTED") && WordAt(ts, end + 1, "BY")) {
				end = SkipParens(ts, end + 2);
			}
			if (WordAt(ts, end, "INTO") && WordAt(ts, end + 2, "BUCKETS")) {
				end += 3;
			}
			Erase(ts, i, end);
			continue;
		}
		if (IsWord(t, "TBLPROPERTIES") || IsWord(t, "OPTIONS") || IsWord(t, "DBPROPERTIES")) {
			auto begin = (i > start && IsWord(ts[i - 1], "WITH")) ? i - 1 : i;
			Erase(ts, begin, SkipParens(ts, i + 1));
			i = begin;
			continue;
		}
		i++;
	}
}

Tokens EmptyResult(idx_t offset) {
	return Lex("SELECT 1 WHERE false", offset);
}

//! Returns true if the statement was replaced completely.
bool RewriteStatement(Tokens &ts, bool strict) {
	if (ts.empty()) {
		return false;
	}
	auto offset = ts[0].offset;
	// ---- CREATE ----
	if (WordAt(ts, 0, "CREATE")) {
		idx_t i = 1;
		if (WordAt(ts, i, "OR") && WordAt(ts, i + 1, "REPLACE")) {
			i += 2;
		}
		if (WordAt(ts, i, "GLOBAL")) {
			Erase(ts, i, i + 1);
		}
		if (WordAt(ts, i, "EXTERNAL")) {
			Erase(ts, i, i + 1);
		}
		if (WordAt(ts, i, "TEMPORARY") || WordAt(ts, i, "TEMP")) {
			i++;
		}
		if (WordAt(ts, i, "DATABASE") || WordAt(ts, i, "NAMESPACE")) {
			ts[i] = MatcherToken("SCHEMA", ts[i].offset, TokenType::KEYWORD);
		}
		// DuckDB parses type strings (json_transform, CAST to a type name, ...) with `CREATE TABLE tbl (dummy <type>)`
		bool type_probe = WordAt(ts, i + 1, "tbl") && OpAt(ts, i + 2, "(") && WordAt(ts, i + 3, "dummy");
		if (strict && WordAt(ts, i, "TABLE") && !type_probe) {
			// without Hive support Spark only creates tables with a data source (USING), also for CREATE TABLE AS
			int64_t depth = 0;
			bool has_using = false;
			for (idx_t k = i + 1; k < ts.size(); k++) {
				if (IsOp(ts[k], "(")) {
					depth++;
				} else if (IsOp(ts[k], ")")) {
					depth--;
				} else if (depth == 0 && (IsWord(ts[k], "USING") || IsWord(ts[k], "LIKE"))) {
					has_using = true;
				} else if (depth == 0 && IsWord(ts[k], "AS")) {
					break;
				}
			}
			if (!has_using) {
				throw ParserException("[NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT] CREATE Hive TABLE is not supported: "
				                      "add a USING clause (for example USING parquet)");
			}
		}
		if (WordAt(ts, i, "TABLE") || WordAt(ts, i, "VIEW") || WordAt(ts, i, "SCHEMA")) {
			StripTableOptions(ts, i + 1);
		}
		if (WordAt(ts, i, "TABLE")) {
			// CHAR(n) columns are blank-padded in Spark
			int64_t depth = 0;
			for (idx_t k = i + 1; k < ts.size(); k++) {
				if (IsOp(ts[k], "(")) {
					depth++;
				} else if (IsOp(ts[k], ")")) {
					depth--;
				} else if (depth == 0 && WordAt(ts, k, "AS")) {
					break;
				}
				if (depth == 1 && (IsWord(ts[k], "CHAR") || IsWord(ts[k], "CHARACTER")) && OpAt(ts, k + 1, "(") &&
				    k + 3 < ts.size() && ts[k + 2].type == TokenType::NUMBER_LITERAL && OpAt(ts, k + 3, ")")) {
					auto n = std::stoll(ts[k + 2].text);
					if (n >= 1 && n <= 255) {
						Replace(ts, k, k + 4, Lex("system . spark . spark_char_" + to_string(n), ts[k].offset));
					}
				}
			}
		}
		return false;
	}
	// ---- DROP DATABASE ----
	if (WordAt(ts, 0, "DROP") && (WordAt(ts, 1, "DATABASE") || WordAt(ts, 1, "NAMESPACE"))) {
		ts[1] = MatcherToken("SCHEMA", ts[1].offset, TokenType::KEYWORD);
		return false;
	}
	// ---- USE default ----
	for (idx_t i = 0; i < ts.size(); i++) {
		if (IsWord(ts[i], "default") &&
		    ((i == 1 && WordAt(ts, 0, "USE")) || (OpAt(ts, i + 1, ".") && (i == 0 || !OpAt(ts, i - 1, "."))))) {
			ts[i] = MatcherToken("main", ts[i].offset, TokenType::IDENTIFIER);
		}
	}
	// ---- DESCRIBE / EXPLAIN ----
	if (WordAt(ts, 0, "DESCRIBE") || WordAt(ts, 0, "DESC")) {
		ts[0] = MatcherToken("DESCRIBE", offset, TokenType::KEYWORD);
		idx_t i = 1;
		if (WordAt(ts, i, "TABLE") || WordAt(ts, i, "QUERY")) {
			Erase(ts, i, i + 1);
		}
		if (WordAt(ts, i, "EXTENDED") || WordAt(ts, i, "FORMATTED")) {
			Erase(ts, i, i + 1);
		}
		return false;
	}
	if (WordAt(ts, 0, "EXPLAIN")) {
		if (WordAt(ts, 1, "EXTENDED") || WordAt(ts, 1, "CODEGEN") || WordAt(ts, 1, "COST") ||
		    WordAt(ts, 1, "FORMATTED")) {
			Erase(ts, 1, 2);
		}
		return false;
	}
	// ---- INSERT ----
	if (WordAt(ts, 0, "INSERT")) {
		bool overwrite = WordAt(ts, 1, "OVERWRITE");
		if (!overwrite && !WordAt(ts, 1, "INTO")) {
			return false;
		}
		idx_t i = 2;
		if (WordAt(ts, i, "TABLE")) {
			Erase(ts, i, i + 1);
		}
		idx_t name_start = i;
		while (i + 1 < ts.size() && OpAt(ts, i + 1, ".")) {
			i += 2;
		}
		idx_t name_end = i + 1;
		if (name_end > ts.size()) {
			return false; // no table name (truncated statement): let the parser report it
		}
		if (WordAt(ts, name_end, "PARTITION") && OpAt(ts, name_end + 1, "(")) {
			Erase(ts, name_end, SkipParens(ts, name_end + 1));
		}
		if (WordAt(ts, name_end, "IF") && WordAt(ts, name_end + 1, "NOT") && WordAt(ts, name_end + 2, "EXISTS")) {
			Erase(ts, name_end, name_end + 3);
		}
		for (idx_t k = name_end; k + 1 < ts.size(); k++) {
			if (IsWord(ts[k], "STRUCT") && OpAt(ts, k + 1, "(")) {
				auto close = SkipParens(ts, k + 1);
				bool named = false;
				for (idx_t m = k + 2; m < close; m++) {
					named = named || IsWord(ts[m], "AS");
				}
				if (!named) {
					ts[k] = MatcherToken("ROW", ts[k].offset, TokenType::KEYWORD);
				}
			}
		}
		if (overwrite) {
			ts[1] = MatcherToken("INTO", ts[1].offset, TokenType::KEYWORD);
			Tokens prefix = Lex("DELETE FROM", offset);
			prefix.insert(prefix.end(), ts.begin() + NumericCast<int64_t>(name_start),
			              ts.begin() + NumericCast<int64_t>(name_end));
			prefix.emplace_back(";", offset, TokenType::TERMINATOR);
			ts.insert(ts.begin(), prefix.begin(), prefix.end());
		}
		return false;
	}
	// ---- CACHE / UNCACHE / REFRESH / CLEAR CACHE / ADD / MSCK / ANALYZE ----
	if (WordAt(ts, 0, "CACHE")) {
		idx_t i = 1;
		if (WordAt(ts, i, "LAZY")) {
			i++;
		}
		if (!WordAt(ts, i, "TABLE")) {
			return false;
		}
		i++;
		idx_t name_start = i;
		while (i + 1 < ts.size() && OpAt(ts, i + 1, ".")) {
			i += 2;
		}
		idx_t name_end = i + 1;
		if (name_end > ts.size()) {
			return false; // no table name (truncated statement): let the parser report it
		}
		Tokens name(ts.begin() + NumericCast<int64_t>(name_start), ts.begin() + NumericCast<int64_t>(name_end));
		idx_t k = name_end;
		if (WordAt(ts, k, "OPTIONS")) {
			k = SkipParens(ts, k + 1);
		}
		if (WordAt(ts, k, "AS")) {
			k++;
		}
		Tokens replacement;
		if (k < ts.size()) {
			replacement = Lex("CREATE OR REPLACE TEMPORARY VIEW", offset);
			replacement.insert(replacement.end(), name.begin(), name.end());
			replacement.emplace_back("AS", offset, TokenType::KEYWORD);
			replacement.insert(replacement.end(), ts.begin() + NumericCast<int64_t>(k), ts.end());
		} else {
			replacement = Lex("SELECT 1 FROM", offset);
			replacement.insert(replacement.end(), name.begin(), name.end());
			auto tail = Lex("WHERE false", offset);
			replacement.insert(replacement.end(), tail.begin(), tail.end());
		}
		ts = replacement;
		return true;
	}
	if (WordAt(ts, 0, "UNCACHE") || (WordAt(ts, 0, "REFRESH") && (WordAt(ts, 1, "TABLE") || ts.size() == 2))) {
		idx_t i = 1;
		if (WordAt(ts, i, "TABLE")) {
			i++;
		}
		bool if_exists = WordAt(ts, i, "IF") && WordAt(ts, i + 1, "EXISTS");
		if (if_exists) {
			ts = EmptyResult(offset);
			return true;
		}
		Tokens replacement = Lex("SELECT 1 FROM", offset);
		replacement.insert(replacement.end(), ts.begin() + NumericCast<int64_t>(i), ts.end());
		auto tail = Lex("WHERE false", offset);
		replacement.insert(replacement.end(), tail.begin(), tail.end());
		ts = replacement;
		return true;
	}
	if ((WordAt(ts, 0, "CLEAR") && WordAt(ts, 1, "CACHE")) || WordAt(ts, 0, "REFRESH") ||
	    (WordAt(ts, 0, "ADD") && (WordAt(ts, 1, "JAR") || WordAt(ts, 1, "FILE") || WordAt(ts, 1, "ARCHIVE") ||
	                              WordAt(ts, 1, "JARS") || WordAt(ts, 1, "FILES") || WordAt(ts, 1, "ARCHIVES"))) ||
	    WordAt(ts, 0, "MSCK") || (WordAt(ts, 0, "ANALYZE") && WordAt(ts, 1, "TABLE"))) {
		ts = EmptyResult(offset);
		return true;
	}
	// ---- SET / RESET ----
	// SET / RESET current_dialect always go to DuckDB: they are how a connection leaves the Spark dialects.
	// Selecting a Spark dialect also set dialect_compatibility_mode = 'spark', which DuckDB's RESET current_dialect
	// leaves in place (it changes DuckDB SQL, e.g. list comprehension indexes become 0-based), so reset it first;
	// SET current_dialect = 'x' then applies x's own mode, if it has one.
	if ((WordAt(ts, 0, "RESET") || WordAt(ts, 0, "SET")) && WordAt(ts, 1, "current_dialect")) {
		Tokens prefix = Lex("RESET dialect_compatibility_mode", offset);
		prefix.emplace_back(";", offset, TokenType::TERMINATOR);
		ts.insert(ts.begin(), prefix.begin(), prefix.end());
		return false;
	}
	if (WordAt(ts, 0, "RESET")) {
		ts = EmptyResult(offset);
		return true;
	}
	if (WordAt(ts, 0, "SET")) {
		if (WordAt(ts, 1, "TIME") && WordAt(ts, 2, "ZONE") && ts.size() > 3) {
			string zone = ts[3].type == TokenType::STRING_LITERAL ? ts[3].text : DuckStringLiteral("UTC");
			ts = Lex("SET TimeZone = " + zone, offset);
			return true;
		}
		idx_t eq = 1;
		while (eq < ts.size() && !OpAt(ts, eq, "=")) {
			eq++;
		}
		auto key = Join(ts, 1, eq);
		if (key.empty() || ts.size() == 1) {
			ts = Lex("SELECT 'key' AS key , 'value' AS value WHERE false", offset);
			return true;
		}
		// Spark stores any key=value; spark_strict never passes a setting on to DuckDB
		bool spark_key = strict || key.find('.') != string::npos || StringUtil::StartsWith(key, "-");
		if (!spark_key) {
			return false;
		}
		if (eq >= ts.size()) {
			ts = Lex("SELECT " + DuckStringLiteral(key) + " AS key , '<undefined>' AS value", offset);
			return true;
		}
		string value;
		for (idx_t i = eq + 1; i < ts.size(); i++) {
			auto &t = ts[i];
			if (t.type == TokenType::STRING_LITERAL && !t.text.empty() && t.text[0] == '\'') {
				value += StringUtil::Replace(t.text.substr(1, t.text.size() - 2), "''", "'");
			} else {
				value += t.text;
			}
		}
		if (StringUtil::CIEquals(key, "spark.sql.session.timeZone")) {
			ts = Lex("SET TimeZone = " + DuckStringLiteral(value), offset);
			return true;
		}
		ts = Lex("SELECT " + DuckStringLiteral(key) + " AS key , " + DuckStringLiteral(value) + " AS value", offset);
		return true;
	}
	// ---- ALTER TABLE ----
	if (WordAt(ts, 0, "ALTER") && WordAt(ts, 1, "TABLE")) {
		idx_t i = 2;
		while (i + 1 < ts.size() && OpAt(ts, i + 1, ".")) {
			i += 2;
		}
		idx_t name_end = i + 1;
		if (name_end > ts.size()) {
			return false; // no table name (truncated statement): let the parser report it
		}
		Tokens head(ts.begin(), ts.begin() + NumericCast<int64_t>(name_end));
		if (WordAt(ts, name_end, "ADD") && (WordAt(ts, name_end + 1, "COLUMNS") || WordAt(ts, name_end + 1, "COLUMN")) &&
		    OpAt(ts, name_end + 2, "(")) {
			idx_t end = SkipParens(ts, name_end + 2);
			Tokens result;
			idx_t col_start = name_end + 3;
			int64_t depth = 0;
			for (idx_t k = col_start; k < end; k++) {
				bool last = k + 1 == end;
				if (IsOp(ts[k], "(")) {
					depth++;
				} else if (IsOp(ts[k], ")") && !last) {
					depth--;
				}
				if ((depth == 0 && IsOp(ts[k], ",")) || last) {
					if (!result.empty()) {
						result.emplace_back(";", offset, TokenType::TERMINATOR);
					}
					result.insert(result.end(), head.begin(), head.end());
					auto add = Lex("ADD COLUMN", offset);
					result.insert(result.end(), add.begin(), add.end());
					Tokens column(ts.begin() + NumericCast<int64_t>(col_start), ts.begin() + NumericCast<int64_t>(k));
					StripTableOptions(column, 0);
					result.insert(result.end(), column.begin(), column.end());
					col_start = k + 1;
				}
			}
			ts = result;
			return true;
		}
		if (WordAt(ts, name_end, "SET") && WordAt(ts, name_end + 1, "TBLPROPERTIES")) {
			ts = EmptyResult(offset);
			return true;
		}
		return false;
	}
	// ---- TRUNCATE TABLE t ----
	if (WordAt(ts, 0, "TRUNCATE") && WordAt(ts, 1, "TABLE")) {
		Erase(ts, 1, 2);
		return false;
	}
	return false;
}

} // namespace

//! Splits the arguments of the call whose '(' is at ts[open] (top-level commas)
vector<Tokens> CallArguments(const Tokens &ts, idx_t open, idx_t close) {
	vector<Tokens> args;
	Tokens current;
	int64_t depth = 0;
	for (idx_t k = open + 1; k + 1 < close; k++) {
		if (IsOp(ts[k], "(")) {
			depth++;
		} else if (IsOp(ts[k], ")")) {
			depth--;
		}
		if (depth == 0 && IsOp(ts[k], ",")) {
			args.push_back(current);
			current.clear();
			continue;
		}
		current.push_back(ts[k]);
	}
	args.push_back(current);
	return args;
}

void Append(Tokens &target, const Tokens &source) {
	target.insert(target.end(), source.begin(), source.end());
}

Tokens Parenthesized(const Tokens &tokens, idx_t offset) {
	Tokens result;
	result.emplace_back("(", offset, TokenType::OPERATOR);
	Append(result, tokens);
	result.emplace_back(")", offset, TokenType::OPERATOR);
	return result;
}

//! GROUP BY ..., window(ts, ...) / session_window(ts, gap): Spark adds a struct column named `window` /
//! `session_window`. The FROM source of that query block becomes a subquery computing the column (sliding
//! windows emit one row per window, sessions merge overlapping gaps per group), and the call in GROUP BY
//! becomes a reference to the column.
bool RewriteGroupingWindow(Tokens &ts) {
	for (idx_t g = 0; g + 1 < ts.size(); g++) {
		if (!WordAt(ts, g, "GROUP") || !WordAt(ts, g + 1, "BY")) {
			continue;
		}
		auto group_end = ClauseEnd(ts, g + 2, QUERY_CLAUSES);
		// the grouping items
		vector<pair<idx_t, idx_t>> items;
		idx_t item_start = g + 2;
		int64_t depth = 0;
		for (idx_t k = g + 2; k <= group_end; k++) {
			if (k < group_end && IsOp(ts[k], "(")) {
				depth++;
			} else if (k < group_end && IsOp(ts[k], ")")) {
				depth--;
			}
			if (k == group_end || (depth == 0 && IsOp(ts[k], ","))) {
				items.emplace_back(item_start, k);
				item_start = k + 1;
			}
		}
		idx_t call = DConstants::INVALID_INDEX;
		string kind;
		for (auto &item : items) {
			if (item.second - item.first >= 3 && (IsWord(ts[item.first], "window") || IsWord(ts[item.first], "session_window")) &&
			    OpAt(ts, item.first + 1, "(") && SkipParens(ts, item.first + 1) == item.second) {
				call = item.first;
				kind = StringUtil::Lower(ts[item.first].text);
			}
		}
		if (call == DConstants::INVALID_INDEX) {
			continue;
		}
		auto offset = ts[call].offset;
		auto call_end = SkipParens(ts, call + 1);
		auto args = CallArguments(ts, call + 1, call_end);
		Tokens call_tokens(ts.begin() + NumericCast<int64_t>(call), ts.begin() + NumericCast<int64_t>(call_end));
		// the FROM / WHERE of this block: the nearest FROM before GROUP BY at the same depth
		idx_t from = DConstants::INVALID_INDEX, where = DConstants::INVALID_INDEX;
		depth = 0;
		for (idx_t k = g; k > 0; k--) {
			idx_t i = k - 1;
			if (IsOp(ts[i], ")")) {
				depth++;
			} else if (IsOp(ts[i], "(")) {
				if (depth == 0) {
					break;
				}
				depth--;
			} else if (depth == 0 && WordAt(ts, i, "WHERE") && where == DConstants::INVALID_INDEX) {
				where = i;
			} else if (depth == 0 && WordAt(ts, i, "FROM")) {
				from = i;
				break;
			}
		}
		if (from == DConstants::INVALID_INDEX) {
			return false;
		}
		idx_t source_end = where != DConstants::INVALID_INDEX ? where : g;
		Tokens source(ts.begin() + NumericCast<int64_t>(from + 1), ts.begin() + NumericCast<int64_t>(source_end));
		Tokens condition;
		if (where != DConstants::INVALID_INDEX) {
			condition.assign(ts.begin() + NumericCast<int64_t>(where), ts.begin() + NumericCast<int64_t>(g));
		}
		auto column = string("\"") + kind + "\"";
		Tokens subquery = Lex("( SELECT * ,", offset);
		if (kind == "window") {
			Tokens fn = Lex(args.size() <= 2 ? "system . spark . __spark_window (" : "__spark_unnest ( system . spark . sliding_windows (", offset);
			for (idx_t a = 0; a < args.size(); a++) {
				if (a > 0) {
					fn.emplace_back(",", offset, TokenType::OPERATOR);
				}
				Append(fn, args[a]);
			}
			fn.emplace_back(")", offset, TokenType::OPERATOR);
			if (args.size() > 2) {
				fn.emplace_back(")", offset, TokenType::OPERATOR);
			}
			Append(subquery, fn);
			subquery.emplace_back("AS", offset, TokenType::KEYWORD);
			subquery.emplace_back(column, offset, TokenType::IDENTIFIER);
			subquery.emplace_back("FROM", offset, TokenType::KEYWORD);
			Append(subquery, source);
			Append(subquery, condition);
		} else {
			if (args.size() != 2) {
				return false;
			}
			// partition by the other grouping keys
			Tokens partition;
			for (auto &item : items) {
				if (item.first == call) {
					continue;
				}
				if (!partition.empty()) {
					partition.emplace_back(",", offset, TokenType::OPERATOR);
				}
				partition.insert(partition.end(), ts.begin() + NumericCast<int64_t>(item.first),
				                 ts.begin() + NumericCast<int64_t>(item.second));
			}
			auto over = [&](const char *rest) {
				Tokens t = Lex("OVER (", offset);
				if (!partition.empty()) {
					Append(t, Lex("PARTITION BY", offset));
					Append(t, partition);
				}
				Append(t, Lex(rest, offset));
				t.emplace_back(")", offset, TokenType::OPERATOR);
				return t;
			};
			Tokens event = Lex("CAST (", offset);
			Append(event, args[0]);
			Append(event, Lex("AS TIMESTAMP )", offset));
			Tokens event_end = Parenthesized(event, offset);
			event_end.emplace_back("+", offset, TokenType::OPERATOR);
			Tokens gap = Lex("CAST (", offset);
			Append(gap, args[1]);
			Append(gap, Lex("AS INTERVAL )", offset));
			Append(event_end, gap);
			// level 1: does this event start a new session?
			Tokens level1 = Lex("( SELECT * , __spark_event AS __spark_ts , CASE WHEN __spark_event < max (", offset);
			level1 = Lex("( SELECT * , CASE WHEN", offset);
			Append(level1, event);
			Append(level1, Lex("<= max (", offset));
			Append(level1, event_end);
			level1.emplace_back(")", offset, TokenType::OPERATOR);
			Tokens order_by = Lex("ORDER BY", offset);
			Append(order_by, event);
			auto prev = over("");
			prev.pop_back();
			Append(prev, order_by);
			Append(prev, Lex("ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )", offset));
			Append(level1, prev);
			Append(level1, Lex("THEN 0 ELSE 1 END AS __spark_new_session FROM", offset));
			Append(level1, source);
			Append(level1, condition);
			level1.emplace_back(")", offset, TokenType::OPERATOR);
			// level 2: session numbers
			Tokens level2 = Lex("( SELECT * , sum ( __spark_new_session )", offset);
			auto running = over("");
			running.pop_back();
			Append(running, order_by);
			Append(running, Lex("ROWS UNBOUNDED PRECEDING )", offset));
			Append(level2, running);
			Append(level2, Lex("AS __spark_session FROM", offset));
			Append(level2, level1);
			level2.emplace_back(")", offset, TokenType::OPERATOR);
			// level 3: the session window struct
			if (!partition.empty()) {
				partition.emplace_back(",", offset, TokenType::OPERATOR);
			}
			partition.emplace_back("__spark_session", offset, TokenType::IDENTIFIER);
			Append(subquery, Lex("__spark_struct_pack ( start := min (", offset));
			Append(subquery, event);
			subquery.emplace_back(")", offset, TokenType::OPERATOR);
			Append(subquery, over(""));
			Append(subquery, Lex(", \"end\" := max (", offset));
			Append(subquery, event_end);
			subquery.emplace_back(")", offset, TokenType::OPERATOR);
			Append(subquery, over(""));
			Append(subquery, Lex(") AS", offset));
			subquery.emplace_back(column, offset, TokenType::IDENTIFIER);
			subquery.emplace_back("FROM", offset, TokenType::KEYWORD);
			Append(subquery, level2);
		}
		subquery.emplace_back(")", offset, TokenType::OPERATOR);
		// Spark drops rows whose window is NULL (a NULL / unparsable time, an invalid gap duration)
		Tokens filtered = Lex("( SELECT * FROM", offset);
		Append(filtered, subquery);
		Append(filtered, Lex("AS __spark_unfiltered WHERE", offset));
		filtered.emplace_back(column, offset, TokenType::IDENTIFIER);
		Append(filtered, Lex(". start IS NOT NULL AND", offset));
		filtered.emplace_back(column, offset, TokenType::IDENTIFIER);
		Append(filtered, Lex(". \"end\" IS NOT NULL )", offset));
		subquery = filtered;
		Append(subquery, Lex("AS __spark_windowed", offset));
		// GROUP BY ..., "window" ; FROM (subquery) ; WHERE moved into the subquery
		Replace(ts, call, call_end, {MatcherToken(column, offset, TokenType::IDENTIFIER)});
		if (where != DConstants::INVALID_INDEX) {
			Erase(ts, where, g);
		}
		Replace(ts, from + 1, source_end, subquery);
		// the same window(...) call in this block's select list is the grouping column too
		auto same_call = [&](idx_t at) {
			if (at + call_tokens.size() > from) {
				return false;
			}
			for (idx_t m = 0; m < call_tokens.size(); m++) {
				if (!StringUtil::CIEquals(ts[at + m].text, call_tokens[m].text)) {
					return false;
				}
			}
			return true;
		};
		int64_t level = 0;
		for (idx_t k = from; k > 0; k--) {
			idx_t i = k - 1;
			if (IsOp(ts[i], ")")) {
				level++;
			} else if (IsOp(ts[i], "(")) {
				if (level == 0) {
					break;
				}
				level--;
			} else if (level == 0 && WordAt(ts, i, "SELECT")) {
				break;
			}
			if (level == 0 && same_call(i)) {
				Replace(ts, i, i + call_tokens.size(), {MatcherToken(column, offset, TokenType::IDENTIFIER)});
			}
		}
		return true;
	}
	return false;
}

//! References to the `window` / `session_window` column: quote the name (a keyword in DuckDB), and name
//! SELECT window.start columns after the field like Spark does
void RewriteWindowColumn(Tokens &ts) {
	while (RewriteGroupingWindow(ts)) {
	}
	for (idx_t i = 0; i < ts.size(); i++) {
		if (!(IsWord(ts[i], "window") || IsWord(ts[i], "session_window")) || OpAt(ts, i + 1, "(") ||
		    (i > 0 && OpAt(ts, i - 1, "."))) {
			continue;
		}
		// WINDOW w AS (...) is the window clause, not the column
		bool column_like = OpAt(ts, i + 1, ".") || OpAt(ts, i + 1, ",") || OpAt(ts, i + 1, ")") ||
		                   WordAt(ts, i + 1, "FROM") || i + 1 >= ts.size() || IsEnd(ts[i + 1]);
		if (!column_like) {
			continue;
		}
		auto offset = ts[i].offset;
		auto column = string("\"") + StringUtil::Lower(ts[i].text) + "\"";
		bool field_projection = i > 0 && (OpAt(ts, i - 1, ",") || WordAt(ts, i - 1, "SELECT")) && OpAt(ts, i + 1, ".") &&
		                        i + 2 < ts.size() && IsWord(ts[i + 2]) &&
		                        (OpAt(ts, i + 3, ",") || WordAt(ts, i + 3, "FROM"));
		if (field_projection) {
			auto field = ts[i + 2];
			Replace(ts, i + 3, i + 3, {MatcherToken("AS", offset, TokenType::KEYWORD), field});
		}
		if (i + 2 < ts.size() && OpAt(ts, i + 1, ".") && IsWord(ts[i + 2], "end")) {
			ts[i + 2] = MatcherToken("\"end\"", ts[i + 2].offset, TokenType::IDENTIFIER);
		}
		ts[i] = MatcherToken(column, offset, TokenType::IDENTIFIER);
	}
}

void RewritePivotAggregates(Tokens &ts) {
	for (idx_t i = 0; i + 1 < ts.size(); i++) {
		if (!WordAt(ts, i, "PIVOT") || !OpAt(ts, i + 1, "(")) {
			continue;
		}
		auto end = SkipParens(ts, i + 1);
		for (idx_t k = i + 2; k < end; k++) {
			if (WordAt(ts, k, "FOR")) {
				break;
			}
			if (!IsWord(ts[k]) || !OpAt(ts, k + 1, "(")) {
				continue;
			}
			auto name = StringUtil::Lower(ts[k].text);
			auto offset = ts[k].offset;
			if (name == "count") {
				auto close = SkipParens(ts, k + 1);
				Tokens arg(ts.begin() + NumericCast<int64_t>(k + 2), ts.begin() + NumericCast<int64_t>(close - 1));
				Tokens replacement = Lex("system . main . sum (", offset);
				if (arg.size() == 1 && IsOp(arg[0], "*")) {
					replacement.emplace_back("1", offset, TokenType::NUMBER_LITERAL);
				} else {
					Append(replacement, Lex("CASE WHEN", offset));
					Append(replacement, Parenthesized(arg, offset));
					Append(replacement, Lex("IS NOT NULL THEN 1 ELSE 0 END", offset));
				}
				replacement.emplace_back(")", offset, TokenType::OPERATOR);
				auto old_size = close - k;
				Replace(ts, k, close, replacement);
				end = end + replacement.size() - old_size;
				k += replacement.size() - 1;
			} else if (name == "sum" || name == "avg") {
				Replace(ts, k, k + 1, Lex(name == "sum" ? "system . spark . spark_sum" : "system . spark . spark_avg", offset));
				end += 4;
				k += 4;
			}
		}
	}
}

//! window(...) is a function in Spark but a reserved word in DuckDB's grammar
void RenameWindowCalls(Tokens &ts) {
	for (idx_t i = 0; i + 1 < ts.size(); i++) {
		if (IsWord(ts[i], "window") && OpAt(ts, i + 1, "(") && !(i > 0 && OpAt(ts, i - 1, "."))) {
			// qualified: a window() the grouping rewrite did not handle (e.g. in a subquery) still resolves
			auto replacement = Lex("system . spark . __spark_window", ts[i].offset);
			Replace(ts, i, i + 1, replacement);
			i += replacement.size() - 1;
		}
	}
}

void SparkRewriteTokens(Tokens &tokens, bool strict) {
	if (strict) {
		// ROW(...) is DuckDB's; the rewrites below generate it themselves for positional struct inserts
		for (idx_t i = 0; i + 1 < tokens.size(); i++) {
			if (IsWord(tokens[i], "ROW") && OpAt(tokens, i + 1, "(")) {
				throw ParserException("Spark SQL does not support ROW(...); use struct(...)");
			}
		}
	}
	Tokens result;
	result.reserve(tokens.size());
	idx_t start = 0;
	for (idx_t i = 0; i <= tokens.size(); i++) {
		if (i < tokens.size() && !IsEnd(tokens[i])) {
			continue;
		}
		Tokens statement(tokens.begin() + NumericCast<int64_t>(start), tokens.begin() + NumericCast<int64_t>(i));
		RewriteLiterals(statement);
		if (!RewriteStatement(statement, strict)) {
			RewriteQueryClauses(statement);
			RewritePivotAggregates(statement);
			RewriteWindowColumn(statement);
			RenameWindowCalls(statement);
		}
		result.insert(result.end(), statement.begin(), statement.end());
		if (i < tokens.size()) {
			result.push_back(tokens[i]);
		}
		start = i + 1;
	}
	tokens = std::move(result);
}

} // namespace duckdb
