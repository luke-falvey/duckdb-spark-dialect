//===----------------------------------------------------------------------===//
// Spark functions around data formats and sketches: from_json / schema_of_json, from_csv / to_csv /
// schema_of_csv, sentences, bitmaps, HLL sketches, monotonically_increasing_id.
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/list.hpp"

#include <atomic>
#include <cmath>

namespace duckdb {

namespace {

using ExprP = unique_ptr<Expression>;

//===----------------------------------------------------------------------===//
// Spark DDL type strings: "a INT, b ARRAY<STRING>" / "STRUCT<a: INT>" / "MAP<STRING, INT>"
//===----------------------------------------------------------------------===//
struct DDLParser {
	explicit DDLParser(string text_p) : text(std::move(text_p)) {
	}
	string text;
	idx_t pos = 0;

	void Skip() {
		while (pos < text.size() && isspace((unsigned char)text[pos])) {
			pos++;
		}
	}
	bool Consume(char c) {
		Skip();
		if (pos < text.size() && text[pos] == c) {
			pos++;
			return true;
		}
		return false;
	}
	string Word() {
		Skip();
		if (pos < text.size() && text[pos] == '`') {
			auto end = text.find('`', pos + 1);
			auto word = text.substr(pos + 1, end - pos - 1);
			pos = end + 1;
			return word;
		}
		idx_t start = pos;
		while (pos < text.size() && (isalnum((unsigned char)text[pos]) || text[pos] == '_')) {
			pos++;
		}
		return text.substr(start, pos - start);
	}
	LogicalType Type() {
		auto name = StringUtil::Upper(Word());
		if (name == "INT" || name == "INTEGER") {
			return LogicalType::INTEGER;
		}
		if (name == "BIGINT" || name == "LONG") {
			return LogicalType::BIGINT;
		}
		if (name == "SMALLINT" || name == "SHORT") {
			return LogicalType::SMALLINT;
		}
		if (name == "TINYINT" || name == "BYTE") {
			return LogicalType::TINYINT;
		}
		if (name == "DOUBLE") {
			return LogicalType::DOUBLE;
		}
		if (name == "FLOAT" || name == "REAL") {
			return LogicalType::FLOAT;
		}
		if (name == "BOOLEAN") {
			return LogicalType::BOOLEAN;
		}
		if (name == "DATE") {
			return LogicalType::DATE;
		}
		if (name == "TIMESTAMP" || name == "TIMESTAMP_NTZ" || name == "TIMESTAMP_LTZ") {
			return LogicalType::TIMESTAMP;
		}
		if (name == "BINARY") {
			return LogicalType::BLOB;
		}
		if (name == "STRING" || name == "VARCHAR" || name == "CHAR") {
			if (Consume('(')) {
				Word();
				Consume(')');
			}
			return LogicalType::VARCHAR;
		}
		if (name == "DECIMAL" || name == "DEC" || name == "NUMERIC") {
			uint8_t p = 10, s = 0;
			if (Consume('(')) {
				p = uint8_t(std::stoi(Word()));
				if (Consume(',')) {
					s = uint8_t(std::stoi(Word()));
				}
				Consume(')');
			}
			return LogicalType::DECIMAL(p, s);
		}
		if (name == "ARRAY") {
			Consume('<');
			auto child = Type();
			Consume('>');
			return LogicalType::LIST(child);
		}
		if (name == "MAP") {
			Consume('<');
			auto key = Type();
			Consume(',');
			auto value = Type();
			Consume('>');
			return LogicalType::MAP(key, value);
		}
		if (name == "STRUCT") {
			Consume('<');
			auto fields = Fields('>');
			Consume('>');
			return LogicalType::STRUCT(std::move(fields));
		}
		throw InvalidInputException("Unsupported data type in schema: %s", name);
	}
	child_list_t<LogicalType> Fields(char terminator) {
		child_list_t<LogicalType> fields;
		while (true) {
			Skip();
			if (pos >= text.size() || text[pos] == terminator) {
				break;
			}
			auto field = Word();
			Consume(':');
			auto type = Type();
			Skip();
			// optional NOT NULL / COMMENT 'x'
			fields.emplace_back(Identifier(field), type);
			if (!Consume(',')) {
				break;
			}
		}
		return fields;
	}
	LogicalType Schema() {
		Skip();
		auto save = pos;
		auto first = StringUtil::Upper(Word());
		Skip();
		if ((first == "STRUCT" || first == "ARRAY" || first == "MAP") && pos < text.size() && text[pos] == '<') {
			pos = save;
			return Type();
		}
		pos = save;
		return LogicalType::STRUCT(Fields('\0'));
	}
};

string JsonStructure(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		string result = "{";
		auto &children = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += "\"" + children[i].first.GetIdentifierName() + "\":" + JsonStructure(children[i].second);
		}
		return result + "}";
	}
	case LogicalTypeId::LIST:
		return "[" + JsonStructure(ListType::GetChildType(type)) + "]";
	default:
		return "\"" + type.ToString() + "\"";
	}
}

Value ConstantArgument(FunctionBindExpressionInput &input, idx_t i, const char *function) {
	if (i >= input.children.size() || !input.children[i]->IsFoldable()) {
		throw BinderException("%s: the schema must be a constant string", function);
	}
	return ExpressionExecutor::EvaluateScalar(input.context, *input.children[i]);
}

string OptionValue(FunctionBindExpressionInput &input, idx_t index, const string &key) {
	if (index >= input.children.size() || !input.children[index]->IsFoldable()) {
		return "";
	}
	auto options = ExpressionExecutor::EvaluateScalar(input.context, *input.children[index]);
	if (options.IsNull() || options.type().id() != LogicalTypeId::MAP) {
		return "";
	}
	for (auto &entry : ListValue::GetChildren(options)) {
		auto &kv = StructValue::GetChildren(entry);
		if (StringUtil::CIEquals(kv[0].ToString(), key) && !kv[1].IsNull()) {
			return kv[1].ToString();
		}
	}
	return "";
}

ExprP Call(ClientContext &context, const string &name, vector<ExprP> args) {
	FunctionBinder binder(context);
	return binder.BindScalarFunction(Identifier(DEFAULT_SCHEMA), Identifier(name), std::move(args));
}

//! from_json(json, schema[, options])
ExprP BindFromJson(FunctionBindExpressionInput &input) {
	if (input.children.size() < 2 || input.children.size() > 3) {
		throw BinderException("from_json expects two or three arguments");
	}
	auto schema = ConstantArgument(input, 1, "from_json").ToString();
	auto type = DDLParser(schema).Schema();
	auto ts_format = OptionValue(input, 2, "timestampFormat");
	auto date_format = OptionValue(input, 2, "dateFormat");
	if (type.id() == LogicalTypeId::STRUCT && (!ts_format.empty() || !date_format.empty())) {
		// parse the formatted fields as strings first
		child_list_t<LogicalType> raw_fields;
		for (auto &field : StructType::GetChildTypes(type)) {
			bool formatted = (field.second.id() == LogicalTypeId::TIMESTAMP && !ts_format.empty()) ||
			                 (field.second.id() == LogicalTypeId::DATE && !date_format.empty());
			raw_fields.emplace_back(field.first, formatted ? LogicalType::VARCHAR : field.second);
		}
		auto raw_type = LogicalType::STRUCT(raw_fields);
		vector<ExprP> raw_args;
		raw_args.push_back(BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), LogicalType::JSON()));
		raw_args.push_back(make_uniq<BoundConstantExpression>(Value(JsonStructure(raw_type))));
		auto raw = Call(input.context, "json_transform", std::move(raw_args));
		FunctionBinder binder(input.context);
		vector<ExprP> values;
		auto &fields = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < fields.size(); i++) {
			vector<ExprP> extract_args;
			extract_args.push_back(raw->Copy());
			extract_args.push_back(make_uniq<BoundConstantExpression>(Value(fields[i].first.GetIdentifierName())));
			auto field = Call(input.context, "struct_extract", std::move(extract_args));
			if (raw_fields[i].second != fields[i].second) {
				vector<ExprP> parse_args;
				parse_args.push_back(std::move(field));
				parse_args.push_back(make_uniq<BoundConstantExpression>(
				    Value(fields[i].second.id() == LogicalTypeId::DATE ? date_format : ts_format)));
				field = binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier("parse_datetime"), std::move(parse_args));
			}
			values.push_back(BoundCastExpression::AddCastToType(input.context, std::move(field), fields[i].second));
		}
		return BoundCastExpression::AddCastToType(input.context, Call(input.context, "row", std::move(values)), type);
	}
	auto json = std::move(input.children[0]);
	vector<ExprP> valid_args;
	valid_args.push_back(json->Copy());
	auto valid = Call(input.context, "json_valid", std::move(valid_args));
	vector<ExprP> args;
	args.push_back(BoundCastExpression::AddCastToType(input.context, std::move(json), LogicalType::JSON()));
	args.push_back(make_uniq<BoundConstantExpression>(Value(JsonStructure(type))));
	auto transformed = Call(input.context, "json_transform", std::move(args));
	auto result_type = transformed->GetReturnType();
	return make_uniq<BoundCaseExpression>(std::move(valid), std::move(transformed),
	                                      make_uniq<BoundConstantExpression>(Value(result_type)));
}

//===----------------------------------------------------------------------===//
// schema_of_json: a minimal JSON reader + Spark's JsonInferSchema
//===----------------------------------------------------------------------===//
struct JsonReader {
	explicit JsonReader(const string &s) : text(s) {
	}
	const string &text;
	idx_t pos = 0;
	void Skip() {
		while (pos < text.size() && isspace((unsigned char)text[pos])) {
			pos++;
		}
	}
	string String() {
		string out;
		pos++; // opening quote
		while (pos < text.size() && text[pos] != '"') {
			if (text[pos] == '\\') {
				pos++;
			}
			out += text[pos++];
		}
		pos++;
		return out;
	}
	//! returns a Spark DDL type name
	string Infer() {
		Skip();
		if (pos >= text.size()) {
			throw InvalidInputException("Malformed JSON");
		}
		char c = text[pos];
		if (c == '{') {
			pos++;
			map<string, string> fields;
			Skip();
			if (pos < text.size() && text[pos] == '}') {
				pos++;
				return "STRUCT<>";
			}
			while (true) {
				Skip();
				auto key = String();
				Skip();
				pos++; // ':'
				auto type = Infer();
				fields[key] = fields.count(key) ? Merge(fields[key], type) : type;
				Skip();
				if (pos < text.size() && text[pos] == ',') {
					pos++;
					continue;
				}
				pos++; // '}'
				break;
			}
			string result = "STRUCT<";
			bool first = true;
			for (auto &f : fields) {
				result += (first ? "" : ", ") + f.first + ": " + f.second;
				first = false;
			}
			return result + ">";
		}
		if (c == '[') {
			pos++;
			string element;
			Skip();
			if (pos < text.size() && text[pos] == ']') {
				pos++;
				return "ARRAY<STRING>";
			}
			while (true) {
				auto type = Infer();
				element = element.empty() ? type : Merge(element, type);
				Skip();
				if (pos < text.size() && text[pos] == ',') {
					pos++;
					continue;
				}
				pos++;
				break;
			}
			return "ARRAY<" + (element == "NULL" ? string("STRING") : element) + ">";
		}
		if (c == '"') {
			String();
			return "STRING";
		}
		if (text.compare(pos, 4, "true") == 0) {
			pos += 4;
			return "BOOLEAN";
		}
		if (text.compare(pos, 5, "false") == 0) {
			pos += 5;
			return "BOOLEAN";
		}
		if (text.compare(pos, 4, "null") == 0) {
			pos += 4;
			return "NULL";
		}
		idx_t start = pos;
		bool fractional = false;
		while (pos < text.size() && (isdigit((unsigned char)text[pos]) || strchr("+-.eE", text[pos]))) {
			if (strchr(".eE", text[pos])) {
				fractional = true;
			}
			pos++;
		}
		if (start == pos) {
			throw InvalidInputException("Malformed JSON");
		}
		return fractional ? "DOUBLE" : "BIGINT";
	}
	static string Merge(const string &a, const string &b) {
		if (a == b || b == "NULL") {
			return a;
		}
		if (a == "NULL") {
			return b;
		}
		if ((a == "BIGINT" && b == "DOUBLE") || (a == "DOUBLE" && b == "BIGINT")) {
			return "DOUBLE";
		}
		return "STRING";
	}
};

void SchemaOfJsonKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		auto str = s.GetString();
		JsonReader reader(str);
		auto type = reader.Infer();
		if (type == "NULL") {
			type = "STRING";
		}
		return StringVector::AddString(result, type);
	});
}

//===----------------------------------------------------------------------===//
// CSV
//===----------------------------------------------------------------------===//
vector<string> SplitCsvLine(const string &line) {
	vector<string> fields;
	string current;
	bool quoted = false;
	for (idx_t i = 0; i < line.size(); i++) {
		char c = line[i];
		if (quoted) {
			if (c == '"' && i + 1 < line.size() && line[i + 1] == '"') {
				current += '"';
				i++;
			} else if (c == '"') {
				quoted = false;
			} else {
				current += c;
			}
		} else if (c == '"') {
			quoted = true;
		} else if (c == ',') {
			fields.push_back(current);
			current.clear();
		} else {
			current += c;
		}
	}
	fields.push_back(current);
	return fields;
}

string InferCsvField(const string &field) {
	string v = field;
	StringUtil::Trim(v);
	if (v.empty()) {
		return "STRING";
	}
	char *end;
	errno = 0;
	auto l = strtoll(v.c_str(), &end, 10);
	if (*end == '\0' && errno == 0) {
		return (l >= NumericLimits<int32_t>::Minimum() && l <= NumericLimits<int32_t>::Maximum()) ? "INT" : "BIGINT";
	}
	strtod(v.c_str(), &end);
	if (*end == '\0') {
		return "DOUBLE";
	}
	auto lower = StringUtil::Lower(v);
	if (lower == "true" || lower == "false") {
		return "BOOLEAN";
	}
	return "STRING";
}

void SchemaOfCsvKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		auto fields = SplitCsvLine(s.GetString());
		string out = "STRUCT<";
		for (idx_t i = 0; i < fields.size(); i++) {
			out += (i > 0 ? ", _c" : "_c") + to_string(i) + ": " + InferCsvField(fields[i]);
		}
		return StringVector::AddString(result, out + ">");
	});
}

void CsvFieldsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat format;
	args.data[0].ToUnifiedFormat(format);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	vector<vector<string>> rows(count);
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		rows[i] = SplitCsvLine(UnifiedVectorFormat::GetData<string_t>(format)[idx].GetString());
		total += rows[i].size();
	}
	ListVector::Reserve(result, total);
	auto &child = ListVector::GetChildMutable(result);
	auto data = FlatVector::GetDataMutable<string_t>(child);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		entries[i] = list_entry_t(offset, rows[i].size());
		for (auto &f : rows[i]) {
			data[offset++] = StringVector::AddString(child, f);
		}
	}
	ListVector::SetListSize(result, total);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! from_csv(str, schema[, options]) -> struct, fields converted with Spark's casts
ExprP BindFromCsv(FunctionBindExpressionInput &input) {
	if (input.children.size() < 2 || input.children.size() > 3) {
		throw BinderException("from_csv expects two or three arguments");
	}
	auto schema = ConstantArgument(input, 1, "from_csv").ToString();
	auto type = DDLParser(schema).Schema();
	if (type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("from_csv: the schema must be a struct");
	}
	FunctionBinder binder(input.context);
	ScalarFunction fields_fn(Identifier("csv_fields"), {LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR),
	                         CsvFieldsKernel);
	vector<ExprP> fn_args;
	fn_args.push_back(BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), LogicalType::VARCHAR));
	auto fields = binder.BindScalarFunction(fields_fn, std::move(fn_args));
	vector<ExprP> values;
	auto &children = StructType::GetChildTypes(type);
	for (idx_t i = 0; i < children.size(); i++) {
		vector<ExprP> extract_args;
		extract_args.push_back(fields->Copy());
		extract_args.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(int64_t(i + 1))));
		auto field = Call(input.context, "list_extract", std::move(extract_args));
		auto ts_format = OptionValue(input, 2, children[i].second.id() == LogicalTypeId::DATE ? "dateFormat" : "timestampFormat");
		if (!ts_format.empty() &&
		    (children[i].second.id() == LogicalTypeId::TIMESTAMP || children[i].second.id() == LogicalTypeId::DATE)) {
			vector<ExprP> parse_args;
			parse_args.push_back(std::move(field));
			parse_args.push_back(make_uniq<BoundConstantExpression>(Value(ts_format)));
			auto parsed = binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier("parse_datetime"), std::move(parse_args));
			values.push_back(BoundCastExpression::AddCastToType(input.context, std::move(parsed), children[i].second));
			continue;
		}
		// the Spark cast of the field to its type, through the spark.cast_value dispatcher
		vector<ExprP> cast_args;
		cast_args.push_back(std::move(field));
		cast_args.push_back(make_uniq<BoundConstantExpression>(Value(children[i].second)));
		values.push_back(binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier("cast_value"), std::move(cast_args)));
	}
	auto row = Call(input.context, "row", std::move(values));
	return BoundCastExpression::AddCastToType(input.context, std::move(row), type);
}

//! to_csv(struct[, options]): fields joined with ',' (NULL as empty)
ExprP BindToCsv(FunctionBindExpressionInput &input) {
	if (input.children.empty() || input.children.size() > 2) {
		throw BinderException("to_csv expects one or two arguments");
	}
	auto value = std::move(input.children[0]);
	auto type = value->GetReturnType();
	if (type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("to_csv expects a struct");
	}
	FunctionBinder binder(input.context);
	vector<ExprP> parts;
	auto &children = StructType::GetChildTypes(type);
	for (idx_t i = 0; i < children.size(); i++) {
		vector<ExprP> extract_args;
		extract_args.push_back(value->Copy());
		extract_args.push_back(make_uniq<BoundConstantExpression>(Value(children[i].first.GetIdentifierName())));
		auto field = Call(input.context, "struct_extract", std::move(extract_args));
		auto field_type = children[i].second;
		auto format = OptionValue(input, 1, field_type.id() == LogicalTypeId::DATE ? "dateFormat" : "timestampFormat");
		if (format.empty() && field_type.id() == LogicalTypeId::TIMESTAMP) {
			format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
		}
		if (!format.empty() && (field_type.id() == LogicalTypeId::TIMESTAMP || field_type.id() == LogicalTypeId::DATE)) {
			vector<ExprP> format_args;
			format_args.push_back(BoundCastExpression::AddCastToType(input.context, std::move(field), LogicalType::TIMESTAMP));
			format_args.push_back(make_uniq<BoundConstantExpression>(Value(format)));
			field = binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier("format_datetime"), std::move(format_args));
		}
		vector<ExprP> str_args;
		str_args.push_back(std::move(field));
		auto str = binder.BindScalarFunction(Identifier(SPARK_SCHEMA), Identifier("as_string"), std::move(str_args));
		vector<ExprP> coalesce_args;
		auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::VARCHAR);
		coalesce->GetChildrenMutable().push_back(std::move(str));
		coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value("")));
		parts.push_back(std::move(coalesce));
	}
	vector<ExprP> args;
	args.push_back(make_uniq<BoundConstantExpression>(Value(",")));
	for (auto &p : parts) {
		args.push_back(std::move(p));
	}
	return Call(input.context, "concat_ws", std::move(args));
}

//===----------------------------------------------------------------------===//
// sentences(str[, lang, country]) approximating java.text.BreakIterator
//===----------------------------------------------------------------------===//
void SentencesKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	UnifiedVectorFormat format;
	args.data[0].ToUnifiedFormat(format);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	vector<vector<vector<string>>> rows(count);
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		auto text = UnifiedVectorFormat::GetData<string_t>(format)[idx].GetString();
		vector<string> sentence;
		string word;
		for (idx_t k = 0; k <= text.size(); k++) {
			char c = k < text.size() ? text[k] : '.';
			if (isalnum((unsigned char)c) || c == '\'' || (unsigned char)c >= 0x80) {
				word += c;
				continue;
			}
			if (!word.empty()) {
				sentence.push_back(word);
				word.clear();
			}
			if ((c == '.' || c == '!' || c == '?') && !sentence.empty()) {
				rows[i].push_back(sentence);
				sentence.clear();
			}
		}
		total += rows[i].size();
	}
	ListVector::Reserve(result, total);
	auto &inner = ListVector::GetChildMutable(result);
	auto inner_entries = FlatVector::GetDataMutable<list_entry_t>(inner);
	idx_t word_total = 0;
	for (auto &row : rows) {
		for (auto &s : row) {
			word_total += s.size();
		}
	}
	ListVector::Reserve(inner, word_total);
	auto &words = ListVector::GetChildMutable(inner);
	auto word_data = FlatVector::GetDataMutable<string_t>(words);
	idx_t offset = 0, word_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		entries[i] = list_entry_t(offset, rows[i].size());
		for (auto &s : rows[i]) {
			inner_entries[offset++] = list_entry_t(word_offset, s.size());
			for (auto &w : s) {
				word_data[word_offset++] = StringVector::AddString(words, w);
			}
		}
	}
	ListVector::SetListSize(inner, word_total);
	ListVector::SetListSize(result, total);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//===----------------------------------------------------------------------===//
// Bitmaps (Spark 3.5): 32768 bits per bucket, little-endian bit order within bytes
//===----------------------------------------------------------------------===//
void BitmapFromPositionsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto &input = args.data[0];
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value = input.GetValue(i);
		string bitmap(4096, '\0');
		if (!value.IsNull()) {
			for (auto &p : ListValue::GetChildren(value)) {
				if (p.IsNull()) {
					continue;
				}
				auto pos = p.GetValue<int64_t>();
				if (pos < 0 || pos >= 32768) {
					throw InvalidInputException("Bitmap position %lld is out of range", (long long)pos);
				}
				bitmap[pos / 8] = char(uint8_t(bitmap[pos / 8]) | (1 << (pos % 8)));
			}
		}
		out[i] = StringVector::AddStringOrBlob(result, bitmap);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void BitmapOrListKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto &input = args.data[0];
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value = input.GetValue(i);
		string bitmap(4096, '\0');
		if (!value.IsNull()) {
			for (auto &b : ListValue::GetChildren(value)) {
				if (b.IsNull()) {
					continue;
				}
				auto &bytes = StringValue::Get(b);
				for (idx_t k = 0; k < MinValue<idx_t>(bytes.size(), 4096); k++) {
					bitmap[k] = char(uint8_t(bitmap[k]) | uint8_t(bytes[k]));
				}
			}
		}
		out[i] = StringVector::AddStringOrBlob(result, bitmap);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void BitmapCountKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(), [](string_t b) {
		int64_t total = 0;
		auto data = b.GetData();
		for (idx_t k = 0; k < b.GetSize(); k++) {
			total += __builtin_popcount(uint8_t(data[k]));
		}
		return total;
	});
}

//===----------------------------------------------------------------------===//
// HLL sketches: the sketch holds the distinct 64-bit hashes, so estimates are exact. Spark's DataSketches
// HLL estimate is exact for small cardinalities and approximate for large ones.
//===----------------------------------------------------------------------===//
string SerializeHashes(set<int64_t> &hashes) {
	string out;
	for (auto h : hashes) {
		out.append(reinterpret_cast<const char *>(&h), 8);
	}
	return out;
}

void HllFromListKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value = args.data[0].GetValue(i);
		set<int64_t> hashes;
		if (!value.IsNull()) {
			for (auto &h : ListValue::GetChildren(value)) {
				if (!h.IsNull()) {
					hashes.insert(h.GetValue<int64_t>());
				}
			}
		}
		out[i] = StringVector::AddStringOrBlob(result, SerializeHashes(hashes));
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void HllUnionListKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value = args.data[0].GetValue(i);
		set<int64_t> hashes;
		if (!value.IsNull()) {
			for (auto &sketch : ListValue::GetChildren(value)) {
				if (sketch.IsNull()) {
					continue;
				}
				auto &bytes = StringValue::Get(sketch);
				for (idx_t k = 0; k + 8 <= bytes.size(); k += 8) {
					int64_t h;
					memcpy(&h, bytes.data() + k, 8);
					hashes.insert(h);
				}
			}
		}
		out[i] = StringVector::AddStringOrBlob(result, SerializeHashes(hashes));
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void HllEstimateKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
	                                           [](string_t b) { return int64_t(b.GetSize() / 8); });
}

//===----------------------------------------------------------------------===//
// monotonically_increasing_id: unique, increasing within the process
//===----------------------------------------------------------------------===//
std::atomic<int64_t> monotonic_counter {0};

void MonotonicIdKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<int64_t>(result);
	auto start = monotonic_counter.fetch_add(int64_t(count));
	for (idx_t i = 0; i < count; i++) {
		out[i] = start + int64_t(i);
	}
}

ScalarFunction Dispatch(const string &name, function_bind_expression_t bind) {
	ScalarFunction fun(Identifier(name), {}, LogicalType::ANY,
	                   [](DataChunk &, ExpressionState &, Vector &) { throw InternalException("not rewritten"); },
	                   nullptr, nullptr, nullptr, LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(bind);
	return fun;
}

} // namespace

void RegisterSparkFormats(ExtensionLoader &loader) {
	RegisterFallible(loader, Dispatch("from_json", BindFromJson));
	RegisterFallible(loader, Dispatch("from_csv", BindFromCsv));
	RegisterFallible(loader, Dispatch("to_csv", BindToCsv));
	RegisterFallible(loader, ScalarFunction(Identifier("schema_of_json_impl"), {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                       SchemaOfJsonKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("schema_of_csv"), {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                       SchemaOfCsvKernel));
	ScalarFunction sentences(Identifier("sentences"), {LogicalType::VARCHAR},
	                         LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)), SentencesKernel);
	RegisterFallible(loader, sentences);
	RegisterFallible(loader, ScalarFunction(Identifier("sentences"), {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)), SentencesKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("sentences"),
	                                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)), SentencesKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("bitmap_from_positions"), {LogicalType::LIST(LogicalType::BIGINT)},
	                                       LogicalType::BLOB, BitmapFromPositionsKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("bitmap_or_list"), {LogicalType::LIST(LogicalType::BLOB)},
	                                       LogicalType::BLOB, BitmapOrListKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("bitmap_count_impl"), {LogicalType::BLOB}, LogicalType::BIGINT, BitmapCountKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("hll_from_list"), {LogicalType::LIST(LogicalType::BIGINT)},
	                                       LogicalType::BLOB, HllFromListKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("hll_union_list"), {LogicalType::LIST(LogicalType::BLOB)},
	                                       LogicalType::BLOB, HllUnionListKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("hll_estimate"), {LogicalType::BLOB}, LogicalType::BIGINT, HllEstimateKernel));
	ScalarFunction monotonic(Identifier("monotonically_increasing_id"), {}, LogicalType::BIGINT, MonotonicIdKernel);
	monotonic.SetVolatile();
	RegisterFallible(loader, monotonic);
	for (auto name : {"from_json", "from_csv", "to_csv", "schema_of_csv", "sentences",
	                  "monotonically_increasing_id"}) {
		AddSparkOverride(name);
	}
}

} // namespace duckdb
