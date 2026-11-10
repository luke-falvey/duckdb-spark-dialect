//===----------------------------------------------------------------------===//
// Spark SQL settings: SET / RESET state of a connection
//
// `SET key = value` is rewritten by the tokenizer into `SELECT spark.spark_conf_set(key, value)`; the state lives in the
// connection (ClientContext::registered_state), is validated like Spark validates it (value type, enum values, static
// and core keys), and is read by the functions that a setting changes through SparkConfGet() at bind time.
// A setting whose effect the dialect does not reproduce is rejected, never ignored (test/settings/inventory.toml).
//===----------------------------------------------------------------------===//
#include "spark_dialect.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <mutex>
#include <regex>
#include <unordered_map>

namespace duckdb {

namespace {

enum class ConfKind { BOOLEAN, INTEGER, DOUBLE, BYTES, TIME, ENUM, STRING, STATIC };
enum class ConfClass { SEMANTIC, COMMAND, STORAGE, EXECUTION, CLIENT, STATIC, UNOBSERVABLE };
enum class ConfStatus { EMULATED, PARTIAL, ACCEPTED, REJECTED };

struct ConfEntry {
	const char *key;
	const char *default_value;
	ConfKind kind;
	ConfClass cls;
	ConfStatus status;
	int tier;
	const char *values; // '|' separated: the legal values of an enum, or the values a partial setting emulates
};

const ConfEntry REGISTRY[] = {
#include "spark_conf_registry.inc"
};

const unordered_map<string, const ConfEntry *> &Registry() {
	static const unordered_map<string, const ConfEntry *> map = [] {
		unordered_map<string, const ConfEntry *> m;
		for (auto &entry : REGISTRY) {
			m[entry.key] = &entry;
		}
		return m;
	}();
	return map;
}

class SparkConfState : public ClientContextState {
public:
	std::mutex lock;
	unordered_map<string, string> values;
};

SparkConfState &State(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<SparkConfState>("spark_conf");
	return *state;
}

string Trim(const string &s) {
	size_t begin = 0, end = s.size();
	while (begin < end && isspace(static_cast<unsigned char>(s[begin]))) {
		begin++;
	}
	while (end > begin && isspace(static_cast<unsigned char>(s[end - 1]))) {
		end--;
	}
	return s.substr(begin, end - begin);
}

vector<string> SplitValues(const char *values) {
	vector<string> out;
	if (!values || !*values) {
		return out;
	}
	return StringUtil::Split(values, '|');
}

//! Spark's validation of a value for a registered setting (ConfigEntry value converters)
void Validate(const ConfEntry &entry, const string &raw) {
	auto value = Trim(raw);
	switch (entry.kind) {
	case ConfKind::BOOLEAN: {
		auto lower = StringUtil::Lower(value);
		if (lower != "true" && lower != "false") {
			throw InvalidInputException("%s should be boolean, but was %s", entry.key, raw);
		}
		break;
	}
	case ConfKind::INTEGER: {
		static const std::regex number("[+-]?[0-9]+");
		if (!std::regex_match(value, number)) {
			throw InvalidInputException("%s should be int, but was %s", entry.key, raw);
		}
		break;
	}
	case ConfKind::DOUBLE: {
		static const std::regex number("[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)([eE][+-]?[0-9]+)?");
		if (!std::regex_match(value, number)) {
			throw InvalidInputException("%s should be double, but was %s", entry.key, raw);
		}
		break;
	}
	case ConfKind::BYTES: {
		static const std::regex size("-?[0-9]+(b|k|kb|m|mb|g|gb|t|tb|p|pb)?", std::regex::icase);
		if (!std::regex_match(value, size)) {
			throw InvalidInputException("Size must be specified as bytes (b), kibibytes (k), mebibytes (m), gibibytes "
			                            "(g), tebibytes (t), or pebibytes(p). E.g. 50b, 100k, or 250m. Failed to parse "
			                            "byte string: %s",
			                            raw);
		}
		break;
	}
	case ConfKind::TIME: {
		static const std::regex time("-?[0-9]+(us|ms|s|m|min|h|d)?", std::regex::icase);
		if (!std::regex_match(value, time)) {
			throw InvalidInputException("Time must be specified as seconds (s), milliseconds (ms), microseconds (us), "
			                            "minutes (m or min), hour (h), or day (d). E.g. 50s, 100ms, or 250us. Failed to "
			                            "parse time string: %s",
			                            raw);
		}
		break;
	}
	case ConfKind::ENUM: {
		bool found = false;
		for (auto &legal : SplitValues(entry.values)) {
			found = found || StringUtil::CIEquals(legal, value);
		}
		if (!found) {
			string list;
			for (auto &legal : SplitValues(entry.values)) {
				list += (list.empty() ? "" : ", ") + legal;
			}
			throw InvalidInputException("The value of %s should be one of %s, but was %s", entry.key, list, raw);
		}
		break;
	}
	default:
		break;
	}
}

//! Does a value equal the setting's default (booleans and enums compare case-insensitively)?
bool IsDefault(const ConfEntry &entry, const string &raw) {
	auto value = Trim(raw);
	if (entry.kind == ConfKind::BOOLEAN || entry.kind == ConfKind::ENUM) {
		return StringUtil::CIEquals(value, entry.default_value);
	}
	return value == entry.default_value;
}

bool IsEmulatedValue(const ConfEntry &entry, const string &raw) {
	auto value = Trim(raw);
	for (auto &legal : SplitValues(entry.values)) {
		if (StringUtil::CIEquals(legal, value)) {
			return true;
		}
	}
	return false;
}

//! Spark core settings (spark.task.cpus, spark.executor.memory, ...) cannot be changed with SET
bool IsCoreKey(const string &key) {
	return StringUtil::StartsWith(key, "spark.") && !StringUtil::StartsWith(key, "spark.sql.") &&
	       !StringUtil::StartsWith(key, "spark.hadoop.") && !StringUtil::StartsWith(key, "spark.hive.");
}

}  // namespace

//! The checks of `SET key = value` that do not depend on the session: static keys, the value's type and legal values,
//! and the rejection of a setting the dialect does not emulate. Raised at parse time by the tokenizer.
void SparkConfCheck(const string &key, const string &value) {
	auto &registry = Registry();
	auto found = registry.find(key);
	if (found == registry.end()) {
		return;
	}
	auto &entry = *found->second;
	if (entry.kind == ConfKind::STATIC || entry.cls == ConfClass::STATIC) {
		throw InvalidInputException("[CANNOT_MODIFY_CONFIG] Cannot modify the value of the Spark config: \"%s\".", key);
	}
	Validate(entry, value);
	bool rejected = entry.status == ConfStatus::REJECTED && !IsDefault(entry, value);
	bool partial = entry.status == ConfStatus::PARTIAL && !IsDefault(entry, value) && !IsEmulatedValue(entry, value);
	if (rejected || partial) {
		throw InvalidInputException("[UNSUPPORTED_FEATURE.SET_CONF] spark_dialect does not emulate %s = %s", key, value);
	}
}

namespace {

void ConfSetKernel(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t key_p, string_t value_p) {
		    auto key = key_p.GetString();
		    auto value = value_p.GetString();
		    auto &conf = State(context);
		    if (Registry().find(key) == Registry().end() && IsCoreKey(key)) {
			    string rejects = "true";
			    {
				    std::lock_guard<std::mutex> guard(conf.lock);
				    auto it = conf.values.find("spark.sql.legacy.setCommandRejectsSparkCoreConfs");
				    if (it != conf.values.end()) {
					    rejects = StringUtil::Lower(Trim(it->second));
				    }
			    }
			    if (rejects == "true") {
				    throw InvalidInputException("[CANNOT_MODIFY_CONFIG] Cannot modify the value of the Spark config: "
				                                "\"%s\".",
				                                key);
			    }
		    }
		    SparkConfCheck(key, value);
		    {
			    std::lock_guard<std::mutex> guard(conf.lock);
			    conf.values[key] = value;
		    }
		    return StringVector::AddString(result, value);
	    });
}

void ConfGetKernel(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t key_p) {
		auto key = key_p.GetString();
		auto &conf = State(context);
		{
			std::lock_guard<std::mutex> guard(conf.lock);
			auto it = conf.values.find(key);
			if (it != conf.values.end()) {
				return StringVector::AddString(result, it->second);
			}
		}
		auto &registry = Registry();
		auto found = registry.find(key);
		if (found != registry.end()) {
			return StringVector::AddString(result, found->second->default_value);
		}
		return StringVector::AddString(result, "<undefined>");
	});
}

void ConfResetKernel(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t key_p) {
		auto key = key_p.GetString();
		auto &registry = Registry();
		auto found = registry.find(key);
		if (found != registry.end() &&
		    (found->second->kind == ConfKind::STATIC || found->second->cls == ConfClass::STATIC)) {
			throw InvalidInputException("[CANNOT_MODIFY_CONFIG] Cannot modify the value of the Spark config: \"%s\".",
			                            key);
		}
		auto &conf = State(context);
		std::lock_guard<std::mutex> guard(conf.lock);
		if (key.empty()) {
			conf.values.clear();
		} else {
			conf.values.erase(key);
		}
		return StringVector::AddString(result, "");
	});
}

void ConfListKernel(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &conf = State(context);
	vector<std::pair<string, string>> rows;
	{
		std::lock_guard<std::mutex> guard(conf.lock);
		for (auto &kv : conf.values) {
			rows.emplace_back(kv.first, kv.second);
		}
	}
	std::sort(rows.begin(), rows.end());
	ListVector::Reserve(result, rows.size());
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	entries[0] = list_entry_t(0, rows.size());
	auto &child = ListVector::GetChildMutable(result);
	auto &fields = StructVector::GetEntries(child);
	auto keys = FlatVector::GetDataMutable<string_t>(fields[0]);
	auto values = FlatVector::GetDataMutable<string_t>(fields[1]);
	for (idx_t i = 0; i < rows.size(); i++) {
		keys[i] = StringVector::AddString(fields[0], rows[i].first);
		values[i] = StringVector::AddString(fields[1], rows[i].second);
	}
	ListVector::SetListSize(result, rows.size());
}

} // namespace

string SparkConfGet(ClientContext &context, const string &key) {
	auto &conf = State(context);
	{
		std::lock_guard<std::mutex> guard(conf.lock);
		auto it = conf.values.find(key);
		if (it != conf.values.end()) {
			return Trim(it->second);
		}
	}
	auto &registry = Registry();
	auto found = registry.find(key);
	return found == registry.end() ? string() : string(found->second->default_value);
}

bool SparkConfBool(ClientContext &context, const string &key) {
	return StringUtil::Lower(SparkConfGet(context, key)) == "true";
}

bool SparkConfEquals(ClientContext &context, const string &key, const string &expected) {
	return StringUtil::CIEquals(SparkConfGet(context, key), expected);
}

void RegisterSparkConf(ExtensionLoader &loader) {
	auto varchar = LogicalType::VARCHAR;
	ScalarFunction set(Identifier("spark_conf_set"), {varchar, varchar}, varchar, ConfSetKernel);
	set.SetVolatile();
	set.SetFallible();
	loader.RegisterFunction(set);
	ScalarFunction get(Identifier("spark_conf_get"), {varchar}, varchar, ConfGetKernel);
	get.SetVolatile();
	loader.RegisterFunction(get);
	ScalarFunction reset(Identifier("spark_conf_reset"), {varchar}, varchar, ConfResetKernel);
	reset.SetVolatile();
	reset.SetFallible();
	loader.RegisterFunction(reset);
	child_list_t<LogicalType> fields;
	fields.emplace_back(Identifier("key"), varchar);
	fields.emplace_back(Identifier("value"), varchar);
	ScalarFunction list(Identifier("spark_conf_list"), {}, LogicalType::LIST(LogicalType::STRUCT(std::move(fields))),
	                    ConfListKernel);
	list.SetVolatile();
	loader.RegisterFunction(list);
}

} // namespace duckdb
