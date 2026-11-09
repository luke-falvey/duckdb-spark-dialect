//===----------------------------------------------------------------------===//
// Spark functions implemented in C++: string utilities, hashes, bit shifts, base conversion,
// URL parsing, regex split with limit, number formatting, JSON / CSV schemas, ...
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <regex>
#include "re2/re2.h"

#include "utf8proc.hpp"

namespace duckdb {

namespace {

//===----------------------------------------------------------------------===//
// Strings
//===----------------------------------------------------------------------===//
//! Unicode code points of a UTF-8 string (invalid bytes are kept as single code points)
vector<int32_t> CodePoints(const string &s) {
	vector<int32_t> out;
	auto data = reinterpret_cast<const utf8proc_uint8_t *>(s.data());
	utf8proc_ssize_t pos = 0, len = utf8proc_ssize_t(s.size());
	while (pos < len) {
		utf8proc_int32_t cp;
		auto n = utf8proc_iterate(data + pos, len - pos, &cp);
		if (n <= 0) {
			cp = data[pos];
			n = 1;
		}
		out.push_back(cp);
		pos += n;
	}
	return out;
}

void AppendCodePoint(string &out, int32_t cp) {
	utf8proc_uint8_t buf[4];
	auto n = utf8proc_encode_char(cp, buf);
	out.append(reinterpret_cast<const char *>(buf), size_t(n));
}

//! Spark's initcap: lower-case everything, then title-case the first letter after each space (Unicode-aware)
string InitCap(const string &s) {
	string out;
	bool start = true;
	for (auto cp : CodePoints(s)) {
		if (cp == ' ') {
			out += ' ';
			start = true;
			continue;
		}
		AppendCodePoint(out, start ? utf8proc_totitle(utf8proc_tolower(cp)) : utf8proc_tolower(cp));
		start = false;
	}
	return out;
}

//! levenshtein over code points, like Spark (DuckDB's counts bytes)
int64_t Levenshtein(const string &a_str, const string &b_str) {
	auto a = CodePoints(a_str), b = CodePoints(b_str);
	vector<int64_t> prev(b.size() + 1), cur(b.size() + 1);
	for (idx_t j = 0; j <= b.size(); j++) {
		prev[j] = int64_t(j);
	}
	for (idx_t i = 1; i <= a.size(); i++) {
		cur[0] = int64_t(i);
		for (idx_t j = 1; j <= b.size(); j++) {
			auto cost = a[i - 1] == b[j - 1] ? 0 : 1;
			cur[j] = MinValue(MinValue(prev[j] + 1, cur[j - 1] + 1), prev[j - 1] + cost);
		}
		std::swap(prev, cur);
	}
	return prev[b.size()];
}

void LevenshteinKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, int32_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [](string_t a, string_t b) { return int32_t(Levenshtein(a.GetString(), b.GetString())); });
}

string Soundex(const string &s) {
	if (s.empty()) {
		return s;
	}
	char first = char(toupper((unsigned char)s[0]));
	if (first < 'A' || first > 'Z') {
		return s;
	}
	//                    ABCDEFGHIJKLMNOPQRSTUVWXYZ
	static const char *codes = "01230120022455012623010202";
	string out(1, first);
	char last = codes[first - 'A'];
	for (idx_t i = 1; i < s.size() && out.size() < 4; i++) {
		char c = char(toupper((unsigned char)s[i]));
		if (c < 'A' || c > 'Z') {
			last = '0';
			continue;
		}
		char code = codes[c - 'A'];
		if (c == 'H' || c == 'W') {
			continue;
		}
		if (code != '0' && code != last) {
			out += code;
		}
		last = code;
	}
	while (out.size() < 4) {
		out += '0';
	}
	return out;
}

bool LuhnCheck(const string &s) {
	if (s.empty()) {
		return false;
	}
	int sum = 0;
	bool alternate = false;
	for (idx_t i = s.size(); i > 0; i--) {
		char c = s[i - 1];
		if (c < '0' || c > '9') {
			return false;
		}
		int digit = c - '0';
		if (alternate) {
			digit *= 2;
			if (digit > 9) {
				digit -= 9;
			}
		}
		sum += digit;
		alternate = !alternate;
	}
	return sum % 10 == 0;
}

void InitCapKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		return StringVector::AddString(result, InitCap(s.GetString()));
	});
}

void SoundexKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		return StringVector::AddString(result, Soundex(s.GetString()));
	});
}

void LuhnKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(),
	                                        [&](string_t s) { return LuhnCheck(s.GetString()); });
}

//! mask(input, upper, lower, digit, other): NULL replacement chars keep the original character
void MaskKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat formats[5];
	for (idx_t a = 0; a < 5; a++) {
		args.data[a].ToUnifiedFormat(formats[a]);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx0 = formats[0].sel->get_index(i);
		if (!formats[0].validity.RowIsValid(idx0)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			continue;
		}
		auto input = UnifiedVectorFormat::GetData<string_t>(formats[0])[idx0].GetString();
		string replacement[4];
		bool has[4];
		for (idx_t a = 1; a < 5; a++) {
			auto idx = formats[a].sel->get_index(i);
			has[a - 1] = formats[a].validity.RowIsValid(idx);
			if (has[a - 1]) {
				replacement[a - 1] = UnifiedVectorFormat::GetData<string_t>(formats[a])[idx].GetString();
			}
		}
		string masked;
		for (auto c : input) {
			int kind = (c >= 'A' && c <= 'Z') ? 0 : (c >= 'a' && c <= 'z') ? 1 : (c >= '0' && c <= '9') ? 2 : 3;
			if (has[kind] && !replacement[kind].empty()) {
				masked += replacement[kind];
			} else {
				masked += c;
			}
		}
		out[i] = StringVector::AddString(result, masked);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! A compiled pattern, cached per thread for the last pattern seen (split's pattern is almost always constant).
//! RE2 (bundled with DuckDB: linear time, no stack recursion) where it accepts the pattern; std::regex for the
//! Java syntax RE2 lacks (backreferences, lookaround).
struct CachedRegex {
	string pattern;
	unique_ptr<duckdb_re2::RE2> re2;
	unique_ptr<std::regex> fallback;
};

CachedRegex &CompileRegex(const string &pattern) {
	static thread_local CachedRegex cached;
	if (cached.pattern != pattern || (!cached.re2 && !cached.fallback)) {
		cached.pattern = pattern;
		duckdb_re2::RE2::Options options;
		options.set_log_errors(false);
		cached.re2 = make_uniq<duckdb_re2::RE2>(pattern, options);
		cached.fallback.reset();
		if (!cached.re2->ok()) {
			cached.re2.reset();
			cached.fallback = make_uniq<std::regex>(pattern);
		}
	}
	return cached;
}

//! Calls on_match(position, length) for successive matches, like java.util.regex.Matcher.find(): after an empty
//! match the search resumes one code point later. Stops when on_match returns false.
template <class ON_MATCH>
void FindRegexMatches(const string &pattern, const char *data, size_t size, ON_MATCH on_match) {
	// a pattern without regex metacharacters (split(s, ','), the common case) is a plain substring search
	if (!pattern.empty() && pattern.find_first_of("\\^$.|?*+()[]{}") == string::npos) {
		const size_t plen = pattern.size();
		size_t pos = 0;
		if (plen == 1) {
			const char c = pattern[0];
			while (pos < size) {
				auto hit = static_cast<const char *>(memchr(data + pos, c, size - pos));
				if (!hit) {
					return;
				}
				pos = size_t(hit - data);
				if (!on_match(idx_t(pos), idx_t(1))) {
					return;
				}
				pos++;
			}
			return;
		}
		while (pos + plen <= size) {
			auto hit = static_cast<const char *>(memmem(data + pos, size - pos, pattern.data(), plen));
			if (!hit) {
				return;
			}
			pos = size_t(hit - data);
			if (!on_match(idx_t(pos), idx_t(plen))) {
				return;
			}
			pos += plen;
		}
		return;
	}
	auto &regex = CompileRegex(pattern);
	if (regex.re2) {
		duckdb_re2::StringPiece input(data, size);
		duckdb_re2::StringPiece match;
		size_t pos = 0;
		while (pos <= size) {
			if (!regex.re2->Match(input, pos, size, duckdb_re2::RE2::UNANCHORED, &match, 1)) {
				return;
			}
			auto start = size_t(match.data() - data);
			if (!on_match(idx_t(start), idx_t(match.size()))) {
				return;
			}
			if (match.empty()) {
				pos = start + 1;
				while (pos < size && (static_cast<unsigned char>(data[pos]) & 0xC0) == 0x80) {
					pos++;
				}
			} else {
				pos = start + match.size();
			}
		}
		return;
	}
	for (auto it = std::cregex_iterator(data, data + size, *regex.fallback); it != std::cregex_iterator(); ++it) {
		if (!on_match(idx_t(it->position()), idx_t(it->length()))) {
			return;
		}
	}
}

//! split(str, regex, limit) with java.lang.String.split semantics (limit <= 0: no limit, trailing empty kept).
//! Parts are written straight into the list child (no per-row allocations); the list child grows geometrically.
void SplitKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat f0, f1, f2;
	args.data[0].ToUnifiedFormat(f0);
	args.data[1].ToUnifiedFormat(f1);
	args.data[2].ToUnifiedFormat(f2);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &child = ListVector::GetChildMutable(result);
	idx_t capacity = ListVector::GetListCapacity(result);
	idx_t offset = 0;
	string_t *child_data = nullptr;
	auto push = [&](const char *ptr, idx_t len) {
		if (offset >= capacity || !child_data) {
			ListVector::Reserve(result, MaxValue<idx_t>(offset + 1, capacity * 2 + 64));
			capacity = ListVector::GetListCapacity(result);
			child_data = FlatVector::GetDataMutable<string_t>(child);
		}
		child_data[offset++] = StringVector::AddString(child, ptr, len);
	};
	for (idx_t i = 0; i < count; i++) {
		auto i0 = f0.sel->get_index(i), i1 = f1.sel->get_index(i), i2 = f2.sel->get_index(i);
		if (!f0.validity.RowIsValid(i0) || !f1.validity.RowIsValid(i1) || !f2.validity.RowIsValid(i2)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		auto &sv = UnifiedVectorFormat::GetData<string_t>(f0)[i0];
		const char *str = sv.GetData();
		const idx_t size = sv.GetSize();
		auto pattern = UnifiedVectorFormat::GetData<string_t>(f1)[i1].GetString();
		auto limit = UnifiedVectorFormat::GetData<int32_t>(f2)[i2];
		const idx_t start_offset = offset;
		if (pattern.empty()) {
			// split into characters (code points, not bytes); Spark keeps a trailing empty string only with limit
			for (idx_t k = 0; k < size;) {
				if (limit > 0 && offset - start_offset + 1 == idx_t(limit)) {
					push(str + k, size - k);
					break;
				}
				idx_t next = k + 1;
				while (next < size && (static_cast<unsigned char>(str[next]) & 0xC0) == 0x80) {
					next++;
				}
				push(str + k, next - k);
				k = next;
			}
			if (size == 0) {
				push("", 0);
			}
		} else {
			idx_t last = 0;
			FindRegexMatches(pattern, str, size, [&](idx_t pos, idx_t len) {
				if (limit > 0 && offset - start_offset + 1 == idx_t(limit)) {
					return false;
				}
				if (!(len == 0 && pos == 0)) {
					push(str + last, pos - last);
					last = pos + len;
				}
				return true;
			});
			push(str + last, size - last);
		}
		entries[i] = list_entry_t(start_offset, offset - start_offset);
	}
	ListVector::SetListSize(result, offset);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! conv(num, from_base, to_base) following Spark's NumberConverter (unsigned 64-bit arithmetic)
string Conv(const string &input, int from_base, int to_base, bool &valid) {
	valid = true;
	if (from_base < 2 || from_base > 36 || std::abs(to_base) < 2 || std::abs(to_base) > 36) {
		valid = false;
		return "";
	}
	string s = input;
	while (!s.empty() && s[0] == ' ') {
		s = s.substr(1);
	}
	if (s.empty()) {
		valid = false; // Spark: NULL for an empty / blank number
		return "";
	}
	bool negative = !s.empty() && s[0] == '-';
	if (negative) {
		s = s.substr(1);
	}
	uint64_t value = 0;
	bool any = false;
	for (auto c : s) {
		int digit;
		if (c >= '0' && c <= '9') {
			digit = c - '0';
		} else if (c >= 'a' && c <= 'z') {
			digit = c - 'a' + 10;
		} else if (c >= 'A' && c <= 'Z') {
			digit = c - 'A' + 10;
		} else {
			break;
		}
		if (digit >= from_base) {
			break;
		}
		any = true;
		// overflow saturates to all ones like Spark
		if (value > (std::numeric_limits<uint64_t>::max() - digit) / uint64_t(from_base)) {
			value = std::numeric_limits<uint64_t>::max();
		} else {
			value = value * from_base + digit;
		}
	}
	if (!any) {
		return "0";
	}
	if (negative && to_base > 0) {
		value = uint64_t(-int64_t(value));
	}
	bool out_negative = false;
	if (to_base < 0) {
		auto signed_value = int64_t(value);
		if (negative) {
			signed_value = -signed_value;
		}
		if (signed_value < 0) {
			out_negative = true;
			value = uint64_t(-signed_value);
		} else {
			value = uint64_t(signed_value);
		}
		to_base = -to_base;
	}
	string out;
	do {
		int digit = int(value % uint64_t(to_base));
		out += char(digit < 10 ? '0' + digit : 'A' + digit - 10);
		value /= uint64_t(to_base);
	} while (value > 0);
	if (out_negative) {
		out += '-';
	}
	std::reverse(out.begin(), out.end());
	return out;
}

void ConvKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t s, int32_t from, int32_t to) -> optional<string_t> {
		    bool valid;
		    auto out = Conv(s.GetString(), from, to, valid);
		    if (!valid) {
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, out);
	    });
}

//===----------------------------------------------------------------------===//
// Bit shifts (Java semantics: the shift distance is taken modulo the width)
//===----------------------------------------------------------------------===//
template <class T, int KIND>
void ShiftKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<T, int32_t, T>(args.data[0], args.data[1], result, args.size(), [](T a, int32_t n) -> T {
		using U = typename std::make_unsigned<T>::type;
		int bits = sizeof(T) * 8;
		int shift = ((n % bits) + bits) % bits;
		if (KIND == 0) {
			return T(U(a) << shift);
		} else if (KIND == 1) {
			return T(a >> shift);
		}
		return T(U(a) >> shift);
	});
}

template <int KIND>
ScalarFunctionSet ShiftFunctions(const string &name) {
	ScalarFunctionSet set(Identifier {name});
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               ShiftKernel<int32_t, KIND>));
	set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::BIGINT, ShiftKernel<int64_t, KIND>));
	return set;
}

//===----------------------------------------------------------------------===//
// Math helpers with Java's exact results
//===----------------------------------------------------------------------===//
template <double (*FN)(double)>
void DoubleUnaryKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<double, double>(args.data[0], result, args.size(), [](double d) { return FN(d); });
}

double Expm1(double d) {
	return std::expm1(d);
}
double Log1p(double d) {
	return std::log1p(d);
}
//! Spark's formulas (not std::asinh / std::acosh): the results differ for large magnitudes
double SparkAsinh(double x) {
	if (x == -std::numeric_limits<double>::infinity()) {
		return x;
	}
	return std::log(x + std::sqrt(x * x + 1.0));
}
double SparkAcosh(double x) {
	return std::log(x + std::sqrt(x * x - 1.0));
}
double Rint(double d) {
	return std::nearbyint(d);
}

void HypotKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<double, double, double>(args.data[0], args.data[1], result, args.size(),
	                                                [](double a, double b) { return std::hypot(a, b); });
}

//===----------------------------------------------------------------------===//
// Hashes
//===----------------------------------------------------------------------===//
uint32_t Crc32(const string &data) {
	static uint32_t table[256];
	static bool init = false;
	if (!init) {
		for (uint32_t i = 0; i < 256; i++) {
			uint32_t c = i;
			for (int k = 0; k < 8; k++) {
				c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
			}
			table[i] = c;
		}
		init = true;
	}
	uint32_t crc = 0xFFFFFFFFu;
	for (auto ch : data) {
		crc = table[(crc ^ uint8_t(ch)) & 0xFF] ^ (crc >> 8);
	}
	return crc ^ 0xFFFFFFFFu;
}

void Crc32Kernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
	                                           [](string_t s) { return int64_t(Crc32(s.GetString())); });
}

// --- SHA-2 (224 / 256 / 384 / 512), FIPS 180-4 ---
struct Sha256 {
	static constexpr uint32_t K[64] = {
	    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
	static uint32_t Rotr(uint32_t x, int n) {
		return (x >> n) | (x << (32 - n));
	}
	static string Hash(const string &msg, bool is224) {
		uint32_t h[8];
		if (is224) {
			uint32_t init[8] = {0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939,
			                    0xffc00b31, 0x68581511, 0x64f98fa7, 0xbefa4fa4};
			memcpy(h, init, sizeof(h));
		} else {
			uint32_t init[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
			                    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
			memcpy(h, init, sizeof(h));
		}
		string data = msg;
		uint64_t bit_len = uint64_t(msg.size()) * 8;
		data += char(0x80);
		while (data.size() % 64 != 56) {
			data += char(0);
		}
		for (int i = 7; i >= 0; i--) {
			data += char((bit_len >> (i * 8)) & 0xFF);
		}
		for (idx_t chunk = 0; chunk < data.size(); chunk += 64) {
			uint32_t w[64];
			for (int i = 0; i < 16; i++) {
				w[i] = (uint32_t(uint8_t(data[chunk + i * 4])) << 24) | (uint32_t(uint8_t(data[chunk + i * 4 + 1])) << 16) |
				       (uint32_t(uint8_t(data[chunk + i * 4 + 2])) << 8) | uint32_t(uint8_t(data[chunk + i * 4 + 3]));
			}
			for (int i = 16; i < 64; i++) {
				uint32_t s0 = Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
				uint32_t s1 = Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
				w[i] = w[i - 16] + s0 + w[i - 7] + s1;
			}
			uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
			for (int i = 0; i < 64; i++) {
				uint32_t s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
				uint32_t ch = (e & f) ^ (~e & g);
				uint32_t t1 = hh + s1 + ch + K[i] + w[i];
				uint32_t s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
				uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
				uint32_t t2 = s0 + maj;
				hh = g;
				g = f;
				f = e;
				e = d + t1;
				d = c;
				c = b;
				b = a;
				a = t1 + t2;
			}
			h[0] += a;
			h[1] += b;
			h[2] += c;
			h[3] += d;
			h[4] += e;
			h[5] += f;
			h[6] += g;
			h[7] += hh;
		}
		string out;
		for (int i = 0; i < (is224 ? 7 : 8); i++) {
			static const char *HEX = "0123456789abcdef";
			for (int shift = 28; shift >= 0; shift -= 4) {
				out += HEX[(h[i] >> shift) & 0xF];
			}
		}
		return out;
	}
};
constexpr uint32_t Sha256::K[64];

struct Sha512 {
	static constexpr uint64_t K[80] = {
	    0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL,
	    0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL,
	    0xd807aa98a3030242ULL, 0x12835b0145706fbeULL, 0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
	    0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL, 0xc19bf174cf692694ULL,
	    0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
	    0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
	    0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL,
	    0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL, 0x06ca6351e003826fULL, 0x142929670a0e6e70ULL,
	    0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
	    0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
	    0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL,
	    0xd192e819d6ef5218ULL, 0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
	    0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL, 0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL,
	    0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL,
	    0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
	    0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL,
	    0xca273eceea26619cULL, 0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL,
	    0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL, 0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
	    0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL, 0x431d67c49c100d4cULL,
	    0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL};
	static uint64_t Rotr(uint64_t x, int n) {
		return (x >> n) | (x << (64 - n));
	}
	static string Hash(const string &msg, bool is384) {
		uint64_t h[8];
		if (is384) {
			uint64_t init[8] = {0xcbbb9d5dc1059ed8ULL, 0x629a292a367cd507ULL, 0x9159015a3070dd17ULL,
			                    0x152fecd8f70e5939ULL, 0x67332667ffc00b31ULL, 0x8eb44a8768581511ULL,
			                    0xdb0c2e0d64f98fa7ULL, 0x47b5481dbefa4fa4ULL};
			memcpy(h, init, sizeof(h));
		} else {
			uint64_t init[8] = {0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL, 0x3c6ef372fe94f82bULL,
			                    0xa54ff53a5f1d36f1ULL, 0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
			                    0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL};
			memcpy(h, init, sizeof(h));
		}
		string data = msg;
		uint64_t bit_len = uint64_t(msg.size()) * 8;
		data += char(0x80);
		while (data.size() % 128 != 112) {
			data += char(0);
		}
		for (int i = 0; i < 8; i++) {
			data += char(0);
		}
		for (int i = 7; i >= 0; i--) {
			data += char((bit_len >> (i * 8)) & 0xFF);
		}
		for (idx_t chunk = 0; chunk < data.size(); chunk += 128) {
			uint64_t w[80];
			for (int i = 0; i < 16; i++) {
				w[i] = 0;
				for (int k = 0; k < 8; k++) {
					w[i] = (w[i] << 8) | uint64_t(uint8_t(data[chunk + i * 8 + k]));
				}
			}
			for (int i = 16; i < 80; i++) {
				uint64_t s0 = Rotr(w[i - 15], 1) ^ Rotr(w[i - 15], 8) ^ (w[i - 15] >> 7);
				uint64_t s1 = Rotr(w[i - 2], 19) ^ Rotr(w[i - 2], 61) ^ (w[i - 2] >> 6);
				w[i] = w[i - 16] + s0 + w[i - 7] + s1;
			}
			uint64_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
			for (int i = 0; i < 80; i++) {
				uint64_t s1 = Rotr(e, 14) ^ Rotr(e, 18) ^ Rotr(e, 41);
				uint64_t ch = (e & f) ^ (~e & g);
				uint64_t t1 = hh + s1 + ch + K[i] + w[i];
				uint64_t s0 = Rotr(a, 28) ^ Rotr(a, 34) ^ Rotr(a, 39);
				uint64_t maj = (a & b) ^ (a & c) ^ (b & c);
				uint64_t t2 = s0 + maj;
				hh = g;
				g = f;
				f = e;
				e = d + t1;
				d = c;
				c = b;
				b = a;
				a = t1 + t2;
			}
			h[0] += a;
			h[1] += b;
			h[2] += c;
			h[3] += d;
			h[4] += e;
			h[5] += f;
			h[6] += g;
			h[7] += hh;
		}
		string out;
		for (int i = 0; i < (is384 ? 6 : 8); i++) {
			// by hand: StringUtil::Format sign-extends values with the top bit set
			static const char *HEX = "0123456789abcdef";
			for (int shift = 60; shift >= 0; shift -= 4) {
				out += HEX[(h[i] >> shift) & 0xF];
			}
		}
		return out;
	}
};
constexpr uint64_t Sha512::K[80];

void Sha2Kernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t s, int32_t bits) -> optional<string_t> {
		    auto msg = s.GetString();
		    string out;
		    switch (bits) {
		    case 0:
		    case 256:
			    out = Sha256::Hash(msg, false);
			    break;
		    case 224:
			    out = Sha256::Hash(msg, true);
			    break;
		    case 384:
			    out = Sha512::Hash(msg, true);
			    break;
		    case 512:
			    out = Sha512::Hash(msg, false);
			    break;
		    default:
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, out);
	    });
}

//===----------------------------------------------------------------------===//
// parse_url (java.net.URI)
//===----------------------------------------------------------------------===//
struct ParsedUrl {
	bool valid = false;
	string protocol, authority, userinfo, host, path, query, ref;
	bool has_query = false, has_ref = false, has_authority = false, has_userinfo = false;
};

ParsedUrl ParseUrl(const string &url) {
	ParsedUrl result;
	auto scheme_end = url.find(':');
	if (scheme_end == string::npos || scheme_end == 0) {
		return result;
	}
	for (idx_t i = 0; i < scheme_end; i++) {
		char c = url[i];
		if (!(StringUtil::CharacterIsAlphaNumeric(c) || c == '+' || c == '-' || c == '.')) {
			return result;
		}
	}
	for (auto c : url) {
		if (c == ' ') {
			return result;
		}
	}
	result.protocol = url.substr(0, scheme_end);
	string rest = url.substr(scheme_end + 1);
	auto hash = rest.find('#');
	if (hash != string::npos) {
		result.ref = rest.substr(hash + 1);
		result.has_ref = true;
		rest = rest.substr(0, hash);
	}
	if (StringUtil::StartsWith(rest, "//")) {
		rest = rest.substr(2);
		auto end = rest.find_first_of("/?");
		result.authority = rest.substr(0, end);
		result.has_authority = true;
		rest = end == string::npos ? "" : rest.substr(end);
		auto at = result.authority.find('@');
		string hostport = result.authority;
		if (at != string::npos) {
			result.userinfo = result.authority.substr(0, at);
			result.has_userinfo = true;
			hostport = result.authority.substr(at + 1);
		}
		auto colon = hostport.rfind(':');
		result.host = colon == string::npos ? hostport : hostport.substr(0, colon);
	}
	auto q = rest.find('?');
	if (q != string::npos) {
		result.query = rest.substr(q + 1);
		result.has_query = true;
		rest = rest.substr(0, q);
	}
	result.path = rest;
	result.valid = true;
	return result;
}

bool UrlPart(const string &url, const string &part, const string *key, string &out) {
	auto parsed = ParseUrl(url);
	if (!parsed.valid) {
		return false;
	}
	if (part == "HOST") {
		if (!parsed.has_authority || parsed.host.empty()) {
			return false;
		}
		out = parsed.host;
		return true;
	}
	if (part == "PATH") {
		out = parsed.path;
		return true;
	}
	if (part == "QUERY") {
		if (!parsed.has_query) {
			return false;
		}
		if (!key) {
			out = parsed.query;
			return true;
		}
		std::regex re("(&|^)" + std::regex_replace(*key, std::regex(R"([.^$|()\[\]{}*+?\\])"), R"(\$&)") +
		              "=([^&]*)");
		std::smatch match;
		if (std::regex_search(parsed.query, match, re)) {
			out = match[2];
			return true;
		}
		return false;
	}
	if (part == "REF") {
		if (!parsed.has_ref) {
			return false;
		}
		out = parsed.ref;
		return true;
	}
	if (part == "PROTOCOL") {
		out = parsed.protocol;
		return true;
	}
	if (part == "FILE") {
		out = parsed.path + (parsed.has_query ? "?" + parsed.query : "");
		return true;
	}
	if (part == "AUTHORITY") {
		if (!parsed.has_authority) {
			return false;
		}
		out = parsed.authority;
		return true;
	}
	if (part == "USERINFO") {
		if (!parsed.has_userinfo) {
			return false;
		}
		out = parsed.userinfo;
		return true;
	}
	return false;
}

void ParseUrlKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t url, string_t part) -> optional<string_t> {
		    string out;
		    if (!UrlPart(url.GetString(), part.GetString(), nullptr, out)) {
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, out);
	    });
}

void ParseUrlKeyKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t url, string_t part, string_t key) -> optional<string_t> {
		    string out;
		    auto k = key.GetString();
		    if (!UrlPart(url.GetString(), part.GetString(), &k, out)) {
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, out);
	    });
}

//===----------------------------------------------------------------------===//
// format_number(x, d): '#,###,###.##' with HALF_EVEN rounding (java.text.DecimalFormat)
//===----------------------------------------------------------------------===//
string GroupThousands(const string &digits) {
	string out;
	int count = 0;
	for (idx_t i = digits.size(); i > 0; i--) {
		out += digits[i - 1];
		if (++count % 3 == 0 && i > 1) {
			out += ',';
		}
	}
	std::reverse(out.begin(), out.end());
	return out;
}

//! value given as an exact decimal string: [-]digits[.digits]
string FormatNumberString(const string &value, int32_t d) {
	bool negative = !value.empty() && value[0] == '-';
	string v = negative ? value.substr(1) : value;
	auto point = v.find('.');
	string int_part = point == string::npos ? v : v.substr(0, point);
	string frac = point == string::npos ? "" : v.substr(point + 1);
	// round HALF_EVEN to d digits
	if (frac.size() > idx_t(d)) {
		string keep = frac.substr(0, d);
		string rest = frac.substr(d);
		bool round_up;
		char first = rest[0];
		bool rest_nonzero = rest.find_first_not_of('0', 1) != string::npos;
		if (first > '5' || (first == '5' && rest_nonzero)) {
			round_up = true;
		} else if (first == '5') {
			char last = d > 0 ? keep.back() : (int_part.empty() ? '0' : int_part.back());
			round_up = (last - '0') % 2 == 1;
		} else {
			round_up = false;
		}
		string all = int_part + keep;
		if (round_up) {
			int k = int(all.size()) - 1;
			while (k >= 0) {
				if (all[k] == '9') {
					all[k] = '0';
					k--;
				} else {
					all[k]++;
					break;
				}
			}
			if (k < 0) {
				all = "1" + all;
			}
		}
		int_part = all.substr(0, all.size() - d);
		frac = all.substr(all.size() - d);
	}
	while (frac.size() < idx_t(d)) {
		frac += '0';
	}
	auto first_nonzero = int_part.find_first_not_of('0');
	int_part = first_nonzero == string::npos ? "0" : int_part.substr(first_nonzero);
	string out = GroupThousands(int_part);
	if (d > 0) {
		out += "." + frac;
	}
	bool is_zero = out.find_first_not_of("0.,") == string::npos;
	return (negative && !is_zero) ? "-" + out : out;
}

void FormatNumberKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t value, int32_t d) -> optional<string_t> {
		    if (d < 0) {
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, FormatNumberString(value.GetString(), d));
	    });
}

//! format_number(x, pattern): '#,##0.00' style java.text.DecimalFormat patterns
void FormatNumberPatternKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t value, string_t pattern_p) {
		    auto pattern = pattern_p.GetString();
		    auto point = pattern.find('.');
		    string frac_pattern = point == string::npos ? "" : pattern.substr(point + 1);
		    int32_t max_frac = 0, min_frac = 0;
		    for (auto c : frac_pattern) {
			    if (c == '0' || c == '#') {
				    max_frac++;
			    }
			    if (c == '0') {
				    min_frac++;
			    }
		    }
		    auto out = FormatNumberString(value.GetString(), max_frac);
		    if (pattern.find(',') == string::npos) {
			    out = StringUtil::Replace(out, ",", "");
		    }
		    if (max_frac > min_frac && out.find('.') != string::npos) {
			    idx_t keep = out.find('.') + 1 + min_frac;
			    while (out.size() > keep && out.back() == '0') {
				    out.pop_back();
			    }
			    if (out.back() == '.') {
				    out.pop_back();
			    }
		    }
		    return StringVector::AddString(result, out);
	    });
}

//! exact decimal text of a double (what java.math.BigDecimal(double) would produce, shortest form is enough for
//! the HALF_EVEN rounding of format_number at typical precisions)
void DoubleToDecimalStringKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<double, string_t>(args.data[0], result, args.size(), [&](double d) {
		char buffer[512];
		snprintf(buffer, sizeof(buffer), "%.40f", d);
		return StringVector::AddString(result, buffer);
	});
}

} // namespace

void RegisterSparkStringKernels(ExtensionLoader &loader);
void CheckLikePatternKernel(DataChunk &args, ExpressionState &state, Vector &result);

//! substring_index(s, delim, count) with Spark's (UTF8String.subStringIndex) search rules
string SubstringIndex(const string &str, const string &delim, int64_t count) {
	if (delim.empty() || count == 0) {
		return "";
	}
	if (count > 0) {
		int64_t idx = -1;
		for (int64_t n = 0; n < count; n++) {
			auto found = str.find(delim, size_t(idx + 1));
			if (found == string::npos) {
				return str;
			}
			idx = int64_t(found);
		}
		return str.substr(0, size_t(idx));
	}
	int64_t idx = int64_t(str.size()) - int64_t(delim.size()) + 1;
	for (int64_t n = 0; n < -count; n++) {
		if (idx - 1 < 0) {
			return str;
		}
		auto found = str.rfind(delim, size_t(idx - 1));
		if (found == string::npos) {
			return str;
		}
		idx = int64_t(found);
	}
	return str.substr(size_t(idx) + delim.size());
}

void SubstringIndexKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<string_t, string_t, int64_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](string_t s, string_t d, int64_t c) {
		    return StringVector::AddString(result, SubstringIndex(s.GetString(), d.GetString(), c));
	    });
}

//! array_join(strings, sep[, null_replacement]): NULL elements are skipped (or replaced); the separator need not
//! be constant (DuckDB's array_to_string requires that)
template <bool NULL_IF_EMPTY>
void ArrayJoinKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	bool has_replacement = args.ColumnCount() > 2;
	UnifiedVectorFormat lists, seps, reps;
	args.data[0].ToUnifiedFormat(lists);
	args.data[1].ToUnifiedFormat(seps);
	if (has_replacement) {
		args.data[2].ToUnifiedFormat(reps);
	}
	auto &child = ListVector::GetChild(args.data[0]);
	UnifiedVectorFormat elems;
	child.ToUnifiedFormat(ListVector::GetListSize(args.data[0]), elems);
	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(lists);
	auto sep_data = UnifiedVectorFormat::GetData<string_t>(seps);
	auto elem_data = UnifiedVectorFormat::GetData<string_t>(elems);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	auto &validity = FlatVector::ValidityMutable(result);
	for (idx_t i = 0; i < count; i++) {
		auto li = lists.sel->get_index(i);
		auto si = seps.sel->get_index(i);
		idx_t ri = has_replacement ? reps.sel->get_index(i) : 0;
		if (!lists.validity.RowIsValid(li) || !seps.validity.RowIsValid(si) ||
		    (has_replacement && !reps.validity.RowIsValid(ri))) {
			validity.SetInvalid(i);
			continue;
		}
		auto sep = sep_data[si].GetString();
		string joined;
		bool first = true;
		auto entry = list_data[li];
		for (idx_t k = entry.offset; k < entry.offset + entry.length; k++) {
			auto ei = elems.sel->get_index(k);
			string value;
			if (elems.validity.RowIsValid(ei)) {
				value = elem_data[ei].GetString();
			} else if (has_replacement) {
				value = UnifiedVectorFormat::GetData<string_t>(reps)[ri].GetString();
			} else {
				continue;
			}
			if (!first) {
				joined += sep;
			}
			joined += value;
			first = false;
		}
		if (NULL_IF_EMPTY && first) {
			validity.SetInvalid(i); // concat_ws skips an array without values, like a NULL argument
			continue;
		}
		out[i] = StringVector::AddString(result, joined);
	}
}

//! url_encode with java.net.URLEncoder rules: [A-Za-z0-9.*_-] unchanged, ' ' -> '+', everything else %XX
void UrlEncodeKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t s) {
		static const char *HEX = "0123456789ABCDEF";
		string out;
		for (auto c : s.GetString()) {
			auto u = static_cast<unsigned char>(c);
			if (isalnum(u) || c == '.' || c == '*' || c == '_' || c == '-') {
				out += c;
			} else if (c == ' ') {
				out += '+';
			} else {
				out += '%';
				out += HEX[u >> 4];
				out += HEX[u & 15];
			}
		}
		return StringVector::AddString(result, out);
	});
}

void RegisterSparkLibrary(ExtensionLoader &loader) {
	auto reg = [&](ScalarFunction fn) {
		RegisterFallible(loader, std::move(fn));
	};
	RegisterSparkStringKernels(loader);
	reg(ScalarFunction(Identifier("check_like_pattern"), {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   CheckLikePatternKernel));
	reg(ScalarFunction(Identifier("initcap"), {LogicalType::VARCHAR}, LogicalType::VARCHAR, InitCapKernel));
	reg(ScalarFunction(Identifier("soundex"), {LogicalType::VARCHAR}, LogicalType::VARCHAR, SoundexKernel));
	reg(ScalarFunction(Identifier("luhn_check"), {LogicalType::VARCHAR}, LogicalType::BOOLEAN, LuhnKernel));
	{
		ScalarFunction mask(Identifier("mask_impl"),
		                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                     LogicalType::VARCHAR},
		                    LogicalType::VARCHAR, MaskKernel);
		mask.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		reg(mask);
	}
	reg(ScalarFunction(Identifier("split_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::VARCHAR), SplitKernel));
	reg(ScalarFunction(Identifier("conv_impl"), {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER},
	                   LogicalType::VARCHAR, ConvKernel));
	loader.RegisterFunction(ShiftFunctions<0>("shiftleft"));
	loader.RegisterFunction(ShiftFunctions<1>("shiftright"));
	loader.RegisterFunction(ShiftFunctions<2>("shiftrightunsigned"));
	reg(ScalarFunction(Identifier("expm1"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, DoubleUnaryKernel<Expm1>));
	reg(ScalarFunction(Identifier("log1p_impl"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, DoubleUnaryKernel<Log1p>));
	reg(ScalarFunction(Identifier("rint"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, DoubleUnaryKernel<Rint>));
	reg(ScalarFunction(Identifier("asinh"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, DoubleUnaryKernel<SparkAsinh>));
	reg(ScalarFunction(Identifier("acosh"), {LogicalType::DOUBLE}, LogicalType::DOUBLE, DoubleUnaryKernel<SparkAcosh>));
	reg(ScalarFunction(Identifier("substring_index_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                   LogicalType::VARCHAR, SubstringIndexKernel));
	{
		ScalarFunction join(Identifier("array_join_impl"), {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
		                    LogicalType::VARCHAR, ArrayJoinKernel<false>);
		join.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		reg(join);
		ScalarFunction join3(Identifier("array_join_impl"),
		                     {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR, LogicalType::VARCHAR},
		                     LogicalType::VARCHAR, ArrayJoinKernel<false>);
		join3.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		reg(join3);
		ScalarFunction ws(Identifier("concat_ws_array"), {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
		                  LogicalType::VARCHAR, ArrayJoinKernel<true>);
		ws.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		reg(ws);
	}
	reg(ScalarFunction(Identifier("url_encode"), {LogicalType::VARCHAR}, LogicalType::VARCHAR, UrlEncodeKernel));
	reg(ScalarFunction(Identifier("levenshtein_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::INTEGER, LevenshteinKernel));
	reg(ScalarFunction(Identifier("hypot"), {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                   HypotKernel));
	reg(ScalarFunction(Identifier("crc32_impl"), {LogicalType::VARCHAR}, LogicalType::BIGINT, Crc32Kernel));
	reg(ScalarFunction(Identifier("crc32_impl"), {LogicalType::BLOB}, LogicalType::BIGINT, Crc32Kernel));
	reg(ScalarFunction(Identifier("sha2_impl"), {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR,
	                   Sha2Kernel));
	reg(ScalarFunction(Identifier("sha2_impl"), {LogicalType::BLOB, LogicalType::INTEGER}, LogicalType::VARCHAR,
	                   Sha2Kernel));
	reg(ScalarFunction(Identifier("parse_url_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   ParseUrlKernel));
	reg(ScalarFunction(Identifier("parse_url_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, ParseUrlKeyKernel));
	reg(ScalarFunction(Identifier("format_number_impl"), {LogicalType::VARCHAR, LogicalType::INTEGER},
	                   LogicalType::VARCHAR, FormatNumberKernel));
	reg(ScalarFunction(Identifier("format_number_impl"), {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, FormatNumberPatternKernel));
	reg(ScalarFunction(Identifier("double_to_decimal_string"), {LogicalType::DOUBLE}, LogicalType::VARCHAR,
	                   DoubleToDecimalStringKernel));
	for (auto name : {"initcap", "soundex", "luhn_check", "shiftleft", "shiftright", "shiftrightunsigned", "expm1",
	                  "rint", "hypot", "asinh", "acosh", "url_encode"}) {
		AddSparkOverride(name);
	}
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
// hash (Murmur3_x86_32, seed 42) and xxhash64 (seed 42) with Spark's value encodings
//===----------------------------------------------------------------------===//
namespace duckdb {
namespace {

struct Murmur3 {
	static uint32_t Rotl(uint32_t x, int r) {
		return (x << r) | (x >> (32 - r));
	}
	static uint32_t MixK1(uint32_t k1) {
		k1 *= 0xcc9e2d51u;
		k1 = Rotl(k1, 15);
		return k1 * 0x1b873593u;
	}
	static uint32_t MixH1(uint32_t h1, uint32_t k1) {
		h1 ^= k1;
		h1 = Rotl(h1, 13);
		return h1 * 5 + 0xe6546b64u;
	}
	static uint32_t Fmix(uint32_t h1, uint32_t length) {
		h1 ^= length;
		h1 ^= h1 >> 16;
		h1 *= 0x85ebca6bu;
		h1 ^= h1 >> 13;
		h1 *= 0xc2b2ae35u;
		h1 ^= h1 >> 16;
		return h1;
	}
	static uint32_t HashInt(int32_t input, uint32_t seed) {
		return Fmix(MixH1(seed, MixK1(uint32_t(input))), 4);
	}
	static uint32_t HashLong(int64_t input, uint32_t seed) {
		uint32_t low = uint32_t(uint64_t(input));
		uint32_t high = uint32_t(uint64_t(input) >> 32);
		uint32_t h1 = MixH1(seed, MixK1(low));
		h1 = MixH1(h1, MixK1(high));
		return Fmix(h1, 8);
	}
	//! Spark's legacy hashUnsafeBytes: trailing bytes are mixed one (sign-extended) byte at a time
	static uint32_t HashBytes(const string &bytes, uint32_t seed) {
		idx_t aligned = bytes.size() - bytes.size() % 4;
		uint32_t h1 = seed;
		for (idx_t i = 0; i < aligned; i += 4) {
			uint32_t word = uint32_t(uint8_t(bytes[i])) | (uint32_t(uint8_t(bytes[i + 1])) << 8) |
			                (uint32_t(uint8_t(bytes[i + 2])) << 16) | (uint32_t(uint8_t(bytes[i + 3])) << 24);
			h1 = MixH1(h1, MixK1(word));
		}
		for (idx_t i = aligned; i < bytes.size(); i++) {
			h1 = MixH1(h1, MixK1(uint32_t(int32_t(int8_t(bytes[i])))));
		}
		return Fmix(h1, uint32_t(bytes.size()));
	}
};

struct XXH64 {
	static constexpr uint64_t P1 = 0x9E3779B185EBCA87ULL;
	static constexpr uint64_t P2 = 0xC2B2AE3D27D4EB4FULL;
	static constexpr uint64_t P3 = 0x165667B19E3779F9ULL;
	static constexpr uint64_t P4 = 0x85EBCA77C2B2AE63ULL;
	static constexpr uint64_t P5 = 0x27D4EB2F165667C5ULL;
	static uint64_t Rotl(uint64_t x, int r) {
		return (x << r) | (x >> (64 - r));
	}
	static uint64_t Fmix(uint64_t h) {
		h ^= h >> 33;
		h *= P2;
		h ^= h >> 29;
		h *= P3;
		h ^= h >> 32;
		return h;
	}
	static uint64_t HashInt(int32_t input, uint64_t seed) {
		uint64_t hash = seed + P5 + 4;
		hash ^= uint64_t(uint32_t(input)) * P1;
		hash = Rotl(hash, 23) * P2 + P3;
		return Fmix(hash);
	}
	static uint64_t HashLong(int64_t input, uint64_t seed) {
		uint64_t hash = seed + P5 + 8;
		hash ^= Rotl(uint64_t(input) * P2, 31) * P1;
		hash = Rotl(hash, 27) * P1 + P4;
		return Fmix(hash);
	}
	static uint64_t Read64(const string &b, idx_t i) {
		uint64_t v = 0;
		for (int k = 7; k >= 0; k--) {
			v = (v << 8) | uint8_t(b[i + k]);
		}
		return v;
	}
	static uint32_t Read32(const string &b, idx_t i) {
		uint32_t v = 0;
		for (int k = 3; k >= 0; k--) {
			v = (v << 8) | uint8_t(b[i + k]);
		}
		return v;
	}
	static uint64_t HashBytes(const string &b, uint64_t seed) {
		idx_t len = b.size(), i = 0;
		uint64_t hash;
		if (len >= 32) {
			uint64_t v1 = seed + P1 + P2, v2 = seed + P2, v3 = seed, v4 = seed - P1;
			for (; i + 32 <= len; i += 32) {
				v1 = Rotl(v1 + Read64(b, i) * P2, 31) * P1;
				v2 = Rotl(v2 + Read64(b, i + 8) * P2, 31) * P1;
				v3 = Rotl(v3 + Read64(b, i + 16) * P2, 31) * P1;
				v4 = Rotl(v4 + Read64(b, i + 24) * P2, 31) * P1;
			}
			hash = Rotl(v1, 1) + Rotl(v2, 7) + Rotl(v3, 12) + Rotl(v4, 18);
			for (auto v : {v1, v2, v3, v4}) {
				v *= P2;
				v = Rotl(v, 31);
				v *= P1;
				hash ^= v;
				hash = hash * P1 + P4;
			}
		} else {
			hash = seed + P5;
		}
		hash += len;
		for (; i + 8 <= len; i += 8) {
			uint64_t k1 = Rotl(Read64(b, i) * P2, 31) * P1;
			hash ^= k1;
			hash = Rotl(hash, 27) * P1 + P4;
		}
		if (i + 4 <= len) {
			hash ^= uint64_t(Read32(b, i)) * P1;
			hash = Rotl(hash, 23) * P2 + P3;
			i += 4;
		}
		for (; i < len; i++) {
			hash ^= uint64_t(uint8_t(b[i])) * P5;
			hash = Rotl(hash, 11) * P1;
		}
		return Fmix(hash);
	}
};

template <class H, class T>
T HashValue(const Value &v, T seed) {
	if (v.IsNull()) {
		return seed;
	}
	auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return H::HashInt(v.GetValue<bool>() ? 1 : 0, seed);
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::INTEGER_LITERAL:
		return H::HashInt(v.GetValue<int32_t>(), seed);
	case LogicalTypeId::DATE:
		return H::HashInt(v.GetValue<date_t>().days, seed);
	case LogicalTypeId::BIGINT:
		return H::HashLong(v.GetValue<int64_t>(), seed);
	case LogicalTypeId::HUGEINT:
		return H::HashLong(v.GetValue<int64_t>(), seed);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return H::HashLong(v.GetValue<timestamp_t>().value, seed);
	case LogicalTypeId::FLOAT: {
		float f = v.GetValue<float>();
		if (f == 0) {
			f = 0;
		}
		int32_t bits;
		if (std::isnan(f)) {
			bits = 0x7fc00000;
		} else {
			memcpy(&bits, &f, 4);
		}
		return H::HashInt(bits, seed);
	}
	case LogicalTypeId::DOUBLE: {
		double d = v.GetValue<double>();
		if (d == 0) {
			d = 0;
		}
		int64_t bits;
		if (std::isnan(d)) {
			bits = 0x7ff8000000000000LL;
		} else {
			memcpy(&bits, &d, 8);
		}
		return H::HashLong(bits, seed);
	}
	case LogicalTypeId::DECIMAL: {
		// unscaled value: the decimal digits without the point
		auto text = v.ToString();
		string digits;
		for (auto c : text) {
			if (c != '.') {
				digits += c;
			}
		}
		return H::HashLong(std::stoll(digits), seed);
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return H::HashBytes(StringValue::Get(v), seed);
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		T h = seed;
		for (auto &child : ListValue::GetChildren(v)) {
			h = HashValue<H, T>(child, h);
		}
		return h;
	}
	case LogicalTypeId::MAP: {
		T h = seed;
		for (auto &entry : ListValue::GetChildren(v)) {
			auto &kv = StructValue::GetChildren(entry);
			h = HashValue<H, T>(kv[0], h);
			h = HashValue<H, T>(kv[1], h);
		}
		return h;
	}
	case LogicalTypeId::STRUCT: {
		T h = seed;
		for (auto &child : StructValue::GetChildren(v)) {
			h = HashValue<H, T>(child, h);
		}
		return h;
	}
	default:
		return H::HashBytes(v.ToString(), seed);
	}
}

template <class H, class T, class RESULT>
void SparkHashKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<RESULT>(result);
	for (idx_t i = 0; i < count; i++) {
		T h = 42;
		for (idx_t c = 0; c < args.ColumnCount(); c++) {
			h = HashValue<H, T>(args.data[c].GetValue(i), h);
		}
		out[i] = RESULT(h);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

//===----------------------------------------------------------------------===//
// substr / lpad / rpad on strings (characters) and binary values (bytes)
//===----------------------------------------------------------------------===//
namespace {

//! the units of a value: UTF-8 characters for strings, bytes for binary
vector<string> Units(const string &s, bool binary) {
	vector<string> units;
	for (idx_t i = 0; i < s.size();) {
		idx_t len = 1;
		if (!binary) {
			auto c = uint8_t(s[i]);
			len = c < 0x80 ? 1 : (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : 4;
		}
		units.push_back(s.substr(i, len));
		i += len;
	}
	return units;
}

string SparkPad(const string &s, int64_t length, const string &pad, bool left, bool binary) {
	auto units = Units(s, binary);
	if (length <= 0) {
		return "";
	}
	if (int64_t(units.size()) >= length) {
		string out;
		for (int64_t i = 0; i < length; i++) {
			out += units[i];
		}
		return out;
	}
	auto pad_units = Units(pad, binary);
	if (pad_units.empty()) {
		return s;
	}
	string fill;
	for (int64_t i = 0; i < length - int64_t(units.size()); i++) {
		fill += pad_units[i % pad_units.size()];
	}
	return left ? fill + s : s + fill;
}

//! Byte offset of code point `index` (UTF-8 lead bytes are those that are not 10xxxxxx)
idx_t CodePointOffset(const char *data, idx_t size, int64_t index) {
	idx_t offset = 0;
	int64_t seen = 0;
	for (; offset < size; offset++) {
		if ((static_cast<unsigned char>(data[offset]) & 0xC0) != 0x80) {
			if (seen == index) {
				return offset;
			}
			seen++;
		}
	}
	return size;
}

//! UTF8String.substringSQL without copying characters: the result is a byte range of the input
template <bool BINARY>
void SubstrKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](string_t s, int64_t pos, int64_t len) {
		    auto data = s.GetData();
		    auto size = s.GetSize();
		    constexpr int64_t SAFE = int64_t(1) << 62;
		    if (pos >= 1 && pos <= SAFE && len >= 0 && len <= SAFE) {
			    // common case: positive start, bounded length. Only the prefix up to the end position is scanned,
			    // and no 128-bit arithmetic is needed (the clamping to the string is done by the offset search)
			    int64_t start = pos - 1;
			    idx_t from, to;
			    if (BINARY) {
				    from = idx_t(MinValue<int64_t>(start, int64_t(size)));
				    to = idx_t(MinValue<int64_t>(start + len, int64_t(size)));
			    } else {
				    from = CodePointOffset(data, size, start);
				    to = from + CodePointOffset(data + from, size - from, len);
			    }
			    return StringVector::AddStringOrBlob(result, string_t(data + from, UnsafeNumericCast<uint32_t>(to - from)));
		    }
		    bool bytes = BINARY;
		    int64_t n = int64_t(size);
		    if (!BINARY) {
			    // code points; ASCII strings (the common case) have one byte per code point
			    int64_t continuation = 0;
			    for (idx_t i = 0; i < size; i++) {
				    continuation += (static_cast<unsigned char>(data[i]) & 0xC0) == 0x80;
			    }
			    n = int64_t(size) - continuation;
			    bytes = continuation == 0;
		    }
		    // 128-bit arithmetic: positions and lengths can be anywhere in the BIGINT range
		    hugeint_t hstart = pos > 0 ? hugeint_t(pos) - hugeint_t(1) : (pos < 0 ? hugeint_t(n) + hugeint_t(pos) : hugeint_t(0));
		    hugeint_t hend = len == NumericLimits<int64_t>::Maximum() ? hugeint_t(n) : hstart + hugeint_t(len);
		    int64_t start = hstart < hugeint_t(0) ? 0 : (hstart > hugeint_t(n) ? n : Hugeint::Cast<int64_t>(hstart));
		    int64_t end = hend < hugeint_t(0) ? 0 : (hend > hugeint_t(n) ? n : Hugeint::Cast<int64_t>(hend));
		    if (end <= start) {
			    return StringVector::AddStringOrBlob(result, string_t(data, 0));
		    }
		    idx_t from = bytes ? idx_t(start) : CodePointOffset(data, size, start);
		    idx_t to = bytes ? idx_t(end) : CodePointOffset(data, size, end);
		    return StringVector::AddStringOrBlob(result, string_t(data + from, UnsafeNumericCast<uint32_t>(to - from)));
	    });
}

template <bool BINARY, bool LEFT>
void PadKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<string_t, int64_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](string_t s, int64_t len, string_t pad) {
		    return StringVector::AddStringOrBlob(result, SparkPad(s.GetString(), len, pad.GetString(), LEFT, BINARY));
	    });
}

} // namespace

void RegisterSparkStringKernels(ExtensionLoader &loader) {
	auto big = LogicalType::BIGINT;
	ScalarFunctionSet substr(Identifier("substr_impl"));
	substr.AddFunction(ScalarFunction({LogicalType::VARCHAR, big, big}, LogicalType::VARCHAR, SubstrKernel<false>));
	substr.AddFunction(ScalarFunction({LogicalType::BLOB, big, big}, LogicalType::BLOB, SubstrKernel<true>));
	loader.RegisterFunction(substr);
	ScalarFunctionSet lpad(Identifier("lpad_impl"));
	lpad.AddFunction(ScalarFunction({LogicalType::VARCHAR, big, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                PadKernel<false, true>));
	lpad.AddFunction(
	    ScalarFunction({LogicalType::BLOB, big, LogicalType::BLOB}, LogicalType::BLOB, PadKernel<true, true>));
	loader.RegisterFunction(lpad);
	ScalarFunctionSet rpad(Identifier("rpad_impl"));
	rpad.AddFunction(ScalarFunction({LogicalType::VARCHAR, big, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                PadKernel<false, false>));
	rpad.AddFunction(
	    ScalarFunction({LogicalType::BLOB, big, LogicalType::BLOB}, LogicalType::BLOB, PadKernel<true, false>));
	loader.RegisterFunction(rpad);
}

//! Spark rejects LIKE patterns where the escape character precedes anything but _, % or itself
void CheckLikePatternKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t p) {
		auto pattern = p.GetString();
		for (idx_t i = 0; i < pattern.size(); i++) {
			if (pattern[i] != '\\') {
				continue;
			}
			if (i + 1 >= pattern.size()) {
				throw InvalidInputException("[INVALID_FORMAT.ESC_AT_THE_END] The format is invalid: '%s'. The escape "
				                            "character is not allowed to end with.",
				                            pattern);
			}
			char next = pattern[i + 1];
			if (next != '_' && next != '%' && next != '\\') {
				throw InvalidInputException("[INVALID_FORMAT.ESC_IN_THE_MIDDLE] The format is invalid: '%s'. The escape "
				                            "character is not allowed to precede '%c'.",
				                            pattern, next);
			}
			i++;
		}
		return StringVector::AddString(result, pattern);
	});
}

uint32_t Murmur3Bytes(const string &bytes, uint32_t seed) {
	return Murmur3::HashBytes(bytes, seed);
}

void RegisterSparkHashes(ExtensionLoader &loader) {
	ScalarFunction hash(Identifier("hash"), {}, LogicalType::INTEGER, SparkHashKernel<Murmur3, uint32_t, int32_t>,
	                    nullptr, nullptr, nullptr, LogicalType::ANY);
	hash.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(hash);
	ScalarFunction xxhash(Identifier("xxhash64"), {}, LogicalType::BIGINT, SparkHashKernel<XXH64, uint64_t, int64_t>,
	                      nullptr, nullptr, nullptr, LogicalType::ANY);
	xxhash.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(xxhash);
	AddSparkOverride("hash");
	AddSparkOverride("xxhash64");
}

} // namespace duckdb
