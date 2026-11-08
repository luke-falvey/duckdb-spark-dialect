// format_string / printf with java.util.Formatter semantics (system.spark.format_string_impl):
// NULL arguments print as "null", %f / %e / %g round the shortest decimal form HALF_UP like Java, %x of a negative
// number prints its two's complement in the width of the argument's type, %b prints false for NULL.

#include "spark_dialect.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>
#include <cstdlib>

namespace duckdb {

namespace {

//! a finite decimal number: 0.d1d2d3... written as digits with the scientific exponent of d1
struct Number {
	bool negative = false;
	string digits = "0";
	int exponent = 0;
};

void StripDigits(Number &n) {
	idx_t lead = 0;
	while (lead + 1 < n.digits.size() && n.digits[lead] == '0') {
		lead++;
		n.exponent--;
	}
	n.digits = n.digits.substr(lead);
	while (n.digits.size() > 1 && n.digits.back() == '0') {
		n.digits.pop_back();
	}
	if (n.digits == "0") {
		n.exponent = 0;
	}
}

//! the shortest digits that read back as the same double (what Double.toString / the Formatter work from)
Number NumberFromDouble(double d, bool is_float) {
	Number n;
	n.negative = std::signbit(d);
	d = std::fabs(d);
	if (d == 0) {
		return n;
	}
	char buffer[64];
	for (int precision = 1; precision <= 17; precision++) {
		snprintf(buffer, sizeof(buffer), "%.*e", precision - 1, d);
		bool same = is_float ? float(strtod(buffer, nullptr)) == float(d) : strtod(buffer, nullptr) == d;
		if (same || precision == 17) {
			break;
		}
	}
	string text = buffer;
	auto e = text.find('e');
	n.exponent = atoi(text.c_str() + e + 1);
	n.digits.clear();
	for (idx_t i = 0; i < e; i++) {
		if (text[i] != '.') {
			n.digits += text[i];
		}
	}
	StripDigits(n);
	return n;
}

//! from the plain text of a decimal ("-123.450")
Number NumberFromDecimal(const string &text) {
	Number n;
	idx_t pos = 0;
	if (pos < text.size() && (text[pos] == '-' || text[pos] == '+')) {
		n.negative = text[pos] == '-';
		pos++;
	}
	string integral, fraction;
	bool seen_point = false;
	for (; pos < text.size(); pos++) {
		if (text[pos] == '.') {
			seen_point = true;
		} else {
			(seen_point ? fraction : integral) += text[pos];
		}
	}
	n.digits = integral + fraction;
	n.exponent = int(integral.size()) - 1;
	StripDigits(n);
	return n;
}

//! "999" + 1 -> "1000"
string IncrementDigits(string digits) {
	for (idx_t i = digits.size(); i-- > 0;) {
		if (digits[i] == '9') {
			digits[i] = '0';
		} else {
			digits[i]++;
			return digits;
		}
	}
	return "1" + digits;
}

//! integral and fractional digits, the fraction rounded HALF_UP to `precision` digits
void FixedParts(const Number &n, int precision, string &integral, string &fraction) {
	if (n.exponent >= 0) {
		string digits = n.digits;
		while (int(digits.size()) < n.exponent + 1) {
			digits += '0';
		}
		integral = digits.substr(0, n.exponent + 1);
		fraction = digits.substr(n.exponent + 1);
	} else {
		integral = "0";
		fraction = string(idx_t(-n.exponent - 1), '0') + n.digits;
	}
	if (int(fraction.size()) > precision) {
		bool round_up = fraction[precision] >= '5';
		fraction = fraction.substr(0, idx_t(precision));
		if (round_up) {
			auto all = IncrementDigits(integral + fraction);
			integral = all.substr(0, all.size() - fraction.size());
			fraction = all.substr(all.size() - fraction.size());
		}
	}
	while (int(fraction.size()) < precision) {
		fraction += '0';
	}
}

string Group(const string &integral) {
	string out;
	for (idx_t i = 0; i < integral.size(); i++) {
		if (i > 0 && (integral.size() - i) % 3 == 0) {
			out += ',';
		}
		out += integral[i];
	}
	return out;
}

//! digits rounded HALF_UP to `count` significant digits (padded with zeros); the exponent may grow
void SignificantDigits(const Number &n, int count, string &digits, int &exponent) {
	digits = n.digits;
	exponent = n.exponent;
	if (int(digits.size()) > count) {
		bool round_up = digits[idx_t(count)] >= '5';
		digits = digits.substr(0, idx_t(count));
		if (round_up) {
			auto bumped = IncrementDigits(digits);
			if (bumped.size() > digits.size()) {
				bumped.pop_back();
				exponent++;
			}
			digits = bumped;
		}
	}
	while (int(digits.size()) < count) {
		digits += '0';
	}
}

string Scientific(const Number &n, int precision, bool upper) {
	string digits;
	int exponent;
	SignificantDigits(n, precision + 1, digits, exponent);
	if (n.digits == "0") {
		exponent = 0;
	}
	string out = digits.substr(0, 1);
	if (precision > 0) {
		out += "." + digits.substr(1);
	}
	char tail[16];
	snprintf(tail, sizeof(tail), "%c%c%02d", upper ? 'E' : 'e', exponent < 0 ? '-' : '+', std::abs(exponent));
	return out + tail;
}

//! Java's Double.toString
string JavaDouble(double d, bool is_float) {
	if (std::isnan(d)) {
		return "NaN";
	}
	if (std::isinf(d)) {
		return d > 0 ? "Infinity" : "-Infinity";
	}
	auto n = NumberFromDouble(d, is_float);
	string sign = n.negative ? "-" : "";
	if (n.digits == "0") {
		return sign + "0.0";
	}
	if (n.exponent >= -3 && n.exponent < 7) {
		string integral, fraction;
		FixedParts(n, MaxValue<int>(1, int(n.digits.size()) - n.exponent - 1), integral, fraction);
		return sign + integral + "." + fraction;
	}
	string mantissa = n.digits.substr(0, 1) + "." + (n.digits.size() > 1 ? n.digits.substr(1) : "0");
	return sign + mantissa + "E" + std::to_string(n.exponent);
}

//! what Spark's formatter prints for %s: internal representations (dates are day numbers, timestamps microseconds)
string SparkToString(const Value &value) {
	if (value.IsNull()) {
		return "null";
	}
	auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return std::to_string(value.GetValue<int32_t>());
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return std::to_string(value.GetValue<int64_t>());
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
		return JavaDouble(value.GetValue<double>(), type.id() == LogicalTypeId::FLOAT);
	case LogicalTypeId::LIST: {
		string out = "[";
		auto &children = ListValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			out += (i ? "," : "") + SparkToString(children[i]);
		}
		return out + "]";
	}
	default:
		return value.ToString();
	}
}

struct Spec {
	int index = -1; // 1-based explicit argument index, 0 = previous ("<")
	bool left = false, zero = false, plus = false, space = false, comma = false, paren = false, alt = false;
	int width = -1;
	int precision = -1;
	char conversion = 0;
	bool upper = false;
};

[[noreturn]] void FormatError(const string &what, const string &detail) {
	throw InvalidInputException("[%s] %s", what, detail);
}

int IntegerBits(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return 8;
	case LogicalTypeId::SMALLINT:
		return 16;
	case LogicalTypeId::INTEGER:
		return 32;
	case LogicalTypeId::BIGINT:
		return 64;
	default:
		return 0;
	}
}

bool IsFloatingType(const LogicalType &type) {
	return type.id() == LogicalTypeId::DOUBLE || type.id() == LogicalTypeId::FLOAT;
}

string Pad(string body, const Spec &spec, bool allow_zero) {
	if (spec.width < 0 || int(body.size()) >= spec.width) {
		return body;
	}
	auto fill = idx_t(spec.width) - body.size();
	if (spec.left) {
		return body + string(fill, ' ');
	}
	if (spec.zero && allow_zero) {
		// zeros go between the sign and the digits
		idx_t at = (!body.empty() && (body[0] == '-' || body[0] == '+' || body[0] == ' ' || body[0] == '(')) ? 1 : 0;
		return body.substr(0, at) + string(fill, '0') + body.substr(at);
	}
	return string(fill, ' ') + body;
}

//! sign, grouping and parentheses around the digits of a number
string SignedNumber(bool negative, string body, const Spec &spec) {
	if (negative) {
		return spec.paren ? "(" + body + ")" : "-" + body;
	}
	return spec.plus ? "+" + body : spec.space ? " " + body : body;
}

string FormatOne(const Spec &spec, const Value &value, idx_t row_unused) {
	(void)row_unused;
	bool is_null = value.IsNull();
	auto &type = value.type();
	string body;
	bool allow_zero = false;
	switch (spec.conversion) {
	case 'b':
		body = is_null ? "false" : type.id() == LogicalTypeId::BOOLEAN ? (value.GetValue<bool>() ? "true" : "false")
		                                                              : "true";
		if (spec.precision >= 0) {
			body = body.substr(0, idx_t(spec.precision));
		}
		break;
	case 's': {
		if (is_null) {
			body = "null";
		} else {
			body = SparkToString(value);
		}
		if (spec.precision >= 0 && idx_t(spec.precision) < body.size()) {
			body = body.substr(0, idx_t(spec.precision));
		}
		break;
	}
	case 'c':
		if (is_null) {
			body = "null";
		} else if (IntegerBits(type) != 0) {
			auto code = value.GetValue<int64_t>();
			if (code < 0 || code > 0x10FFFF) {
				FormatError("ILLEGAL_FORMAT_CODE_POINT", "invalid code point");
			}
			char utf8[8] = {0};
			int length = 0;
			if (code < 0x80) {
				utf8[length++] = char(code);
			} else if (code < 0x800) {
				utf8[length++] = char(0xC0 | (code >> 6));
				utf8[length++] = char(0x80 | (code & 0x3F));
			} else if (code < 0x10000) {
				utf8[length++] = char(0xE0 | (code >> 12));
				utf8[length++] = char(0x80 | ((code >> 6) & 0x3F));
				utf8[length++] = char(0x80 | (code & 0x3F));
			} else {
				utf8[length++] = char(0xF0 | (code >> 18));
				utf8[length++] = char(0x80 | ((code >> 12) & 0x3F));
				utf8[length++] = char(0x80 | ((code >> 6) & 0x3F));
				utf8[length++] = char(0x80 | (code & 0x3F));
			}
			body = string(utf8, idx_t(length));
		} else {
			FormatError("ILLEGAL_FORMAT_CONVERSION", "%c is not applicable to " + type.ToString());
		}
		break;
	case 'd':
	case 'x':
	case 'o': {
		if (is_null) {
			body = "null";
			break;
		}
		int bits = IntegerBits(type);
		if (bits == 0 && type.id() != LogicalTypeId::HUGEINT && type.id() != LogicalTypeId::DECIMAL) {
			FormatError("ILLEGAL_FORMAT_CONVERSION", string("%") + spec.conversion + " is not applicable to " + type.ToString());
		}
		if (type.id() == LogicalTypeId::DECIMAL && spec.conversion != 'd') {
			FormatError("ILLEGAL_FORMAT_CONVERSION", string("%") + spec.conversion + " is not applicable to decimals");
		}
		allow_zero = true;
		if (spec.conversion == 'd') {
			auto text = type.id() == LogicalTypeId::DECIMAL ? value.ToString() : value.ToString();
			if (type.id() == LogicalTypeId::DECIMAL) {
				// BigDecimal is not an integral type for %d
				FormatError("ILLEGAL_FORMAT_CONVERSION", "%d is not applicable to decimals");
			}
			bool negative = !text.empty() && text[0] == '-';
			string digits = negative ? text.substr(1) : text;
			if (spec.comma) {
				digits = Group(digits);
			}
			body = SignedNumber(negative, digits, spec);
		} else {
			uint64_t bits_value = uint64_t(value.GetValue<int64_t>());
			if (bits > 0 && bits < 64) {
				bits_value &= (uint64_t(1) << bits) - 1;
			}
			char text[40];
			snprintf(text, sizeof(text), spec.conversion == 'x' ? "%llx" : "%llo", (unsigned long long)bits_value);
			body = text;
			if (spec.alt) {
				body = (spec.conversion == 'x' ? "0x" : "0") + body;
			}
		}
		break;
	}
	case 'f':
	case 'e':
	case 'g': {
		if (is_null) {
			body = "null";
			break;
		}
		// Spark hands the formatter its internal Decimal, which Java's %f does not accept
		if (!IsFloatingType(type)) {
			FormatError("ILLEGAL_FORMAT_CONVERSION", string("%") + spec.conversion + " is not applicable to " + type.ToString());
		}
		allow_zero = true;
		if (IsFloatingType(type)) {
			double d = value.GetValue<double>();
			if (std::isnan(d) || std::isinf(d)) {
				allow_zero = false;
				body = std::isnan(d) ? "NaN" : SignedNumber(d < 0, "Infinity", spec);
				break;
			}
		}
		Number n = IsFloatingType(type) ? NumberFromDouble(value.GetValue<double>(), type.id() == LogicalTypeId::FLOAT)
		                                : NumberFromDecimal(value.ToString());
		int precision = spec.precision < 0 ? 6 : spec.precision;
		string text;
		if (spec.conversion == 'f') {
			string integral, fraction;
			FixedParts(n, precision, integral, fraction);
			text = (spec.comma ? Group(integral) : integral) + (precision > 0 ? "." + fraction : spec.alt ? "." : "");
		} else if (spec.conversion == 'e') {
			text = Scientific(n, precision, spec.upper);
		} else {
			if (precision == 0) {
				precision = 1;
			}
			string digits;
			int exponent;
			SignificantDigits(n, precision, digits, exponent);
			bool zero = n.digits == "0";
			if (!zero && (exponent < -4 || exponent >= precision)) {
				text = Scientific(n, precision - 1, spec.upper);
			} else {
				string integral, fraction;
				FixedParts(n, MaxValue(0, precision - (zero ? 1 : exponent + 1)), integral, fraction);
				text = (spec.comma ? Group(integral) : integral) + (fraction.empty() ? "" : "." + fraction);
			}
		}
		body = SignedNumber(n.negative, text, spec);
		break;
	}
	default:
		FormatError("UNKNOWN_FORMAT_CONVERSION", string("%") + spec.conversion);
	}
	if (spec.upper && spec.conversion != 'e' && spec.conversion != 'g') {
		body = StringUtil::Upper(body);
	}
	return Pad(body, spec, allow_zero);
}

//! A parsed format string: literal text, or a conversion applied to the argument in column `argument` + 1.
struct Piece {
	string literal;
	bool is_spec = false;
	Spec spec;
	idx_t argument = 0;
};

vector<Piece> ParseFormat(const string &format, idx_t column_count) {
	vector<Piece> pieces;
	auto add_literal = [&](const string &text) {
		if (pieces.empty() || pieces.back().is_spec) {
			pieces.emplace_back();
		}
		pieces.back().literal += text;
	};
	idx_t next_argument = 0;
	int last_argument = -1;
	for (idx_t i = 0; i < format.size();) {
		if (format[i] != '%') {
			add_literal(string(1, format[i++]));
			continue;
		}
		i++;
		Spec spec;
		// argument index: digits followed by '$', or '<'
		idx_t start = i;
		int number = 0;
		while (i < format.size() && isdigit(uint8_t(format[i]))) {
			number = number * 10 + (format[i] - '0');
			i++;
		}
		if (i < format.size() && format[i] == '$' && i > start) {
			spec.index = number;
			i++;
		} else {
			i = start;
			if (i < format.size() && format[i] == '<') {
				spec.index = 0;
				i++;
			}
		}
		for (; i < format.size(); i++) {
			char c = format[i];
			if (c == '-') {
				spec.left = true;
			} else if (c == '0') {
				spec.zero = true;
			} else if (c == '+') {
				spec.plus = true;
			} else if (c == ' ') {
				spec.space = true;
			} else if (c == ',') {
				spec.comma = true;
			} else if (c == '(') {
				spec.paren = true;
			} else if (c == '#') {
				spec.alt = true;
			} else {
				break;
			}
		}
		if (i < format.size() && isdigit(uint8_t(format[i]))) {
			spec.width = 0;
			while (i < format.size() && isdigit(uint8_t(format[i]))) {
				spec.width = spec.width * 10 + (format[i++] - '0');
			}
		}
		if (i < format.size() && format[i] == '.') {
			i++;
			if (i >= format.size() || !isdigit(uint8_t(format[i]))) {
				FormatError("UNKNOWN_FORMAT_CONVERSION", "bad precision");
			}
			spec.precision = 0;
			while (i < format.size() && isdigit(uint8_t(format[i]))) {
				spec.precision = spec.precision * 10 + (format[i++] - '0');
			}
		}
		if (i >= format.size()) {
			FormatError("UNKNOWN_FORMAT_CONVERSION", "format string ends with '%'");
		}
		char conversion = format[i++];
		if (conversion == '%') {
			add_literal(Pad("%", spec, false));
			continue;
		}
		if (conversion == 'n') {
			add_literal("\n");
			continue;
		}
		if (isupper(uint8_t(conversion))) {
			spec.upper = true;
			conversion = char(tolower(conversion));
		}
		spec.conversion = conversion;
		if (string("bscdxofeg").find(conversion) == string::npos) {
			FormatError("UNKNOWN_FORMAT_CONVERSION", string("%") + format[i - 1]);
		}
		idx_t argument;
		if (spec.index > 0) {
			argument = idx_t(spec.index - 1);
		} else if (spec.index == 0) {
			if (last_argument < 0) {
				FormatError("MISSING_FORMAT_ARGUMENT", "no previous argument");
			}
			argument = idx_t(last_argument);
		} else {
			argument = next_argument++;
		}
		if (argument + 1 >= column_count) {
			FormatError("MISSING_FORMAT_ARGUMENT", "format specifier requires argument " + std::to_string(argument + 1));
		}
		last_argument = int(argument);
		pieces.emplace_back();
		pieces.back().is_spec = true;
		pieces.back().spec = spec;
		pieces.back().argument = argument;
	}
	return pieces;
}

//! Format arguments of a chunk: the common types are read without creating a Value per row.
struct ArgumentColumn {
	UnifiedVectorFormat data;
	bool is_string = false;
	bool is_integer = false;
	PhysicalType physical = PhysicalType::INVALID;

	int64_t Integer(idx_t index) const {
		switch (physical) {
		case PhysicalType::INT8:
			return UnifiedVectorFormat::GetData<int8_t>(data)[index];
		case PhysicalType::INT16:
			return UnifiedVectorFormat::GetData<int16_t>(data)[index];
		case PhysicalType::INT32:
			return UnifiedVectorFormat::GetData<int32_t>(data)[index];
		default:
			return UnifiedVectorFormat::GetData<int64_t>(data)[index];
		}
	}
};

string FormatPieces(const vector<Piece> &pieces, DataChunk &args, vector<ArgumentColumn> &columns, idx_t row) {
	string out;
	for (auto &piece : pieces) {
		if (!piece.is_spec) {
			out += piece.literal;
			continue;
		}
		auto &spec = piece.spec;
		auto &column = columns[piece.argument + 1];
		if (column.is_string && spec.conversion == 's' && !spec.upper && spec.precision < 0) {
			auto index = column.data.sel->get_index(row);
			if (!column.data.validity.RowIsValid(index)) {
				out += Pad("null", spec, false);
			} else {
				out += Pad(UnifiedVectorFormat::GetData<string_t>(column.data)[index].GetString(), spec, false);
			}
			continue;
		}
		if (column.is_integer && spec.conversion == 'd' && !spec.comma && !spec.paren && !spec.plus && !spec.space &&
		    !spec.upper) {
			auto index = column.data.sel->get_index(row);
			if (!column.data.validity.RowIsValid(index)) {
				out += Pad("null", spec, true);
			} else {
				out += Pad(std::to_string(column.Integer(index)), spec, true);
			}
			continue;
		}
		out += FormatOne(spec, args.data[piece.argument + 1].GetValue(row), row);
	}
	return out;
}

void FormatStringKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	UnifiedVectorFormat format_data;
	args.data[0].ToUnifiedFormat(count, format_data);
	auto formats = UnifiedVectorFormat::GetData<string_t>(format_data);
	// the arguments in a form that is cheap to read: strings and integers without a Value per row
	vector<ArgumentColumn> columns(args.ColumnCount());
	for (idx_t c = 1; c < args.ColumnCount(); c++) {
		auto &type = args.data[c].GetType();
		auto &column = columns[c];
		switch (type.id()) {
		case LogicalTypeId::VARCHAR:
			column.is_string = true;
			args.data[c].ToUnifiedFormat(count, column.data);
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			column.is_integer = true;
			column.physical = type.InternalType();
			args.data[c].ToUnifiedFormat(count, column.data);
			break;
		default:
			break;
		}
	}
	vector<Piece> pieces;
	string last_format;
	bool have_pieces = false;
	for (idx_t row = 0; row < count; row++) {
		auto format_index = format_data.sel->get_index(row);
		if (!format_data.validity.RowIsValid(format_index)) {
			FlatVector::SetNull(result, row, true);
			continue;
		}
		auto format = formats[format_index].GetString();
		if (!have_pieces || format != last_format) {
			pieces = ParseFormat(format, args.ColumnCount());
			last_format = format;
			have_pieces = true;
		}
		out[row] = StringVector::AddString(result, FormatPieces(pieces, args, columns, row));
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

void RegisterSparkFormatString(ExtensionLoader &loader) {
	ScalarFunction format(Identifier("format_string_impl"), {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      FormatStringKernel, nullptr, nullptr, nullptr, LogicalType::ANY);
	format.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	RegisterFallible(loader, std::move(format));
}

} // namespace duckdb
