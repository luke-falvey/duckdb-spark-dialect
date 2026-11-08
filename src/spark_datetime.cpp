//===----------------------------------------------------------------------===//
// Spark datetime patterns (java.time.format.DateTimeFormatter style, as used by
// date_format, from_unixtime, to_timestamp(str, fmt), unix_timestamp(str, fmt), ...)
// and other datetime functions that need more than SQL.
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cmath>

namespace duckdb {

namespace {

const char *MONTH_NAMES[] = {"January", "February", "March",     "April",   "May",      "June",
                             "July",    "August",   "September", "October", "November", "December"};
const char *DAY_NAMES[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

struct PatternToken {
	char letter; // 0 for literal text
	idx_t count;
	string literal;
};

vector<PatternToken> ParsePattern(const string &pattern) {
	vector<PatternToken> tokens;
	idx_t i = 0;
	while (i < pattern.size()) {
		char c = pattern[i];
		if (c == '\'') {
			// quoted literal; '' is a single quote
			if (i + 1 < pattern.size() && pattern[i + 1] == '\'') {
				tokens.push_back({0, 0, "'"});
				i += 2;
				continue;
			}
			string text;
			i++;
			while (i < pattern.size()) {
				if (pattern[i] == '\'') {
					if (i + 1 < pattern.size() && pattern[i + 1] == '\'') {
						text += '\'';
						i += 2;
						continue;
					}
					break;
				}
				text += pattern[i++];
			}
			i++;
			tokens.push_back({0, 0, text});
			continue;
		}
		if (StringUtil::CharacterIsAlpha(c)) {
			idx_t j = i;
			while (j < pattern.size() && pattern[j] == c) {
				j++;
			}
			tokens.push_back({c, j - i, ""});
			i = j;
			continue;
		}
		if (c == '[' || c == ']') {
			// optional sections: formatting prints them, parsing treats them as optional (approximation)
			i++;
			continue;
		}
		tokens.push_back({0, 0, string(1, c)});
		i++;
	}
	return tokens;
}

string Pad(int64_t value, idx_t width) {
	bool negative = value < 0;
	auto digits = to_string(negative ? -value : value);
	while (digits.size() < width) {
		digits = "0" + digits;
	}
	return negative ? "-" + digits : digits;
}

bool FormatTimestamp(timestamp_t ts, const vector<PatternToken> &tokens, string &out) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(ts, date, time);
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	int32_t hour, minute, second, micros;
	Time::Convert(time, hour, minute, second, micros);
	for (auto &t : tokens) {
		if (t.letter == 0) {
			out += t.literal;
			continue;
		}
		switch (t.letter) {
		case 'y':
		case 'u':
			if (t.count == 2) {
				out += Pad(((year % 100) + 100) % 100, 2);
			} else {
				out += Pad(year, t.count);
			}
			break;
		case 'M':
		case 'L':
			if (t.count <= 2) {
				out += Pad(month, t.count);
			} else if (t.count == 3) {
				out += string(MONTH_NAMES[month - 1]).substr(0, 3);
			} else if (t.count == 4) {
				out += MONTH_NAMES[month - 1];
			} else {
				out += string(MONTH_NAMES[month - 1]).substr(0, 1);
			}
			break;
		case 'd':
			out += Pad(day, t.count);
			break;
		case 'D':
			out += Pad(Date::ExtractDayOfTheYear(date), t.count);
			break;
		case 'Q':
		case 'q': {
			auto quarter = (month - 1) / 3 + 1;
			if (t.count <= 2) {
				out += Pad(quarter, t.count);
			} else if (t.count == 3) {
				out += "Q" + to_string(quarter);
			} else {
				const char *names[] = {"1st quarter", "2nd quarter", "3rd quarter", "4th quarter"};
				out += names[quarter - 1];
			}
			break;
		}
		case 'E': {
			auto dow = Date::ExtractISODayOfTheWeek(date); // 1 = Monday
			string name = DAY_NAMES[dow - 1];
			out += t.count >= 4 ? name : name.substr(0, 3);
			break;
		}
		case 'a':
			out += hour < 12 ? "AM" : "PM";
			break;
		case 'H':
			out += Pad(hour, t.count);
			break;
		case 'k':
			out += Pad(hour == 0 ? 24 : hour, t.count);
			break;
		case 'h':
			out += Pad(hour % 12 == 0 ? 12 : hour % 12, t.count);
			break;
		case 'K':
			out += Pad(hour % 12, t.count);
			break;
		case 'm':
			out += Pad(minute, t.count);
			break;
		case 's':
			out += Pad(second, t.count);
			break;
		case 'S': {
			auto frac = Pad(micros, 6) + "000";
			out += frac.substr(0, MinValue<idx_t>(t.count, 9));
			for (idx_t k = 9; k < t.count; k++) {
				out += '0';
			}
			break;
		}
		case 'z':
		case 'V':
			out += "UTC";
			break;
		case 'Z':
			out += t.count == 4 ? "GMT" : (t.count == 5 ? "Z" : "+0000");
			break;
		case 'X':
			out += "Z";
			break;
		case 'x':
			out += t.count == 1 ? "+00" : (t.count == 2 || t.count == 4 ? "+0000" : "+00:00");
			break;
		case 'O':
			out += "GMT";
			break;
		case 'G':
			out += year > 0 ? "AD" : "BC";
			break;
		default:
			return false;
		}
	}
	return true;
}

//! Strict-ish parse of `input` with the Spark pattern; missing fields default to 1970-01-01 00:00:00
bool ParseTimestamp(const string &input, const vector<PatternToken> &tokens, timestamp_t &result) {
	int64_t year = 1970, month = 1, day = 1, hour = 0, minute = 0, second = 0, micros = 0, doy = -1;
	bool pm = false, has_ampm = false, twelve_hour = false;
	idx_t pos = 0;
	auto read_number = [&](idx_t min_digits, idx_t max_digits, int64_t &value) -> bool {
		idx_t start = pos;
		bool negative = false;
		if (pos < input.size() && (input[pos] == '-' || input[pos] == '+') && min_digits >= 4) {
			negative = input[pos] == '-';
			pos++;
			start = pos;
		}
		value = 0;
		while (pos < input.size() && pos - start < max_digits && StringUtil::CharacterIsDigit(input[pos])) {
			value = value * 10 + (input[pos] - '0');
			pos++;
		}
		if (pos - start < min_digits) {
			return false;
		}
		if (negative) {
			value = -value;
		}
		return true;
	};
	auto read_text = [&](const char *const *names, idx_t name_count, idx_t prefix, int64_t &value) -> bool {
		for (idx_t n = 0; n < name_count; n++) {
			string name = names[n];
			if (prefix > 0) {
				name = name.substr(0, prefix);
			}
			if (input.size() - pos >= name.size() && StringUtil::CIEquals(input.substr(pos, name.size()), name)) {
				value = int64_t(n) + 1;
				pos += name.size();
				return true;
			}
		}
		return false;
	};
	for (idx_t ti = 0; ti < tokens.size(); ti++) {
		auto &t = tokens[ti];
		// a numeric field directly followed by another numeric field reads a fixed width
		bool adjacent_numeric = ti + 1 < tokens.size() && tokens[ti + 1].letter != 0 &&
		                        strchr("yMdHhmsSDkK", tokens[ti + 1].letter) != nullptr;
		if (t.letter == 0) {
			if (input.compare(pos, t.literal.size(), t.literal) != 0) {
				return false;
			}
			pos += t.literal.size();
			continue;
		}
		idx_t width = t.count;
		idx_t max_width = adjacent_numeric ? width : 10;
		switch (t.letter) {
		case 'y':
		case 'u':
			if (t.count == 2) {
				if (!read_number(2, 2, year)) {
					return false;
				}
				year += 2000;
			} else if (!read_number(t.count, adjacent_numeric ? t.count : 10, year)) {
				return false;
			}
			break;
		case 'M':
		case 'L':
			if (t.count >= 4) {
				if (!read_text(MONTH_NAMES, 12, 0, month)) {
					return false;
				}
			} else if (t.count == 3) {
				if (!read_text(MONTH_NAMES, 12, 3, month)) {
					return false;
				}
			} else if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), month)) {
				return false;
			}
			break;
		case 'd':
			if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), day)) {
				return false;
			}
			break;
		case 'D':
			if (!read_number(width, 3, doy)) {
				return false;
			}
			break;
		case 'H':
		case 'k':
			if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), hour)) {
				return false;
			}
			if (t.letter == 'k' && hour == 24) {
				hour = 0;
			}
			break;
		case 'h':
		case 'K':
			twelve_hour = true;
			if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), hour)) {
				return false;
			}
			break;
		case 'm':
			if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), minute)) {
				return false;
			}
			break;
		case 's':
			if (!read_number(width, t.count == 2 ? 2 : MinValue<idx_t>(max_width, 2), second)) {
				return false;
			}
			break;
		case 'S': {
			idx_t start = pos;
			int64_t fraction = 0;
			if (!read_number(1, t.count, fraction)) {
				return false;
			}
			idx_t digits = pos - start;
			while (digits < 6) {
				fraction *= 10;
				digits++;
			}
			while (digits > 6) {
				fraction /= 10;
				digits--;
			}
			micros = fraction;
			break;
		}
		case 'a': {
			const char *ampm[] = {"AM", "PM"};
			int64_t v;
			if (!read_text(ampm, 2, 0, v)) {
				return false;
			}
			has_ampm = true;
			pm = v == 2;
			break;
		}
		case 'E': {
			int64_t v;
			if (!read_text(DAY_NAMES, 7, t.count >= 4 ? 0 : 3, v)) {
				return false;
			}
			break;
		}
		case 'z':
		case 'V':
		case 'Z':
		case 'X':
		case 'x':
		case 'O': {
			// accept UTC / Z / +hh[:mm] offsets
			int64_t offset_minutes = 0;
			if (pos < input.size() && (input[pos] == 'Z' || input[pos] == 'z')) {
				pos++;
			} else if (input.compare(pos, 3, "UTC") == 0 || input.compare(pos, 3, "GMT") == 0) {
				pos += 3;
			} else if (pos < input.size() && (input[pos] == '+' || input[pos] == '-')) {
				bool negative = input[pos] == '-';
				pos++;
				int64_t hh = 0, mm = 0;
				if (!read_number(2, 2, hh)) {
					return false;
				}
				if (pos < input.size() && input[pos] == ':') {
					pos++;
				}
				read_number(0, 2, mm);
				offset_minutes = (hh * 60 + mm) * (negative ? -1 : 1);
			} else {
				return false;
			}
			minute -= offset_minutes;
			break;
		}
		default:
			return false;
		}
	}
	if (pos != input.size()) {
		return false;
	}
	if (twelve_hour) {
		if (hour < 1 || hour > 12) {
			return false;
		}
		hour = hour % 12 + ((has_ampm && pm) ? 12 : 0);
	} else if (has_ampm && pm && hour < 12) {
		hour += 12;
	}
	if (hour > 23 || second > 59) {
		return false;
	}
	date_t date;
	if (doy >= 0) {
		if (!Date::IsValid(int32_t(year), 1, 1)) {
			return false;
		}
		date = Date::FromDate(int32_t(year), 1, 1) + int32_t(doy - 1);
	} else {
		if (!Date::IsValid(int32_t(year), int32_t(month), int32_t(day))) {
			return false;
		}
		date = Date::FromDate(int32_t(year), int32_t(month), int32_t(day));
	}
	int64_t total_micros = ((hour * 60 + minute) * 60 + second) * Interval::MICROS_PER_SEC + micros;
	auto base = Timestamp::FromDatetime(date, dtime_t(0));
	result = timestamp_t(base.value + total_micros);
	return true;
}

void FormatDatetimeKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<timestamp_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](timestamp_t ts, string_t fmt) -> optional<string_t> {
		    if (ts == timestamp_t::infinity() || ts == timestamp_t::ninfinity()) {
			    return optional<string_t>();
		    }
		    string out;
		    if (!FormatTimestamp(ts, ParsePattern(fmt.GetString()), out)) {
			    throw InvalidInputException("Unsupported datetime pattern: %s", fmt.GetString());
		    }
		    return StringVector::AddString(result, out);
	    });
}

void ParseDatetimeKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, timestamp_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, string_t fmt) -> optional<timestamp_t> {
		    timestamp_t ts;
		    if (!ParseTimestamp(input.GetString(), ParsePattern(fmt.GetString()), ts)) {
			    return optional<timestamp_t>();
		    }
		    return ts;
	    });
}

//! months_between(t1, t2, roundOff) following Spark's DateTimeUtils.monthsBetween
void MonthsBetweenKernel(DataChunk &args, ExpressionState &, Vector &result) {
	TernaryExecutor::Execute<timestamp_t, timestamp_t, bool, double>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](timestamp_t t1, timestamp_t t2, bool round_off) -> double {
		    date_t d1, d2;
		    dtime_t tm1, tm2;
		    Timestamp::Convert(t1, d1, tm1);
		    Timestamp::Convert(t2, d2, tm2);
		    int32_t y1, m1, day1, y2, m2, day2;
		    Date::Convert(d1, y1, m1, day1);
		    Date::Convert(d2, y2, m2, day2);
		    int64_t months = int64_t(y1 - y2) * 12 + (m1 - m2);
		    bool last1 = day1 == Date::MonthDays(y1, m1);
		    bool last2 = day2 == Date::MonthDays(y2, m2);
		    if (day1 == day2 || (last1 && last2)) {
			    return double(months);
		    }
		    double seconds1 = double(day1 - 1) * 86400 + double(tm1.value) / 1e6;
		    double seconds2 = double(day2 - 1) * 86400 + double(tm2.value) / 1e6;
		    double diff = double(months) + (seconds1 - seconds2) / (31.0 * 86400);
		    if (round_off) {
			    return std::round(diff * 1e8) / 1e8;
		    }
		    return diff;
	    });
}

} // namespace

string FormatDatetimeValue(timestamp_t ts, const string &pattern) {
	string out;
	if (!FormatTimestamp(ts, ParsePattern(pattern), out)) {
		throw InvalidInputException("Unsupported datetime pattern: %s", pattern);
	}
	return out;
}

void RegisterSparkDatetime(ExtensionLoader &loader) {
	RegisterFallible(loader, ScalarFunction(Identifier("format_datetime"), {LogicalType::TIMESTAMP, LogicalType::VARCHAR},
	                                       LogicalType::VARCHAR, FormatDatetimeKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("parse_datetime"), {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::TIMESTAMP, ParseDatetimeKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("months_between_impl"),
	                                       {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::BOOLEAN},
	                                       LogicalType::DOUBLE, MonthsBetweenKernel));
}

} // namespace duckdb
