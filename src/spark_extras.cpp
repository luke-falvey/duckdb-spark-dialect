//===----------------------------------------------------------------------===//
// Less common Spark functions with exact Spark output: aes_encrypt / aes_decrypt (ECB, CBC, GCM),
// count_min_sketch (Spark's CountMinSketchImpl serialization), to_char / to_number number formats,
// and the xpath family (a small XML reader and an XPath subset).
//===----------------------------------------------------------------------===//

#include "spark_dialect.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "spark_keywords.hpp"

#include <cmath>
#include <random>

namespace duckdb {

//! Spark's Murmur3_x86_32.hashUnsafeBytes (spark_library.cpp)
uint32_t Murmur3Bytes(const string &bytes, uint32_t seed);

namespace {

//===----------------------------------------------------------------------===//
// AES (FIPS-197)
//===----------------------------------------------------------------------===//
const uint8_t SBOX[256] = {
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76, 0xca, 0x82, 0xc9,
    0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0, 0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f,
    0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15, 0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07,
    0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75, 0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3,
    0x29, 0xe3, 0x2f, 0x84, 0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58,
    0xcf, 0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8, 0x51, 0xa3,
    0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2, 0xcd, 0x0c, 0x13, 0xec, 0x5f,
    0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73, 0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88,
    0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb, 0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac,
    0x62, 0x91, 0x95, 0xe4, 0x79, 0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a,
    0xae, 0x08, 0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a, 0x70,
    0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e, 0xe1, 0xf8, 0x98, 0x11,
    0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf, 0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42,
    0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16};

uint8_t InvSbox(uint8_t x) {
	static uint8_t inv[256];
	static bool init = false;
	if (!init) {
		for (int i = 0; i < 256; i++) {
			inv[SBOX[i]] = uint8_t(i);
		}
		init = true;
	}
	return inv[x];
}

uint8_t Xtime(uint8_t x) {
	return uint8_t((x << 1) ^ ((x & 0x80) ? 0x1b : 0));
}

uint8_t Mul(uint8_t a, uint8_t b) {
	uint8_t r = 0;
	while (b) {
		if (b & 1) {
			r ^= a;
		}
		a = Xtime(a);
		b >>= 1;
	}
	return r;
}

struct Aes {
	explicit Aes(const string &key) {
		if (key.size() != 16 && key.size() != 24 && key.size() != 32) {
			throw InvalidInputException("[INVALID_PARAMETER_VALUE.AES_KEY_LENGTH] The value of parameter(s) `key` in "
			                            "`aes_encrypt`/`aes_decrypt` is invalid: expects a binary value with 16, 24 or "
			                            "32 bytes, but got %llu bytes.",
			                            (unsigned long long)key.size());
		}
		int nk = int(key.size() / 4);
		rounds = nk + 6;
		int words = 4 * (rounds + 1);
		w.resize(words * 4);
		memcpy(w.data(), key.data(), key.size());
		uint8_t rcon = 1;
		for (int i = nk; i < words; i++) {
			uint8_t t[4] = {w[(i - 1) * 4], w[(i - 1) * 4 + 1], w[(i - 1) * 4 + 2], w[(i - 1) * 4 + 3]};
			if (i % nk == 0) {
				uint8_t tmp = t[0];
				t[0] = uint8_t(SBOX[t[1]] ^ rcon);
				t[1] = SBOX[t[2]];
				t[2] = SBOX[t[3]];
				t[3] = SBOX[tmp];
				rcon = Xtime(rcon);
			} else if (nk > 6 && i % nk == 4) {
				for (auto &b : t) {
					b = SBOX[b];
				}
			}
			for (int k = 0; k < 4; k++) {
				w[i * 4 + k] = uint8_t(w[(i - nk) * 4 + k] ^ t[k]);
			}
		}
	}
	int rounds;
	vector<uint8_t> w;

	void AddRoundKey(uint8_t *s, int round) const {
		for (int i = 0; i < 16; i++) {
			s[i] ^= w[round * 16 + i];
		}
	}
	void Encrypt(uint8_t *s) const {
		AddRoundKey(s, 0);
		for (int round = 1; round <= rounds; round++) {
			for (int i = 0; i < 16; i++) {
				s[i] = SBOX[s[i]];
			}
			uint8_t t[16];
			for (int c = 0; c < 4; c++) {
				for (int r = 0; r < 4; r++) {
					t[c * 4 + r] = s[((c + r) % 4) * 4 + r];
				}
			}
			memcpy(s, t, 16);
			if (round != rounds) {
				for (int c = 0; c < 4; c++) {
					uint8_t *col = s + c * 4;
					uint8_t a0 = col[0], a1 = col[1], a2 = col[2], a3 = col[3];
					col[0] = uint8_t(Mul(a0, 2) ^ Mul(a1, 3) ^ a2 ^ a3);
					col[1] = uint8_t(a0 ^ Mul(a1, 2) ^ Mul(a2, 3) ^ a3);
					col[2] = uint8_t(a0 ^ a1 ^ Mul(a2, 2) ^ Mul(a3, 3));
					col[3] = uint8_t(Mul(a0, 3) ^ a1 ^ a2 ^ Mul(a3, 2));
				}
			}
			AddRoundKey(s, round);
		}
	}
	void Decrypt(uint8_t *s) const {
		AddRoundKey(s, rounds);
		for (int round = rounds - 1; round >= 0; round--) {
			uint8_t t[16];
			for (int c = 0; c < 4; c++) {
				for (int r = 0; r < 4; r++) {
					t[((c + r) % 4) * 4 + r] = s[c * 4 + r];
				}
			}
			memcpy(s, t, 16);
			for (int i = 0; i < 16; i++) {
				s[i] = InvSbox(s[i]);
			}
			AddRoundKey(s, round);
			if (round != 0) {
				for (int c = 0; c < 4; c++) {
					uint8_t *col = s + c * 4;
					uint8_t a0 = col[0], a1 = col[1], a2 = col[2], a3 = col[3];
					col[0] = uint8_t(Mul(a0, 14) ^ Mul(a1, 11) ^ Mul(a2, 13) ^ Mul(a3, 9));
					col[1] = uint8_t(Mul(a0, 9) ^ Mul(a1, 14) ^ Mul(a2, 11) ^ Mul(a3, 13));
					col[2] = uint8_t(Mul(a0, 13) ^ Mul(a1, 9) ^ Mul(a2, 14) ^ Mul(a3, 11));
					col[3] = uint8_t(Mul(a0, 11) ^ Mul(a1, 13) ^ Mul(a2, 9) ^ Mul(a3, 14));
				}
			}
		}
	}
};

string RandomBytes(idx_t n) {
	static thread_local std::random_device device;
	string out(n, '\0');
	for (auto &c : out) {
		c = char(device() & 0xFF);
	}
	return out;
}

// GF(2^128) multiplication for GHASH (NIST SP 800-38D, algorithm 1)
void GfMul(uint8_t *x, const uint8_t *y) {
	uint8_t z[16] = {0};
	uint8_t v[16];
	memcpy(v, y, 16);
	for (int i = 0; i < 128; i++) {
		if (x[i / 8] & (0x80 >> (i % 8))) {
			for (int k = 0; k < 16; k++) {
				z[k] ^= v[k];
			}
		}
		bool lsb = v[15] & 1;
		for (int k = 15; k > 0; k--) {
			v[k] = uint8_t((v[k] >> 1) | (v[k - 1] << 7));
		}
		v[0] >>= 1;
		if (lsb) {
			v[0] ^= 0xe1;
		}
	}
	memcpy(x, z, 16);
}

string Ghash(const uint8_t *h, const string &aad, const string &cipher) {
	uint8_t y[16] = {0};
	auto absorb = [&](const string &data) {
		for (idx_t i = 0; i < data.size(); i += 16) {
			for (idx_t k = 0; k < 16 && i + k < data.size(); k++) {
				y[k] ^= uint8_t(data[i + k]);
			}
			GfMul(y, h);
		}
	};
	absorb(aad);
	absorb(cipher);
	uint8_t lengths[16];
	uint64_t a_bits = uint64_t(aad.size()) * 8, c_bits = uint64_t(cipher.size()) * 8;
	for (int k = 0; k < 8; k++) {
		lengths[k] = uint8_t(a_bits >> (56 - 8 * k));
		lengths[8 + k] = uint8_t(c_bits >> (56 - 8 * k));
	}
	for (int k = 0; k < 16; k++) {
		y[k] ^= lengths[k];
	}
	GfMul(y, h);
	return string(reinterpret_cast<char *>(y), 16);
}

string GcmCrypt(const Aes &aes, const string &iv, const string &input) {
	uint8_t counter[16] = {0};
	memcpy(counter, iv.data(), 12);
	counter[15] = 1;
	string out = input;
	for (idx_t i = 0; i < input.size(); i += 16) {
		// inc32
		for (int k = 15; k >= 12; k--) {
			if (++counter[k] != 0) {
				break;
			}
		}
		uint8_t block[16];
		memcpy(block, counter, 16);
		aes.Encrypt(block);
		for (idx_t k = 0; k < 16 && i + k < input.size(); k++) {
			out[i + k] = char(uint8_t(out[i + k]) ^ block[k]);
		}
	}
	return out;
}

string GcmTag(const Aes &aes, const string &iv, const string &aad, const string &cipher) {
	uint8_t h[16] = {0};
	aes.Encrypt(h);
	auto s = Ghash(h, aad, cipher);
	uint8_t j0[16] = {0};
	memcpy(j0, iv.data(), 12);
	j0[15] = 1;
	aes.Encrypt(j0);
	for (int k = 0; k < 16; k++) {
		s[k] = char(uint8_t(s[k]) ^ j0[k]);
	}
	return s;
}

string Pkcs7Pad(const string &input) {
	auto pad = 16 - input.size() % 16;
	return input + string(pad, char(pad));
}

bool Pkcs7Unpad(string &data) {
	if (data.empty() || data.size() % 16 != 0) {
		return false;
	}
	auto pad = uint8_t(data.back());
	if (pad == 0 || pad > 16 || pad > data.size()) {
		return false;
	}
	for (idx_t k = data.size() - pad; k < data.size(); k++) {
		if (uint8_t(data[k]) != pad) {
			return false;
		}
	}
	data.resize(data.size() - pad);
	return true;
}

string ResolveMode(const string &mode_p, const string &padding_p) {
	auto mode = StringUtil::Upper(mode_p);
	auto padding = StringUtil::Upper(padding_p);
	if (mode == "GCM" && (padding == "DEFAULT" || padding == "NONE")) {
		return "GCM";
	}
	if ((mode == "ECB" || mode == "CBC") && (padding == "DEFAULT" || padding == "PKCS")) {
		return mode;
	}
	throw InvalidInputException("[UNSUPPORTED_FEATURE.AES_MODE] The feature is not supported: AES-%s with the padding "
	                            "%s by the `aes_encrypt`/`aes_decrypt` function.",
	                            mode, padding);
}

string AesEncrypt(const string &input, const string &key, const string &mode_p, const string &padding,
                  const string &iv_p, const string &aad) {
	Aes aes(key);
	auto mode = ResolveMode(mode_p, padding);
	if (mode == "ECB") {
		auto data = Pkcs7Pad(input);
		for (idx_t i = 0; i < data.size(); i += 16) {
			aes.Encrypt(reinterpret_cast<uint8_t *>(&data[i]));
		}
		return data;
	}
	if (mode == "CBC") {
		string iv = iv_p.empty() ? RandomBytes(16) : iv_p;
		if (iv.size() != 16) {
			throw InvalidInputException("[INVALID_PARAMETER_VALUE.AES_IV_LENGTH] CBC requires a 16-byte IV");
		}
		auto data = Pkcs7Pad(input);
		string prev = iv;
		for (idx_t i = 0; i < data.size(); i += 16) {
			for (int k = 0; k < 16; k++) {
				data[i + k] = char(uint8_t(data[i + k]) ^ uint8_t(prev[k]));
			}
			aes.Encrypt(reinterpret_cast<uint8_t *>(&data[i]));
			prev = data.substr(i, 16);
		}
		return iv + data;
	}
	string iv = iv_p.empty() ? RandomBytes(12) : iv_p;
	if (iv.size() != 12) {
		throw InvalidInputException("[INVALID_PARAMETER_VALUE.AES_IV_LENGTH] GCM requires a 12-byte IV");
	}
	auto cipher = GcmCrypt(aes, iv, input);
	return iv + cipher + GcmTag(aes, iv, aad, cipher);
}

bool AesDecrypt(const string &input, const string &key, const string &mode_p, const string &padding, const string &aad,
                string &out) {
	Aes aes(key);
	auto mode = ResolveMode(mode_p, padding);
	if (mode == "ECB") {
		out = input;
		if (out.size() % 16 != 0) {
			return false;
		}
		for (idx_t i = 0; i < out.size(); i += 16) {
			aes.Decrypt(reinterpret_cast<uint8_t *>(&out[i]));
		}
		return Pkcs7Unpad(out);
	}
	if (mode == "CBC") {
		if (input.size() < 32 || input.size() % 16 != 0) {
			return false;
		}
		string prev = input.substr(0, 16);
		out = input.substr(16);
		for (idx_t i = 0; i < out.size(); i += 16) {
			auto block = out.substr(i, 16);
			aes.Decrypt(reinterpret_cast<uint8_t *>(&out[i]));
			for (int k = 0; k < 16; k++) {
				out[i + k] = char(uint8_t(out[i + k]) ^ uint8_t(prev[k]));
			}
			prev = block;
		}
		return Pkcs7Unpad(out);
	}
	if (input.size() < 28) {
		return false;
	}
	auto iv = input.substr(0, 12);
	auto cipher = input.substr(12, input.size() - 28);
	auto tag = input.substr(input.size() - 16);
	if (GcmTag(aes, iv, aad, cipher) != tag) {
		return false;
	}
	out = GcmCrypt(aes, iv, cipher);
	return true;
}

string ArgString(DataChunk &args, idx_t column, idx_t row) {
	auto value = args.data[column].GetValue(row);
	return value.IsNull() ? string() : StringValue::Get(value);
}

bool ArgNull(DataChunk &args, idx_t column, idx_t row) {
	return args.data[column].GetValue(row).IsNull();
}

void AesEncryptKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (ArgNull(args, 0, i) || ArgNull(args, 1, i)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			continue;
		}
		auto encrypted = AesEncrypt(ArgString(args, 0, i), ArgString(args, 1, i), ArgString(args, 2, i),
		                            ArgString(args, 3, i), ArgString(args, 4, i), ArgString(args, 5, i));
		out[i] = StringVector::AddStringOrBlob(result, encrypted);
	}
}

template <bool TRY>
void AesDecryptKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		if (ArgNull(args, 0, i) || ArgNull(args, 1, i)) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			continue;
		}
		string decrypted;
		bool ok;
		try {
			ok = AesDecrypt(ArgString(args, 0, i), ArgString(args, 1, i), ArgString(args, 2, i), ArgString(args, 3, i),
			                ArgString(args, 4, i), decrypted);
		} catch (std::exception &) {
			if (!TRY) {
				throw;
			}
			ok = false;
		}
		if (!ok) {
			if (TRY) {
				FlatVector::ValidityMutable(result).SetInvalid(i);
				continue;
			}
			throw InvalidInputException("[INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR] The value of parameter(s) `expr`, "
			                            "`key` in `aes_decrypt` is invalid: detail message: decryption failed");
		}
		out[i] = StringVector::AddStringOrBlob(result, decrypted);
	}
}

//===----------------------------------------------------------------------===//
// count_min_sketch: org.apache.spark.util.sketch.CountMinSketchImpl
//===----------------------------------------------------------------------===//
struct JavaRandom {
	explicit JavaRandom(int64_t seed) : state((uint64_t(seed) ^ 0x5DEECE66DULL) & ((1ULL << 48) - 1)) {
	}
	uint64_t state;
	int32_t Next(int bits) {
		state = (state * 0x5DEECE66DULL + 0xBULL) & ((1ULL << 48) - 1);
		return int32_t(int64_t(state) >> (48 - bits));
	}
	int32_t NextInt(int32_t bound) {
		if ((bound & -bound) == bound) {
			return int32_t((int64_t(bound) * int64_t(Next(31))) >> 31);
		}
		int32_t bits, val;
		do {
			bits = Next(31);
			val = bits % bound;
		} while (bits - val + (bound - 1) < 0);
		return val;
	}
};

void CountMinSketchKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		auto items = args.data[0].GetValue(row);
		double eps = args.data[1].GetValue(row).GetValue<double>();
		double confidence = args.data[2].GetValue(row).GetValue<double>();
		int32_t seed = args.data[3].GetValue(row).GetValue<int32_t>();
		int32_t width = int32_t(std::ceil(2 / eps));
		int32_t depth = int32_t(std::ceil(-std::log(1 - confidence) / std::log(2)));
		vector<int64_t> hash_a(depth);
		JavaRandom random(seed);
		for (int32_t i = 0; i < depth; i++) {
			hash_a[i] = random.NextInt(NumericLimits<int32_t>::Maximum());
		}
		vector<int64_t> table(size_t(depth) * width, 0);
		int64_t total = 0;
		if (!items.IsNull()) {
			for (auto &item : ListValue::GetChildren(items)) {
				if (item.IsNull()) {
					continue;
				}
				auto &type = item.type();
				if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::BLOB) {
					auto &bytes = StringValue::Get(item);
					auto h1 = int32_t(Murmur3Bytes(bytes, 0));
					auto h2 = int32_t(Murmur3Bytes(bytes, uint32_t(h1)));
					for (int32_t i = 0; i < depth; i++) {
						int32_t h = int32_t(uint32_t(h1) + uint32_t(i) * uint32_t(h2));
						if (h < 0) {
							h = ~h;
						}
						table[size_t(i) * width + h % width]++;
					}
				} else {
					auto value = item.GetValue<int64_t>();
					for (int32_t i = 0; i < depth; i++) {
						int64_t hash = int64_t(uint64_t(hash_a[i]) * uint64_t(value));
						hash += int64_t(uint64_t(hash) >> 32);
						hash &= (1LL << 31) - 1;
						table[size_t(i) * width + (int32_t(hash) % width)]++;
					}
				}
				total++;
			}
		}
		string bytes;
		auto put32 = [&](int32_t v) {
			for (int k = 3; k >= 0; k--) {
				bytes += char((uint32_t(v) >> (8 * k)) & 0xFF);
			}
		};
		auto put64 = [&](int64_t v) {
			for (int k = 7; k >= 0; k--) {
				bytes += char((uint64_t(v) >> (8 * k)) & 0xFF);
			}
		};
		put32(1);
		put64(total);
		put32(depth);
		put32(width);
		for (auto h : hash_a) {
			put64(h);
		}
		for (auto t : table) {
			put64(t);
		}
		out[row] = StringVector::AddStringOrBlob(result, bytes);
	}
}

//===----------------------------------------------------------------------===//
// to_char / to_number (Spark's ToNumberParser format strings)
//===----------------------------------------------------------------------===//
struct NumberFormat {
	string format;
	idx_t int_digits = 0, frac_digits = 0;
	bool has_point = false, leading_zero = false;
	string prefix_currency;
	bool sign_end = false, sign_start = false, mi_end = false, brackets = false;

	explicit NumberFormat(const string &fmt) : format(StringUtil::Upper(fmt)) {
		bool after_point = false;
		for (idx_t i = 0; i < format.size(); i++) {
			char c = format[i];
			if (c == '0' || c == '9') {
				if (after_point) {
					frac_digits++;
				} else {
					if (int_digits == 0 && c == '0') {
						leading_zero = true;
					}
					int_digits++;
				}
			} else if (c == '.' || c == 'D') {
				after_point = true;
				has_point = true;
			} else if (c == 'S') {
				if (i == 0) {
					sign_start = true;
				} else {
					sign_end = true;
				}
			} else if (format.compare(i, 2, "MI") == 0) {
				mi_end = true;
				i++;
			} else if (format.compare(i, 2, "PR") == 0) {
				brackets = true;
				i++;
			}
		}
	}

	//! digits of `value` rendered into the format, or nullopt if they do not fit
	bool Format(const string &number, string &out) const {
		bool negative = !number.empty() && number[0] == '-';
		string v = negative ? number.substr(1) : number;
		auto point = v.find('.');
		string int_part = point == string::npos ? v : v.substr(0, point);
		string frac = point == string::npos ? "" : v.substr(point + 1);
		while (int_part.size() > 1 && int_part[0] == '0') {
			int_part = int_part.substr(1);
		}
		if (int_part == "0" && int_digits > 0) {
			int_part = leading_zero ? "0" : "0";
		}
		if (int_part.size() > int_digits) {
			out.clear();
			for (auto c : format) {
				out += (c == '0' || c == '9') ? '#' : c;
			}
			return true;
		}
		while (frac.size() < frac_digits) {
			frac += '0';
		}
		frac = frac.substr(0, frac_digits);
		// fill the integer digit positions from the right
		string padded = string(int_digits - int_part.size(), ' ') + int_part;
		idx_t int_index = 0, frac_index = 0;
		bool after_point = false;
		bool started = false;
		out.clear();
		for (idx_t i = 0; i < format.size(); i++) {
			char c = format[i];
			if (c == '0' || c == '9') {
				if (after_point) {
					out += frac[frac_index++];
				} else {
					char d = padded[int_index++];
					if (d == ' ' && (c == '0' || leading_zero)) {
						d = '0';
					}
					if (d != ' ') {
						started = true;
					}
					// unused leading positions are spaces: the result has the width of the format, like Spark
					out += d;
				}
			} else if (c == '.' || c == 'D') {
				out += '.';
				after_point = true;
			} else if (c == ',' || c == 'G') {
				out += started ? ',' : ' ';
			} else if (c == '$') {
				out += '$';
			} else if (c == 'S') {
				out += negative ? '-' : '+';
			} else if (format.compare(i, 2, "MI") == 0) {
				out += negative ? '-' : ' ';
				i++;
			} else if (format.compare(i, 2, "PR") == 0) {
				i++;
			} else {
				out += c;
			}
		}
		if (brackets && negative) {
			out = "<" + out + ">";
		}
		// without S / MI / PR in the format, Spark prints the absolute value
		return true;
	}

	//! parses `input`; the result is the unscaled digits and a sign
	bool Parse(const string &input, string &number) const {
		string digits_int, digits_frac;
		bool negative = false, after_point = false;
		idx_t i = 0;
		string s = input;
		if (brackets && s.size() >= 2 && s.front() == '<' && s.back() == '>') {
			negative = true;
			s = s.substr(1, s.size() - 2);
		}
		for (; i < s.size(); i++) {
			char c = s[i];
			if (c >= '0' && c <= '9') {
				(after_point ? digits_frac : digits_int) += c;
			} else if (c == '.') {
				if (!has_point || after_point) {
					return false;
				}
				after_point = true;
			} else if (c == ',') {
				continue;
			} else if (c == '$') {
				if (format.find('$') == string::npos) {
					return false;
				}
			} else if (c == '-') {
				if (!sign_start && !sign_end && !mi_end) {
					return false;
				}
				negative = true;
			} else if (c == '+') {
				if (!sign_start && !sign_end) {
					return false;
				}
			} else if (c == ' ') {
				continue;
			} else {
				return false;
			}
		}
		if (digits_int.size() > int_digits || digits_frac.size() > frac_digits) {
			return false;
		}
		if (digits_int.empty() && digits_frac.empty()) {
			return false;
		}
		number = (negative ? "-" : "") + (digits_int.empty() ? "0" : digits_int) + "." + digits_frac;
		return true;
	}
};

void ToCharKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t number, string_t fmt) -> optional<string_t> {
		    NumberFormat format(fmt.GetString());
		    string out;
		    if (!format.Format(number.GetString(), out)) {
			    return optional<string_t>();
		    }
		    return StringVector::AddString(result, out);
	    });
}

template <bool TRY>
void ToNumberKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, string_t fmt) -> optional<string_t> {
		    NumberFormat format(fmt.GetString());
		    string number;
		    if (!format.Parse(input.GetString(), number)) {
			    if (TRY) {
				    return optional<string_t>();
			    }
			    throw InvalidInputException("[INVALID_FORMAT.MISMATCH_INPUT] The input '%s' does not match the format "
			                                "'%s'.",
			                                input.GetString(), fmt.GetString());
		    }
		    return StringVector::AddString(result, number);
	    });
}

//===----------------------------------------------------------------------===//
// XML + XPath subset: location paths (/, //, *, text(), @attr, [n]), sum(), count()
//===----------------------------------------------------------------------===//
struct XmlNode {
	string name; // element name, "#text" or "@attr"
	string value;
	vector<unique_ptr<XmlNode>> children;
	vector<unique_ptr<XmlNode>> attributes;

	string StringValue() const {
		if (name == "#text" || (!name.empty() && name[0] == '@')) {
			return value;
		}
		string out;
		for (auto &child : children) {
			out += child->StringValue();
		}
		return out;
	}
};

struct XmlParser {
	explicit XmlParser(const string &s) : text(s) {
	}
	const string &text;
	idx_t pos = 0;

	static string Decode(const string &s) {
		string out;
		for (idx_t i = 0; i < s.size(); i++) {
			if (s[i] == '&') {
				auto end = s.find(';', i);
				if (end != string::npos) {
					auto entity = s.substr(i + 1, end - i - 1);
					if (entity == "lt") {
						out += '<';
					} else if (entity == "gt") {
						out += '>';
					} else if (entity == "amp") {
						out += '&';
					} else if (entity == "quot") {
						out += '"';
					} else if (entity == "apos") {
						out += '\'';
					} else {
						out += s.substr(i, end - i + 1);
					}
					i = end;
					continue;
				}
			}
			out += s[i];
		}
		return out;
	}

	unique_ptr<XmlNode> ParseDocument() {
		auto root = make_uniq<XmlNode>();
		root->name = "#document";
		while (pos < text.size()) {
			SkipMisc();
			if (pos >= text.size()) {
				break;
			}
			if (text[pos] != '<') {
				throw InvalidInputException("Invalid XML document");
			}
			root->children.push_back(ParseElement());
			SkipMisc();
		}
		if (root->children.empty()) {
			throw InvalidInputException("Invalid XML document");
		}
		return root;
	}
	void SkipMisc() {
		while (pos < text.size()) {
			if (isspace((unsigned char)text[pos])) {
				pos++;
			} else if (text.compare(pos, 2, "<?") == 0) {
				pos = text.find("?>", pos) + 2;
			} else if (text.compare(pos, 4, "<!--") == 0) {
				pos = text.find("-->", pos) + 3;
			} else {
				break;
			}
		}
	}
	unique_ptr<XmlNode> ParseElement() {
		pos++; // '<'
		auto node = make_uniq<XmlNode>();
		while (pos < text.size() && !isspace((unsigned char)text[pos]) && text[pos] != '>' && text[pos] != '/') {
			node->name += text[pos++];
		}
		while (true) {
			while (pos < text.size() && isspace((unsigned char)text[pos])) {
				pos++;
			}
			if (pos >= text.size()) {
				throw InvalidInputException("Invalid XML document");
			}
			if (text[pos] == '/') {
				pos += 2;
				return node;
			}
			if (text[pos] == '>') {
				pos++;
				break;
			}
			string attr;
			while (pos < text.size() && text[pos] != '=' && !isspace((unsigned char)text[pos])) {
				attr += text[pos++];
			}
			while (pos < text.size() && text[pos] != '"' && text[pos] != '\'') {
				pos++;
			}
			char quote = text[pos++];
			auto end = text.find(quote, pos);
			auto attribute = make_uniq<XmlNode>();
			attribute->name = "@" + attr;
			attribute->value = Decode(text.substr(pos, end - pos));
			node->attributes.push_back(std::move(attribute));
			pos = end + 1;
		}
		while (pos < text.size()) {
			if (text.compare(pos, 2, "</") == 0) {
				pos = text.find('>', pos) + 1;
				return node;
			}
			if (text.compare(pos, 4, "<!--") == 0) {
				pos = text.find("-->", pos) + 3;
				continue;
			}
			if (text.compare(pos, 9, "<![CDATA[") == 0) {
				auto end = text.find("]]>", pos);
				auto t = make_uniq<XmlNode>();
				t->name = "#text";
				t->value = text.substr(pos + 9, end - pos - 9);
				node->children.push_back(std::move(t));
				pos = end + 3;
				continue;
			}
			if (text[pos] == '<') {
				node->children.push_back(ParseElement());
				continue;
			}
			auto end = text.find('<', pos);
			auto t = make_uniq<XmlNode>();
			t->name = "#text";
			t->value = Decode(text.substr(pos, end - pos));
			node->children.push_back(std::move(t));
			pos = end;
		}
		throw InvalidInputException("Invalid XML document");
	}
};

using NodeSet = vector<const XmlNode *>;

void Descendants(const XmlNode &node, NodeSet &out) {
	for (auto &child : node.children) {
		out.push_back(child.get());
		Descendants(*child, out);
	}
}

NodeSet EvaluatePath(const XmlNode &document, const string &path_p) {
	string path = path_p;
	StringUtil::Trim(path);
	NodeSet current = {&document};
	idx_t i = 0;
	bool descendant = false;
	if (StringUtil::StartsWith(path, "//")) {
		descendant = true;
		i = 2;
	} else if (StringUtil::StartsWith(path, "/")) {
		i = 1;
	}
	while (i <= path.size()) {
		auto end = path.find('/', i);
		string step = path.substr(i, end == string::npos ? string::npos : end - i);
		int64_t index = -1;
		auto bracket = step.find('[');
		if (bracket != string::npos) {
			index = std::stoll(step.substr(bracket + 1, step.find(']') - bracket - 1));
			step = step.substr(0, bracket);
		}
		NodeSet next;
		for (auto node : current) {
			NodeSet candidates;
			if (descendant) {
				Descendants(*node, candidates);
			} else {
				for (auto &child : node->children) {
					candidates.push_back(child.get());
				}
			}
			NodeSet matched;
			if (!step.empty() && step[0] == '@') {
				NodeSet owners = descendant ? candidates : NodeSet {node};
				for (auto owner : owners) {
					for (auto &attr : owner->attributes) {
						if (step == "@*" || attr->name == step) {
							matched.push_back(attr.get());
						}
					}
				}
			} else {
				for (auto candidate : candidates) {
					bool is_text = candidate->name == "#text";
					if ((step == "text()" && is_text) || (step == "node()") ||
					    (!is_text && (step == "*" || candidate->name == step))) {
						matched.push_back(candidate);
					}
				}
			}
			if (index > 0) {
				if (idx_t(index) <= matched.size()) {
					next.push_back(matched[index - 1]);
				}
			} else {
				next.insert(next.end(), matched.begin(), matched.end());
			}
		}
		current = next;
		descendant = false;
		if (end == string::npos) {
			break;
		}
		i = end + 1;
		if (i < path.size() && path[i] == '/') {
			descendant = true;
			i++;
		}
	}
	return current;
}

double ToNumber(const string &s) {
	string v = s;
	StringUtil::Trim(v);
	char *end;
	double d = strtod(v.c_str(), &end);
	if (v.empty() || *end != '\0') {
		return std::nan("");
	}
	return d;
}

//! evaluates an XPath expression to a number (sum(), count(), or the first node's value)
double EvaluateNumber(const XmlNode &document, const string &expr_p) {
	string expr = expr_p;
	StringUtil::Trim(expr);
	if (StringUtil::StartsWith(expr, "sum(") && expr.back() == ')') {
		double total = 0;
		for (auto node : EvaluatePath(document, expr.substr(4, expr.size() - 5))) {
			total += ToNumber(node->StringValue());
		}
		return total;
	}
	if (StringUtil::StartsWith(expr, "count(") && expr.back() == ')') {
		return double(EvaluatePath(document, expr.substr(6, expr.size() - 7)).size());
	}
	auto nodes = EvaluatePath(document, expr);
	if (nodes.empty()) {
		return std::nan("");
	}
	return ToNumber(nodes[0]->StringValue());
}

void XPathListKernel(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	vector<vector<pair<bool, string>>> rows(count);
	vector<bool> valid(count, true);
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		auto xml = args.data[0].GetValue(i);
		auto path = args.data[1].GetValue(i);
		if (xml.IsNull() || path.IsNull()) {
			valid[i] = false;
			continue;
		}
		auto document = XmlParser(StringValue::Get(xml)).ParseDocument();
		for (auto node : EvaluatePath(*document, StringValue::Get(path))) {
			// org.w3c.dom.Node.getNodeValue(): null for elements
			bool has_value = node->name == "#text" || node->name[0] == '@';
			rows[i].emplace_back(has_value, node->value);
		}
		total += rows[i].size();
	}
	ListVector::Reserve(result, total);
	auto &child = ListVector::GetChildMutable(result);
	auto data = FlatVector::GetDataMutable<string_t>(child);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!valid[i]) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		entries[i] = list_entry_t(offset, rows[i].size());
		for (auto &item : rows[i]) {
			if (item.first) {
				data[offset] = StringVector::AddString(child, item.second);
			} else {
				FlatVector::ValidityMutable(child).SetInvalid(offset);
			}
			offset++;
		}
	}
	ListVector::SetListSize(result, total);
}

void XPathStringKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(args.data[0], args.data[1], result, args.size(),
	                                                      [&](string_t xml, string_t path) {
		                                                      auto document =
		                                                          XmlParser(xml.GetString()).ParseDocument();
		                                                      auto nodes = EvaluatePath(*document, path.GetString());
		                                                      string value = nodes.empty() ? "" : nodes[0]->StringValue();
		                                                      return StringVector::AddString(result, value);
	                                                      });
}

void XPathBooleanKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, bool>(args.data[0], args.data[1], result, args.size(),
	                                                  [&](string_t xml, string_t path) {
		                                                  auto document = XmlParser(xml.GetString()).ParseDocument();
		                                                  return !EvaluatePath(*document, path.GetString()).empty();
	                                                  });
}

void XPathNumberKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, double>(args.data[0], args.data[1], result, args.size(),
	                                                    [&](string_t xml, string_t path) {
		                                                    auto document = XmlParser(xml.GetString()).ParseDocument();
		                                                    return EvaluateNumber(*document, path.GetString());
	                                                    });
}

} // namespace

void RegisterSparkExtras(ExtensionLoader &loader) {
	{
		string values;
		for (auto &entry : SPARK_SQL_KEYWORDS) {
			values += string(values.empty() ? "" : ", ") + "('" + entry.keyword + "', " + (entry.reserved ? "true" : "false") + ")";
		}
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery("CREATE MACRO sql_keywords() AS TABLE SELECT * FROM (VALUES " + values +
		                  ") AS t(keyword, reserved)");
		auto &create = parser.statements[0]->Cast<CreateStatement>();
		auto &info = create.info->Cast<CreateMacroInfo>();
		info.SetQualifiedName(QualifiedName({Identifier(SPARK_SCHEMA)}, Identifier("sql_keywords")));
		info.internal = true;
		loader.RegisterFunction(info);
	}
	// Spark's range() table function: one BIGINT column named id (DuckDB's is named range); one macro per
	// argument count (the tokenizer picks it), the optional 4th argument (numPartitions) is ignored
	for (auto &def : vector<pair<string, string>> {
	         {"spark_range_1(a)", "range(CAST(a AS BIGINT))"},
	         {"spark_range_2(a, b)", "range(CAST(a AS BIGINT), CAST(b AS BIGINT))"},
	         {"spark_range_3(a, b, c)", "range(CAST(a AS BIGINT), CAST(b AS BIGINT), CAST(c AS BIGINT))"},
	         {"spark_range_4(a, b, c, d)", "range(CAST(a AS BIGINT), CAST(b AS BIGINT), CAST(c AS BIGINT))"}}) {
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery("CREATE MACRO " + def.first + " AS TABLE SELECT range AS id FROM " + def.second);
		auto &create = parser.statements[0]->Cast<CreateStatement>();
		auto &info = create.info->Cast<CreateMacroInfo>();
		info.SetQualifiedName(QualifiedName({Identifier(SPARK_SCHEMA)}, Identifier(def.first.substr(0, 13))));
		info.internal = true;
		loader.RegisterFunction(info);
	}
	auto blob = LogicalType::BLOB;
	auto varchar = LogicalType::VARCHAR;
	ScalarFunction encrypt(Identifier("aes_encrypt_impl"), {blob, blob, varchar, varchar, blob, blob}, blob,
	                       AesEncryptKernel);
	encrypt.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	encrypt.SetVolatile();
	RegisterFallible(loader, encrypt);
	ScalarFunction decrypt(Identifier("aes_decrypt_impl"), {blob, blob, varchar, varchar, blob}, blob,
	                       AesDecryptKernel<false>);
	decrypt.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	RegisterFallible(loader, decrypt);
	ScalarFunction try_decrypt(Identifier("try_aes_decrypt_impl"), {blob, blob, varchar, varchar, blob}, blob,
	                           AesDecryptKernel<true>);
	try_decrypt.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	RegisterFallible(loader, try_decrypt);
	RegisterFallible(loader, ScalarFunction(Identifier("count_min_sketch_impl"),
	                                       {LogicalType::LIST(LogicalType::ANY), LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                        LogicalType::INTEGER},
	                                       blob, CountMinSketchKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("to_char_impl"), {varchar, varchar}, varchar, ToCharKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("to_number_impl"), {varchar, varchar}, varchar, ToNumberKernel<false>));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("try_to_number_impl"), {varchar, varchar}, varchar, ToNumberKernel<true>));
	RegisterFallible(loader, ScalarFunction(Identifier("xpath"), {varchar, varchar}, LogicalType::LIST(varchar),
	                                       XPathListKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("xpath_string"), {varchar, varchar}, varchar, XPathStringKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("xpath_boolean"), {varchar, varchar}, LogicalType::BOOLEAN, XPathBooleanKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("xpath_number_impl"), {varchar, varchar}, LogicalType::DOUBLE, XPathNumberKernel));
	for (auto name : {"xpath", "xpath_string", "xpath_boolean"}) {
		AddSparkOverride(name);
	}
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
// ANSI intervals: Spark's year-month and day-time intervals are both DuckDB INTERVALs here
//===----------------------------------------------------------------------===//
namespace duckdb {

string FormatDatetimeValue(timestamp_t ts, const string &pattern);
string SparkScalarString(const Value &v);

namespace {

bool IsYearMonth(const interval_t &i) {
	return i.days == 0 && i.micros == 0;
}

//! a number for an interval: months for year-month intervals, microseconds for day-time intervals
double IntervalNumber(const interval_t &i) {
	if (IsYearMonth(i)) {
		return double(i.months);
	}
	return double(i.days) * 86400e6 + double(i.micros) + double(i.months) * 30 * 86400e6;
}

void IntervalDivKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<interval_t, interval_t, int64_t>(
	    args.data[0], args.data[1], result, args.size(), [](interval_t a, interval_t b) -> optional<int64_t> {
		    double d = IntervalNumber(b);
		    if (d == 0) {
			    return optional<int64_t>();
		    }
		    return int64_t(std::trunc(IntervalNumber(a) / d));
	    });
}

void IntervalDivideKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<interval_t, double, interval_t>(
	    args.data[0], args.data[1], result, args.size(), [](interval_t a, double n) -> optional<interval_t> {
		    if (n == 0) {
			    return optional<interval_t>();
		    }
		    interval_t r;
		    if (IsYearMonth(a)) {
			    r.months = int32_t(std::round(double(a.months) / n));
			    r.days = 0;
			    r.micros = 0;
		    } else {
			    double micros = (double(a.days) * 86400e6 + double(a.micros)) / n;
			    r.months = 0;
			    r.days = 0;
			    r.micros = int64_t(std::round(micros));
		    }
		    return r;
	    });
}

void IntervalNumberKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<interval_t, double>(args.data[0], result, args.size(),
	                                           [](interval_t i) { return IntervalNumber(i); });
}

//! back from IntervalNumber, using `sample` to tell year-month from day-time
void NumberToIntervalKernel(DataChunk &args, ExpressionState &, Vector &result) {
	BinaryExecutor::Execute<double, interval_t, interval_t>(args.data[0], args.data[1], result, args.size(),
	                                                        [](double n, interval_t sample) {
		                                                        interval_t r;
		                                                        if (IsYearMonth(sample)) {
			                                                        r.months = int32_t(std::round(n));
			                                                        r.days = 0;
			                                                        r.micros = 0;
		                                                        } else {
			                                                        r.months = 0;
			                                                        r.days = 0;
			                                                        r.micros = int64_t(std::round(n));
		                                                        }
		                                                        return r;
	                                                        });
}

void NumbersToIntervalsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
	vector<vector<Value>> rows(count);
	vector<bool> valid(count, true);
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		auto list = args.data[0].GetValue(i);
		auto sample = args.data[1].GetValue(i);
		if (list.IsNull()) {
			valid[i] = false;
			continue;
		}
		bool year_month = !sample.IsNull() && IsYearMonth(sample.GetValue<interval_t>());
		for (auto &n : ListValue::GetChildren(list)) {
			if (n.IsNull()) {
				rows[i].emplace_back(LogicalType::INTERVAL);
				continue;
			}
			auto d = n.GetValue<double>();
			interval_t r;
			r.months = year_month ? int32_t(std::round(d)) : 0;
			r.days = 0;
			r.micros = year_month ? 0 : int64_t(std::round(d));
			rows[i].push_back(Value::INTERVAL(r));
		}
		total += rows[i].size();
	}
	ListVector::Reserve(result, total);
	auto &child = ListVector::GetChildMutable(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!valid[i]) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			entries[i] = list_entry_t(offset, 0);
			continue;
		}
		entries[i] = list_entry_t(offset, rows[i].size());
		for (auto &v : rows[i]) {
			child.SetValue(offset++, v);
		}
	}
	ListVector::SetListSize(result, total);
}

void IntervalIsYearMonthKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<interval_t, bool>(args.data[0], result, args.size(),
	                                         [](interval_t i) { return IsYearMonth(i) && i.months != 0; });
}

void IntervalSignKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<interval_t, double>(args.data[0], result, args.size(), [](interval_t i) {
		auto n = IntervalNumber(i);
		return n > 0 ? 1.0 : (n < 0 ? -1.0 : 0.0);
	});
}

void IntervalAbsKernel(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<interval_t, interval_t>(args.data[0], result, args.size(), [](interval_t i) {
		if (IntervalNumber(i) < 0) {
			i.months = -i.months;
			i.days = -i.days;
			i.micros = -i.micros;
		}
		return i;
	});
}

//===----------------------------------------------------------------------===//
// to_json with Spark's formatting (timestamps, null fields dropped, options)
//===----------------------------------------------------------------------===//
string JsonEscape(const string &s) {
	string out = "\"";
	for (auto c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if ((unsigned char)c < 0x20) {
				out += StringUtil::Format("\\u%04x", (unsigned char)c);
			} else {
				out += c;
			}
		}
	}
	return out + "\"";
}

string ToJson(const Value &v, const string &ts_format, const string &date_format) {
	if (v.IsNull()) {
		return "null";
	}
	auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		string out = "{";
		auto &children = StructValue::GetChildren(v);
		auto &fields = StructType::GetChildTypes(type);
		bool first = true;
		for (idx_t i = 0; i < children.size(); i++) {
			if (children[i].IsNull()) {
				continue;
			}
			out += (first ? "" : ",") + JsonEscape(fields[i].first.GetIdentifierName()) + ":" +
			       ToJson(children[i], ts_format, date_format);
			first = false;
		}
		return out + "}";
	}
	case LogicalTypeId::MAP: {
		string out = "{";
		bool first = true;
		for (auto &entry : ListValue::GetChildren(v)) {
			auto &kv = StructValue::GetChildren(entry);
			string key;
			if (kv[0].type().id() == LogicalTypeId::STRUCT) {
				key = "[";
				auto &kc = StructValue::GetChildren(kv[0]);
				for (idx_t i = 0; i < kc.size(); i++) {
					key += (i > 0 ? "," : "") + SparkScalarString(kc[i]);
				}
				key += "]";
			} else {
				key = SparkScalarString(kv[0]);
			}
			out += (first ? "" : ",") + JsonEscape(key) + ":" + ToJson(kv[1], ts_format, date_format);
			first = false;
		}
		return out + "}";
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		string out = "[";
		auto &children = ListValue::GetChildren(v);
		for (idx_t i = 0; i < children.size(); i++) {
			out += (i > 0 ? "," : "") + ToJson(children[i], ts_format, date_format);
		}
		return out + "]";
	}
	case LogicalTypeId::VARCHAR:
		return JsonEscape(StringValue::Get(v));
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return JsonEscape(FormatDatetimeValue(v.GetValue<timestamp_t>(), ts_format));
	case LogicalTypeId::DATE:
		return JsonEscape(FormatDatetimeValue(Timestamp::FromDatetime(v.GetValue<date_t>(), dtime_t(0)), date_format));
	case LogicalTypeId::BOOLEAN:
		return v.GetValue<bool>() ? "true" : "false";
	default:
		return SparkScalarString(v);
	}
}

void ToJsonKernel(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto value = args.data[0].GetValue(i);
		if (value.IsNull()) {
			FlatVector::ValidityMutable(result).SetInvalid(i);
			continue;
		}
		auto ts_format = StringValue::Get(args.data[1].GetValue(i));
		auto date_format = StringValue::Get(args.data[2].GetValue(i));
		out[i] = StringVector::AddString(result, ToJson(value, ts_format, date_format));
	}
}

} // namespace

void RegisterSparkIntervals(ExtensionLoader &loader) {
	auto interval = LogicalType::INTERVAL;
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("interval_div"), {interval, interval}, LogicalType::BIGINT, IntervalDivKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("interval_divide"), {interval, LogicalType::DOUBLE}, interval,
	                                       IntervalDivideKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("interval_number"), {interval}, LogicalType::DOUBLE, IntervalNumberKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("number_to_interval"), {LogicalType::DOUBLE, interval}, interval,
	                                       NumberToIntervalKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("numbers_to_intervals"),
	                                       {LogicalType::LIST(LogicalType::DOUBLE), interval}, LogicalType::LIST(interval),
	                                       NumbersToIntervalsKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("interval_is_year_month"), {interval}, LogicalType::BOOLEAN,
	                                       IntervalIsYearMonthKernel));
	RegisterFallible(loader, 
	    ScalarFunction(Identifier("interval_sign"), {interval}, LogicalType::DOUBLE, IntervalSignKernel));
	RegisterFallible(loader, ScalarFunction(Identifier("interval_abs"), {interval}, interval, IntervalAbsKernel));
	ScalarFunction to_json(Identifier("to_json_impl"), {LogicalType::ANY, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       LogicalType::VARCHAR, ToJsonKernel);
	to_json.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	RegisterFallible(loader, to_json);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
// Casts: positional STRUCT casts (Spark matches fields by position) and CHAR(n) columns
//===----------------------------------------------------------------------===//
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace {

//! CHAR(n): blank-padded to n characters on write; longer values are an error like in Spark
struct CharCastData : public BoundCastData {
	explicit CharCastData(idx_t length_p) : length(length_p) {
	}
	idx_t length;
	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<CharCastData>(length);
	}
};

idx_t Utf8Chars(const string &s) {
	idx_t n = 0;
	for (auto c : s) {
		if ((uint8_t(c) & 0xC0) != 0x80) {
			n++;
		}
	}
	return n;
}

bool VarcharToChar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto length = parameters.cast_data->Cast<CharCastData>().length;
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t s) {
		auto str = s.GetString();
		while (!str.empty() && str.back() == ' ' && Utf8Chars(str) > length) {
			str.pop_back();
		}
		auto chars = Utf8Chars(str);
		if (chars > length) {
			throw InvalidInputException("[EXCEED_LIMIT_LENGTH] Exceeds char/varchar type length limitation: %llu",
			                            (unsigned long long)length);
		}
		str += string(length - chars, ' ');
		return StringVector::AddString(result, str);
	});
	return true;
}

} // namespace

LogicalType SparkCharType(idx_t length) {
	return LogicalType(LogicalTypeId::VARCHAR,
	                   make_uniq<LogicalTypeInfo>(LogicalTypeInfoType::GENERIC_TYPE_INFO, "spark_char_" + to_string(length)));
}

void RegisterSparkCasts(ExtensionLoader &loader) {
	for (idx_t n = 1; n <= 255; n++) {
		auto char_type = SparkCharType(n);
		loader.RegisterType("spark_char_" + to_string(n), char_type);
		loader.RegisterCastFunction(LogicalType::VARCHAR, char_type,
		                            BoundCastInfo(VarcharToChar, make_uniq<CharCastData>(n)), 1000);
		loader.RegisterCastFunction(char_type, LogicalType::VARCHAR, DefaultCasts::ReinterpretCast, 1);
	}
}

} // namespace duckdb
