// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "checksum.hpp"

#include <endian.h>
#include <openssl/evp.h>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include <array>
#include <cstring>
#include <stdexcept>

namespace kvrgw {

namespace {

// ---------------------------------------------------------------------------
// Reflected CRC tables (slice-by-8) and GF(2) combine, generic over width.
// ---------------------------------------------------------------------------

constexpr uint32_t kPolyCrc32  = 0xEDB88320u;
constexpr uint32_t kPolyCrc32c = 0x82F63B78u;
constexpr uint64_t kPolyCrc64N = 0x9A6C9329AC4BC9B5ull;

template <typename T>
struct CrcTables {
  static constexpr size_t kBits = sizeof(T) * 8;
  T t[8][256];
  T x2n[kBits];  // x^(2^k) mod P, reflected
  T poly;

  explicit CrcTables(T p) : poly(p) {
    for (int i = 0; i < 256; ++i) {
      T c = static_cast<T>(i);
      for (int k = 0; k < 8; ++k) {
        c = (c & 1) ? (c >> 1) ^ p : (c >> 1);
      }
      t[0][i] = c;
    }
    for (int i = 0; i < 256; ++i) {
      for (int s = 1; s < 8; ++s) {
        T prev = t[s - 1][i];
        t[s][i] = (prev >> 8) ^ t[0][prev & 0xff];
      }
    }
    T pw = static_cast<T>(1) << (kBits - 2);  // x^1
    x2n[0] = pw;
    for (size_t k = 1; k < kBits; ++k) {
      pw = multmodp(pw, pw);
      x2n[k] = pw;
    }
  }

  // a * b mod P (reflected representation; x^0 == top bit).
  T multmodp(T a, T b) const {
    T m = static_cast<T>(1) << (kBits - 1);
    T p = 0;
    for (;;) {
      if (a & m) {
        p ^= b;
        if ((a & (m - 1)) == 0) {
          break;
        }
      }
      m >>= 1;
      b = (b & 1) ? (b >> 1) ^ poly : (b >> 1);
    }
    return p;
  }

  // x^(8*n) mod P
  T x8nmodp(uint64_t n) const {
    T p = static_cast<T>(1) << (kBits - 1);  // x^0
    size_t k = 3;
    while (n) {
      if (n & 1) {
        p = multmodp(x2n[k % kBits], p);
      }
      n >>= 1;
      ++k;
    }
    return p;
  }

  T combine(T crc1, T crc2, uint64_t len2) const {
    return multmodp(x8nmodp(len2), crc1) ^ crc2;
  }
};

const CrcTables<uint32_t>& tab_crc32() {
  static const CrcTables<uint32_t> t(kPolyCrc32);
  return t;
}
const CrcTables<uint32_t>& tab_crc32c() {
  static const CrcTables<uint32_t> t(kPolyCrc32c);
  return t;
}
const CrcTables<uint64_t>& tab_crc64n() {
  static const CrcTables<uint64_t> t(kPolyCrc64N);
  return t;
}

// Raw running state (no init/xorout applied); caller inverts at start/end.
uint32_t crc32_update(const CrcTables<uint32_t>& tb, uint32_t c, const uint8_t* p, size_t n) {
  while (n >= 8) {
    uint32_t lo, hi;
    std::memcpy(&lo, p, 4);
    std::memcpy(&hi, p + 4, 4);
    lo = le32toh(lo) ^ c;
    hi = le32toh(hi);
    c = tb.t[7][lo & 0xff] ^ tb.t[6][(lo >> 8) & 0xff] ^
        tb.t[5][(lo >> 16) & 0xff] ^ tb.t[4][lo >> 24] ^
        tb.t[3][hi & 0xff] ^ tb.t[2][(hi >> 8) & 0xff] ^
        tb.t[1][(hi >> 16) & 0xff] ^ tb.t[0][hi >> 24];
    p += 8;
    n -= 8;
  }
  while (n--) {
    c = tb.t[0][(c ^ *p++) & 0xff] ^ (c >> 8);
  }
  return c;
}

uint64_t crc64_update(const CrcTables<uint64_t>& tb, uint64_t c, const uint8_t* p, size_t n) {
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    v = le64toh(v) ^ c;
    c = tb.t[7][v & 0xff] ^ tb.t[6][(v >> 8) & 0xff] ^
        tb.t[5][(v >> 16) & 0xff] ^ tb.t[4][(v >> 24) & 0xff] ^
        tb.t[3][(v >> 32) & 0xff] ^ tb.t[2][(v >> 40) & 0xff] ^
        tb.t[1][(v >> 48) & 0xff] ^ tb.t[0][v >> 56];
    p += 8;
    n -= 8;
  }
  while (n--) {
    c = tb.t[0][(c ^ *p++) & 0xff] ^ (c >> 8);
  }
  return c;
}

void store_be(uint64_t v, uint8_t* out, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    out[i] = static_cast<uint8_t>(v >> (8 * (len - 1 - i)));
  }
}

uint64_t load_be(const uint8_t* in, size_t len) {
  uint64_t v = 0;
  for (size_t i = 0; i < len; ++i) {
    v = (v << 8) | in[i];
  }
  return v;
}

bool is_crc(CksumAlg a) {
  return a == CksumAlg::CRC32 || a == CksumAlg::CRC32C || a == CksumAlg::CRC64NVME;
}

// EVP-backed algorithms; nullptr if `a` is not one.
const EVP_MD* evp_md(CksumAlg a) {
  switch (a) {
    case CksumAlg::SHA1:   return EVP_sha1();
    case CksumAlg::SHA256: return EVP_sha256();
    case CksumAlg::SHA512: return EVP_sha512();
    case CksumAlg::MD5:    return EVP_md5();
    default:               return nullptr;
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// Names / metadata
// ---------------------------------------------------------------------------

size_t cksum_digest_len(CksumAlg alg) {
  switch (alg) {
    case CksumAlg::CRC32:     return 4;
    case CksumAlg::CRC32C:    return 4;
    case CksumAlg::SHA1:      return 20;
    case CksumAlg::SHA256:    return 32;
    case CksumAlg::CRC64NVME: return 8;
    case CksumAlg::SHA512:    return 64;
    case CksumAlg::MD5:       return 16;
    case CksumAlg::XXHash3:   return 8;
    case CksumAlg::XXHash64:  return 8;
    case CksumAlg::XXHash128: return 16;
    default:                  return 0;
  }
}

CksumAlg cksum_alg_from_name(std::string_view n) {
  if (n == "CRC32")     return CksumAlg::CRC32;
  if (n == "CRC32C")    return CksumAlg::CRC32C;
  if (n == "SHA1")      return CksumAlg::SHA1;
  if (n == "SHA256")    return CksumAlg::SHA256;
  if (n == "CRC64NVME") return CksumAlg::CRC64NVME;
  if (n == "SHA512")    return CksumAlg::SHA512;
  if (n == "MD5")       return CksumAlg::MD5;
  if (n == "XXHASH3")   return CksumAlg::XXHash3;
  if (n == "XXHASH64")  return CksumAlg::XXHash64;
  if (n == "XXHASH128") return CksumAlg::XXHash128;
  return CksumAlg::NONE;
}

CksumType cksum_type_from_name(std::string_view n) {
  if (n == "COMPOSITE")   return CksumType::COMPOSITE;
  if (n == "FULL_OBJECT") return CksumType::FULL_OBJECT;
  return CksumType::NONE;
}

const char* cksum_alg_name(CksumAlg alg) {
  switch (alg) {
    case CksumAlg::CRC32:     return "CRC32";
    case CksumAlg::CRC32C:    return "CRC32C";
    case CksumAlg::SHA1:      return "SHA1";
    case CksumAlg::SHA256:    return "SHA256";
    case CksumAlg::CRC64NVME: return "CRC64NVME";
    case CksumAlg::SHA512:    return "SHA512";
    case CksumAlg::MD5:       return "MD5";
    case CksumAlg::XXHash3:   return "XXHASH3";
    case CksumAlg::XXHash64:  return "XXHASH64";
    case CksumAlg::XXHash128: return "XXHASH128";
    default:                  return "";
  }
}

const char* cksum_type_name(CksumType type) {
  switch (type) {
    case CksumType::COMPOSITE:   return "COMPOSITE";
    case CksumType::FULL_OBJECT: return "FULL_OBJECT";
    default:                     return "";
  }
}

bool cksum_type_valid_for_alg(CksumAlg alg, CksumType type) {
  if (alg == CksumAlg::NONE || static_cast<uint8_t>(alg) > kCksumAlgMax) {
    return false;
  }
  if (alg == CksumAlg::CRC64NVME) {
    return type == CksumType::FULL_OBJECT;
  }
  if (alg == CksumAlg::CRC32 || alg == CksumAlg::CRC32C) {
    return type == CksumType::COMPOSITE || type == CksumType::FULL_OBJECT;
  }
  return type == CksumType::COMPOSITE;
}

CksumType cksum_default_type(CksumAlg alg) {
  switch (alg) {
    case CksumAlg::CRC64NVME: return CksumType::FULL_OBJECT;
    case CksumAlg::NONE:      return CksumType::NONE;
    default:                  return CksumType::COMPOSITE;
  }
}

// ---------------------------------------------------------------------------
// Base64
// ---------------------------------------------------------------------------

static const char kB64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

size_t base64_encode_to(const uint8_t* d, size_t len, char* out) {
  char* o = out;
  size_t i = 0;
  for (; i + 3 <= len; i += 3) {
    uint32_t v = (d[i] << 16) | (d[i + 1] << 8) | d[i + 2];
    *o++ = kB64[(v >> 18) & 63];
    *o++ = kB64[(v >> 12) & 63];
    *o++ = kB64[(v >> 6) & 63];
    *o++ = kB64[v & 63];
  }
  if (len - i == 1) {
    uint32_t v = d[i] << 16;
    *o++ = kB64[(v >> 18) & 63];
    *o++ = kB64[(v >> 12) & 63];
    *o++ = '=';
    *o++ = '=';
  } else if (len - i == 2) {
    uint32_t v = (d[i] << 16) | (d[i + 1] << 8);
    *o++ = kB64[(v >> 18) & 63];
    *o++ = kB64[(v >> 12) & 63];
    *o++ = kB64[(v >> 6) & 63];
    *o++ = '=';
  }
  return static_cast<size_t>(o - out);
}

std::string base64_encode(const uint8_t* d, size_t len) {
  std::string out((len + 2) / 3 * 4, '\0');
  out.resize(base64_encode_to(d, len, out.data()));
  return out;
}

size_t cksum_render(CksumType type, const uint8_t* raw, size_t n, uint32_t parts, char* out) {
  size_t w = base64_encode_to(raw, n, out);
  if (type == CksumType::COMPOSITE) {
    out[w++] = '-';
    char tmp[12];
    size_t t = 0;
    do {
      tmp[t++] = static_cast<char>('0' + parts % 10);
      parts /= 10;
    } while (parts != 0);
    while (t > 0) {
      out[w++] = tmp[--t];
    }
  }
  return w;
}

bool base64_decode_to(std::string_view in, uint8_t* out, size_t cap, size_t* n) {
  *n = 0;
  if (in.size() % 4 != 0 || in.size() / 4 * 3 > cap + 2) {
    return false;
  }
  auto val = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
  };
  size_t w = 0;
  auto put = [&](int v) -> bool {
    if (w >= cap) return false;
    out[w++] = static_cast<uint8_t>(v);
    return true;
  };
  for (size_t i = 0; i < in.size(); i += 4) {
    int a = val(in[i]), b = val(in[i + 1]);
    if (a < 0 || b < 0) return false;
    bool last = (i + 4 == in.size());
    if (in[i + 2] == '=') {
      if (!last || in[i + 3] != '=' || (b & 0x0f)) return false;
      if (!put((a << 2) | (b >> 4))) return false;
      continue;
    }
    int c = val(in[i + 2]);
    if (c < 0) return false;
    if (in[i + 3] == '=') {
      if (!last || (c & 0x03)) return false;
      if (!put((a << 2) | (b >> 4)) || !put(((b & 0x0f) << 4) | (c >> 2))) return false;
      continue;
    }
    int d = val(in[i + 3]);
    if (d < 0) return false;
    if (!put((a << 2) | (b >> 4)) || !put(((b & 0x0f) << 4) | (c >> 2)) ||
        !put(((c & 0x03) << 6) | d)) {
      return false;
    }
  }
  *n = w;
  return true;
}

bool base64_decode(std::string_view in, std::string* out) {
  out->assign(in.size() / 4 * 3, '\0');
  size_t n = 0;
  if (!base64_decode_to(in, reinterpret_cast<uint8_t*>(out->data()), out->size(), &n)) {
    out->clear();
    return false;
  }
  out->resize(n);
  return true;
}

// ---------------------------------------------------------------------------
// CksumHasher
// ---------------------------------------------------------------------------

struct CksumHasher::Impl {
  CksumAlg alg;
  EVP_MD_CTX* ctx{nullptr};
  XXH3_state_t* x3{nullptr};   // XXHash3 / XXHash128
  XXH64_state_t* x64{nullptr}; // XXHash64
  uint64_t crc{0};

  explicit Impl(CksumAlg a) : alg(a) {
    if (is_crc(a)) {
      crc = (a == CksumAlg::CRC64NVME) ? ~0ull : 0xFFFFFFFFull;
    } else if (const EVP_MD* md = evp_md(a)) {
      ctx = EVP_MD_CTX_new();
      if (!ctx || EVP_DigestInit_ex(ctx, md, nullptr) != 1) {
        if (ctx) EVP_MD_CTX_free(ctx);
        throw std::runtime_error("checksum: EVP init failed");
      }
    } else if (a == CksumAlg::XXHash3 || a == CksumAlg::XXHash128) {
      x3 = XXH3_createState();
      if (!x3) {
        throw std::runtime_error("checksum: xxh3 state alloc failed");
      }
      if (a == CksumAlg::XXHash3) {
        XXH3_64bits_reset(x3);
      } else {
        XXH3_128bits_reset(x3);
      }
    } else if (a == CksumAlg::XXHash64) {
      x64 = XXH64_createState();
      if (!x64) {
        throw std::runtime_error("checksum: xxh64 state alloc failed");
      }
      XXH64_reset(x64, 0);
    } else {
      throw std::invalid_argument("checksum: unsupported algorithm");
    }
  }
  ~Impl() {
    if (ctx) EVP_MD_CTX_free(ctx);
    if (x3) XXH3_freeState(x3);
    if (x64) XXH64_freeState(x64);
  }
};

CksumHasher::CksumHasher(CksumAlg alg) : impl_(new Impl(alg)) {}
CksumHasher::~CksumHasher() = default;

void CksumHasher::update(const void* data, size_t len) {
  auto* p = static_cast<const uint8_t*>(data);
  switch (impl_->alg) {
    case CksumAlg::CRC32:
      impl_->crc = crc32_update(tab_crc32(), static_cast<uint32_t>(impl_->crc), p, len);
      break;
    case CksumAlg::CRC32C:
      impl_->crc = crc32_update(tab_crc32c(), static_cast<uint32_t>(impl_->crc), p, len);
      break;
    case CksumAlg::CRC64NVME:
      impl_->crc = crc64_update(tab_crc64n(), impl_->crc, p, len);
      break;
    case CksumAlg::XXHash3:
      XXH3_64bits_update(impl_->x3, p, len);
      break;
    case CksumAlg::XXHash128:
      XXH3_128bits_update(impl_->x3, p, len);
      break;
    case CksumAlg::XXHash64:
      XXH64_update(impl_->x64, p, len);
      break;
    default:
      EVP_DigestUpdate(impl_->ctx, p, len);
      break;
  }
}

size_t CksumHasher::finalize(uint8_t* out) {
  size_t n = cksum_digest_len(impl_->alg);
  switch (impl_->alg) {
    case CksumAlg::CRC32:
    case CksumAlg::CRC32C:
      store_be(static_cast<uint32_t>(~static_cast<uint32_t>(impl_->crc)), out, 4);
      break;
    case CksumAlg::CRC64NVME:
      store_be(~impl_->crc, out, 8);
      break;
    case CksumAlg::XXHash3:
      store_be(XXH3_64bits_digest(impl_->x3), out, 8);
      break;
    case CksumAlg::XXHash64:
      store_be(XXH64_digest(impl_->x64), out, 8);
      break;
    case CksumAlg::XXHash128: {
      XXH128_hash_t h = XXH3_128bits_digest(impl_->x3);  // canonical: high64 || low64
      store_be(h.high64, out, 8);
      store_be(h.low64, out + 8, 8);
      break;
    }
    default: {
      unsigned int l = 0;
      EVP_DigestFinal_ex(impl_->ctx, out, &l);
      break;
    }
  }
  return n;
}

std::string cksum_digest(CksumAlg alg, const void* data, size_t len) {
  CksumHasher h(alg);
  h.update(data, len);
  uint8_t buf[kCksumMaxDigestBytes];
  size_t n = h.finalize(buf);
  return std::string(reinterpret_cast<char*>(buf), n);
}

// ---------------------------------------------------------------------------
// CRC combine
// ---------------------------------------------------------------------------

void crc_combine(CksumAlg alg, uint8_t* acc, const uint8_t* next, uint64_t len_b) {
  switch (alg) {
    case CksumAlg::CRC32:
      store_be(tab_crc32().combine(static_cast<uint32_t>(load_be(acc, 4)),
                                   static_cast<uint32_t>(load_be(next, 4)), len_b),
               acc, 4);
      break;
    case CksumAlg::CRC32C:
      store_be(tab_crc32c().combine(static_cast<uint32_t>(load_be(acc, 4)),
                                    static_cast<uint32_t>(load_be(next, 4)), len_b),
               acc, 4);
      break;
    case CksumAlg::CRC64NVME:
      store_be(tab_crc64n().combine(load_be(acc, 8), load_be(next, 8), len_b), acc, 8);
      break;
    default:
      break;
  }
}

// ---------------------------------------------------------------------------
// CombinedCksumBuilder
// ---------------------------------------------------------------------------

struct CombinedCksumBuilder::Impl {
  CksumAlg alg;
  CksumType type;
  uint32_t parts{0};
  std::unique_ptr<CksumHasher> hasher;              // COMPOSITE
  std::array<uint8_t, kCksumMaxDigestBytes> acc{};  // FULL_OBJECT

  Impl(CksumAlg a, CksumType t) : alg(a), type(t) {
    if (!cksum_type_valid_for_alg(a, t)) {
      throw std::invalid_argument("checksum: invalid algorithm/type pair");
    }
    if (t == CksumType::COMPOSITE) {
      hasher = std::make_unique<CksumHasher>(a);
    }
  }
};

CombinedCksumBuilder::CombinedCksumBuilder(CksumAlg alg, CksumType type)
    : impl_(new Impl(alg, type)) {}
CombinedCksumBuilder::~CombinedCksumBuilder() = default;

void CombinedCksumBuilder::fold(const uint8_t* part_digest, uint64_t part_size) {
  size_t n = cksum_digest_len(impl_->alg);
  if (impl_->type == CksumType::COMPOSITE) {
    impl_->hasher->update(part_digest, n);
  } else if (impl_->parts == 0) {
    std::memcpy(impl_->acc.data(), part_digest, n);
  } else {
    crc_combine(impl_->alg, impl_->acc.data(), part_digest, part_size);
  }
  ++impl_->parts;
}

size_t CombinedCksumBuilder::finalize_raw(uint8_t* out) {
  size_t n = cksum_digest_len(impl_->alg);
  if (impl_->type == CksumType::COMPOSITE) {
    return impl_->hasher->finalize(out);
  }
  std::memcpy(out, impl_->acc.data(), n);
  return n;
}

std::string CombinedCksumBuilder::finalize_string() {
  uint8_t buf[kCksumMaxDigestBytes];
  size_t n = finalize_raw(buf);
  std::string s = base64_encode(buf, n);
  if (impl_->type == CksumType::COMPOSITE) {
    s += '-';
    s += std::to_string(impl_->parts);
  }
  return s;
}

}  // namespace kvrgw
