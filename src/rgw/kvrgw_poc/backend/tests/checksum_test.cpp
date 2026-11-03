// Unit tests for checksum.{hpp,cpp}.  Vectors come from the s3-tests
// multipart checksum tests: three 5 MiB parts filled with 'A', 'B', 'C'.

#include "checksum.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace kvrgw;

static constexpr size_t kPart = 5 * 1024 * 1024;

static std::string b64(const std::string& raw) {
  return base64_encode(reinterpret_cast<const uint8_t*>(raw.data()), raw.size());
}

struct Vec {
  CksumAlg alg;
  CksumType type;
  const char* part[3];
  const char* combined;
};

static const Vec kVecs[] = {
  {CksumAlg::SHA256, CksumType::COMPOSITE,
   {"275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
    "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
    "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow="},
   "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3"},
  {CksumAlg::SHA1, CksumType::COMPOSITE,
   {"iIaTCGbm+vdVjNqIMF2S0T7ibMk=", "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
    "6DDwovUaHwrKNXDMzOGbuvj9kxI="},
   "sizjvY4eud3MrcHdZM3cQ/ol39o=-3"},
  {CksumAlg::CRC64NVME, CksumType::FULL_OBJECT,
   {"L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="}, "i+6LR0y3eFo="},
};

static void test_vectors() {
  for (const Vec& v : kVecs) {
    CombinedCksumBuilder b(v.alg, v.type);
    for (int i = 0; i < 3; ++i) {
      std::string buf(kPart, static_cast<char>('A' + i));
      std::string d = cksum_digest(v.alg, buf.data(), buf.size());
      assert(d.size() == cksum_digest_len(v.alg));
      assert(b64(d) == v.part[i]);
      b.fold(reinterpret_cast<const uint8_t*>(d.data()), kPart);
    }
    assert(b.finalize_string() == v.combined);
  }
}

// Combined CRC over parts must equal the CRC of the whole object, including
// when fed through the incremental hasher in odd-sized chunks.
static void test_crc_combine_matches_whole() {
  for (CksumAlg alg : {CksumAlg::CRC32, CksumAlg::CRC32C, CksumAlg::CRC64NVME}) {
    std::string whole;
    std::array<uint8_t, kCksumMaxDigestBytes> acc{};
    auto b = std::make_unique<CombinedCksumBuilder>(alg, CksumType::FULL_OBJECT);
    const size_t sizes[] = {1, 7, 4096, 12345, 1};
    for (size_t i = 0; i < 5; ++i) {
      std::string p(sizes[i], static_cast<char>('a' + i));
      whole += p;
      std::string d = cksum_digest(alg, p.data(), p.size());
      const auto* dp = reinterpret_cast<const uint8_t*>(d.data());
      if (b) {
        b->fold(dp, p.size());
      }
      if (i == 0) {
        std::memcpy(acc.data(), dp, d.size());
      } else {
        crc_combine(alg, acc.data(), dp, p.size());
      }
    }
    std::string wd = cksum_digest(alg, whole.data(), whole.size());

    CksumHasher h(alg);
    for (size_t off = 0; off < whole.size(); off += 13) {
      h.update(whole.data() + off, std::min<size_t>(13, whole.size() - off));
    }
    uint8_t inc[kCksumMaxDigestBytes];
    size_t n = h.finalize(inc);
    assert(std::string(reinterpret_cast<char*>(inc), n) == wd);

    assert(std::memcmp(acc.data(), wd.data(), wd.size()) == 0);
    if (b) {
      assert(b->finalize_string() == b64(wd));
    }
  }
}

static std::string hex(const std::string& raw) {
  static const char* d = "0123456789abcdef";
  std::string s;
  for (unsigned char c : raw) {
    s += d[c >> 4];
    s += d[c & 15];
  }
  return s;
}

// Known-answer vectors for the algorithms added by the o_value layout plan.
static void test_new_algs() {
  assert(hex(cksum_digest(CksumAlg::MD5, "abc", 3)) == "900150983cd24fb0d6963f7d28e17f72");
  assert(hex(cksum_digest(CksumAlg::SHA512, "abc", 3)) ==
         "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a"
         "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f");
  assert(hex(cksum_digest(CksumAlg::XXHash64, "", 0)) == "ef46db3751d8e999");
  assert(hex(cksum_digest(CksumAlg::XXHash3, "", 0)) == "2d06800538d394c2");
  assert(hex(cksum_digest(CksumAlg::XXHash128, "", 0)) == "99aa06d3014798d86001c324468d497f");

  const struct { CksumAlg a; size_t len; } lens[] = {
    {CksumAlg::NONE, 0}, {CksumAlg::CRC32, 4}, {CksumAlg::CRC32C, 4}, {CksumAlg::SHA1, 20},
    {CksumAlg::SHA256, 32}, {CksumAlg::CRC64NVME, 8}, {CksumAlg::SHA512, 64},
    {CksumAlg::MD5, 16}, {CksumAlg::XXHash3, 8}, {CksumAlg::XXHash64, 8},
    {CksumAlg::XXHash128, 16}};
  static_assert(kCksumMaxDigestBytes == 64);
  for (const auto& l : lens) {
    assert(cksum_digest_len(l.a) == l.len);
    assert(l.len <= kCksumMaxDigestBytes);
    if (l.a == CksumAlg::NONE) {
      continue;
    }
    assert(cksum_alg_from_name(cksum_alg_name(l.a)) == l.a);
    // incremental == one-shot
    std::string buf(1000, 'q');
    CksumHasher h(l.a);
    h.update(buf.data(), 333);
    h.update(buf.data() + 333, 667);
    uint8_t out[kCksumMaxDigestBytes];
    assert(h.finalize(out) == l.len);
    assert(std::string(reinterpret_cast<char*>(out), l.len) ==
           cksum_digest(l.a, buf.data(), buf.size()));
  }
  // COMPOSITE multipart fold works for the widest digest.
  CombinedCksumBuilder b(CksumAlg::SHA512, CksumType::COMPOSITE);
  std::string d = cksum_digest(CksumAlg::SHA512, "x", 1);
  b.fold(reinterpret_cast<const uint8_t*>(d.data()), 1);
  assert(b.finalize_string().ends_with("-1"));
}

static void test_single_part_composite() {
  std::string buf(1024, 'A');
  assert(b64(cksum_digest(CksumAlg::SHA256, buf.data(), buf.size())) ==
         "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=");
  std::string d = cksum_digest(CksumAlg::SHA256, buf.data(), buf.size());
  CombinedCksumBuilder b(CksumAlg::SHA256, CksumType::COMPOSITE);
  b.fold(reinterpret_cast<const uint8_t*>(d.data()), buf.size());
  assert(b.finalize_string() == "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1");
}

static void test_buffer_variants() {
  for (std::string s : {std::string(), std::string("a"), std::string("ab"),
                        std::string("abc"), std::string("abcd"), std::string(32, '\xff')}) {
    char enc[64];
    const size_t n = base64_encode_to(reinterpret_cast<const uint8_t*>(s.data()), s.size(), enc);
    assert(std::string(enc, n) == base64_encode(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
    uint8_t dec[40];
    size_t dn = 0;
    assert(base64_decode_to(std::string_view(enc, n), dec, sizeof(dec), &dn));
    assert(dn == s.size() && std::memcmp(dec, s.data(), dn) == 0);
    if (!s.empty()) {  // too small a buffer is rejected
      assert(!base64_decode_to(std::string_view(enc, n), dec, s.size() - 1, &dn));
    }
  }
  uint8_t d[32];
  std::string raw = cksum_digest(CksumAlg::SHA256, "x", 1);
  std::memcpy(d, raw.data(), 32);
  char out[96];
  size_t n = cksum_render(CksumType::COMPOSITE, d, 32, 10000, out);
  assert(std::string(out, n) == b64(raw) + "-10000");
  n = cksum_render(CksumType::FULL_OBJECT, d, 32, 3, out);
  assert(std::string(out, n) == b64(raw));
}

static void test_base64_and_names() {
  std::string out;
  for (std::string s : {std::string(), std::string("a"), std::string("ab"),
                        std::string("abc"), std::string("abcd")}) {
    assert(base64_decode(base64_encode(reinterpret_cast<const uint8_t*>(s.data()), s.size()), &out));
    assert(out == s);
  }
  assert(!base64_decode("abc", &out));
  assert(!base64_decode("ab!d", &out));
  assert(!base64_decode("a===", &out));

  assert(cksum_alg_from_name("SHA256") == CksumAlg::SHA256);
  assert(cksum_alg_from_name("sha256") == CksumAlg::NONE);
  assert(cksum_type_from_name("FULL_OBJECT") == CksumType::FULL_OBJECT);
  // Multipart rule: CRC64NVME is FULL_OBJECT only; CRC32/CRC32C take either;
  // the rest are COMPOSITE only.
  assert(cksum_type_valid_for_alg(CksumAlg::CRC32, CksumType::FULL_OBJECT));
  assert(cksum_type_valid_for_alg(CksumAlg::CRC32C, CksumType::FULL_OBJECT));
  assert(cksum_type_valid_for_alg(CksumAlg::CRC32, CksumType::COMPOSITE));
  assert(cksum_type_valid_for_alg(CksumAlg::XXHash128, CksumType::COMPOSITE));
  assert(!cksum_type_valid_for_alg(CksumAlg::SHA256, CksumType::FULL_OBJECT));
  assert(cksum_type_valid_for_alg(CksumAlg::CRC64NVME, CksumType::FULL_OBJECT));
  assert(!cksum_type_valid_for_alg(CksumAlg::CRC64NVME, CksumType::COMPOSITE));
  assert(!cksum_type_valid_for_alg(CksumAlg::NONE, CksumType::COMPOSITE));
  assert(cksum_default_type(CksumAlg::CRC64NVME) == CksumType::FULL_OBJECT);
  assert(cksum_default_type(CksumAlg::SHA1) == CksumType::COMPOSITE);
}

int main() {
  test_vectors();
  test_crc_combine_matches_whole();
  test_new_algs();
  test_single_part_composite();
  test_buffer_variants();
  test_base64_and_names();
  std::cout << "checksum_test: OK\n";
  return 0;
}
