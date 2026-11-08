// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <algorithm>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "include/ceph_features_ext.h"
#include "include/feature_set.h"
#include "mds/cephfs_features.h"
#include "mds/mdstypes.h"

using ceph::feature_set_t;
using ceph::bufferlist;

// Each feature_set_t is checked against a std::set<unsigned> holding the
// same bits; bit numbers are kept below kMaxBit so sets span several words
// without large allocations.
static constexpr unsigned kMaxBit = 5 * feature_set_t::bits_per_block;

static std::set<unsigned> bits_of(const feature_set_t& s)
{
  std::set<unsigned> r;
  s.for_each([&](unsigned b) { r.insert(b); });
  return r;
}

static feature_set_t from_bits(const std::set<unsigned>& bits)
{
  feature_set_t s;
  for (auto b : bits) {
    s.insert(b);
  }
  return s;
}

static std::set<unsigned> random_bits(std::mt19937_64& rng)
{
  std::set<unsigned> r;
  // mostly a few words, sometimes empty
  unsigned top = rng() % (kMaxBit + 1);
  unsigned n = rng() % 12;
  for (unsigned i = 0; top && i < n; ++i) {
    r.insert(rng() % top);
  }
  return r;
}

// numeric order of the sets read as unbounded binary numbers
static bool ref_less(const std::set<unsigned>& a, const std::set<unsigned>& b)
{
  std::set<unsigned> diff;
  std::set_symmetric_difference(a.begin(), a.end(), b.begin(), b.end(),
                                std::inserter(diff, diff.end()));
  return !diff.empty() && b.count(*diff.rbegin());
}

static bufferlist encoded(const feature_set_t& s)
{
  bufferlist bl;
  encode(s, bl);
  return bl;
}

static feature_set_t decoded(const bufferlist& bl)
{
  feature_set_t s;
  auto p = bl.cbegin();
  decode(s, p);
  EXPECT_TRUE(p.end());
  return s;
}

static bufferlist raw_set(uint32_t len, const std::vector<uint64_t>& words)
{
  bufferlist bl;
  encode(len, bl);
  for (auto w : words) {
    encode(w, bl);
  }
  return bl;
}

TEST(feature_set, bits)
{
  feature_set_t s;
  EXPECT_TRUE(s.empty());
  EXPECT_FALSE(s.test(0));
  EXPECT_FALSE(s.test(~0u));

  s.insert(0);
  s.insert(63);
  s.insert(64);
  s.insert(200);
  s.insert(200);
  EXPECT_FALSE(s.empty());
  EXPECT_EQ((std::set<unsigned>{0, 63, 64, 200}), bits_of(s));
  EXPECT_TRUE(s.test(63));
  EXPECT_FALSE(s.test(62));
  EXPECT_FALSE(s.test(201));
  EXPECT_FALSE(s.test(~0u));

  s.erase(1000);
  s.erase(1);
  EXPECT_EQ((feature_set_t{0, 63, 64, 200}), s);
  // erasing the top bit drops the empty words, so equality still holds
  s.erase(200);
  EXPECT_EQ((feature_set_t{0, 63, 64}), s);
  s.erase(0);
  s.erase(63);
  s.erase(64);
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(feature_set_t{}, s);

  s.insert(5);
  s.clear();
  EXPECT_TRUE(s.empty());

  std::ostringstream out;
  out << feature_set_t{} << " " << feature_set_t{64, 0, 3};
  EXPECT_EQ("{} {0,3,64}", out.str());
}

TEST(feature_set, set_ops)
{
  const feature_set_t a{0, 1, 64, 130};
  const feature_set_t b{1, 130, 200};
  EXPECT_EQ((feature_set_t{0, 64}), a - b);
  EXPECT_EQ((feature_set_t{200}), b - a);
  EXPECT_EQ((feature_set_t{1, 130}), a & b);
  EXPECT_TRUE((a - a).empty());
  EXPECT_TRUE((a & feature_set_t{}).empty());
  EXPECT_EQ(a, a - feature_set_t{});
  feature_set_t u = a;
  u |= b;
  EXPECT_EQ((feature_set_t{0, 1, 64, 130, 200}), u);
  EXPECT_TRUE(u.contains(a));
  EXPECT_TRUE(u.contains(b));
  EXPECT_FALSE(a.contains(b));
  EXPECT_TRUE(a.contains(feature_set_t{}));
  EXPECT_FALSE(feature_set_t{}.contains(a));
  // results whose top words became zero compare equal to a fresh set
  EXPECT_EQ((feature_set_t{0}),
            (feature_set_t{0, 200} & feature_set_t{0, 201}));
  EXPECT_EQ((feature_set_t{0}), (feature_set_t{0, 200} - feature_set_t{200}));

  std::mt19937_64 rng(1);
  for (int i = 0; i < 20000; ++i) {
    auto ra = random_bits(rng);
    auto rb = random_bits(rng);
    auto sa = from_bits(ra);
    auto sb = from_bits(rb);
    std::set<unsigned> diff, inter, uni;
    std::set_difference(ra.begin(), ra.end(), rb.begin(), rb.end(),
                        std::inserter(diff, diff.end()));
    std::set_intersection(ra.begin(), ra.end(), rb.begin(), rb.end(),
                          std::inserter(inter, inter.end()));
    std::set_union(ra.begin(), ra.end(), rb.begin(), rb.end(),
                   std::inserter(uni, uni.end()));
    ASSERT_EQ(from_bits(diff), sa - sb);
    ASSERT_EQ(diff, bits_of(sa - sb));
    ASSERT_EQ(from_bits(inter), sa & sb);
    ASSERT_EQ(inter, bits_of(sa & sb));
    auto su = sa;
    su |= sb;
    ASSERT_EQ(from_bits(uni), su);
    ASSERT_EQ(diff.empty(), sb.contains(sa));
    ASSERT_EQ(ra == rb, sa == sb);
    ASSERT_EQ(ra.empty(), sa.empty());
  }
}

TEST(feature_set, random_operations)
{
  std::mt19937_64 rng(2);
  std::set<unsigned> ref;
  feature_set_t s;
  for (int i = 0; i < 20000; ++i) {
    unsigned bit = rng() % kMaxBit;
    auto other = random_bits(rng);
    switch (rng() % 7) {
    case 0:
    case 1:
      s.insert(bit);
      ref.insert(bit);
      break;
    case 2:
    case 3:
      s.erase(bit);
      ref.erase(bit);
      break;
    case 4:
      s = s - from_bits(other);
      for (auto b : other) {
        ref.erase(b);
      }
      break;
    case 5:
      if (rng() % 4 == 0) {
        s = s & from_bits(other);
        std::set<unsigned> r;
        for (auto b : ref) {
          if (other.count(b)) {
            r.insert(b);
          }
        }
        ref = r;
      } else {
        s |= from_bits(other);
        ref.insert(other.begin(), other.end());
      }
      break;
    case 6:
      if (rng() % 16 == 0) {
        s.clear();
        ref.clear();
      }
      break;
    }
    ASSERT_EQ(ref, bits_of(s));
    ASSERT_EQ(ref.empty(), s.empty());
    ASSERT_EQ(from_bits(ref), s);
    ASSERT_FALSE(s < from_bits(ref));
    ASSERT_FALSE(from_bits(ref) < s);
    ASSERT_EQ(s, decoded(encoded(s)));
  }
}

TEST(feature_set, ordering)
{
  EXPECT_FALSE(feature_set_t{} < feature_set_t{});
  EXPECT_TRUE(feature_set_t{} < feature_set_t{0});
  EXPECT_TRUE(feature_set_t{0} < feature_set_t{1});
  EXPECT_TRUE((feature_set_t{0, 1, 2, 3}) < feature_set_t{64});
  EXPECT_TRUE((feature_set_t{63}) < (feature_set_t{0, 64}));
  EXPECT_TRUE((feature_set_t{0, 64}) < (feature_set_t{1, 64}));
  EXPECT_FALSE((feature_set_t{1, 64}) < (feature_set_t{0, 64}));

  std::mt19937_64 rng(3);
  std::vector<std::set<unsigned>> refs;
  for (int i = 0; i < 2000; ++i) {
    refs.push_back(random_bits(rng));
  }
  for (size_t i = 0; i < refs.size(); ++i) {
    auto a = from_bits(refs[i]);
    auto& rb = refs[(i * 7919 + 1) % refs.size()];
    auto b = from_bits(rb);
    ASSERT_EQ(ref_less(refs[i], rb), a < b);
    ASSERT_EQ(ref_less(rb, refs[i]), b < a);
    ASSERT_FALSE(a < a);
    ASSERT_EQ(a == b, !(a < b) && !(b < a));
  }

  // sorting agrees with the numeric order, so the order is transitive
  std::vector<feature_set_t> sets;
  for (auto& r : refs) {
    sets.push_back(from_bits(r));
  }
  std::sort(sets.begin(), sets.end());
  std::sort(refs.begin(), refs.end(), ref_less);
  for (size_t i = 0; i < sets.size(); ++i) {
    ASSERT_EQ(refs[i], bits_of(sets[i]));
  }

  // a map key (as in FeatureMap) finds equal sets however they were built
  std::map<feature_set_t, int> m;
  m[feature_set_t{0, 1}] = 1;
  m[feature_set_t{}] = 2;
  EXPECT_EQ(1, m.at((feature_set_t{0, 1, 300} - feature_set_t{300})));
  EXPECT_EQ(2, m.at((feature_set_t{200} & feature_set_t{201})));
  EXPECT_EQ(2u, m.size());
}

TEST(feature_set, encoding)
{
  // a byte length, then little-endian 64-bit words
  bufferlist bl = encoded(feature_set_t{});
  EXPECT_EQ(std::string("\0\0\0\0", 4), bl.to_str());
  bl = encoded(feature_set_t{0, 9});
  EXPECT_EQ(std::string("\x08\0\0\0" "\x01\x02\0\0\0\0\0\0", 12), bl.to_str());
  bl = encoded(feature_set_t{64, 127});
  EXPECT_EQ(std::string("\x10\0\0\0" "\0\0\0\0\0\0\0\0"
                        "\x01\0\0\0\0\0\0\x80", 20), bl.to_str());

  for (auto& s : {feature_set_t{}, feature_set_t{0}, feature_set_t{63},
                  feature_set_t{64}, feature_set_t{0, 1, 64, 129, 1000},
                  feature_set_t{64 * feature_set_t::max_blocks - 1},
                  ceph_features_ext_supported()}) {
    EXPECT_EQ(s, decoded(encoded(s)));
  }

  // words left zero by an encoder that does not trim them are dropped
  bl = raw_set(24, {5, 0, 0});
  EXPECT_EQ((feature_set_t{0, 2}), decoded(bl));
  EXPECT_EQ(12u, encoded(decoded(bl)).length());
  EXPECT_TRUE(decoded(raw_set(16, {0, 0})).empty());

  // the decoder leaves what follows the set alone
  bl = encoded(feature_set_t{3});
  encode(uint32_t(0xdeadbeef), bl);
  feature_set_t s;
  auto p = bl.cbegin();
  decode(s, p);
  uint32_t trailer;
  decode(trailer, p);
  EXPECT_EQ((feature_set_t{3}), s);
  EXPECT_EQ(0xdeadbeefu, trailer);
}

TEST(feature_set, decode_limits)
{
  feature_set_t s;
  std::vector<uint64_t> full(feature_set_t::max_blocks, 1);
  auto longest = decoded(raw_set(8 * feature_set_t::max_blocks, full));
  EXPECT_EQ(feature_set_t::max_blocks, bits_of(longest).size());

  for (uint32_t len : {1u, 4u, 7u, 9u, 15u,
                       8u * feature_set_t::max_blocks + 1}) {
    std::vector<uint64_t> words((len + 7) / 8, 1);
    EXPECT_THROW(s = decoded(raw_set(len, words)),
                 ceph::buffer::malformed_input) << len;
  }
  // words past the limit are skipped: a later release may send them
  full.push_back(1);
  full.push_back(1);
  EXPECT_EQ(longest,
            decoded(raw_set(8 * (feature_set_t::max_blocks + 2), full)));
  // huge lengths allocate nothing more than the limit
  for (uint32_t len : {0xfffffff8u, 0x80000000u}) {
    EXPECT_THROW(s = decoded(raw_set(len, {})),
                 ceph::buffer::end_of_buffer) << len;
  }
  EXPECT_THROW(s = decoded(raw_set(0xffffffffu, {})),
               ceph::buffer::malformed_input);
  EXPECT_THROW(s = decoded(raw_set(16, {1})), ceph::buffer::end_of_buffer);
  bufferlist bl;
  bl.append("\x08\0", 2);
  EXPECT_THROW(s = decoded(bl), ceph::buffer::end_of_buffer);

  // a set never holds a bit no peer could decode
  feature_set_t too_far{feature_set_t::max_bits, feature_set_t::max_bits - 2};
  EXPECT_EQ(std::set<unsigned>{feature_set_t::max_bits - 2},
            bits_of(too_far));
  EXPECT_EQ(too_far, decoded(encoded(too_far)));
}

static void append_le32(std::string& s, uint32_t v)
{
  for (int i = 0; i < 4; ++i) {
    s.push_back(char(v >> (8 * i)));
  }
}

static uint32_t read_le32(const std::string& s)
{
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= uint32_t(uint8_t(s[i])) << (8 * i);
  }
  return v;
}

TEST(feature_set, fuzz)
{
  std::mt19937_64 rng(4);
  const unsigned max_len = 8 * feature_set_t::max_blocks;
  unsigned decoded_ok = 0;
  for (int i = 0; i < 100000; ++i) {
    std::string in;
    uint32_t len;
    switch (rng() % 3) {
    case 0: // any short string of bytes
      in.resize(rng() % 80);
      for (auto& c : in) {
        c = rng();
      }
      break;
    case 1: // a plausible length, some zero words, maybe truncated
      len = 8 * (rng() % (feature_set_t::max_blocks + 3));
      if (rng() % 4 == 0) {
        len += rng() % 8;
      }
      append_le32(in, len);
      for (unsigned n = (rng() % 3) ? len : rng() % (len + 9); n; --n) {
        in.push_back((rng() % 2) ? char(rng()) : 0);
      }
      break;
    case 2: // any length
      len = rng();
      append_le32(in, len);
      for (unsigned n = rng() % 600; n; --n) {
        in.push_back(rng());
      }
      break;
    }

    // what decode() must do with these bytes
    bool truncated = in.size() < 4;
    bool malformed = false;
    std::set<unsigned> want;
    if (!truncated) {
      len = read_le32(in);
      malformed = len % 8;
      truncated = !malformed && in.size() - 4 < len;
      for (unsigned j = 0; !malformed && !truncated && j < std::min(len, max_len);
           ++j) {
        for (unsigned b = 0; b < 8; ++b) {
          if (uint8_t(in[4 + j]) & (1u << b)) {
            want.insert(8 * j + b);
          }
        }
      }
    }

    bufferlist bl;
    bl.append(in);
    auto p = bl.cbegin();
    feature_set_t s;
    if (malformed) {
      ASSERT_THROW(decode(s, p), ceph::buffer::malformed_input) << i;
    } else if (truncated) {
      ASSERT_THROW(decode(s, p), ceph::buffer::end_of_buffer) << i;
    } else {
      ASSERT_NO_THROW(decode(s, p)) << i;
      ASSERT_EQ(4u + len, p.get_off()) << i;
      ASSERT_EQ(want, bits_of(s)) << i;
      bufferlist again = encoded(s);
      ASSERT_LE(again.length(), 4u + max_len) << i;
      ASSERT_EQ(s, decoded(again)) << i;
      ++decoded_ok;
    }
  }
  EXPECT_GT(decoded_ok, 10000u);
}

// CephFS's feature_bitset_t has the same encoding: a set encoded by one
// decodes as the other.
TEST(feature_set, cephfs_feature_bitset_t)
{
  std::mt19937_64 rng(5);
  for (int i = 0; i < 2000; ++i) {
    auto ref = random_bits(rng);
    std::vector<size_t> v(ref.begin(), ref.end());
    feature_bitset_t fs(v);
    feature_set_t s = from_bits(ref);

    bufferlist from_fs;
    encode(fs, from_fs);
    ASSERT_EQ(encoded(s).to_str(), from_fs.to_str());
    ASSERT_EQ(s, decoded(from_fs));

    bufferlist from_s = encoded(s);
    feature_bitset_t back;
    auto p = from_s.cbegin();
    decode(back, p);
    ASSERT_TRUE(p.end());
    ASSERT_EQ(fs, back);
    for (unsigned b = 0; b < kMaxBit; ++b) {
      ASSERT_EQ(s.test(b), back.test(b)) << b;
    }
  }

  feature_bitset_t all(std::vector<size_t>(CEPHFS_FEATURES_ALL));
  bufferlist bl;
  encode(all, bl);
  feature_set_t s = decoded(bl);
  for (unsigned b = 0; b <= CEPHFS_FEATURE_MAX + 64; ++b) {
    EXPECT_EQ(all.test(b), s.test(b)) << b;
  }

  // feature_bitset_t's -= keeps zero top words; feature_set_t drops them
  feature_bitset_t untrimmed(std::vector<size_t>{1, 200});
  untrimmed -= feature_bitset_t(std::vector<size_t>{200});
  bl.clear();
  encode(untrimmed, bl);
  EXPECT_EQ(36u, bl.length());
  EXPECT_EQ((feature_set_t{1}), decoded(bl));

  // feature_bitset_t takes any length, where we skip the words past our
  // limit, and a partial last word, which we refuse
  bl = raw_set(8 * feature_set_t::max_blocks + 8,
               std::vector<uint64_t>(feature_set_t::max_blocks + 1, 1));
  feature_bitset_t big;
  auto p = bl.cbegin();
  decode(big, p);
  EXPECT_TRUE(big.test(64 * feature_set_t::max_blocks));
  s = decoded(bl);
  EXPECT_TRUE(s.test(64 * (feature_set_t::max_blocks - 1)));
  EXPECT_FALSE(s.test(64 * feature_set_t::max_blocks));
  bl.clear();
  encode(uint32_t(3), bl);
  bl.append("\x05\0\0", 3);
  feature_bitset_t partial;
  p = bl.cbegin();
  decode(partial, p);
  EXPECT_TRUE(partial.test(0));
  EXPECT_TRUE(partial.test(2));
  EXPECT_THROW(s = decoded(bl), ceph::buffer::malformed_input);
}

TEST(features_ext, allocated_bits)
{
  const auto& supported = ceph_features_ext_supported();
  EXPECT_FALSE(supported.empty());
  EXPECT_EQ(0u, CEPH_FEATURE_EXT_DEV_TEST_A);
  EXPECT_EQ(1u, CEPH_FEATURE_EXT_DEV_TEST_B);
  EXPECT_TRUE(HAVE_FEATURE_EXT(supported, DEV_TEST_A));
  EXPECT_TRUE(HAVE_FEATURE_EXT(supported, DEV_TEST_B));
  EXPECT_FALSE(HAVE_FEATURE_EXT(feature_set_t{}, DEV_TEST_A));
  EXPECT_FALSE(HAVE_FEATURE_EXT(feature_set_t{1}, DEV_TEST_A));
  EXPECT_STREQ("dev_test_a",
               ceph_feature_ext_name(CEPH_FEATURE_EXT_DEV_TEST_A));

  // every allocated bit has one unique name, and only those have names
  std::set<std::string> names;
  supported.for_each([&](unsigned b) {
    const char* name = ceph_feature_ext_name(b);
    ASSERT_NE(nullptr, name) << b;
    ASSERT_NE('\0', name[0]) << b;
    ASSERT_TRUE(names.insert(name).second) << name;
  });
  for (unsigned b = 0; b < 64 * feature_set_t::max_blocks; ++b) {
    EXPECT_EQ(supported.test(b), ceph_feature_ext_name(b) != nullptr) << b;
  }
  EXPECT_EQ(nullptr, ceph_feature_ext_name(~0u));
}

TEST(features_ext, grease)
{
  for (unsigned b = 0; b < 64 * feature_set_t::max_blocks; ++b) {
    ASSERT_EQ(b % 64 == 63, ceph_feature_ext_is_grease(b)) << b;
  }
  const auto& supported = ceph_features_ext_supported();
  supported.for_each([](unsigned b) {
    EXPECT_FALSE(ceph_feature_ext_is_grease(b)) << b << " is a GREASE bit";
  });

  // a peer's GREASE bits never survive into what both ends support
  for (unsigned w = 0; w < feature_set_t::max_blocks; ++w) {
    feature_set_t peer = supported;
    peer.insert(64 * w + 63);
    ASSERT_EQ(peer, decoded(encoded(peer)));
    ASSERT_EQ(supported, peer & supported);
    ASSERT_EQ(feature_set_t{64 * w + 63}, peer - supported);
    ASSERT_TRUE(peer.contains(supported));
  }
}

TEST(features_ext, parse)
{
  std::string bad;
  EXPECT_EQ(feature_set_t({0, 1, 5}), ceph_parse_features_ext(" 0, 1 ,,5 ", &bad));
  EXPECT_EQ("", bad);
  // not a number, out of range, or a GREASE bit: skipped, and named
  EXPECT_EQ(feature_set_t({2}),
            ceph_parse_features_ext("x,63,4096,-1,2,127,3a", &bad));
  EXPECT_EQ("x,63,4096,-1,127,3a", bad);
  EXPECT_TRUE(ceph_parse_features_ext("").empty());
}

TEST(features_ext, describe_missing)
{
  EXPECT_EQ("features 0x10", ceph_describe_missing_features(0x10, {}));
  EXPECT_EQ("extended features {3}", ceph_describe_missing_features(0, {3}));
  EXPECT_EQ("features 0x10 and extended features {0,3}",
            ceph_describe_missing_features(0x10, {0, 3}));
}
