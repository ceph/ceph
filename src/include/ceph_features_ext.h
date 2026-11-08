// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <charconv>
#include <sstream>
#include <string>
#include <string_view>

#include "include/feature_set.h"

/*
 * Extended features: bit numbers without a limit, exchanged in msgr2
 * when both peers support CEPH_MSGR2_FEATURE_EXTENDED_FEATURES.  A peer
 * that cannot exchange them (msgr1, releases before vampire, kernel
 * clients) has none of them.
 *
 * Allocate the next free number.  Never reuse or renumber one: a
 * feature that is no longer needed stays listed until no supported
 * release could send it.
 */
#define CEPH_FEATURES_EXT_LIST(X)                     \
  X(0, DEV_TEST_A, "dev_test_a") /* for tests only */ \
  X(1, DEV_TEST_B, "dev_test_b") /* for tests only */

#define CEPH_FEATURE_EXT_DEFINE_(bit, name, str) CEPH_FEATURE_EXT_##name = bit,
enum : unsigned {
  CEPH_FEATURES_EXT_LIST(CEPH_FEATURE_EXT_DEFINE_)
};
#undef CEPH_FEATURE_EXT_DEFINE_

/// The top bit of every 64-bit word is never allocated.  Peers may
/// advertise such bits at random, so that receivers which mishandle
/// unknown bits, or sets longer than they know, are caught by tests.
constexpr bool ceph_feature_ext_is_grease(unsigned bit) {
  return bit % ceph::feature_set_t::bits_per_block ==
    ceph::feature_set_t::bits_per_block - 1;
}

/// the GREASE bit of word @p word
constexpr unsigned ceph_feature_ext_grease_bit(unsigned word) {
  return word * ceph::feature_set_t::bits_per_block +
    ceph::feature_set_t::bits_per_block - 1;
}

#define CEPH_FEATURE_EXT_CHECK_(bit, name, str) \
  static_assert(!ceph_feature_ext_is_grease(bit), #name " is a GREASE bit");
CEPH_FEATURES_EXT_LIST(CEPH_FEATURE_EXT_CHECK_)
#undef CEPH_FEATURE_EXT_CHECK_

/// every extended feature this build supports
inline const ceph::feature_set_t& ceph_features_ext_supported() {
#define CEPH_FEATURE_EXT_BIT_(bit, name, str) bit,
  static const ceph::feature_set_t all = {
    CEPH_FEATURES_EXT_LIST(CEPH_FEATURE_EXT_BIT_)
  };
#undef CEPH_FEATURE_EXT_BIT_
  return all;
}

/// the name of a known extended feature, or nullptr
inline const char* ceph_feature_ext_name(unsigned bit) {
#define CEPH_FEATURE_EXT_NAME_(b, name, str) case b: return str;
  switch (bit) {
    CEPH_FEATURES_EXT_LIST(CEPH_FEATURE_EXT_NAME_)
  }
#undef CEPH_FEATURE_EXT_NAME_
  return nullptr;
}

#define HAVE_FEATURE_EXT(set, name) ((set).test(CEPH_FEATURE_EXT_##name))

/// Parse comma-separated extended feature numbers (from options); invalid,
/// out-of-range and GREASE entries are skipped and returned in @p bad.
inline ceph::feature_set_t ceph_parse_features_ext(std::string_view s,
                                                   std::string *bad = nullptr)
{
  ceph::feature_set_t r;
  while (!s.empty()) {
    auto comma = s.find(',');
    auto item = s.substr(0, comma);
    s = comma == std::string_view::npos ? std::string_view{} : s.substr(comma + 1);
    while (!item.empty() && item.front() == ' ') item.remove_prefix(1);
    while (!item.empty() && item.back() == ' ') item.remove_suffix(1);
    if (item.empty()) {
      continue;
    }
    unsigned bit = 0;
    auto [p, ec] = std::from_chars(item.data(), item.data() + item.size(), bit);
    if (ec != std::errc() || p != item.data() + item.size() ||
        bit >= ceph::feature_set_t::max_bits || ceph_feature_ext_is_grease(bit)) {
      if (bad) {
        *bad += (bad->empty() ? "" : ",") + std::string(item);
      }
      continue;
    }
    r.insert(bit);
  }
  return r;
}

/// "features 0x10 and extended features {3}", naming what a peer lacks
inline std::string ceph_describe_missing_features(
  uint64_t missing, const ceph::feature_set_t& missing_ext)
{
  std::ostringstream ss;
  if (missing) {
    ss << "features 0x" << std::hex << missing << std::dec;
  }
  if (!missing_ext.empty()) {
    ss << (missing ? " and " : "") << "extended features " << missing_ext;
  }
  return ss.str();
}
