// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "mon/KVMonitor.h"

#include "gtest/gtest.h"

using namespace std;

using KeyOps = map<string, optional<bufferlist>>;
using RangeOps = vector<KVMonitor::RangeDeleteOp>;

static bufferlist mkval(const char *s)
{
  bufferlist bl;
  bl.append(s);
  return bl;
}

// --- prefix_upper_bound ---

TEST(KVMonitorPrefixUpperBound, increments_last_byte) {
  EXPECT_EQ("bulk3", KVMonitor::prefix_upper_bound("bulk2"));
  EXPECT_EQ("test0", KVMonitor::prefix_upper_bound("test/"));
}

TEST(KVMonitorPrefixUpperBound, covers_bytes_above_tilde) {
  // A "prefix + ~" bound would miss anything sorting above 0x7e.
  const string p = "tl/";
  const string b = KVMonitor::prefix_upper_bound(p);
  for (string suffix : { "", "aaa", "}mid", "~high", "\x7f", "\xe6\x97\xa5" }) {
    const string key = p + suffix;
    EXPECT_LE(p, key) << "key=" << key;
    EXPECT_LT(key, b) << "key=" << key << " bound=" << b;
  }
}

TEST(KVMonitorPrefixUpperBound, excludes_the_bound_and_siblings) {
  const string b = KVMonitor::prefix_upper_bound("test/");
  EXPECT_FALSE(string("test0") < b);      // the bound itself
  EXPECT_FALSE(string("test1/x") < b);    // a different prefix
}

TEST(KVMonitorPrefixUpperBound, carries_over_trailing_0xff) {
  string p = "ab";
  p += static_cast<char>(0xff);
  EXPECT_EQ("ac", KVMonitor::prefix_upper_bound(p));
}

TEST(KVMonitorPrefixUpperBound, unbounded_cases) {
  EXPECT_TRUE(KVMonitor::prefix_upper_bound("").empty());
  EXPECT_TRUE(KVMonitor::prefix_upper_bound(string(3, char(0xff))).empty());
}

// --- RangeDeleteOp::get_bounds ---

TEST(KVMonitorRangeBounds, both_bounds_are_half_open) {
  KVMonitor::RangeDeleteOp op("test/range", "key1", "key3");
  string begin, end;
  op.get_bounds(&begin, &end);
  EXPECT_EQ("test/range/key1", begin);
  EXPECT_EQ("test/range/key3", end);
  EXPECT_TRUE(string("test/range/key2") >= begin &&
              string("test/range/key2") < end);
  EXPECT_FALSE(string("test/range/key3") < end);
}

TEST(KVMonitorRangeBounds, prefix_only_covers_every_suffix) {
  KVMonitor::RangeDeleteOp op("bulk2", "", "");
  string begin, end;
  op.get_bounds(&begin, &end);
  EXPECT_EQ("bulk2", begin);
  EXPECT_EQ("bulk3", end);
  for (string k : { "bulk2", "bulk2/k00001", "bulk2/~x" }) {
    EXPECT_LE(begin, k) << k;
    EXPECT_LT(k, end) << k;
  }
  EXPECT_FALSE(string("bulk20") < begin);   // sibling prefix untouched
}

TEST(KVMonitorRangeBounds, start_only_is_open_above) {
  KVMonitor::RangeDeleteOp op("sb", "kb", "");
  string begin, end;
  op.get_bounds(&begin, &end);
  EXPECT_EQ("sb/kb", begin);
  EXPECT_EQ(KVMonitor::prefix_upper_bound("sb"), end);
  EXPECT_FALSE(string("sb/ka") >= begin);
  EXPECT_TRUE(string("sb/kd") >= begin && string("sb/kd") < end);
}

TEST(KVMonitorRangeBounds, end_only_is_open_below) {
  KVMonitor::RangeDeleteOp op("eb", "", "kc");
  string begin, end;
  op.get_bounds(&begin, &end);
  EXPECT_EQ("eb", begin);
  EXPECT_EQ("eb/kc", end);
  EXPECT_TRUE(string("eb/ka") >= begin && string("eb/ka") < end);
  EXPECT_FALSE(string("eb/kc") < end);
}

// --- RangeDeleteOp::overlaps_prefix ---
// This drives whether a subscriber gets a full dump, so it has to be exact.

TEST(KVMonitorRangeOverlap, exact_and_nested_prefixes_overlap) {
  KVMonitor::RangeDeleteOp op("config/mgr.x", "", "");
  EXPECT_TRUE(op.overlaps_prefix("config/mgr.x"));   // same
  EXPECT_TRUE(op.overlaps_prefix("config/"));        // subscriber is broader
  EXPECT_TRUE(op.overlaps_prefix("config/mgr.x/a")); // subscriber is narrower
}

TEST(KVMonitorRangeOverlap, sibling_prefixes_do_not_overlap) {
  KVMonitor::RangeDeleteOp op("config/mgr.x", "", "");
  EXPECT_FALSE(op.overlaps_prefix("config/osd"));
  EXPECT_FALSE(op.overlaps_prefix("mgrstat/"));
  EXPECT_FALSE(op.overlaps_prefix("config/mgr.y"));
}

TEST(KVMonitorRangeOverlap, bounded_range_respects_the_bounds) {
  // removes [p/b, p/d): a subscriber under p/a or p/d is unaffected
  KVMonitor::RangeDeleteOp op("p", "b", "d");
  EXPECT_TRUE(op.overlaps_prefix("p/b"));
  EXPECT_TRUE(op.overlaps_prefix("p/c"));
  EXPECT_TRUE(op.overlaps_prefix("p"));
  EXPECT_FALSE(op.overlaps_prefix("p/a"));
  EXPECT_FALSE(op.overlaps_prefix("p/d"));
  EXPECT_FALSE(op.overlaps_prefix("p/e"));
}

TEST(KVMonitorRangeOverlap, empty_subscriber_prefix_matches_everything) {
  KVMonitor::RangeDeleteOp op("anything", "", "");
  EXPECT_TRUE(op.overlaps_prefix(""));
}

// --- forwarding a range to a subscriber ---
//
// A removal is handed to subscribers as an interval, clamped to the prefix
// they watch. Reproduce that clamping and check it against an ordered
// container, which is what a subscriber applies it to.

static void clamp_to_prefix(const KVMonitor::RangeDeleteOp& op,
                            const string& sub_prefix,
                            string *begin, string *end)
{
  op.get_bounds(begin, end);
  const string sub_end = KVMonitor::prefix_upper_bound(sub_prefix);
  if (*begin < sub_prefix) {
    *begin = sub_prefix;
  }
  if (!sub_end.empty() && (end->empty() || *end > sub_end)) {
    *end = sub_end;
  }
}

static vector<string> apply_range(const vector<string>& keys,
                                  const string& begin, const string& end)
{
  // std::map is ordered, so a subscriber can erase the interval directly
  map<string,int> m;
  for (auto& k : keys) m[k] = 1;
  m.erase(m.lower_bound(begin),
          end.empty() ? m.end() : m.lower_bound(end));
  vector<string> left;
  for (auto& p : m) left.push_back(p.first);
  return left;
}

TEST(KVMonitorRangeForward, clamped_to_the_subscriber_prefix) {
  // removal is broader than what the subscriber watches
  KVMonitor::RangeDeleteOp op("config", "", "");
  string begin, end;
  clamp_to_prefix(op, "config/", &begin, &end);
  EXPECT_EQ("config/", begin);
  EXPECT_EQ("config0", end);
}

TEST(KVMonitorRangeForward, narrow_removal_is_not_widened) {
  KVMonitor::RangeDeleteOp op("config/mgr.x/rng", "", "");
  string begin, end;
  clamp_to_prefix(op, "config/", &begin, &end);
  EXPECT_EQ("config/mgr.x/rng", begin);
  EXPECT_EQ("config/mgr.x/rnh", end);
}

TEST(KVMonitorRangeForward, applying_it_removes_exactly_the_interval) {
  const vector<string> keys = {
    "config/global/a",
    "config/mgr.x/bulk_1",
    "config/mgr.x/bulk_2",
    "config/mgr.x/keep",
    "config/osd/z",
  };
  KVMonitor::RangeDeleteOp op("config/mgr.x/bulk_", "", "");
  string begin, end;
  clamp_to_prefix(op, "config/", &begin, &end);

  const vector<string> left = apply_range(keys, begin, end);
  EXPECT_EQ(vector<string>({ "config/global/a",
                             "config/mgr.x/keep",
                             "config/osd/z" }), left);
}

TEST(KVMonitorRangeForward, applying_a_clamped_broad_removal_spares_outsiders) {
  const vector<string> keys = {
    "aaa_before/k", "config/a", "config/b", "zzz_after/k",
  };
  KVMonitor::RangeDeleteOp op("config", "", "");
  string begin, end;
  clamp_to_prefix(op, "config/", &begin, &end);

  const vector<string> left = apply_range(keys, begin, end);
  EXPECT_EQ(vector<string>({ "aaa_before/k", "zzz_after/k" }), left);
}

TEST(KVMonitorRangeForward, bounded_removal_keeps_its_end_bound) {
  const vector<string> keys = {
    "p/ka", "p/kb", "p/kc", "p/kd",
  };
  KVMonitor::RangeDeleteOp op("p", "kb", "kd");
  string begin, end;
  clamp_to_prefix(op, "p", &begin, &end);
  EXPECT_EQ("p/kb", begin);
  EXPECT_EQ("p/kd", end);

  const vector<string> left = apply_range(keys, begin, end);
  EXPECT_EQ(vector<string>({ "p/ka", "p/kd" }), left);
}

TEST(KVMonitorRangeForward, interval_reaches_keys_above_tilde) {
  const vector<string> keys = {
    "tl/aaa", "tl/}mid", "tl/~high", "tm/other",
  };
  KVMonitor::RangeDeleteOp op("tl", "", "");
  string begin, end;
  clamp_to_prefix(op, "tl", &begin, &end);

  const vector<string> left = apply_range(keys, begin, end);
  EXPECT_EQ(vector<string>({ "tm/other" }), left);
}

// --- delta encode/decode ---

TEST(KVMonitorDelta, without_ranges_uses_the_original_format) {
  KeyOps in;
  in["a"] = mkval("1");
  in["b"] = std::nullopt;

  bufferlist bl;
  KVMonitor::encode_delta(in, {}, &bl);

  // byte-for-byte identical to a bare map encode, so a monitor without
  // range support reads it unchanged
  bufferlist expected;
  encode(in, expected);
  EXPECT_TRUE(bl.contents_equal(expected));

  KeyOps key_ops;
  RangeOps range_ops;
  KVMonitor::decode_delta(bl, &key_ops, &range_ops);
  EXPECT_EQ(2u, key_ops.size());
  EXPECT_TRUE(range_ops.empty());
  EXPECT_TRUE(key_ops["a"].has_value());
  EXPECT_FALSE(key_ops["b"].has_value());
}

TEST(KVMonitorDelta, with_ranges_roundtrips) {
  KeyOps in;
  in["k"] = mkval("v");
  RangeOps rin = { {"pfx", "s", "e"}, {"other", "", ""} };

  bufferlist bl;
  KVMonitor::encode_delta(in, rin, &bl);

  KeyOps key_ops;
  RangeOps range_ops;
  KVMonitor::decode_delta(bl, &key_ops, &range_ops);

  ASSERT_EQ(1u, key_ops.size());
  EXPECT_EQ("v", key_ops["k"]->to_str());
  ASSERT_EQ(2u, range_ops.size());
  EXPECT_EQ("pfx", range_ops[0].prefix);
  EXPECT_EQ("s", range_ops[0].start);
  EXPECT_EQ("e", range_ops[0].end);
  EXPECT_EQ("other", range_ops[1].prefix);
  EXPECT_TRUE(range_ops[1].start.empty());
}

TEST(KVMonitorDelta, decoding_is_self_describing) {
  // The format must be recoverable from the blob alone: a delta written
  // before range support existed has to keep decoding correctly afterwards,
  // rather than being reinterpreted according to current features.
  KeyOps legacy_in;
  legacy_in["x"] = mkval("1");
  legacy_in["y"] = mkval("2");
  bufferlist legacy;
  encode(legacy_in, legacy);              // exactly what an old monitor wrote

  KeyOps key_ops;
  RangeOps range_ops;
  KVMonitor::decode_delta(legacy, &key_ops, &range_ops);
  EXPECT_EQ(2u, key_ops.size());
  EXPECT_TRUE(range_ops.empty());
}

TEST(KVMonitorDelta, empty_delta_decodes) {
  bufferlist bl;
  KVMonitor::encode_delta({}, {}, &bl);
  KeyOps key_ops;
  RangeOps range_ops;
  KVMonitor::decode_delta(bl, &key_ops, &range_ops);
  EXPECT_TRUE(key_ops.empty());
  EXPECT_TRUE(range_ops.empty());
}

TEST(KVMonitorDelta, range_only_delta_is_compact) {
  // The point of recording the range rather than the keys: the delta does
  // not grow with the number of keys removed.
  bufferlist bl;
  KVMonitor::encode_delta({}, { {"bulk", "", ""} }, &bl);
  EXPECT_LT(bl.length(), 64u);
}

// --- validate_range_params ---

TEST(KVMonitorValidateRange, accepts_valid_forms) {
  stringstream ss;
  EXPECT_EQ(0, KVMonitor::validate_range_params("t/r", "k1", "k3", ss));
  EXPECT_EQ(0, KVMonitor::validate_range_params("t/r", "", "", ss));
  EXPECT_EQ(0, KVMonitor::validate_range_params("t/r", "k1", "", ss));
  EXPECT_EQ(0, KVMonitor::validate_range_params("t/r", "", "k9", ss));
}

TEST(KVMonitorValidateRange, rejects_empty_prefix) {
  stringstream ss;
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("", "a", "z", ss));
  EXPECT_NE(string::npos, ss.str().find("prefix cannot be empty"));
}

TEST(KVMonitorValidateRange, rejects_start_not_below_end) {
  stringstream ss;
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("t", "k1", "k1", ss));
  stringstream ss2;
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("t", "zzz", "aaa", ss2));
  EXPECT_NE(string::npos, ss2.str().find("invalid range"));
}

TEST(KVMonitorValidateRange, rejects_non_printable_in_any_argument) {
  const string bad = string("bad") + '\x01' + "x";
  stringstream a, b, c;
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params(bad, "", "", a));
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("t", bad, "zzz", b));
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("t", "aaa", bad, c));
}

TEST(KVMonitorValidateRange, rejects_high_bytes_regardless_of_char_sign) {
  const string hi = string("t") + static_cast<char>(0x80);
  stringstream a, b;
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params(hi, "", "", a));
  EXPECT_EQ(-EINVAL, KVMonitor::validate_range_params("t", "a", hi, b));
}

TEST(KVMonitorValidateRange, accepts_a_wide_single_char_range) {
  stringstream ss;
  EXPECT_EQ(0, KVMonitor::validate_range_params("t", "a", "~", ss));
}
