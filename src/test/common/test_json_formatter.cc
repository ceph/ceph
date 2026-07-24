// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 IBM
 * Copyright (C) 2018 Red Hat Inc.
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <gtest/gtest.h>

#include "common/ceph_json.h"
#include "common/Clock.h"
#include "common/JSONFormatter.h"
#include "common/StackStringStream.h"

#include <sstream>

using namespace std;

const string outstring = 
"{\"pg_ready\":true, \"pg_stats\":[ { \"pgid\":\"1.0\", \"version\":\"16'56\",\"reported_seq\":\"62\",\"reported_epoch\":\"20\",\"state\":\"active+clean+inconsistent\",\"last_fresh\":\"2018-12-18 15:21:22.173804\",\"last_change\":\"2018-12-18 15:21:22.173804\",\"last_active\":\"2018-12-18 15:21:22.173804\",\"last_peered\":\"2018-12-18 15:21:22.173804\",\"last_clean\":\"2018-12-18 15:21:22.173804\",\"last_became_active\":\"2018-12-18 15:21:21.347685\",\"last_became_peered\":\"2018-12-18 15:21:21.347685\",\"last_unstale\":\"2018-12-18 15:21:22.173804\",\"last_undegraded\":\"2018-12-18 15:21:22.173804\",\"last_fullsized\":\"2018-12-18 15:21:22.173804\",\"mapping_epoch\":19,\"log_start\":\"0'0\",\"ondisk_log_start\":\"0'0\",\"created\":7,\"last_epoch_clean\":20,\"parent\":\"0.0\",\"parent_split_bits\":0,\"last_scrub\":\"16'56\",\"last_scrub_stamp\":\"2018-12-18 15:21:22.173684\",\"last_deep_scrub\":\"0'0\",\"last_deep_scrub_stamp\":\"2018-12-18 15:21:06.514438\",\"last_clean_scrub_stamp\":\"2018-12-18 15:21:06.514438\",\"log_size\":56,\"ondisk_log_size\":56,\"stats_invalid\":false,\"dirty_stats_invalid\":false,\"omap_stats_invalid\":false,\"hitset_stats_invalid\":false,\"hitset_bytes_stats_invalid\":false,\"pin_stats_invalid\":false,\"manifest_stats_invalid\":false,\"snaptrimq_len\":0,\"stat_sum\":{\"num_bytes\":24448,\"num_objects\":36,\"num_object_clones\":20,\"num_object_copies\":36,\"num_objects_missing_on_primary\":0,\"num_objects_missing\":0,\"num_objects_degraded\":0,\"num_objects_misplaced\":0,\"num_objects_unfound\":0,\"num_objects_dirty\":36,\"num_whiteouts\":3,\"num_read\":0,\"num_read_kb\":0,\"num_write\":36,\"num_write_kb\":50,\"num_scrub_errors\":20,\"num_shallow_scrub_errors\":20,\"num_deep_scrub_errors\":0,\"num_objects_recovered\":0,\"num_bytes_recovered\":0,\"num_keys_recovered\":0,\"num_objects_omap\":0,\"num_objects_hit_set_archive\":0,\"num_bytes_hit_set_archive\":0,\"num_flush\":0,\"num_flush_kb\":0,\"num_evict\":0,\"num_evict_kb\":0,\"num_promote\":0,\"num_flush_mode_high\":0,\"num_flush_mode_low\":0,\"num_evict_mode_some\":0,\"num_evict_mode_full\":0,\"num_objects_pinned\":0,\"num_legacy_snapsets\":0,\"num_large_omap_objects\":0,\"num_objects_manifest\":0},\"up\":[0],\"acting\":[0],\"blocked_by\":[],\"up_primary\":0,\"acting_primary\":0,\"purged_snaps\":[] }]}";

TEST(formatter, bug_37706) {
  vector<std::string> pgs;

   JSONParser parser;
   ASSERT_TRUE(parser.parse(outstring.c_str(), outstring.size()));

   vector<string> v;

   ASSERT_TRUE (!parser.is_array());

   JSONObj *pgstat_obj = parser.find_obj("pg_stats");
   ASSERT_TRUE (!!pgstat_obj);
   auto s = pgstat_obj->get_data();

   ASSERT_TRUE(!s.empty());
   JSONParser pg_stats;
   ASSERT_TRUE(pg_stats.parse(s.c_str(), s.length()));
   v = pg_stats.get_array_elements();

   for (auto i : v) {
     JSONParser pg_json;
     ASSERT_TRUE(pg_json.parse(i.c_str(), i.length()));
     string pgid;
     JSONDecoder::decode_json("pgid", pgid, &pg_json);
     pgs.emplace_back(std::move(pgid));
   }

   ASSERT_EQ(pgs.back(), "1.0");
}

TEST(formatter, utime)
{
  JSONFormatter formatter;

  utime_t input = ceph_clock_now();
  input.gmtime_nsec(formatter.dump_stream("timestamp"));

  bufferlist bl;
  formatter.flush(bl);

  JSONParser parser;
  EXPECT_TRUE(parser.parse(bl.c_str(), bl.length()));

  cout << input << " -> '" << std::string(bl.c_str(), bl.length())
       << std::endl;

  utime_t output;
  decode_json_obj(output, &parser);
  cout << " -> " << output << std::endl;
  EXPECT_EQ(input.sec(), output.sec());
  EXPECT_EQ(input.nsec(), output.nsec());
}

TEST(formatter, dump_inf_or_nan)
{
  JSONFormatter formatter;
  formatter.open_object_section("inf_and_nan");
  double inf = std::numeric_limits<double>::infinity();
  formatter.dump_float("positive_infinity", inf);
  formatter.dump_float("negative_infinity", -inf);
  formatter.dump_float("nan_val", std::numeric_limits<double>::quiet_NaN());
  formatter.dump_float("nan_val_alt", std::nan(""));
  formatter.close_section();
  bufferlist bl;
  formatter.flush(bl);
  std::cout << std::string(bl.c_str(), bl.length()) << std::endl;
  JSONParser parser;
  parser.parse(bl.c_str(), bl.length());
  EXPECT_TRUE(parser.parse(bl.c_str(), bl.length()));
  EXPECT_EQ(parser.find_obj("positive_infinity")->get_data(), "null");
  EXPECT_EQ(parser.find_obj("negative_infinity")->get_data(), "null");
  EXPECT_EQ(parser.find_obj("nan_val")->get_data(), "null");
  EXPECT_EQ(parser.find_obj("nan_val_alt")->get_data(), "null");
}

TEST(formatter, dump_large_item) {
  JSONFormatter formatter;
  formatter.open_object_section("large_item");

  std::string base_url("http://example.com");
  std::string bucket_name("bucket");
  std::string object_key(1024, 'a');

  std::string full_url = base_url + "/" + bucket_name + "/" + object_key;
  formatter.dump_format("Location", "%s/%s/%s", base_url.c_str(), bucket_name.c_str(), object_key.c_str());

  formatter.close_section();
  bufferlist bl;
  formatter.flush(bl);

  // std::cout << std::string(bl.c_str(), bl.length()) << std::endl;

  JSONParser parser;
  parser.parse(bl.c_str(), bl.length());

  EXPECT_TRUE(parser.parse(bl.c_str(), bl.length()));
  EXPECT_EQ(parser.find_obj("Location")->get_data(), full_url);
}

TEST(formatter, parse_types) {
  // Check that JSONParser::parse() works with expected data types 
  // (otherwise at least indirectly tested elsewhere):

  const auto json_input = R"({"expiration": "2025-01-17T10:26:46Z", "conditions": [{"bucket": "user-hqfadib9zxyj2pygevewzf45t-1"}, ["starts-with", "$key", "foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 1024]]})";
  
  { 
  JSONParser parser;

  ASSERT_TRUE(parser.parse(outstring));

  JSONObj *pgstat_obj = parser.find_obj("pg_stats");
  EXPECT_TRUE(pgstat_obj);
  }

  { 
  JSONParser parser;

  ASSERT_TRUE(parser.parse(std::string_view { outstring }));

  JSONObj *pgstat_obj = parser.find_obj("pg_stats");
  EXPECT_TRUE(pgstat_obj);
  }

  {
  JSONParser parser;

  buffer::list bl;
  bl.append(outstring);
  EXPECT_TRUE(parser.parse(bl.c_str(), bl.length()));
  
  JSONObj *pgstat_obj = parser.find_obj("pg_stats");
  EXPECT_TRUE(pgstat_obj);
  }

  {
  JSONParser parser;
 
  buffer::list bl;
  bl.append(json_input);

  ASSERT_TRUE(parser.parse(bl.c_str(), bl.length()));

  JSONObjIter oi = parser.find_first();

  JSONObj *o0 = *oi;
  ASSERT_TRUE(nullptr != o0);

  ++oi;
  JSONObj *o1 = *oi;
  ASSERT_TRUE(nullptr != o1);
  JSONObj *conditions = parser.find_obj("conditions");
  ASSERT_TRUE(nullptr != conditions);

  JSONObj *expiration = parser.find_obj("expiration");
  ASSERT_TRUE(nullptr != expiration);
  }
}

/* JFW: both of these fail again, AS THEY SHOULD:
    -- IMO we need to fix any b0rken feeds
    -- but, let's re-run this in the test suite so that we can get a comparison
    before we patch things...
TEST(formatter, malformed_input_as_string) {

  // A couple of the common error cases I see now that we're using a more conformant JSON parser come
  // from differences in what malformed input should be accepted as. The two most common cases are:
  // 1) parsing fails at first character, a non-delimited field meant to be a string;
  // 2) parsing fails just before the end, extra NULL byte
  // ...ceph_json should honor these gnarly inputs, at least for now.

  const string_view bare_url = "http://turkey.straw.com";
  const char garbage_at_end[] = { 's', 't', 'r', '\x00', '\x00' };

 JFW: tmp
  {
  JSONParser parser;
  ASSERT_TRUE(parser.parse(bare_url));
  }
 
  {
  JSONParser parser;
  ASSERT_TRUE(parser.parse(garbage_at_end));
  }
}*/

namespace {

void assert_parse(JSONParser& parser, std::string_view json)
{
  ASSERT_TRUE(parser.parse(json.data(), json.size())) << json;
}

JSONObj* assert_find(JSONObj& obj, const char *name)
{
  auto *child = obj.find_obj(name);
  EXPECT_NE(nullptr, child) << name;
  return child;
}

struct compat_record {
  string name;
  int count = 0;
  bool enabled = false;
  vector<int> values;
  map<string, int> by_name;

  void dump(Formatter *f) const {
    encode_json("name", name, f);
    encode_json("count", count, f);
    encode_json("enabled", enabled, f);
    encode_json("values", values, f);
    encode_json("by_name", by_name, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("name", name, obj, true);
    JSONDecoder::decode_json("count", count, obj, true);
    JSONDecoder::decode_json("enabled", enabled, obj, true);
    JSONDecoder::decode_json("values", values, obj, true);
    JSONDecoder::decode_json("by_name", by_name, obj, true);
  }
};

} // anonymous namespace

TEST(formatter, ceph_json_scalar_root_compatibility)
{
  struct scalar_case {
    string_view input;
    string_view data;
    bool quoted;
  };

  const scalar_case cases[] = {
    {R"("plain string")", "plain string", true},
    {"-42", "-42", false},
    {"42", "42", false},
    {"true", "true", false},
    {"false", "false", false},
    {"null", "null", false},
  };

  for (const auto& c : cases) {
    JSONParser parser;
    assert_parse(parser, c.input);

    EXPECT_FALSE(parser.is_array()) << c.input;
    EXPECT_FALSE(parser.is_object()) << c.input;
    EXPECT_EQ(c.data, parser.get_data()) << c.input;
    EXPECT_EQ(c.quoted, parser.get_data_val().quoted) << c.input;
  }
}

TEST(formatter, ceph_json_native_handle_access)
{
  static_assert(std::same_as<JSONParser::native_handle_type, ceph_json::value>);

  constexpr string_view json = R"({"name":"fast","values":[1,2,3]})";

  JSONParser parser;
  assert_parse(parser, json);

  auto& native = parser.native_handle();
  ASSERT_TRUE(native.is_object());

  const auto& const_parser = parser;
  static_assert(std::same_as<decltype(const_parser.native_handle()),
                const JSONParser::native_handle_type&>);
  EXPECT_TRUE(const_parser.native_handle().is_object());

  auto *name = assert_find(parser, "name");
  const auto& native_name = name->native_handle();
  ASSERT_TRUE(native_name.is_string());
  EXPECT_EQ("fast", native_name.as<std::string>());

  auto *values_obj = assert_find(parser, "values");
  const auto *values = values_obj->native_handle().get_if<ceph_json::value::array_t>();
  ASSERT_NE(nullptr, values);
  EXPECT_EQ(3u, values->size());
}

TEST(formatter, ceph_json_object_tree_compatibility)
{
  constexpr string_view json =
    R"({"name":"main","nested":{"n":7,"flag":false},)"
    R"("tricky":"{not: [json], comma, quote \" }","escaped\"key":"ok",)"
    R"("array":[1,"two",{"three":3},null],"empty":[]})";

  JSONParser parser;
  assert_parse(parser, json);

  ASSERT_TRUE(parser.is_object());

  auto *name = assert_find(parser, "name");
  ASSERT_NE(nullptr, name);
  EXPECT_EQ("main", name->get_data());
  EXPECT_TRUE(name->get_data_val().quoted);

  auto *tricky = assert_find(parser, "tricky");
  ASSERT_NE(nullptr, tricky);
  EXPECT_EQ(R"({not: [json], comma, quote " })", tricky->get_data());

  auto *escaped_key = assert_find(parser, R"(escaped"key)");
  ASSERT_NE(nullptr, escaped_key);
  EXPECT_EQ("ok", escaped_key->get_data());

  JSONObj::data_val name_attr;
  ASSERT_TRUE(name->get_attr("name", name_attr));
  EXPECT_EQ("main", name_attr.str);
  EXPECT_TRUE(name_attr.quoted);

  auto *nested = assert_find(parser, "nested");
  ASSERT_NE(nullptr, nested);
  EXPECT_TRUE(nested->is_object());

  int n = 0;
  ASSERT_TRUE(JSONDecoder::decode_json("n", n, nested, true));
  EXPECT_EQ(7, n);

  bool flag = true;
  ASSERT_TRUE(JSONDecoder::decode_json("flag", flag, nested, true));
  EXPECT_FALSE(flag);

  auto *missing = parser.find_obj("missing");
  EXPECT_EQ(nullptr, missing);
}

TEST(formatter, ceph_json_named_find_first_stops_after_named_child)
{
  constexpr string_view json = R"({"alpha":1,"needle":"found","z_tail":2})";

  JSONParser parser;
  assert_parse(parser, json);

  auto iter = parser.find_first("needle");
  ASSERT_FALSE(iter.end());
  EXPECT_EQ("needle", (*iter)->get_name());
  EXPECT_EQ("found", (*iter)->get_data());

  ++iter;
  EXPECT_TRUE(iter.end());
}

TEST(formatter, ceph_json_duplicate_object_members_known_regression)
{
  /*
   * The legacy JSONObj tree could preserve repeated object members with the
   * same name. JSON itself only says object names SHOULD be unique, and an
   * audit of Ceph and src/tools users found no semantic dependency on this:
   * ordinary decoders use the first named field, map-like decoders expect
   * explicit {"key", "val"} entries, and object iteration is used for arrays
   * or generic display. We intentionally follow Glaze's object model here.
   */
  constexpr string_view json =
    R"({"dupe":"first","dupe":"second",)"
    R"("nested":{"again":"first","again":"second"},)"
    R"("array":[{"three":3,"three":4}]})";

  JSONParser parser;
  assert_parse(parser, json);

  auto dupes = parser.find("dupe");
  vector<string> values;
  for (; !dupes.end(); ++dupes) {
    values.push_back((*dupes)->get_data());
  }

  ASSERT_EQ(1u, values.size());
  EXPECT_TRUE(values[0] == "first" || values[0] == "second");

  auto *nested = assert_find(parser, "nested");
  auto again = nested->find("again");
  vector<string> nested_values;
  for (; !again.end(); ++again) {
    nested_values.push_back((*again)->get_data());
  }

  ASSERT_EQ(1u, nested_values.size());
  EXPECT_TRUE(nested_values[0] == "first" || nested_values[0] == "second");

  auto *array = assert_find(parser, "array");
  const auto elements = array->get_array_elements();
  ASSERT_EQ(1u, elements.size());

  JSONParser object_element;
  assert_parse(object_element, elements[0]);

  auto threes = object_element.find("three");
  vector<string> object_values;
  for (; !threes.end(); ++threes) {
    object_values.push_back((*threes)->get_data());
  }

  ASSERT_EQ(1u, object_values.size());
  EXPECT_TRUE(object_values[0] == "3" || object_values[0] == "4");
}

TEST(formatter, ceph_json_array_elements_compatibility)
{
  constexpr string_view json =
    R"({"array":[1,"two",{"three":3},[4,5],null],"empty":[]})";

  JSONParser parser;
  assert_parse(parser, json);

  auto *array = assert_find(parser, "array");
  ASSERT_NE(nullptr, array);
  ASSERT_TRUE(array->is_array());

  const auto elements = array->get_array_elements();
  ASSERT_EQ(5u, elements.size());
  EXPECT_EQ("1", elements[0]);
  EXPECT_EQ(R"("two")", elements[1]);
  EXPECT_EQ("null", elements[4]);

  JSONParser object_element;
  assert_parse(object_element, elements[2]);
  int three = 0;
  ASSERT_TRUE(JSONDecoder::decode_json("three", three, &object_element, true));
  EXPECT_EQ(3, three);

  JSONParser array_element;
  assert_parse(array_element, elements[3]);
  vector<int> nested;
  decode_json_obj(nested, &array_element);
  ASSERT_EQ(2u, nested.size());
  EXPECT_EQ(4, nested[0]);
  EXPECT_EQ(5, nested[1]);

  auto *empty = assert_find(parser, "empty");
  ASSERT_NE(nullptr, empty);
  EXPECT_TRUE(empty->get_array_elements().empty());
}

TEST(formatter, ceph_json_decoder_compatibility)
{
  constexpr string_view json =
    R"({"s":"text","i":-42,"u":42,"b_true":true,"b_false":false,)"
    R"("b_num":1,"values":[1,2,3],)"
    R"("by_name":[{"key":"alpha","val":10},{"key":"beta","val":20}],)"
    R"("maybe":99})";

  JSONParser parser;
  assert_parse(parser, json);

  string s;
  ASSERT_TRUE(JSONDecoder::decode_json("s", s, &parser, true));
  EXPECT_EQ("text", s);

  int i = 0;
  ASSERT_TRUE(JSONDecoder::decode_json("i", i, &parser, true));
  EXPECT_EQ(-42, i);

  unsigned u = 0;
  ASSERT_TRUE(JSONDecoder::decode_json("u", u, &parser, true));
  EXPECT_EQ(42u, u);

  bool b = false;
  ASSERT_TRUE(JSONDecoder::decode_json("b_true", b, &parser, true));
  EXPECT_TRUE(b);

  b = true;
  ASSERT_TRUE(JSONDecoder::decode_json("b_false", b, &parser, true));
  EXPECT_FALSE(b);

  b = false;
  ASSERT_TRUE(JSONDecoder::decode_json("b_num", b, &parser, true));
  EXPECT_TRUE(b);

  vector<int> values;
  ASSERT_TRUE(JSONDecoder::decode_json("values", values, &parser, true));
  ASSERT_EQ(3u, values.size());
  EXPECT_EQ(1, values[0]);
  EXPECT_EQ(2, values[1]);
  EXPECT_EQ(3, values[2]);

  map<string, int> by_name;
  ASSERT_TRUE(JSONDecoder::decode_json("by_name", by_name, &parser, true));
  ASSERT_EQ(2u, by_name.size());
  EXPECT_EQ(10, by_name["alpha"]);
  EXPECT_EQ(20, by_name["beta"]);

  boost::optional<int> maybe;
  ASSERT_TRUE(JSONDecoder::decode_json("maybe", maybe, &parser, true));
  ASSERT_TRUE(maybe);
  EXPECT_EQ(99, *maybe);

  int missing = 123;
  EXPECT_FALSE(JSONDecoder::decode_json("missing", missing, &parser));
  EXPECT_EQ(0, missing);

  boost::optional<int> missing_optional = 7;
  EXPECT_FALSE(JSONDecoder::decode_json("missing_optional", missing_optional, &parser));
  EXPECT_FALSE(missing_optional);

  EXPECT_THROW(JSONDecoder::decode_json("missing_required", missing, &parser, true),
               JSONDecoder::err);
}

TEST(formatter, ceph_json_encode_decode_compatibility)
{
  compat_record input;
  input.name = "encoded";
  input.count = 17;
  input.enabled = true;
  input.values = {3, 5, 8};
  input.by_name = {{"first", 1}, {"second", 2}};

  JSONFormatter formatter;
  formatter.open_object_section("root");
  encode_json("record", input, &formatter);
  formatter.close_section();

  stringstream ss;
  formatter.flush(ss);

  JSONParser parser;
  assert_parse(parser, ss.str());

  auto *record_obj = assert_find(parser, "record");
  ASSERT_NE(nullptr, record_obj);

  compat_record output;
  decode_json_obj(output, record_obj);

  EXPECT_EQ(input.name, output.name);
  EXPECT_EQ(input.count, output.count);
  EXPECT_EQ(input.enabled, output.enabled);
  EXPECT_EQ(input.values, output.values);
  EXPECT_EQ(input.by_name, output.by_name);
}
