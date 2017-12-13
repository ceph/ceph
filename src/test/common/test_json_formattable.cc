// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
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

#include <sstream>

using namespace std;


static void get_jf(const string& s, JSONFormattable *f)
{
  JSONParser p;
  bool result = p.parse(s.c_str(), s.size());
  ASSERT_EQ(true, result);
  try {
    decode_json_obj(*f, &p);
  } catch (JSONDecoder::err& e) {
    ASSERT_TRUE(0 == "Failed to decode JSON object");
  }
}

TEST(formatable, str) {
  JSONFormattable f;
  get_jf("{ \"foo\": \"bar\" }", &f);
  ASSERT_EQ((string)f["foo"], "bar");
  ASSERT_EQ((string)f["fooz"], "");
  ASSERT_EQ((string)f["fooz"]("lala"), "lala");
}

TEST(formatable, str2) {
  JSONFormattable f;
  get_jf("{ \"foo\": \"bar\" }", &f);
  ASSERT_EQ((string)f["foo"], "bar");
  ASSERT_EQ((string)f["fooz"], "");
  ASSERT_EQ((string)f["fooz"]("lala"), "lala");

  JSONFormattable f2;
  get_jf("{ \"foo\": \"bar\", \"fooz\": \"zzz\" }", &f2);
  ASSERT_EQ((string)f2["foo"], "bar");
  ASSERT_NE((string)f2["fooz"], "");
  ASSERT_EQ((string)f2["fooz"], "zzz");
  ASSERT_EQ((string)f2["fooz"]("lala"), "zzz");

}

TEST(formatable, int) {
  JSONFormattable f;
  get_jf("{ \"foo\": 1 }", &f);
  ASSERT_EQ((int)f["foo"], 1);
  ASSERT_EQ((int)f["fooz"], 0);
  ASSERT_EQ((int)f["fooz"](3), 3);

  JSONFormattable f2;
  get_jf("{ \"foo\": \"bar\", \"fooz\": \"123\" }", &f2);
  ASSERT_EQ((string)f2["foo"], "bar");
  ASSERT_NE((int)f2["fooz"], 0);
  ASSERT_EQ((int)f2["fooz"], 123);
  ASSERT_EQ((int)f2["fooz"](111), 123);
}

TEST(formatable, bool) {
  JSONFormattable f;
  get_jf("{ \"foo\": \"true\" }", &f);
  ASSERT_EQ((bool)f["foo"], true);
  ASSERT_EQ((bool)f["fooz"], false);
  ASSERT_EQ((bool)f["fooz"](true), true);

  JSONFormattable f2;
  get_jf("{ \"foo\": \"false\" }", &f);
  ASSERT_EQ((bool)f["foo"], false);
}

TEST(formatable, nested) {
  JSONFormattable f;
  get_jf("{ \"obj\": { \"foo\": 1, \"inobj\": { \"foo\": 2 } } }", &f);
  ASSERT_EQ((int)f["foo"], 0);
  ASSERT_EQ((int)f["obj"]["foo"], 1);
  ASSERT_EQ((int)f["obj"]["inobj"]["foo"], 2);
}

TEST(formatable, array) {
  JSONFormattable f;
  get_jf("{ \"arr\": [ { \"foo\": 1, \"inobj\": { \"foo\": 2 } }," 
         "{ \"foo\": 2 } ] }", &f);

  int i = 1;
  for (auto a : f.array()) {
    ASSERT_EQ((int)a["foo"], i);
    ++i;
  }

  JSONFormattable f2;
  get_jf("{ \"arr\": [ 0, 1, 2, 3, 4 ]}", &f2);

  i = 0;
  for (auto a : f2.array()) {
    ASSERT_EQ((int)a, i);
    ++i;
  }
}

TEST(formatable, bin_encode) {
  JSONFormattable f, f2;
  get_jf("{ \"arr\": [ { \"foo\": 1, \"bar\": \"aaa\", \"inobj\": { \"foo\": 2 } }," 
         "{ \"foo\": 2, \"inobj\": { \"foo\": 3 } } ] }", &f);

  int i = 1;
  for (auto a : f.array()) {
    ASSERT_EQ((int)a["foo"], i);
    ASSERT_EQ((int)a["foo"]["inobj"], i + 1);
    ASSERT_EQ((string)a["bar"], "aaa");
    ++i;
  }

  bufferlist bl;
  ::encode(f, bl);
  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(f2, iter);
  } catch (buffer::error& err) {
    ASSERT_TRUE(0 == "Failed to decode object");
  }

  i = 1;
  for (auto a : f2.array()) {
    ASSERT_EQ((int)a["foo"], i);
    ASSERT_EQ((int)a["foo"]["inobj"], i + 1);
    ASSERT_EQ((string)a["bar"], "aaa");
    ++i;
  }

}

TEST(formatable, json_encode) {
  JSONFormattable f, f2;
  get_jf("{ \"arr\": [ { \"foo\": 1, \"bar\": \"aaa\", \"inobj\": { \"foo\": 2 } }," 
         "{ \"foo\": 2, \"inobj\": { \"foo\": 3 } } ] }", &f);

  JSONFormatter formatter;
  formatter.open_object_section("bla");
  ::encode_json("f", f, &formatter);
  formatter.close_section();

  stringstream ss;
  formatter.flush(ss);

  get_jf(ss.str(), &f2);

  int i = 1;
  for (auto a : f2.array()) {
    ASSERT_EQ((int)a["foo"], i);
    ASSERT_EQ((int)a["foo"]["inobj"], i + 1);
    ASSERT_EQ((string)a["bar"], "aaa");
    ++i;
  }

}

