// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
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

#include <sstream>

using namespace std;


static void get_jf(const string& s, JSONFormattable *f)
{
  JSONParser p;
  bool result = p.parse(s.c_str(), s.size());
  if (!result) {
    cout << "Failed to parse: '" << s << "'" << std::endl;
  }
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
  auto iter = bl.cbegin();
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

TEST(formatable, set) {
  JSONFormattable f, f2;

  f.set("", "{ \"abc\": \"xyz\"}");
  ASSERT_EQ((string)f["abc"], "xyz");

  f.set("aaa", "111");
  ASSERT_EQ((string)f["abc"], "xyz");
  ASSERT_EQ((int)f["aaa"], 111);

  f.set("obj", "{ \"a\": \"10\", \"b\": \"20\"}");
  ASSERT_EQ((int)f["obj"]["a"], 10);
  ASSERT_EQ((int)f["obj"]["b"], 20);

  f.set("obj.c", "30");

  ASSERT_EQ((int)f["obj"]["c"], 30);
}

TEST(formatable, erase) {
  JSONFormattable f, f2;

  f.set("", "{ \"abc\": \"xyz\"}");
  ASSERT_EQ((string)f["abc"], "xyz");

  f.set("aaa", "111");
  ASSERT_EQ((string)f["abc"], "xyz");
  ASSERT_EQ((int)f["aaa"], 111);
  f.erase("aaa");
  ASSERT_EQ((int)f["aaa"], 0);

  f.set("obj", "{ \"a\": \"10\", \"b\": \"20\"}");
  ASSERT_EQ((int)f["obj"]["a"], 10);
  ASSERT_EQ((int)f["obj"]["b"], 20);
  f.erase("obj.a");
  ASSERT_EQ((int)f["obj"]["a"], 0);
  ASSERT_EQ((int)f["obj"]["b"], 20);
}

template <class T>
static void dumpt(const T& t, const char *n)
{
  JSONFormatter formatter;
  formatter.open_object_section("bla");
  ::encode_json(n, t, &formatter);
  formatter.close_section();
  formatter.flush(cout);
}

static void dumpf(const JSONFormattable& f) {
  dumpt(f, "f");
}

TEST(formatable, set_array) {
  JSONFormattable f, f2;

  f.set("asd[0]", "\"xyz\"");
  ASSERT_EQ(1u, f["asd"].array().size());
  ASSERT_EQ((string)f["asd"][0], "xyz");

  f.set("bbb[0][0]", "10");
  f.set("bbb[0][1]", "20");
  ASSERT_EQ(1u, f["bbb"].array().size());
  ASSERT_EQ(2u, f["bbb"][0].array().size());
  ASSERT_EQ("10", (string)f["bbb"][0][0]);
  ASSERT_EQ(20, (int)f["bbb"][0][1]);
  f.set("bbb[0][1]", "25");
  ASSERT_EQ(2u, f["bbb"][0].array().size());
  ASSERT_EQ(25, (int)f["bbb"][0][1]);

  f.set("bbb[0][]", "26"); /* append operation */
  ASSERT_EQ(26, (int)f["bbb"][0][2]);
  f.set("bbb[0][-1]", "27"); /* replace last */
  ASSERT_EQ(27, (int)f["bbb"][0][2]);
  ASSERT_EQ(3u, f["bbb"][0].array().size());

  f.set("foo.asd[0][0]", "{ \"field\": \"xyz\"}");
  ASSERT_EQ((string)f["foo"]["asd"][0][0]["field"], "xyz");

  ASSERT_EQ(f.set("foo[0]", "\"zzz\""), -EINVAL); /* can't assign array to an obj entity */

  f2.set("[0]", "{ \"field\": \"xyz\"}");
  ASSERT_EQ((string)f2[0]["field"], "xyz");
}

TEST(formatable, erase_array) {
  JSONFormattable f;

  f.set("asd[0]", "\"xyz\"");
  ASSERT_EQ(1u, f["asd"].array().size());
  ASSERT_EQ("xyz", (string)f["asd"][0]);
  ASSERT_TRUE(f["asd"].exists(0));
  f.erase("asd[0]");
  ASSERT_FALSE(f["asd"].exists(0));
  f.set("asd[0]", "\"xyz\"");
  ASSERT_TRUE(f["asd"].exists(0));
  f["asd"].erase("[0]");
  ASSERT_FALSE(f["asd"].exists(0));
  f.set("asd[0]", "\"xyz\"");
  ASSERT_TRUE(f["asd"].exists(0));
  f.erase("asd");
  ASSERT_FALSE(f["asd"].exists(0));
  ASSERT_FALSE(f.exists("asd"));

  f.set("bbb[]", "10");
  f.set("bbb[]", "20");
  f.set("bbb[]", "30");
  ASSERT_EQ((int)f["bbb"][0], 10);
  ASSERT_EQ((int)f["bbb"][1], 20);
  ASSERT_EQ((int)f["bbb"][2], 30);
  f.erase("bbb[-2]");
  ASSERT_FALSE(f.exists("bbb[2]"));

  ASSERT_EQ((int)f["bbb"][0], 10);
  ASSERT_EQ((int)f["bbb"][1], 30);

  if (0) { /* for debugging when needed */
    dumpf(f);
  }
}

void formatter_convert(JSONFormatter& formatter, JSONFormattable *dest)
{
  stringstream ss;
  formatter.flush(ss);
  get_jf(ss.str(), dest);
}

TEST(formatable, encode_simple) {
  JSONFormattable f;

  encode_json("foo", "bar", &f);

  ASSERT_EQ((string)f["foo"], "bar");


  JSONFormatter formatter;
  {
    Formatter::ObjectSection s(formatter, "os");
    encode_json("f", f, &formatter);
  }

  JSONFormattable jf2;
  formatter_convert(formatter, &jf2);

  ASSERT_EQ((string)jf2["f"]["foo"], "bar");
}


struct struct1 {
  long i;
  string s;
  bool b;

  struct1() {
    void *p = (void *)this;
    i = (long)p;
    char buf[32];
    snprintf(buf, sizeof(buf), "%p", p);
    s = buf;
    b = (bool)(i % 2);
  }

  void dump(Formatter *f) const {
    encode_json("i", i, f);
    encode_json("s", s, f);
    encode_json("b", b, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("i", i, obj);
    JSONDecoder::decode_json("s", s, obj);
    JSONDecoder::decode_json("b", b, obj);
  }

  bool compare(const JSONFormattable& jf) const {
    bool ret = (s == (string)jf["s"] &&
            i == (long)jf["i"] &&
            b == (bool)jf["b"]);

    if (!ret) {
      cout << "failed comparison: s=" << s << " jf[s]=" << (string)jf["s"] << 
        " i=" << i << " jf[i]=" << (long)jf["i"] << " b=" << b << " jf[b]=" << (bool)jf["b"] << std::endl;
      dumpf(jf);
    }

    return ret;
  }
};


struct struct2 {
  struct1 s1;
  vector<struct1> v;

  struct2() {
    void *p = (void *)this;
    long i = (long)p;
    v.resize((i >> 16) % 16 + 1);
  }

  void dump(Formatter *f) const {
    encode_json("s1", s1, f);
    encode_json("v", v, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("s1", s1, obj);
    JSONDecoder::decode_json("v", v, obj);
  }

  bool compare(const JSONFormattable& jf) const {
    if (!s1.compare(jf["s1"])) {
      cout << "s1.compare(jf[s1] failed" << std::endl;
      return false;
    }

    if (v.size() != jf["v"].array().size()) {
      cout << "v.size()=" << v.size() << " jf[v].array().size()=" << jf["v"].array().size() << std::endl;
      return false;
    }

    auto viter = v.begin();
    auto jiter = jf["v"].array().begin();

    for (; viter != v.end(); ++viter, ++jiter) {
      if (!viter->compare(*jiter)) {
        return false;
      }
    }
    return true;
  }
};


TEST(formatable, encode_struct) {
  JSONFormattable f;

  struct2 s2;

  {
    Formatter::ObjectSection s(f, "section");
    encode_json("foo", "bar", &f);
    encode_json("s2", s2, &f);
  }

  dumpt(s2, "s2");
  cout << std::endl;
  cout << std::endl;

  ASSERT_EQ((string)f["foo"], "bar");
  ASSERT_TRUE(s2.compare(f["s2"]));


  JSONFormatter formatter;
  encode_json("f", f, &formatter);

  JSONFormattable jf2;

  formatter_convert(formatter, &jf2);

  ASSERT_EQ((string)jf2["foo"], "bar");
  ASSERT_TRUE(s2.compare(jf2["s2"]));
}

