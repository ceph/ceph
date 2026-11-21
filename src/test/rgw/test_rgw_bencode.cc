// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw_torrent.h"

using namespace std;

TEST(Bencode, String)
{
  bufferlist bl;

  bencode("foo", bl);
  bencode("bar", bl);
  bencode("baz", bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("3:foo3:bar3:baz", s.c_str());
}

TEST(Bencode, Integers)
{
  bufferlist bl;

  bencode(0, bl);
  bencode(-3, bl);
  bencode(7, bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("i0ei-3ei7e", s.c_str());
}

TEST(Bencode, LargeIntegers)
{
  bufferlist bl;

  // object lengths up to rgw_torrent_max_size (5G by default) and
  // creation dates past 2038 don't fit in 32 bits
  bencode(uint64_t{5} << 30, bl);
  bencode(INT64_C(4102444800), bl);
  bencode(INT64_MIN, bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("i5368709120ei4102444800ei-9223372036854775808e", s.c_str());
}

TEST(Bencode, LargeDictValue)
{
  bufferlist bl;

  bencode_dict(bl);
  bencode("length", size_t{3} << 30, bl);
  bencode_end(bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("d6:lengthi3221225472ee", s.c_str());
}

TEST(Bencode, Dict)
{
  bufferlist bl;

  bencode_dict(bl);
  bencode("foo", 5, bl);
  bencode("bar", "baz", bl);
  bencode_end(bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("d3:fooi5e3:bar3:baze", s.c_str());
}

TEST(Bencode, List)
{
  bufferlist bl;

  bencode_list(bl);
  bencode("foo", 5, bl);
  bencode("bar", "baz", bl);
  bencode_end(bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("l3:fooi5e3:bar3:baze", s.c_str());
}
