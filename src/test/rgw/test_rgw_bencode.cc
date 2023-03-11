// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
