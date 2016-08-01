// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "rgw/rgw_torrent.h"

TEST(Bencode, String)
{
  TorrentBencode decode;
  bufferlist bl;

  decode.bencode("foo", bl);
  decode.bencode("bar", bl);
  decode.bencode("baz", bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("3:foo3:bar3:baz", s.c_str());
}

TEST(Bencode, Integers)
{
  TorrentBencode decode;
  bufferlist bl;

  decode.bencode(0, bl);
  decode.bencode(-3, bl);
  decode.bencode(7, bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("i0ei-3ei7e", s.c_str());
}

TEST(Bencode, Dict)
{
  TorrentBencode decode;  
  bufferlist bl;

  decode.bencode_dict(bl);
  decode.bencode("foo", 5, bl);
  decode.bencode("bar", "baz", bl);
  decode.bencode_end(bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("d3:fooi5e3:bar3:baze", s.c_str());
}

TEST(Bencode, List)
{
  TorrentBencode decode;
  bufferlist bl;

  decode.bencode_list(bl);
  decode.bencode("foo", 5, bl);
  decode.bencode("bar", "baz", bl);
  decode.bencode_end(bl);

  string s(bl.c_str(), bl.length());

  ASSERT_STREQ("l3:fooi5e3:bar3:baze", s.c_str());
}
