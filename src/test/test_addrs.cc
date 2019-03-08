// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "include/stringify.h"
#include "msg/msg_types.h"
#include "gtest/gtest.h"

#include <sstream>

// input, parsed+printed addr output, leftover
// if the parse fails, output + leftover should both be blank.
const char *addr_checks[][3] = {
  { "127.0.0.1", "v2:127.0.0.1:0/0", "" },
  { "127.0.0.1 foo", "v2:127.0.0.1:0/0", " foo" },
  { "127.0.0.1:1234 foo", "v2:127.0.0.1:1234/0", " foo" },
  { "127.0.0.1:1234/5678 foo", "v2:127.0.0.1:1234/5678", " foo" },
  { "1.2.3:4 a", "", "1.2.3:4 a" },
  { "2607:f298:4:2243::5522", "v2:[2607:f298:4:2243::5522]:0/0", "" },
  { "[2607:f298:4:2243::5522]", "v2:[2607:f298:4:2243::5522]:0/0", "" },
  { "2607:f298:4:2243::5522a", "", "2607:f298:4:2243::5522a" },
  { "[2607:f298:4:2243::5522]a", "v2:[2607:f298:4:2243::5522]:0/0", "a" },
  { "[2607:f298:4:2243::5522]:1234a", "v2:[2607:f298:4:2243::5522]:1234/0", "a" },
  { "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "v2:[2001:db8:85a3::8a2e:370:7334]:0/0", "" },
  { "2001:2db8:85a3:4334:4324:8a2e:1370:7334", "v2:[2001:2db8:85a3:4334:4324:8a2e:1370:7334]:0/0", "" },
  { "::", "v2:[::]:0/0", "" },
  { "::zz", "v2:[::]:0/0", "zz" },
  { ":: 12:34", "v2:[::]:0/0", " 12:34" },
  { "-", "-", "" },
  { "-asdf", "-", "asdf" },
  { "v1:1.2.3.4", "v1:1.2.3.4:0/0", "" },
  { "v1:1.2.3.4:12", "v1:1.2.3.4:12/0", "" },
  { "v1:1.2.3.4:12/34", "v1:1.2.3.4:12/34", "" },
  { "v2:1.2.3.4", "v2:1.2.3.4:0/0", "" },
  { "v2:1.2.3.4:12", "v2:1.2.3.4:12/0", "" },
  { "v2:1.2.3.4:12/34", "v2:1.2.3.4:12/34", "" },
  { NULL, NULL, NULL },
};

const char *addr_only_checks[][3] = {
  // we shouldn't parse an addrvec...
  { "[v2:1.2.3.4:111/0,v1:5.6.7.8:222/0]", "", "[v2:1.2.3.4:111/0,v1:5.6.7.8:222/0]" },
  { NULL, NULL, NULL },
};



TEST(Msgr, TestAddrParsing)
{
  for (auto& addr_checks : { addr_checks, addr_only_checks }) {
    for (unsigned i = 0; addr_checks[i][0]; ++i) {
      entity_addr_t a;
      const char *end = "";
      bool ok = a.parse(addr_checks[i][0], &end);
      string out;
      if (ok) {
	stringstream ss;
	ss << a;
	getline(ss, out);
      }
      string left = end;
      
      cout << "'" << addr_checks[i][0] << "' -> '" << out << "' + '" << left << "'" << std::endl;

      ASSERT_EQ(out, addr_checks[i][1]);
      ASSERT_EQ(left, addr_checks[i][2]);
      if (addr_checks[i][0] == end) {
	ASSERT_FALSE(ok);
      } else {
	ASSERT_TRUE(ok);
      }
    }
  }
}

// check that legacy encoding to new decoding behaves

const char *addr_checks2[][3] = {
  { "v1:127.0.0.1", "v1:127.0.0.1:0/0", "" },
  { "v1:127.0.0.1 foo", "v1:127.0.0.1:0/0", " foo" },
  { "v1:127.0.0.1:1234 foo", "v1:127.0.0.1:1234/0", " foo" },
  { "v1:127.0.0.1:1234/5678 foo", "v1:127.0.0.1:1234/5678", " foo" },
  { "v1:2607:f298:4:2243::5522", "v1:[2607:f298:4:2243::5522]:0/0", "" },
  { "v1:[2607:f298:4:2243::5522]", "v1:[2607:f298:4:2243::5522]:0/0", "" },
  { "v1:[2607:f298:4:2243::5522]a", "v1:[2607:f298:4:2243::5522]:0/0", "a" },
  { "v1:[2607:f298:4:2243::5522]:1234a", "v1:[2607:f298:4:2243::5522]:1234/0", "a" },
  { "v1:2001:0db8:85a3:0000:0000:8a2e:0370:7334", "v1:[2001:db8:85a3::8a2e:370:7334]:0/0", "" },
  { "v1:2001:2db8:85a3:4334:4324:8a2e:1370:7334", "v1:[2001:2db8:85a3:4334:4324:8a2e:1370:7334]:0/0", "" },
  { "v1:1.2.3.4", "v1:1.2.3.4:0/0", "" },
  { "v1:1.2.3.4:12", "v1:1.2.3.4:12/0", "" },
  { "v1:1.2.3.4:12/34", "v1:1.2.3.4:12/34", "" },
  { NULL, NULL, NULL },
};

TEST(Msgr, TestAddrEncodeAddrvecDecode)
{
  for (unsigned i = 0; addr_checks2[i][0]; ++i) {
    entity_addr_t addr;
    entity_addrvec_t addrvec;
    const char *end = "";
    bool ok = addr.parse(addr_checks2[i][0], &end);
    ASSERT_TRUE(ok);
    bufferlist bl;
    addr.encode(bl, 0);
    auto bli = bl.cbegin();
    addrvec.decode(bli);
    cout << addr_checks2[i][0] << " " << addr << " " << addrvec << std::endl;
    ASSERT_EQ(addr, addrvec.v[0]);
    if (addr_checks2[i][0] == end) {
      ASSERT_FALSE(ok);
    } else {
      ASSERT_TRUE(ok);
    }
  }
}

TEST(Msgr, TestAddrvec0EncodeAddrDecode)
{
  for (unsigned i = 0; addr_checks2[i][0]; ++i) {
    entity_addr_t addr;
    entity_addrvec_t addrvec;
    bufferlist bl;
    const char *end = "";
    bool ok = addr.parse(addr_checks2[i][0], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
    addrvec.encode(bl, 0);
    auto bli = bl.cbegin();
    entity_addr_t a;
    a.decode(bli);
    ASSERT_EQ(addr, a);
  }
}

TEST(Msgr, TestEmptyAddrvecEncodeAddrDecode)
{
  entity_addrvec_t addrvec;
  entity_addr_t addr;
  bufferlist bl;
  addrvec.encode(bl, 0);
  auto bli = bl.cbegin();
  addr.decode(bli);
  ASSERT_EQ(addr, entity_addr_t());
}

const char *addrvec_checks[][4] = {
  { "v1:1.2.3.4", "v2:1.2.3.4", "v1:1.2.3.4", "v2:1.2.3.4" },
  { "v2:1.2.3.5", "v1:1.2.3.5", "v1:1.2.3.5", "v2:1.2.3.5" },
  { "v2:1.2.3.6", "v2:1.2.3.6", "v1:1.2.3.6", "v2:1.2.3.6" },
  { "v2:1.2.3.7", "v1:1.2.3.7", "v1:1.2.3.7", "v2:1.2.3.7" },
  { NULL, NULL, NULL, NULL },
};

/*
 * multiple addrs where one is legacy and others are not
 * legacy addr is in position 0
 */
TEST(Msgr, TestAddrvecEncodeAddrDecode0)
{
  entity_addr_t addr;
  entity_addrvec_t addrvec;
  bufferlist bl;

  for (unsigned i = 0; addrvec_checks[i][0]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][0], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  auto bli = bl.cbegin();

  addr.decode(bli);

  ASSERT_EQ(addr, addrvec.v[0]);
}

/*
 * multiple addrs where one is legacy and others are not
 * legacy addr is not in position 0
 */
TEST(Msgr, TestAddrvecEncodeAddrDecode1)
{
  entity_addr_t addr, a;
  entity_addrvec_t addrvec;
  bufferlist bl;
  bool flag = true;

  for (unsigned i = 0; addrvec_checks[i][1]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][1], &end);
    ASSERT_TRUE(ok);
    if (addr.type == entity_addr_t::TYPE_LEGACY && flag) {
      a = addr;
      flag = !flag;
    }
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  auto bli = bl.cbegin();

  addr.decode(bli);

  ASSERT_EQ(addr, a);
}

/* multiple legacy addrs */
TEST(Msgr, TestAddrvecEncodeAddrDecode2)
{
  entity_addr_t addr;
  entity_addrvec_t addrvec;
  bufferlist bl;

  for (unsigned i = 0; addrvec_checks[i][2]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][2], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  auto bli = bl.cbegin();

  addr.decode(bli);

  ASSERT_EQ(addr, addrvec.v[0]);
}

/* all non-legacy addrs */
TEST(Msgr, TestAddrvecEncodeAddrDecode3)
{
  entity_addr_t addr;
  entity_addrvec_t addrvec;
  bufferlist bl;

  for (unsigned i = 0; addrvec_checks[i][3]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][3], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  auto bli = bl.cbegin();

  addr.decode(bli);
  //cout << addrvec << " (legacy " << addrvec.legacy_addr()
  //<< ") -> " << addr << std::endl;

  ASSERT_NE(addr, addrvec.v[0]); // it's not the first addr(which is non-legacy)
  ASSERT_EQ(addr, entity_addr_t()); // it's not a blank addr either
}

const char *addrvec_parse_checks[][3] = {
  { "", "", "" },
  { "foo", "", "foo" },
  { " foo", "", " foo" },
  { "127.0.0.1", "v2:127.0.0.1:0/0", "" },
  { "127.0.0.1 foo", "v2:127.0.0.1:0/0", " foo" },
  { "[127.0.0.1]", "v2:127.0.0.1:0/0", "" },
  { "[127.0.0.1] foo", "v2:127.0.0.1:0/0", " foo" },
  { "127.0.0.1,::,- foo", "v2:127.0.0.1:0/0", ",::,- foo" },
  { "[127.0.0.1,::,-] foo", "[v2:127.0.0.1:0/0,v2:[::]:0/0,-]", " foo" },
  { "[127.0.0.1,::],- foo", "[v2:127.0.0.1:0/0,v2:[::]:0/0]", ",- foo" },
  { "[1.2.3.4,::,foo]", "", "[1.2.3.4,::,foo]" },
  { "[1.2.3.4,::,- foo", "", "[1.2.3.4,::,- foo" },
  { "[[::],1.2.3.4]", "[v2:[::]:0/0,v2:1.2.3.4:0/0]", "" },
  { "[::],1.2.3.4", "v2:[::]:0/0", ",1.2.3.4" },
  { NULL, NULL, NULL },
};

TEST(entity_addrvec_t, parse)
{
  entity_addrvec_t addrvec;

  for (auto v : { addr_checks, addr_checks2, addrvec_parse_checks }) {
    for (unsigned i = 0; v[i][0]; ++i) {
      const char *end = "";
      bool ret = addrvec.parse(v[i][0], &end);
      string out = stringify(addrvec);
      string left = end;
      cout << "'" << v[i][0] << "' -> '" << out << "' + '" << left << "'"
	   << std::endl;
      ASSERT_EQ(out, v[i][1]);
      ASSERT_EQ(left, v[i][2]);
      ASSERT_TRUE(out.empty() || ret);
    }
  }
}

TEST(entity_addrvec_t, legacy_equals)
{
  entity_addr_t a1, a2;
  ASSERT_TRUE(a1.parse("v1:1.2.3.4:567/890"));
  ASSERT_TRUE(a2.parse("v2:1.2.3.4:567/890"));
  entity_addrvec_t av1(a1);
  entity_addrvec_t av21;
  av21.v.push_back(a2);
  av21.v.push_back(a1);
  ASSERT_TRUE(av1.legacy_equals(av1));
  ASSERT_TRUE(av21.legacy_equals(av21));
  ASSERT_TRUE(av1.legacy_equals(av21));
  ASSERT_TRUE(av21.legacy_equals(av1));

  entity_addr_t b1, b2;
  ASSERT_TRUE(b1.parse("v1:1.2.3.5:567/8"));
  ASSERT_TRUE(b2.parse("v2:1.2.3.5:567/8"));
  entity_addrvec_t bv1(b1);
  entity_addrvec_t bv21;
  bv21.v.push_back(b2);
  bv21.v.push_back(b1);
  ASSERT_TRUE(bv1.legacy_equals(bv21));
  ASSERT_TRUE(bv21.legacy_equals(bv1));

  ASSERT_FALSE(av1.legacy_equals(bv1));
  ASSERT_FALSE(av21.legacy_equals(bv21));
  ASSERT_FALSE(av21.legacy_equals(bv1));
  ASSERT_FALSE(av1.legacy_equals(bv21));
}
