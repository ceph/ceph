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
#include "msg/msg_types.h"
#include "gtest/gtest.h"

#include <sstream>

// input, parsed+printed addr output, leftover
// if the parse fails, output + leftover should both be blank.
const char *addr_checks[][3] = {
  { "127.0.0.1", "127.0.0.1:0/0", "" },
  { "127.0.0.1 foo", "127.0.0.1:0/0", " foo" },
  { "127.0.0.1:1234 foo", "127.0.0.1:1234/0", " foo" },
  { "127.0.0.1:1234/5678 foo", "127.0.0.1:1234/5678", " foo" },
  { "1.2.3:4 a", "", "" },
  { "2607:f298:4:2243::5522", "[2607:f298:4:2243::5522]:0/0", "" },
  { "[2607:f298:4:2243::5522]", "[2607:f298:4:2243::5522]:0/0", "" },
  { "2607:f298:4:2243::5522a", "", "" },
  { "[2607:f298:4:2243::5522]a", "[2607:f298:4:2243::5522]:0/0", "a" },
  { "[2607:f298:4:2243::5522]:1234a", "[2607:f298:4:2243::5522]:1234/0", "a" },
  { "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "[2001:db8:85a3::8a2e:370:7334]:0/0", "" },
  { "2001:2db8:85a3:4334:4324:8a2e:1370:7334", "[2001:2db8:85a3:4334:4324:8a2e:1370:7334]:0/0", "" },
  { "::", "[::]:0/0", "" },
  { "::zz", "[::]:0/0", "zz" },
  { ":: 12:34", "[::]:0/0", " 12:34" },
  { "-", "-", "" },
  { "-asdf", "-", "asdf" },
  { "legacy:1.2.3.4", "1.2.3.4:0/0", "" },
  { "legacy:1.2.3.4:12", "1.2.3.4:12/0", "" },
  { "legacy:1.2.3.4:12/34", "1.2.3.4:12/34", "" },
  { "msgr2:1.2.3.4", "msgr2:1.2.3.4:0/0", "" },
  { "msgr2:1.2.3.4:12", "msgr2:1.2.3.4:12/0", "" },
  { "msgr2:1.2.3.4:12/34", "msgr2:1.2.3.4:12/34", "" },
  { NULL, NULL, NULL },
};

TEST(Msgr, TestAddrParsing)
{
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
  }
}

const char *addr_checks2[][3] = {
  { "127.0.0.1", "127.0.0.1:0/0", "" },
  { "127.0.0.1 foo", "127.0.0.1:0/0", " foo" },
  { "127.0.0.1:1234 foo", "127.0.0.1:1234/0", " foo" },
  { "127.0.0.1:1234/5678 foo", "127.0.0.1:1234/5678", " foo" },
  { "2607:f298:4:2243::5522", "[2607:f298:4:2243::5522]:0/0", "" },
  { "[2607:f298:4:2243::5522]", "[2607:f298:4:2243::5522]:0/0", "" },
  { "[2607:f298:4:2243::5522]a", "[2607:f298:4:2243::5522]:0/0", "a" },
  { "[2607:f298:4:2243::5522]:1234a", "[2607:f298:4:2243::5522]:1234/0", "a" },
  { "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "[2001:db8:85a3::8a2e:370:7334]:0/0", "" },
  { "2001:2db8:85a3:4334:4324:8a2e:1370:7334", "[2001:2db8:85a3:4334:4324:8a2e:1370:7334]:0/0", "" },
  { "legacy:1.2.3.4", "1.2.3.4:0/0", "" },
  { "legacy:1.2.3.4:12", "1.2.3.4:12/0", "" },
  { "legacy:1.2.3.4:12/34", "1.2.3.4:12/34", "" },
  { NULL, NULL, NULL },
};

TEST(Msgr, TestAddrEncodeAddrvecDecode)
{
  for (unsigned i = 0; addr_checks2[i][0]; ++i) {
    entity_addr_t addr;
    entity_addrvec_t addrvec;
    bufferlist::iterator bli;
    const char *end = "";
    bool ok = addr.parse(addr_checks2[i][0], &end);
    ASSERT_TRUE(ok);
    bufferlist bl;
    addr.encode(bl, 0);
    bli = bl.begin();
    addrvec.decode(bli);
    ASSERT_EQ(addr, addrvec.v[0]);
  }
}

TEST(Msgr, TestAddrvec0EncodeAddrDecode)
{
  for (unsigned i = 0; addr_checks2[i][0]; ++i) {
    entity_addr_t addr;
    entity_addrvec_t addrvec;
    bufferlist bl;
    bufferlist::iterator bli;
    const char *end = "";
    bool ok = addr.parse(addr_checks2[i][0], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
    addrvec.encode(bl, 0);
    bli = bl.begin();
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
  bufferlist::iterator bli;
  addrvec.encode(bl, 0);
  bli = bl.begin();
  addr.decode(bli);
  ASSERT_EQ(addr, entity_addr_t(1, 0));
}

const char *addrvec_checks[][4] = {
  { "legacy:1.2.3.4", "msgr2:1.2.3.4", "legacy:1.2.3.4", "msgr2:1.2.3.4" },
  { "msgr2:1.2.3.5", "legacy:1.2.3.5", "legacy:1.2.3.5", "msgr2:1.2.3.5" },
  { "msgr2:1.2.3.6", "msgr2:1.2.3.6", "legacy:1.2.3.6", "msgr2:1.2.3.6" },
  { "msgr2:1.2.3.7", "legacy:1.2.3.7", "legacy:1.2.3.7", "msgr2:1.2.3.7" },
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
  bufferlist::iterator bli;

  for (unsigned i = 0; addrvec_checks[i][0]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][0], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  bli = bl.begin();

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
  bufferlist::iterator bli;
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
  bli = bl.begin();

  addr.decode(bli);

  ASSERT_EQ(addr, a);
}

/* multiple legacy addrs */
TEST(Msgr, TestAddrvecEncodeAddrDecode2)
{
  entity_addr_t addr;
  entity_addrvec_t addrvec;
  bufferlist bl;
  bufferlist::iterator bli;

  for (unsigned i = 0; addrvec_checks[i][2]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][2], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  bli = bl.begin();

  addr.decode(bli);

  ASSERT_EQ(addr, addrvec.v[0]);
}

/* all non-legacy addrs */
TEST(Msgr, TestAddrvecEncodeAddrDecode3)
{
  entity_addr_t addr;
  entity_addrvec_t addrvec;
  bufferlist bl;
  bufferlist::iterator bli;

  for (unsigned i = 0; addrvec_checks[i][3]; ++i) {
    const char *end = "";
    bool ok = addr.parse(addrvec_checks[i][3], &end);
    ASSERT_TRUE(ok);
    addrvec.v.push_back(addr);
  }

  addrvec.encode(bl, 0);
  bli = bl.begin();

  addr.decode(bli);

  ASSERT_NE(addr, addrvec.v[0]); // it's not the first addr(which is non-legacy)
  ASSERT_NE(addr, entity_addr_t(1, 0)); // it's not a blank addr either
}
