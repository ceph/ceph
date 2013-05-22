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
#include "osd/osd_types.h"
#include "gtest/gtest.h"

#include <sstream>

TEST(hobject, prefixes0)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 12;
  int64_t pool = 0;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000000.02A"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes1)
{
  uint32_t mask = 0x0000000F;
  uint32_t bits = 6;
  int64_t pool = 20;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000014.F0"));
  prefixes_correct.insert(string("0000000000000014.F4"));
  prefixes_correct.insert(string("0000000000000014.F8"));
  prefixes_correct.insert(string("0000000000000014.FC"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes2)
{
  uint32_t mask = 0xDEADBEAF;
  uint32_t bits = 25;
  int64_t pool = 0;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000000.FAEBDA0"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA2"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA4"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA6"));
  prefixes_correct.insert(string("0000000000000000.FAEBDA8"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAA"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAC"));
  prefixes_correct.insert(string("0000000000000000.FAEBDAE"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes3)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 32;
  int64_t pool = 0x23;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000023.02AF749E"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes4)
{
  uint32_t mask = 0xE947FA20;
  uint32_t bits = 0;
  int64_t pool = 0x23;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000000000023."));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(hobject, prefixes5)
{
  uint32_t mask = 0xDEADBEAF;
  uint32_t bits = 1;
  int64_t pool = 0x34AC5D00;

  set<string> prefixes_correct;
  prefixes_correct.insert(string("0000000034AC5D00.1"));
  prefixes_correct.insert(string("0000000034AC5D00.3"));
  prefixes_correct.insert(string("0000000034AC5D00.5"));
  prefixes_correct.insert(string("0000000034AC5D00.7"));
  prefixes_correct.insert(string("0000000034AC5D00.9"));
  prefixes_correct.insert(string("0000000034AC5D00.B"));
  prefixes_correct.insert(string("0000000034AC5D00.D"));
  prefixes_correct.insert(string("0000000034AC5D00.F"));

  set<string> prefixes_out(hobject_t::get_prefixes(bits, mask, pool));
  ASSERT_EQ(prefixes_out, prefixes_correct);
}

TEST(pg_t, split)
{
  pg_t pgid(0, 0, -1);
  set<pg_t> s;
  bool b;

  s.clear();
  b = pgid.is_split(1, 1, &s);
  ASSERT_TRUE(!b);

  s.clear();
  b = pgid.is_split(2, 4, NULL);
  ASSERT_TRUE(b);
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(6, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, NULL);
  ASSERT_TRUE(!b);
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(1, 0, -1);
  
  s.clear();
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 6, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(4, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(3, 0, -1);

  s.clear();
  b = pgid.is_split(7, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 12, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(11, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 11, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

}

TEST(pg_missing_t, constructor)
{
  pg_missing_t missing;
  EXPECT_EQ((unsigned int)0, missing.num_missing());
  EXPECT_FALSE(missing.have_missing());
}

TEST(pg_missing_t, add_next_event)
{
  pg_missing_t missing;

  // adding a DELETE entry
  hobject_t oid(object_t("objname"), "key", 123, 456, 0);
  eversion_t version(1,2);
  eversion_t prior_version(3,4);
  pg_log_entry_t e(pg_log_entry_t::DELETE, oid, version, prior_version,
		   osd_reqid_t(entity_name_t::CLIENT(777), 8, 999), utime_t(8,9));
  EXPECT_FALSE(missing.have_missing());
  missing.add_next_event(e);
  EXPECT_FALSE(missing.have_missing());

  // adding a MODIFY entry
  e.op = pg_log_entry_t::MODIFY;
  EXPECT_FALSE(missing.have_missing());
  missing.add_next_event(e);
  EXPECT_TRUE(missing.have_missing());
}

// Local Variables:
// compile-command: "cd .. ; make unittest_osd_types ; ./unittest_osd_types # --gtest_filter=pg_missing_t.constructor "
// End:
