// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "osd/PGLog.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

class PGLogTest : public ::testing::Test, protected PGLog {
public:
  virtual void SetUp() { }

  virtual void TearDown() {
    clear();
  }
  
  void clear() {
    ondisklog.zero();
    ondisklog.has_checksums = true;
    ondisklog.divergent_priors.clear();
    missing.clear();
    log.zero();
  }
};

TEST_F(PGLogTest, merge_old_entry) {
  // entries > last_backfill are silently ignored
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    info.last_backfill = hobject_t();
    info.last_backfill.hash = 1;
    oe.soid.hash = 2;

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(0U, ondisklog.length());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(0U, ondisklog.length());
  }

  // a clone with no non-divergent log entry is deleted
  {
    clear();
    
    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    oe.op = pg_log_entry_t::CLONE;

    oe.soid.snap = CEPH_NOSNAP;
    EXPECT_THROW(merge_old_entry(t, oe, info, remove_snap, dirty_log), FailedAssertion);
    oe.soid.snap = 1U;
    missing.add(oe.soid, eversion_t(), eversion_t());

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.have_missing());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(0U, ondisklog.length());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

    EXPECT_FALSE(dirty_log);
    EXPECT_EQ(oe.soid, remove_snap.front());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(0U, ondisklog.length());
  }

  // the new entry (from the logs) old entry (from the log entry
  // given in argument) have the same version : do nothing and return true.
  {
    clear();
    
    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    oe.version = eversion_t(1,1);
    log.add(oe);

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());

    EXPECT_TRUE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());
  }

  // the new entry (from the logs) has a version that is higher than
  // the old entry (from the log entry given in argument) : do 
  // nothing and return false
  {
    clear();
    
    ObjectStore::Transaction t;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    pg_log_entry_t ne;
    ne.version = eversion_t(2,1);
    log.add(ne);

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(ne.version, log.log.front().version);
    EXPECT_EQ(0U, ondisklog.length());

    // the newer entry ( from the logs ) can be DELETE
    {
      log.log.front().op = pg_log_entry_t::DELETE;
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));
    }

    // if the newer entry is not DELETE, the object must be in missing
    {
      pg_log_entry_t &ne = log.log.front();
      ne.op = pg_log_entry_t::MODIFY;
      missing.add_next_event(ne);
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

      missing.rm(ne.soid, ne.version);
    }

    // throw if the newer entry is not DELETE and not in missing
    {
      pg_log_entry_t &ne = log.log.front();
      ne.op = pg_log_entry_t::MODIFY;
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_THROW(merge_old_entry(t, oe, info, remove_snap, dirty_log), FailedAssertion);
    }

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(ne.version, log.log.front().version);
    EXPECT_EQ(0U, ondisklog.length());

  }

  // the new entry (from the logs) has a version that is lower than
  // the old entry (from the log entry given in argument) and
  // old and new are delete : do nothing and return false
  {
    clear();
    
    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    pg_log_entry_t ne;
    ne.version = eversion_t(1,1);
    ne.op = pg_log_entry_t::DELETE;
    log.add(ne);

    oe.version = eversion_t(2,1);
    oe.op = pg_log_entry_t::DELETE;

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());
  }

  // the new entry (from the logs) has a version that is lower than
  // the old entry (from the log entry given in argument) and
  // old is DELETE and new is update : 
  // if the object is not already in missing, add it
  // if the object is already in missing, revise the version it needs
  // return false
  {
    clear();
    
    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_log = false;

    pg_log_entry_t ne;
    ne.version = eversion_t(1,1);
    ne.op = pg_log_entry_t::MODIFY;
    log.add(ne);

    oe.version = eversion_t(2,1);
    oe.op = pg_log_entry_t::DELETE;

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());

    eversion_t old_version(0, 0);
    // if the object is not already in missing, add it
    {
      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

      EXPECT_TRUE(missing.is_missing(ne.soid, ne.version));
      EXPECT_FALSE(missing.is_missing(ne.soid, old_version));
    }
    // if the object is already in missing, revise the version it needs
    {
      missing.revise_need(ne.soid, old_version);
      EXPECT_TRUE(missing.is_missing(ne.soid, old_version));

      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap, dirty_log));

      EXPECT_TRUE(missing.is_missing(ne.soid, ne.version));
      EXPECT_FALSE(missing.is_missing(ne.soid, old_version));
    }

    EXPECT_FALSE(dirty_log);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(ne.soid));
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(0U, ondisklog.length());
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_pglog ; ./unittest_pglog --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* "
// End:
