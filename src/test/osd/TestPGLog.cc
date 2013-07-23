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
};

TEST_F(PGLogTest, rewind_divergent_log) {
  // newhead > log.tail : throw an assert
  {
    clear();

    ObjectStore::Transaction t;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    log.tail = eversion_t(2, 1);
    EXPECT_THROW(rewind_divergent_log(t, eversion_t(1, 1), info, remove_snap,
				      dirty_info, dirty_big_info),
		 FailedAssertion);
  }

  /*        +----------------+
            |  log           |
            +--------+-------+
            |        |object |
            |version | hash  |
            |        |       |
       tail > (1,1)  |  x5   |
            |        |       |
            |        |       |
            | (1,4)  |  x9   < newhead
            | MODIFY |       |
            |        |       |
       head > (1,5)  |  x9   |
            | DELETE |       |
            |        |       |
            +--------+-------+

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;
    eversion_t divergent_version;
    eversion_t newhead;

    hobject_t divergent;
    divergent.hash = 0x9;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = newhead = eversion_t(1, 4);
      e.soid = divergent;
      e.op = pg_log_entry_t::MODIFY;
      log.log.push_back(e);
      e.version = divergent_version = eversion_t(1, 5);
      e.soid = divergent;
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;
      info.last_complete = log.head;
    }

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(3U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_EQ(log.head, info.last_complete);
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    rewind_divergent_log(t, newhead, info, remove_snap,
			 dirty_info, dirty_big_info);

    EXPECT_TRUE(log.objects.count(divergent));
    EXPECT_TRUE(missing.is_missing(divergent_object));
    EXPECT_EQ(1U, log.objects.count(divergent_object));
    EXPECT_EQ(2U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(newhead, info.last_update);
    EXPECT_EQ(newhead, info.last_complete);
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

  /*        +----------------+
            |  log           |
            +--------+-------+
            |        |object |
            |version | hash  |
            |        |       |
       tail > (1,1)  | NULL  |
            |        |       |
            | (1,4)  | NULL  < newhead
            |        |       |
       head > (1,5)  |  x9   |
            |        |       |
            +--------+-------+

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;
    eversion_t divergent_version;
    eversion_t prior_version;
    eversion_t newhead;
    {
      pg_log_entry_t e;

      info.log_tail = log.tail = eversion_t(1, 1);
      newhead = eversion_t(1, 3);
      e.version = divergent_version = eversion_t(1, 5);
      e.soid.hash = 0x9;
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      e.prior_version = prior_version = eversion_t(0, 2);
      log.log.push_back(e);
      log.head = e.version;
    }

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    rewind_divergent_log(t, newhead, info, remove_snap,
			 dirty_info, dirty_big_info);

    EXPECT_TRUE(missing.is_missing(divergent_object));
    EXPECT_EQ(0U, log.objects.count(divergent_object));
    EXPECT_TRUE(log.empty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }
}

TEST_F(PGLogTest, merge_old_entry) {
  // entries > last_backfill are silently ignored
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.last_backfill = hobject_t();
    info.last_backfill.hash = 1;
    oe.soid.hash = 2;

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
  }

  // a clone with no non-divergent log entry is deleted
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    oe.op = pg_log_entry_t::CLONE;

    oe.soid.snap = CEPH_NOSNAP;
    EXPECT_THROW(merge_old_entry(t, oe, info, remove_snap), FailedAssertion);
    oe.soid.snap = 1U;
    missing.add(oe.soid, eversion_t(), eversion_t());

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.have_missing());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_EQ(oe.soid, remove_snap.front());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
  }

  // the new entry (from the logs) old entry (from the log entry
  // given in argument) have the same version : do nothing and return true.
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    oe.version = eversion_t(1,1);
    log.add(oe);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());

    EXPECT_TRUE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
  }

  // the new entry (from the logs) has a version that is higher than
  // the old entry (from the log entry given in argument) : do
  // nothing and return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_info_t info;
    list<hobject_t> remove_snap;

    pg_log_entry_t ne;
    ne.version = eversion_t(2,1);
    log.add(ne);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(ne.version, log.log.front().version);

    // the newer entry ( from the logs ) can be DELETE
    {
      log.log.front().op = pg_log_entry_t::DELETE;
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));
    }

    // if the newer entry is not DELETE, the object must be in missing
    {
      pg_log_entry_t &ne = log.log.front();
      ne.op = pg_log_entry_t::MODIFY;
      missing.add_next_event(ne);
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

      missing.rm(ne.soid, ne.version);
    }

    // throw if the newer entry is not DELETE and not in missing
    {
      pg_log_entry_t &ne = log.log.front();
      ne.op = pg_log_entry_t::MODIFY;
      pg_log_entry_t oe;
      oe.version = eversion_t(1,1);

      EXPECT_THROW(merge_old_entry(t, oe, info, remove_snap), FailedAssertion);
    }

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_EQ(ne.version, log.log.front().version);

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

    pg_log_entry_t ne;
    ne.version = eversion_t(1,1);
    ne.op = pg_log_entry_t::DELETE;
    log.add(ne);

    oe.version = eversion_t(2,1);
    oe.op = pg_log_entry_t::DELETE;

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
  }

  // the new entry (from the logs) has a version that is lower than
  // the old entry (from the log entry given in argument) and
  // new is update :
  // if the object is not already in missing, add it
  // if the object is already in missing, revise the version it needs
  // return false
  {
    __s32 ops[2] = { pg_log_entry_t::MODIFY, pg_log_entry_t::DELETE };
    for (int i = 0; i < 2; i++) {
      __s32 oe_op = ops[i];

      clear();

      ObjectStore::Transaction t;
      pg_log_entry_t oe;
      pg_info_t info;
      list<hobject_t> remove_snap;

      pg_log_entry_t ne;
      ne.version = eversion_t(1,1);
      ne.op = pg_log_entry_t::MODIFY;
      log.add(ne);

      oe.version = eversion_t(2,1);
      oe.op = oe_op;

      EXPECT_FALSE(is_dirty());
      EXPECT_TRUE(remove_snap.empty());
      EXPECT_TRUE(t.empty());
      EXPECT_FALSE(missing.have_missing());
      EXPECT_EQ(1U, log.log.size());

      eversion_t old_version(0, 0);
      // if the object is not already in missing, add it
      {
        EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

        EXPECT_TRUE(missing.is_missing(ne.soid, ne.version));
        EXPECT_FALSE(missing.is_missing(ne.soid, old_version));
      }
      // if the object is already in missing, revise the version it needs
      {
        missing.revise_need(ne.soid, old_version);
        EXPECT_TRUE(missing.is_missing(ne.soid, old_version));

        EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

        EXPECT_TRUE(missing.is_missing(ne.soid, ne.version));
        EXPECT_FALSE(missing.is_missing(ne.soid, old_version));
      }

      EXPECT_FALSE(is_dirty());
      EXPECT_TRUE(remove_snap.empty());
      EXPECT_TRUE(t.empty());
      EXPECT_TRUE(missing.is_missing(ne.soid));
      EXPECT_EQ(1U, log.log.size());
    }
  }

  // the new entry (from the logs) has a version that is lower than
  // the old entry (from the log entry given in argument) and
  // old is update and new is DELETE :
  // if the object is in missing, it is removed
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    pg_log_entry_t ne;
    ne.version = eversion_t(1,1);
    ne.op = pg_log_entry_t::DELETE;
    log.add(ne);

    oe.version = eversion_t(2,1);
    oe.op = pg_log_entry_t::MODIFY;
    missing.add_next_event(oe);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_EQ(1U, log.log.size());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
  }

  // there is no new entry (from the logs) and
  // the old entry (from the log entry given in argument) is not a CLONE and
  // the old entry prior_version is greater than the tail of the log :
  // do nothing and return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(1,1);
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t(2,1);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
  }

  // there is no new entry (from the logs) and
  // the old entry (from the log entry given in argument) is not a CLONE and
  // the old entry (from the log entry given in argument) is not a DELETE and
  // the old entry prior_version is lower than the tail of the log :
  //   add the old object to the remove_snap list and 
  //   add the old object to divergent priors and
  //   add or update the prior_version of the object to missing and
  //   return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(2,1);
    oe.soid.hash = 1;
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t(1,1);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_TRUE(is_dirty());
    EXPECT_EQ(oe.soid, remove_snap.front());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(oe.soid, divergent_priors[oe.prior_version]);
  }

  // there is no new entry (from the logs) and
  // the old entry (from the log entry given in argument) is not a CLONE and
  // the old entry (from the log entry given in argument) is a DELETE and
  // the old entry prior_version is lower than the tail of the log :
  //   add the old object to divergent priors and
  //   add or update the prior_version of the object to missing and
  //   return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(2,1);
    oe.soid.hash = 1;
    oe.op = pg_log_entry_t::DELETE;
    oe.prior_version = eversion_t(1,1);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());
    EXPECT_EQ(oe.soid, divergent_priors[oe.prior_version]);
  }

  // there is no new entry (from the logs) and
  // the old entry (from the log entry given in argument) is not a CLONE and
  // the old entry (from the log entry given in argument) is a DELETE and
  // the old entry prior_version is eversion_t() :
  //   remove the prior_version of the object from missing, if any and
  //   return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(10,1);
    oe.soid.hash = 1;
    oe.op = pg_log_entry_t::DELETE;
    oe.prior_version = eversion_t();

    missing.add(oe.soid, eversion_t(1,1), eversion_t());

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
  }

  // there is no new entry (from the logs) and
  // the old entry (from the log entry given in argument) is not a CLONE and
  // the old entry (from the log entry given in argument) is not a DELETE and
  // the old entry prior_version is eversion_t() :
  //   add the old object to the remove_snap list and
  //   remove the prior_version of the object from missing, if any and
  //   return false
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(10,1);
    oe.soid.hash = 1;
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t();

    missing.add(oe.soid, eversion_t(1,1), eversion_t());

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());

    EXPECT_FALSE(merge_old_entry(t, oe, info, remove_snap));

    EXPECT_FALSE(is_dirty());
    EXPECT_EQ(oe.soid, remove_snap.front());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
  }

}

TEST_F(PGLogTest, merge_log) {
  // head and tail match, last_backfill is set:
  // noop
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t last_backfill(object_t("oname"), string("key"), 1, 234, 1, "");
    info.last_backfill = last_backfill;
    eversion_t stat_version(10, 1);
    info.stats.version = stat_version;
    log.tail = olog.tail = eversion_t(1, 1);
    log.head = olog.head = eversion_t(2, 1);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    merge_log(t, oinfo, olog, fromosd, info, remove_snap,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);
  }

  // head and tail match, last_backfill is not set: info.stats is
  // copied from oinfo.stats but info.stats.reported_* is guaranteed to
  // never be replaced by a lower version
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    eversion_t stat_version(10, 1);
    oinfo.stats.version = stat_version;
    info.stats.reported_seq = 1;
    info.stats.reported_epoch = 10;
    oinfo.stats.reported_seq = 1;
    oinfo.stats.reported_epoch = 1;
    log.tail = olog.tail = eversion_t(1, 1);
    log.head = olog.head = eversion_t(2, 1);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(eversion_t(), info.stats.version);
    EXPECT_EQ(1ull, info.stats.reported_seq);
    EXPECT_EQ(10u, info.stats.reported_epoch);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(info.last_backfill.is_max());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    merge_log(t, oinfo, olog, fromosd, info, remove_snap,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_EQ(1ull, info.stats.reported_seq);
    EXPECT_EQ(10u, info.stats.reported_epoch);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);
  }

  /*         Before
            +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
            |        |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
       tail > (1,4)  |  x7   |         |
            |        |       |         |
            |        |       |         |
       head > (1,5)  |  x9   |  (1,5)  < head
            |        |       |         |
            |        |       |         |
            +--------+-------+---------+

             After
            +-----------------
            |  log           |
            +--------+-------+
            |        |object |
            |version | hash  |
            |        |       |
       tail > (1,1)  |  x5   |
            |        |       |
            |        |       |
            | (1,4)  |  x7   |
            |        |       |
            |        |       |
       head > (1,5)  |  x9   |
            |        |       |
            |        |       |
            +--------+-------+
  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 4);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.hash = 0x9;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.hash = 0x9;
      olog.log.push_back(e);
      olog.head = e.version;
    }

    hobject_t last_backfill(object_t("oname"), string("key"), 1, 234, 1, "");
    info.last_backfill = last_backfill;
    eversion_t stat_version(10, 1);
    info.stats.version = stat_version;

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(2U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    merge_log(t, oinfo, olog, fromosd, info, remove_snap,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(3U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  < lower_bound
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | DELETE |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  |
            |        |       |  MODIFY |
            |        |       |         |
            |        |  x7   |  (2,4)  < head
            |        |       |  DELETE |
            +--------+-------+---------+

      The log entry (1,3) deletes the object x9 but the olog entry (2,3) modifies
      it and is authoritative : the log entry (1,3) is divergent.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.hash = 0x3;
      log.log.push_back(e);
      e.version = eversion_t(1,3);
      e.soid.hash = 0x9;
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.hash = 0x3;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      e.version = eversion_t(2, 4);
      e.soid.hash = 0x7;
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      olog.head = e.version;
    }

    snapid_t purged_snap(1);
    {
      oinfo.last_update = olog.head;
      oinfo.purged_snaps.insert(purged_snap);
    }

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.objects.count(divergent_object));
    EXPECT_EQ(3U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    merge_log(t, oinfo, olog, fromosd, info, remove_snap,
              dirty_info, dirty_big_info);

    /* When the divergent entry is a DELETE and the authoritative
       entry is a MODIFY, the object will be added to missing : it is
       a verifiable side effect proving the entry was identified
       to be divergent.
    */
    EXPECT_TRUE(missing.is_missing(divergent_object));
    EXPECT_EQ(1U, log.objects.count(divergent_object));
    EXPECT_EQ(4U, log.log.size());
    /* DELETE entries from olog that are appended to the hed of the
       log are also added to remove_snap.
     */
    EXPECT_EQ(0x7U, remove_snap.front().hash);
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_TRUE(info.purged_snaps.contains(purged_snap));
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,4)  |  x7   |  (1,4)  < head
            |        |       |         |
            |        |       |         |
       head > (1,5)  |  x9   |         |
            |        |       |         |
            |        |       |         |
            +--------+-------+---------+

      The head of the log entry (1,5) is divergent because it is greater than the
      head of olog.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 4);
      e.soid.hash = 0x7;
      log.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.hash = 0x9;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 4);
      e.soid.hash = 0x7;
      olog.log.push_back(e);
      olog.head = e.version;
    }

    hobject_t last_backfill(object_t("oname"), string("key"), 1, 234, 1, "");
    info.last_backfill = last_backfill;
    eversion_t stat_version(10, 1);
    info.stats.version = stat_version;

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(3U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    merge_log(t, oinfo, olog, fromosd, info, remove_snap,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(2U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_EQ(0x9U, remove_snap.front().hash);
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

  // If our log is empty, the incoming log needs to have not been trimmed.
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    // olog has been trimmed
    olog.tail = eversion_t(1, 1);

    ASSERT_THROW(merge_log(t, oinfo, olog, fromosd, info, remove_snap,
                           dirty_info, dirty_big_info), FailedAssertion);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |         |
            |        |       |         |
            |        |       |         |
       head > (1,2)  |  x3   |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < tail
            |        |       |         |
            |        |       |         |
            |        |  x5   |  (2,4)  < head
            |        |       |         |
            +--------+-------+---------+

      If the logs do not overlap, throw an exception.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    int fromosd = -1;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.hash = 0x3;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      olog.log.push_back(e);
      e.version = eversion_t(2, 4);
      e.soid.hash = 0x5;
      olog.log.push_back(e);
      olog.head = e.version;
    }

    EXPECT_THROW(merge_log(t, oinfo, olog, fromosd, info, remove_snap,
                           dirty_info, dirty_big_info), FailedAssertion);
  }
}

TEST_F(PGLogTest, proc_replica_log) {
  // empty log : no side effect
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    eversion_t last_update(1, 1);
    oinfo.last_update = last_update;
    eversion_t last_complete(2, 1);
    oinfo.last_complete = last_complete;

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_complete, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_update, oinfo.last_complete);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
            |        |  x3   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
       tail > (1,2)  |  x5   |         |
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | DELETE |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < head
            |        |       |  DELETE |
            |        |       |         |
            +--------+-------+---------+
	    
      The log entry (1,3) deletes the object x9 and the olog entry
      (2,3) also deletes it : do nothing. The olog tail is ignored
      because it is before the log tail.
      
  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 2);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x3;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
            | (1,3)  |  x9   |         |
            | MODIFY |       |         |
            |        |       |         |
            |        |  x7   |  (1,4)  |
            |        |       |         |
            | (1,6)  |  x8   |  (1,5)  |
            |        |       |         |
            |        |  x9   |  (2,3)  < head < last_backfill
            |        |       |  DELETE |
            |        |       |         |
       head > (1,7)  |  xa   |  (2,5)  |
            |        |       |         |
            +--------+-------+---------+

      The log entry (1,3) modifies the object x9 but the olog entry
      (2,3) deletes it : log is authoritative and the object is added
      to missing. x7 is divergent and ignored. x8 has a more recent
      version in the log and the olog entry is ignored. xa is past
      last_backfill and ignored.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      log.log.push_back(e);
      e.version = eversion_t(1,3);
      e.soid.hash = 0x9;
      divergent_object = e.soid;
      e.op = pg_log_entry_t::MODIFY;
      log.log.push_back(e);
      e.version = eversion_t(1, 6);
      e.soid.hash = 0x8;
      log.log.push_back(e);
      e.version = eversion_t(1, 7);
      e.soid.hash = 0xa;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      olog.log.push_back(e);
      e.version = eversion_t(1, 4); // divergent entry : will be ignored
      e.soid.hash = 0x7;
      olog.log.push_back(e);
      e.version = eversion_t(1, 5); // log has newer entry : it will be ignored
      e.soid.hash = 0x8;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      oinfo.last_backfill = e.soid;
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      e.version = eversion_t(2, 4);
      e.soid.hash = 0xa; // > last_backfill are ignored
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(eversion_t(1, 3), omissing.missing[divergent_object].need);
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_update, oinfo.last_complete);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | DELETE |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < head
            |        |       |  DELETE |
            |        |       |         |
            +--------+-------+---------+

      The log entry (1,3) deletes the object x9 and the olog entry
      (2,3) also deletes it : do nothing.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    eversion_t last_update(1, 2);

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      log.log.push_back(e);
      e.version = eversion_t(1,3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_update, oinfo.last_complete);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | DELETE |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < head
            |        |       |  MODIFY |
            |        |       |         |
            +--------+-------+---------+

      The log entry (1,3) deletes the object x9 but the olog entry
      (2,3) modifies it : remove it from omissing.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      log.log.push_back(e);
      e.version = eversion_t(1, 3);
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.hash = 0x9;
      divergent_object = e.soid;
      omissing.add(divergent_object, e.version, eversion_t());
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(eversion_t(2, 3), omissing.missing[divergent_object].need);
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_update, oinfo.last_complete);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x5   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | MODIFY |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < head
            |        |       |  MODIFY |
            |        |       |         |
            +--------+-------+---------+

      The log entry (1,3) deletes the object x9 but the olog entry
      (2,3) modifies it : remove it from omissing.

  */
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    int from = -1;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;
    eversion_t new_version(1, 3);
    eversion_t divergent_version(2, 3);

    {
      pg_log_entry_t e;

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      log.log.push_back(e);
      e.version = new_version;
      e.soid.hash = 0x9;
      e.op = pg_log_entry_t::MODIFY;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.hash = 0x5;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.hash = 0x3;
      olog.log.push_back(e);
      e.version = divergent_version;
      e.soid.hash = 0x9;
      divergent_object = e.soid;
      omissing.add(divergent_object, e.version, eversion_t());
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(divergent_version, omissing.missing[divergent_object].need);
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    proc_replica_log(t, oinfo, olog, omissing, from);

    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(new_version, omissing.missing[divergent_object].need);
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_update, oinfo.last_complete);
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
