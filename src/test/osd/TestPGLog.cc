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
#include "gtest/gtest.h"
#include "osd/PGLog.h"
#include "osd/OSDMap.h"
#include "include/coredumpctl.h"
#include "../objectstore/store_test_fixture.h"


struct PGLogTestBase {
  static hobject_t mk_obj(unsigned id) {
    hobject_t hoid;
    stringstream ss;
    ss << "obj_" << id;
    hoid.oid = ss.str();
    hoid.set_hash(id);
    hoid.pool = 1;
    return hoid;
  }
  static eversion_t mk_evt(unsigned ep, unsigned v) {
    return eversion_t(ep, v);
  }
  static pg_log_entry_t mk_ple_mod(
    const hobject_t &hoid, eversion_t v, eversion_t pv, osd_reqid_t reqid) {
    pg_log_entry_t e;
    e.mark_unrollbackable();
    e.op = pg_log_entry_t::MODIFY;
    e.soid = hoid;
    e.version = v;
    e.prior_version = pv;
    e.reqid = reqid;
    return e;
  }
  static pg_log_entry_t mk_ple_dt(
    const hobject_t &hoid, eversion_t v, eversion_t pv, osd_reqid_t reqid) {
    pg_log_entry_t e;
    e.mark_unrollbackable();
    e.op = pg_log_entry_t::DELETE;
    e.soid = hoid;
    e.version = v;
    e.prior_version = pv;
    e.reqid = reqid;
    return e;
  }
  static pg_log_entry_t mk_ple_ldt(
    const hobject_t &hoid, eversion_t v, eversion_t pv) {
    pg_log_entry_t e;
    e.mark_unrollbackable();
    e.op = pg_log_entry_t::LOST_DELETE;
    e.soid = hoid;
    e.version = v;
    e.prior_version = pv;
    return e;
  }
  static pg_log_entry_t mk_ple_mod_rb(
    const hobject_t &hoid, eversion_t v, eversion_t pv, osd_reqid_t reqid) {
    pg_log_entry_t e;
    e.op = pg_log_entry_t::MODIFY;
    e.soid = hoid;
    e.version = v;
    e.prior_version = pv;
    e.reqid = reqid;
    return e;
  }
  static pg_log_entry_t mk_ple_dt_rb(
    const hobject_t &hoid, eversion_t v, eversion_t pv, osd_reqid_t reqid) {
    pg_log_entry_t e;
    e.op = pg_log_entry_t::DELETE;
    e.soid = hoid;
    e.version = v;
    e.prior_version = pv;
    e.reqid = reqid;
    return e;
  }
  static pg_log_entry_t mk_ple_err(
    const hobject_t &hoid, eversion_t v, osd_reqid_t reqid) {
    pg_log_entry_t e;
    e.op = pg_log_entry_t::ERROR;
    e.soid = hoid;
    e.version = v;
    e.prior_version = eversion_t(0, 0);
    e.reqid = reqid;
    return e;
  }
  static pg_log_entry_t mk_ple_mod(
    const hobject_t &hoid, eversion_t v, eversion_t pv) {
    return mk_ple_mod(hoid, v, pv, osd_reqid_t());
  }
  static pg_log_entry_t mk_ple_dt(
    const hobject_t &hoid, eversion_t v, eversion_t pv) {
    return mk_ple_dt(hoid, v, pv, osd_reqid_t());
  }
  static pg_log_entry_t mk_ple_mod_rb(
    const hobject_t &hoid, eversion_t v, eversion_t pv) {
    return mk_ple_mod_rb(hoid, v, pv, osd_reqid_t());
  }
  static pg_log_entry_t mk_ple_dt_rb(
    const hobject_t &hoid, eversion_t v, eversion_t pv) {
    return mk_ple_dt_rb(hoid, v, pv, osd_reqid_t());
  }
  static pg_log_entry_t mk_ple_err(
    const hobject_t &hoid, eversion_t v) {
    return mk_ple_err(hoid, v, osd_reqid_t());
  }
}; // PGLogTestBase


class PGLogTest : virtual public ::testing::Test, protected PGLog, public PGLogTestBase  {
public:
  PGLogTest() : PGLog(g_ceph_context) {}
  void SetUp() override {
    missing.may_include_deletes = true;
  }

#include "common/ceph_context.h"
#include "common/config.h"

  void TearDown() override {
    clear();
  }


  struct TestCase {
    list<pg_log_entry_t> base;
    list<pg_log_entry_t> auth;
    list<pg_log_entry_t> div;

    pg_missing_t init;
    pg_missing_t final;

    set<hobject_t> toremove;
    list<pg_log_entry_t> torollback;
    bool deletes_during_peering;

  private:
    IndexedLog fullauth;
    IndexedLog fulldiv;
    pg_info_t authinfo;
    pg_info_t divinfo;
  public:
    TestCase() : deletes_during_peering(false) {}
    void setup() {
      init.may_include_deletes = !deletes_during_peering;
      final.may_include_deletes = !deletes_during_peering;
      fullauth.log.insert(fullauth.log.end(), base.begin(), base.end());
      fullauth.log.insert(fullauth.log.end(), auth.begin(), auth.end());
      fulldiv.log.insert(fulldiv.log.end(), base.begin(), base.end());
      fulldiv.log.insert(fulldiv.log.end(), div.begin(), div.end());

      fullauth.head = authinfo.last_update = fullauth.log.rbegin()->version;
      authinfo.last_complete = fullauth.log.rbegin()->version;
      authinfo.log_tail = fullauth.log.begin()->version;
      authinfo.log_tail.version--;
      fullauth.tail = authinfo.log_tail;
      authinfo.last_backfill = hobject_t::get_max();

      fulldiv.head = divinfo.last_update = fulldiv.log.rbegin()->version;
      divinfo.last_complete = eversion_t();
      divinfo.log_tail = fulldiv.log.begin()->version;
      divinfo.log_tail.version--;
      fulldiv.tail = divinfo.log_tail;
      divinfo.last_backfill = hobject_t::get_max();

      if (init.get_items().empty()) {
	divinfo.last_complete = divinfo.last_update;
      } else {
	eversion_t fmissing = init.get_items().at(init.get_rmissing().begin()->second).need;
	for (list<pg_log_entry_t>::const_iterator i = fulldiv.log.begin();
	     i != fulldiv.log.end();
	     ++i) {
	  if (i->version < fmissing)
	    divinfo.last_complete = i->version;
	  else
	    break;
	}
      }

      fullauth.index();
      fulldiv.index();
    }
    void set_div_bounds(eversion_t head, eversion_t tail) {
      fulldiv.tail = divinfo.log_tail = tail;
      fulldiv.head = divinfo.last_update = head;
    }
    void set_auth_bounds(eversion_t head, eversion_t tail) {
      fullauth.tail = authinfo.log_tail = tail;
      fullauth.head = authinfo.last_update = head;
    }
    const IndexedLog &get_fullauth() const { return fullauth; }
    const IndexedLog &get_fulldiv() const { return fulldiv; }
    const pg_info_t &get_authinfo() const { return authinfo; }
    const pg_info_t &get_divinfo() const { return divinfo; }
  }; // struct TestCase

  struct LogHandler : public PGLog::LogEntryHandler {
    set<hobject_t> removed;
    list<pg_log_entry_t> rolledback;

    void rollback(
      const pg_log_entry_t &entry) override {
      rolledback.push_back(entry);
    }
    void rollforward(
      const pg_log_entry_t &entry) override {}
    void remove(
      const hobject_t &hoid) override {
      removed.insert(hoid);
    }
    void try_stash(const hobject_t &, version_t) override {
      // lost/unfound cases are not tested yet
    }
    void trim(
      const pg_log_entry_t &entry) override {}
  };

  template <typename missing_t>
  void verify_missing(
    const TestCase &tcase,
    const missing_t &missing) {
    ASSERT_EQ(tcase.final.get_items().size(), missing.get_items().size());
    for (auto i = missing.get_items().begin();
	 i != missing.get_items().end();
	 ++i) {
      EXPECT_TRUE(tcase.final.get_items().count(i->first));
      EXPECT_EQ(tcase.final.get_items().find(i->first)->second.need, i->second.need);
      EXPECT_EQ(tcase.final.get_items().find(i->first)->second.have, i->second.have);
    }
    bool correct = missing.debug_verify_from_init(tcase.init, &(std::cout));
    ASSERT_TRUE(correct);
  }

  void verify_sideeffects(
    const TestCase &tcase,
    const LogHandler &handler) {
    ASSERT_EQ(tcase.toremove.size(), handler.removed.size());
    ASSERT_EQ(tcase.torollback.size(), handler.rolledback.size());

    {
      list<pg_log_entry_t>::const_iterator titer = tcase.torollback.begin();
      list<pg_log_entry_t>::const_iterator hiter = handler.rolledback.begin();
      for (; titer != tcase.torollback.end(); ++titer, ++hiter) {
	EXPECT_EQ(titer->version, hiter->version);
      }
    }

    {
      set<hobject_t>::const_iterator titer = tcase.toremove.begin();
      set<hobject_t>::const_iterator hiter = handler.removed.begin();
      for (; titer != tcase.toremove.end(); ++titer, ++hiter) {
	EXPECT_EQ(*titer, *hiter);
      }
    }
  }

  void test_merge_log(const TestCase &tcase) {
    clear();
    log = tcase.get_fulldiv();
    pg_info_t info = tcase.get_divinfo();

    missing = tcase.init;
    missing.flush();

    IndexedLog olog;
    olog = tcase.get_fullauth();
    pg_info_t oinfo = tcase.get_authinfo();

    LogHandler h;
    bool dirty_info = false;
    bool dirty_big_info = false;
    merge_log(
      oinfo, olog, pg_shard_t(1, shard_id_t(0)), info,
      &h, dirty_info, dirty_big_info);

    ASSERT_EQ(info.last_update, oinfo.last_update);
    verify_missing(tcase, missing);
    verify_sideeffects(tcase, h);
  }

  void test_proc_replica_log(const TestCase &tcase) {
    clear();
    log = tcase.get_fullauth();
    pg_info_t info = tcase.get_authinfo();

    pg_missing_t omissing = tcase.init;

    IndexedLog olog;
    olog = tcase.get_fulldiv();
    pg_info_t oinfo = tcase.get_divinfo();

    proc_replica_log(
      oinfo, olog, omissing, pg_shard_t(1, shard_id_t(0)));

    ceph_assert(oinfo.last_update >= log.tail);

    if (!tcase.base.empty()) {
      ASSERT_EQ(tcase.base.rbegin()->version, oinfo.last_update);
    }

    for (list<pg_log_entry_t>::const_iterator i = tcase.auth.begin();
	 i != tcase.auth.end();
	 ++i) {
      if (i->version > oinfo.last_update) {
	if (i->is_delete() && tcase.deletes_during_peering) {
	  omissing.rm(i->soid, i->version);
	} else {
	  omissing.add_next_event(*i);
	}
      }
    }
    verify_missing(tcase, omissing);
  } // test_proc_replica_log

  void run_test_case(const TestCase &tcase) {
    test_merge_log(tcase);
    test_proc_replica_log(tcase);
  }
}; // class PGLogTest

struct TestHandler : public PGLog::LogEntryHandler {
  list<hobject_t> &removed;
  explicit TestHandler(list<hobject_t> &removed) : removed(removed) {}

  void rollback(
    const pg_log_entry_t &entry) override {}
  void rollforward(
    const pg_log_entry_t &entry) override {}
  void remove(
    const hobject_t &hoid) override {
    removed.push_back(hoid);
  }
  void cant_rollback(const pg_log_entry_t &entry) {}
  void try_stash(const hobject_t &, version_t) override {
    // lost/unfound cases are not tested yet
  }
  void trim(
    const pg_log_entry_t &entry) override {}
};

TEST_F(PGLogTest, rewind_divergent_log) {
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

    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;
    eversion_t divergent_version;
    eversion_t newhead;

    hobject_t divergent;
    divergent.set_hash(0x9);

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = newhead = eversion_t(1, 4);
      e.soid = divergent;
      e.op = pg_log_entry_t::MODIFY;
      log.log.push_back(e);
      e.version = divergent_version = eversion_t(1, 5);
      e.prior_version = eversion_t(1, 4);
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
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_EQ(log.head, info.last_complete);
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    rewind_divergent_log(newhead, info, &h,
			 dirty_info, dirty_big_info);

    EXPECT_TRUE(log.objects.count(divergent));
    EXPECT_TRUE(missing.is_missing(divergent_object));
    EXPECT_EQ(1U, log.objects.count(divergent_object));
    EXPECT_EQ(2U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
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
      e.mark_unrollbackable();

      info.log_tail = log.tail = eversion_t(1, 1);
      newhead = eversion_t(1, 3);
      e.version = divergent_version = eversion_t(1, 5);
      e.soid.set_hash(0x9);
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      e.prior_version = prior_version = eversion_t(0, 2);
      log.log.push_back(e);
      log.head = e.version;
    }

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    rewind_divergent_log(newhead, info, &h,
			 dirty_info, dirty_big_info);

    EXPECT_TRUE(missing.is_missing(divergent_object));
    EXPECT_EQ(0U, log.objects.count(divergent_object));
    EXPECT_TRUE(log.empty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

  // Test for 13965
  {
    clear();

    list<hobject_t> remove_snap;
    pg_info_t info;
    info.log_tail = log.tail = eversion_t(1, 5);
    info.last_update = eversion_t(1, 6);
    bool dirty_info = false;
    bool dirty_big_info = false;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();
      e.version = eversion_t(1, 5);
      e.soid.set_hash(0x9);
      add(e);
    }
    {
      pg_log_entry_t e;
      e.mark_unrollbackable();
      e.version = eversion_t(1, 6);
      e.soid.set_hash(0x10);
      add(e);
    }
    TestHandler h(remove_snap);
    roll_forward_to(eversion_t(1, 6), &h);
    rewind_divergent_log(eversion_t(1, 5), info, &h,
			 dirty_info, dirty_big_info);
    pg_log_t log;
    reset_backfill_claim_log(log, &h);
  }
}

TEST_F(PGLogTest, merge_old_entry) {
  // entries > last_backfill are silently ignored
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.last_backfill = hobject_t();
    info.last_backfill.set_hash(100);
    oe.soid.set_hash(2);
    ASSERT_GT(oe.soid, info.last_backfill);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());
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
    ne.mark_unrollbackable();
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
      oe.mark_unrollbackable();
      oe.version = eversion_t(1,1);

      TestHandler h(remove_snap);
      merge_old_entry(t, oe, info, &h);
    }

    // if the newer entry is not DELETE, the object must be in missing
    {
      pg_log_entry_t &ne = log.log.front();
      ne.op = pg_log_entry_t::MODIFY;
      missing.add_next_event(ne);
      pg_log_entry_t oe;
      oe.mark_unrollbackable();
      oe.version = eversion_t(1,1);

      TestHandler h(remove_snap);
      merge_old_entry(t, oe, info, &h);

      missing.rm(ne.soid, ne.version);
    }

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(remove_snap.empty());
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
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    pg_log_entry_t ne;
    ne.mark_unrollbackable();
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

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(1U, log.log.size());
  }

  // the new entry (from the logs) has a version that is lower than
  // the old entry (from the log entry given in argument) and
  // old is update and new is DELETE :
  // if the object is in missing, it is removed
  {
    clear();

    ObjectStore::Transaction t;
    pg_log_entry_t oe;
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    pg_log_entry_t ne;
    ne.mark_unrollbackable();
    ne.version = eversion_t(1,1);
    ne.op = pg_log_entry_t::DELETE;
    log.add(ne);

    oe.version = eversion_t(2,1);
    oe.op = pg_log_entry_t::MODIFY;
    missing.add_next_event(oe);

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_EQ(1U, log.log.size());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.size() > 0);
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
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(1,1);
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t(2,1);
    missing_add(oe.soid, oe.prior_version, eversion_t());

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(log.empty());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
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
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(2,1);
    oe.soid.set_hash(1);
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t(1,1);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    EXPECT_TRUE(is_dirty());
    EXPECT_EQ(oe.soid, remove_snap.front());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());
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
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(2,1);
    oe.soid.set_hash(1);
    oe.op = pg_log_entry_t::DELETE;
    oe.prior_version = eversion_t(1,1);

    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(missing.have_missing());
    EXPECT_TRUE(log.empty());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
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
    oe.mark_unrollbackable();
    pg_info_t info;
    list<hobject_t> remove_snap;

    info.log_tail = eversion_t(10,1);
    oe.soid.set_hash(1);
    oe.op = pg_log_entry_t::MODIFY;
    oe.prior_version = eversion_t();

    missing.add(oe.soid, eversion_t(1,1), eversion_t(), false);

    missing.flush();
    EXPECT_FALSE(is_dirty());
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(missing.is_missing(oe.soid));
    EXPECT_TRUE(log.empty());

    TestHandler h(remove_snap);
    merge_old_entry(t, oe, info, &h);

    missing.flush();
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
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
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    merge_log(oinfo, olog, fromosd, info, &h,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
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
    missing.may_include_deletes = false;

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(eversion_t(), info.stats.version);
    EXPECT_EQ(1ull, info.stats.reported_seq);
    EXPECT_EQ(10u, info.stats.reported_epoch);
    EXPECT_TRUE(remove_snap.empty());
    EXPECT_TRUE(info.last_backfill.is_max());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    merge_log(oinfo, olog, fromosd, info, &h,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(0U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_EQ(1ull, info.stats.reported_seq);
    EXPECT_EQ(10u, info.stats.reported_epoch);
    EXPECT_TRUE(remove_snap.empty());
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;
    missing.may_include_deletes = false;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 4);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.set_hash(0x9);
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.set_hash(0x9);
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
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    merge_log(oinfo, olog, fromosd, info, &h,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(3U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_TRUE(remove_snap.empty());
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;
    missing.may_include_deletes = true;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.set_hash(0x3);
      log.log.push_back(e);
      e.version = eversion_t(1,3);
      e.soid.set_hash(0x9);
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.set_hash(0x3);
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.set_hash(0x9);
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      e.version = eversion_t(2, 4);
      e.soid.set_hash(0x7);
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
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    merge_log(oinfo, olog, fromosd, info, &h,
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
       log, and the divergent version of the object is removed (added
       to remove_snap)
     */
    EXPECT_EQ(0x9U, remove_snap.front().get_hash());
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    hobject_t divergent_object;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.set_hash(0x3);
      log.log.push_back(e);
      e.version = eversion_t(1,3);
      e.soid.set_hash(0x9);
      divergent_object = e.soid;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 2);
      e.soid.set_hash(0x3);
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.set_hash(0x9);
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      e.version = eversion_t(2, 4);
      e.soid.set_hash(0x7);
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
    EXPECT_EQ(log.head, info.last_update);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    missing.may_include_deletes = false;
    merge_log(oinfo, olog, fromosd, info, &h,
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
       log, and the divergent version of the object is removed (added
       to remove_snap). When peering handles deletes, it is the earlier
       version that is in the removed list.
     */
    EXPECT_EQ(0x7U, remove_snap.front().get_hash());
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_shard_t fromosd;
    pg_info_t info;
    list<hobject_t> remove_snap;
    bool dirty_info = false;
    bool dirty_big_info = false;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 4);
      e.soid.set_hash(0x7);
      log.log.push_back(e);
      e.version = eversion_t(1, 5);
      e.soid.set_hash(0x9);
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      info.last_update = log.head;

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x5);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(1, 4);
      e.soid.set_hash(0x7);
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
    EXPECT_EQ(last_backfill, info.last_backfill);
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_FALSE(is_dirty());
    EXPECT_FALSE(dirty_info);
    EXPECT_FALSE(dirty_big_info);

    TestHandler h(remove_snap);
    missing.may_include_deletes = false;
    merge_log(oinfo, olog, fromosd, info, &h,
              dirty_info, dirty_big_info);

    EXPECT_FALSE(missing.have_missing());
    EXPECT_EQ(2U, log.log.size());
    EXPECT_EQ(stat_version, info.stats.version);
    EXPECT_EQ(0x9U, remove_snap.front().get_hash());
    EXPECT_TRUE(info.purged_snaps.empty());
    EXPECT_TRUE(is_dirty());
    EXPECT_TRUE(dirty_info);
    EXPECT_TRUE(dirty_big_info);
  }

}

TEST_F(PGLogTest, proc_replica_log) {
  // empty log : no side effect
  {
    clear();

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    eversion_t last_update(1, 1);
    log.head = olog.head = oinfo.last_update = last_update;
    eversion_t last_complete(1, 1);
    oinfo.last_complete = last_complete;

    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(last_complete, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 2);
      e.soid.set_hash(0x5);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = eversion_t(1, 3);
      e.soid.set_hash(0x9);
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x3);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.soid.set_hash(0x9);
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

    EXPECT_FALSE(omissing.have_missing());
  }

 {
    clear();

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    hobject_t divergent_object;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      {
	e.soid = divergent_object;
	e.soid.set_hash(0x1);
	e.version = eversion_t(1, 1);
	log.tail = e.version;
	log.log.push_back(e);

	e.soid = divergent_object;
	e.prior_version = eversion_t(1, 1);
	e.version = eversion_t(1, 2);
	log.tail = e.version;
	log.log.push_back(e);

	e.soid.set_hash(0x3);
	e.version = eversion_t(1, 4);
	log.log.push_back(e);

	e.soid.set_hash(0x7);
	e.version = eversion_t(1, 5);
	log.log.push_back(e);

	e.soid.set_hash(0x8);
	e.version = eversion_t(1, 6);
	log.log.push_back(e);

	e.soid.set_hash(0x9);
	e.op = pg_log_entry_t::DELETE;
	e.version = eversion_t(2, 7);
	log.log.push_back(e);

	e.soid.set_hash(0xa);
	e.version = eversion_t(2, 8);
	log.head = e.version;
	log.log.push_back(e);
      }
      log.index();

      {
	e.soid = divergent_object;
	e.soid.set_hash(0x1);
	e.version = eversion_t(1, 1);
	olog.tail = e.version;
	olog.log.push_back(e);

	e.soid = divergent_object;
	e.prior_version = eversion_t(1, 1);
	e.version = eversion_t(1, 2);
	olog.log.push_back(e);

	e.prior_version = eversion_t(0, 0);
	e.soid.set_hash(0x3);
	e.version = eversion_t(1, 4);
	olog.log.push_back(e);

	e.soid.set_hash(0x7);
	e.version = eversion_t(1, 5);
	olog.log.push_back(e);

	e.soid.set_hash(0x8);
	e.version = eversion_t(1, 6);
	olog.log.push_back(e);

	e.soid.set_hash(0x9); // should not be added to missing, create
	e.op = pg_log_entry_t::MODIFY;
	e.version = eversion_t(1, 7);
	olog.log.push_back(e);

	e.soid = divergent_object; // should be added to missing at 1,2
	e.op = pg_log_entry_t::MODIFY;
	e.version = eversion_t(1, 8);
	e.prior_version = eversion_t(1, 2);
	olog.log.push_back(e);
	olog.head = e.version;
      }
      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(eversion_t(1, 2), omissing.get_items().at(divergent_object).need);
    EXPECT_EQ(eversion_t(1, 6), oinfo.last_update);
    EXPECT_EQ(eversion_t(1, 1), oinfo.last_complete);
  }

  /*        +--------------------------+
            |  olog              log   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x9   |  (1,1)  < tail
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

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;
    divergent_object.set_hash(0x9);

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid = divergent_object;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      log.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.prior_version = eversion_t(1, 1);
      e.soid = divergent_object;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid = divergent_object;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      olog.log.push_back(e);
      e.version = eversion_t(1, 3);
      e.prior_version = eversion_t(1, 1);
      e.soid = divergent_object;
      e.op = pg_log_entry_t::DELETE;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_FALSE(omissing.have_missing());
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(omissing.get_items().at(divergent_object).have, eversion_t(0, 0));
    EXPECT_EQ(omissing.get_items().at(divergent_object).need, eversion_t(1, 1));
    EXPECT_EQ(last_update, oinfo.last_update);
  }

  /*        +--------------------------+
            |  olog              log   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x9   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
       head > (1,3)  |  x9   |         |
            | MODIFY |       |         |
            |        |       |         |
            |        |  x9   |  (2,3)  < head
            |        |       |  DELETE |
            |        |       |         |
            +--------+-------+---------+

      The log entry (1,3) deletes the object x9 but the olog entry
      (2,3) modifies it : remove it from omissing.

  */
  {
    clear();

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid = divergent_object;
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      log.log.push_back(e);
      e.version = eversion_t(2, 3);
      e.prior_version = eversion_t(1, 1);
      e.soid = divergent_object;
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.version = eversion_t(1, 1);
      e.soid = divergent_object;
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      olog.log.push_back(e);
      e.version = eversion_t(1, 3);
      e.prior_version = eversion_t(1, 1);
      e.soid = divergent_object;
      divergent_object = e.soid;
      omissing.add(divergent_object, e.version, eversion_t(), false);
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(eversion_t(1, 3), omissing.get_items().at(divergent_object).need);
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(omissing.get_items().at(divergent_object).have, eversion_t(0, 0));
    EXPECT_EQ(omissing.get_items().at(divergent_object).need, eversion_t(1, 1));
    EXPECT_EQ(last_update, oinfo.last_update);
  }

  /*        +--------------------------+
            |  log              olog   |
            +--------+-------+---------+
            |        |object |         |
            |version | hash  | version |
            |        |       |         |
       tail > (1,1)  |  x9   |  (1,1)  < tail
            |        |       |         |
            |        |       |         |
            | (1,2)  |  x3   |  (1,2)  |
            |        |       |         |
            |        |       |         |
            |        |  x9   |  (1,3)  < head
            |        |       |  MODIFY |
            |        |       |         |
       head > (2,3)  |  x9   |         |
            | DELETE |       |         |
            |        |       |         |
            +--------+-------+---------+

      The log entry (2,3) deletes the object x9 but the olog entry
      (1,3) modifies it : proc_replica_log should adjust missing to
      1,1 for that object until add_next_event in PG::activate processes
      the delete.
  */
  {
    clear();

    pg_log_t olog;
    pg_info_t oinfo;
    pg_missing_t omissing;
    pg_shard_t from;

    eversion_t last_update(1, 2);
    hobject_t divergent_object;
    eversion_t new_version(2, 3);
    eversion_t divergent_version(1, 3);

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();

      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x9);
      log.tail = e.version;
      log.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      log.log.push_back(e);
      e.version = new_version;
      e.prior_version = eversion_t(1, 1);
      e.soid.set_hash(0x9);
      e.op = pg_log_entry_t::DELETE;
      log.log.push_back(e);
      log.head = e.version;
      log.index();

      e.op = pg_log_entry_t::MODIFY;
      e.version = eversion_t(1, 1);
      e.soid.set_hash(0x9);
      olog.tail = e.version;
      olog.log.push_back(e);
      e.version = last_update;
      e.soid.set_hash(0x3);
      olog.log.push_back(e);
      e.version = divergent_version;
      e.prior_version = eversion_t(1, 1);
      e.soid.set_hash(0x9);
      divergent_object = e.soid;
      omissing.add(divergent_object, e.version, eversion_t(), false);
      e.op = pg_log_entry_t::MODIFY;
      olog.log.push_back(e);
      olog.head = e.version;

      oinfo.last_update = olog.head;
      oinfo.last_complete = olog.head;
    }

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.is_missing(divergent_object));
    EXPECT_EQ(divergent_version, omissing.get_items().at(divergent_object).need);
    EXPECT_EQ(olog.head, oinfo.last_update);
    EXPECT_EQ(olog.head, oinfo.last_complete);

    missing.may_include_deletes = false;
    proc_replica_log(oinfo, olog, omissing, from);

    EXPECT_TRUE(omissing.have_missing());
    EXPECT_TRUE(omissing.get_items().begin()->second.need == eversion_t(1, 1));
    EXPECT_EQ(last_update, oinfo.last_update);
    EXPECT_EQ(eversion_t(0, 0), oinfo.last_complete);
  }

}

TEST_F(PGLogTest, merge_log_1) {
  TestCase t;
  t.base.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));

  t.final.add(mk_obj(1), mk_evt(10, 100), mk_evt(0, 0), false);

  t.toremove.insert(mk_obj(1));

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_2) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));
  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 102), mk_evt(10, 101)));

  t.torollback.insert(
    t.torollback.begin(), t.div.rbegin(), t.div.rend());

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_3) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));
  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 102), mk_evt(10, 101)));

  t.final.add(mk_obj(1), mk_evt(10, 100), mk_evt(0, 0), false);

  t.toremove.insert(mk_obj(1));

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_4) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));
  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 102), mk_evt(10, 101)));

  t.init.add(mk_obj(1), mk_evt(10, 102), mk_evt(0, 0), false);
  t.final.add(mk_obj(1), mk_evt(10, 100), mk_evt(0, 0), false);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_5) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));
  t.div.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 102), mk_evt(10, 101)));

  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.final.add(mk_obj(1), mk_evt(11, 101), mk_evt(0, 0), false);

  t.toremove.insert(mk_obj(1));

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_6) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.final.add(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100), false);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_7) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.init.add(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80), false);
  t.final.add(mk_obj(1), mk_evt(11, 101), mk_evt(8, 80), false);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_8) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.auth.push_back(mk_ple_dt(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.init.add(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80), false);
  t.final.add(mk_obj(1), mk_evt(11, 101), mk_evt(8, 80), true);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_9) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.auth.push_back(mk_ple_dt(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.init.add(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80), false);
  t.toremove.insert(mk_obj(1));
  t.deletes_during_peering = true;

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_10) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.auth.push_back(mk_ple_ldt(mk_obj(1), mk_evt(11, 101), mk_evt(10, 100)));

  t.init.add(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80), false);
  t.final.add(mk_obj(1), mk_evt(11, 101), mk_evt(8, 80), true);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_prior_version_have) {
  TestCase t;
  t.base.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 80)));

  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100)));

  t.init.add(mk_obj(1), mk_evt(10, 101), mk_evt(10, 100), false);

  t.setup();
  run_test_case(t);
}

TEST_F(PGLogTest, merge_log_split_missing_entries_at_head) {
  TestCase t;
  t.auth.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  t.auth.push_back(mk_ple_mod_rb(mk_obj(1), mk_evt(15, 150), mk_evt(10, 100)));

  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(8, 70), mk_evt(8, 65)));

  t.setup();
  t.set_div_bounds(mk_evt(9, 79), mk_evt(8, 69));
  t.set_auth_bounds(mk_evt(15, 160), mk_evt(9, 77));
  t.final.add(mk_obj(1), mk_evt(15, 150), mk_evt(8, 70), false);
  run_test_case(t);
}

TEST_F(PGLogTest, olog_tail_gt_log_tail_split) {
  TestCase t;
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(15, 150), mk_evt(10, 100)));
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(15, 155), mk_evt(15, 150)));

  t.setup();
  t.set_div_bounds(mk_evt(15, 153), mk_evt(15, 151));
  t.set_auth_bounds(mk_evt(15, 156), mk_evt(10, 99));
  t.final.add(mk_obj(1), mk_evt(15, 155), mk_evt(15, 150), false);
  run_test_case(t);
}

TEST_F(PGLogTest, olog_tail_gt_log_tail_split2) {
  TestCase t;
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(15, 150), mk_evt(10, 100)));
  t.auth.push_back(mk_ple_mod(mk_obj(1), mk_evt(16, 155), mk_evt(15, 150)));
  t.div.push_back(mk_ple_mod(mk_obj(1), mk_evt(15, 153), mk_evt(15, 150)));

  t.setup();
  t.set_div_bounds(mk_evt(15, 153), mk_evt(15, 151));
  t.set_auth_bounds(mk_evt(16, 156), mk_evt(10, 99));
  t.final.add(mk_obj(1), mk_evt(16, 155), mk_evt(0, 0), false);
  t.toremove.insert(mk_obj(1));
  run_test_case(t);
}

TEST_F(PGLogTest, filter_log_1) {
  {
    clear();

    int osd_id = 1;
    epoch_t epoch = 40;
    int64_t pool_id = 1;
    int bits = 2;
    int max_osd = 4;
    int pg_num = max_osd << bits;
    int num_objects = 1000;
    int num_internal = 10;

    // Set up splitting map
    OSDMap *osdmap = new OSDMap;
    uuid_d test_uuid;
    test_uuid.generate_random();
    osdmap->build_simple_with_pool(g_ceph_context, epoch, test_uuid, max_osd, bits, bits);
    osdmap->set_state(osd_id, CEPH_OSD_EXISTS);

    const string hit_set_namespace("internal");

    {
      pg_log_entry_t e;
      e.mark_unrollbackable();
      e.op = pg_log_entry_t::MODIFY;
      e.soid.pool = pool_id;

      uuid_d uuid_name;
      int i;
      for (i = 1; i <= num_objects; ++i) {
        e.version = eversion_t(epoch, i);
        // Use this to generate random file names
        uuid_name.generate_random();
        ostringstream name;
        name << uuid_name;
        e.soid.oid.name = name.str();
	// First has no namespace
        if (i != 1) {
           // num_internal have the internal namspace
          if (i <= num_internal + 1) {
            e.soid.nspace = hit_set_namespace;
          } else { // rest have different namespaces
            ostringstream ns;
            ns << "ns" << i;
            e.soid.nspace = ns.str();
          }
        }
        log.log.push_back(e);
        if (i == 1)
          log.tail = e.version;
      }
      log.head = e.version;
      log.index();
    }

    spg_t pgid(pg_t(2, pool_id), shard_id_t::NO_SHARD);

    // See if we created the right number of entries
    int total = log.log.size();
    ASSERT_EQ(total, num_objects);

    // Some should be removed
    {
      pg_log_t filtered, reject;
      pg_log_t::filter_log(
	pgid, *osdmap, hit_set_namespace, log, filtered, reject);
      log = IndexedLog(filtered);
    }
    EXPECT_LE(log.log.size(), (size_t)total);

    // If we filter a second time, there should be the same total
    total = log.log.size();
    {
      pg_log_t filtered, reject;
      pg_log_t::filter_log(
	pgid, *osdmap, hit_set_namespace, log, filtered, reject);
      log = IndexedLog(filtered);
    }
    EXPECT_EQ(log.log.size(), (size_t)total);

    // Increase pg_num as if there would be a split
    int new_pg_num = pg_num * 16;
    OSDMap::Incremental inc(epoch + 1);
    inc.fsid = test_uuid;
    const pg_pool_t *pool = osdmap->get_pg_pool(pool_id);
    pg_pool_t newpool;
    newpool = *pool;
    newpool.set_pg_num(new_pg_num);
    newpool.set_pgp_num(new_pg_num);
    inc.new_pools[pool_id] = newpool;
    int ret = osdmap->apply_incremental(inc);
    ASSERT_EQ(ret, 0);

    // We should have fewer entries after a filter
    {
      pg_log_t filtered, reject;
      pg_log_t::filter_log(
	pgid, *osdmap, hit_set_namespace, log, filtered, reject);
      log = IndexedLog(filtered);
    }
    EXPECT_LE(log.log.size(), (size_t)total);

    // Make sure all internal entries are retained
    int count = 0;
    for (list<pg_log_entry_t>::iterator i = log.log.begin();
         i != log.log.end(); ++i) {
      if (i->soid.nspace == hit_set_namespace) count++;
    }
    EXPECT_EQ(count, num_internal);
  }
}

TEST_F(PGLogTest, get_request) {
  clear();

  // make sure writes, deletes, and errors are found
  vector<pg_log_entry_t> entries;
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  entries.push_back(
    pg_log_entry_t(pg_log_entry_t::ERROR, oid, eversion_t(6,2), eversion_t(3,4),
		   1, osd_reqid_t(entity_name_t::CLIENT(777), 8, 1),
		   utime_t(0,1), -ENOENT));
  entries.push_back(
    pg_log_entry_t(pg_log_entry_t::MODIFY, oid, eversion_t(6,3), eversion_t(3,4),
		   2, osd_reqid_t(entity_name_t::CLIENT(777), 8, 2),
		   utime_t(1,2), 0));
  entries.push_back(
    pg_log_entry_t(pg_log_entry_t::DELETE, oid, eversion_t(7,4), eversion_t(7,4),
		   3, osd_reqid_t(entity_name_t::CLIENT(777), 8, 3),
		   utime_t(10,2), 0));
  entries.push_back(
    pg_log_entry_t(pg_log_entry_t::ERROR, oid, eversion_t(7,5), eversion_t(7,4),
		   3, osd_reqid_t(entity_name_t::CLIENT(777), 8, 4),
		   utime_t(20,1), -ENOENT));

  for (auto &entry : entries) {
    log.add(entry);
  }

  for (auto &entry : entries) {
    eversion_t replay_version;
    version_t user_version;
    int return_code = 0;
    bool got = log.get_request(
      entry.reqid, &replay_version, &user_version, &return_code);
    EXPECT_TRUE(got);
    EXPECT_EQ(entry.return_code, return_code);
    EXPECT_EQ(entry.version, replay_version);
    EXPECT_EQ(entry.user_version, user_version);
  }
}

TEST_F(PGLogTest, ErrorNotIndexedByObject) {
  clear();

  // make sure writes, deletes, and errors are found
  hobject_t oid(object_t("objname"), "key", 123, 456, 0, "");
  log.add(
    pg_log_entry_t(pg_log_entry_t::ERROR, oid, eversion_t(6,2), eversion_t(3,4),
		   1, osd_reqid_t(entity_name_t::CLIENT(777), 8, 1),
		   utime_t(0,1), -ENOENT));

  EXPECT_FALSE(log.logged_object(oid));

  pg_log_entry_t modify(pg_log_entry_t::MODIFY, oid, eversion_t(6,3),
			eversion_t(3,4), 2,
			osd_reqid_t(entity_name_t::CLIENT(777), 8, 2),
			utime_t(1,2), 0);
  log.add(modify);

  EXPECT_TRUE(log.logged_object(oid));
  pg_log_entry_t *entry = log.objects[oid];
  EXPECT_EQ(modify.op, entry->op);
  EXPECT_EQ(modify.version, entry->version);
  EXPECT_EQ(modify.prior_version, entry->prior_version);
  EXPECT_EQ(modify.user_version, entry->user_version);
  EXPECT_EQ(modify.reqid, entry->reqid);

  pg_log_entry_t del(pg_log_entry_t::DELETE, oid, eversion_t(7,4),
		     eversion_t(7,4), 3,
		     osd_reqid_t(entity_name_t::CLIENT(777), 8, 3),
		     utime_t(10,2), 0);
  log.add(del);

  EXPECT_TRUE(log.logged_object(oid));
  entry = log.objects[oid];
  EXPECT_EQ(del.op, entry->op);
  EXPECT_EQ(del.version, entry->version);
  EXPECT_EQ(del.prior_version, entry->prior_version);
  EXPECT_EQ(del.user_version, entry->user_version);
  EXPECT_EQ(del.reqid, entry->reqid);

  log.add(
    pg_log_entry_t(pg_log_entry_t::ERROR, oid, eversion_t(7,5), eversion_t(7,4),
		   3, osd_reqid_t(entity_name_t::CLIENT(777), 8, 4),
		   utime_t(20,1), -ENOENT));

  EXPECT_TRUE(log.logged_object(oid));
  entry = log.objects[oid];
  EXPECT_EQ(del.op, entry->op);
  EXPECT_EQ(del.version, entry->version);
  EXPECT_EQ(del.prior_version, entry->prior_version);
  EXPECT_EQ(del.user_version, entry->user_version);
  EXPECT_EQ(del.reqid, entry->reqid);
}

TEST_F(PGLogTest, split_into_preserves_may_include_deletes) {
  clear();

  {
    rebuilt_missing_with_deletes = false;
    missing.may_include_deletes = true;
    PGLog child_log(cct);
    pg_t child_pg;
    split_into(child_pg, 6, &child_log);
    ASSERT_TRUE(child_log.get_missing().may_include_deletes);
    ASSERT_TRUE(child_log.get_rebuilt_missing_with_deletes());
  }

  {
    rebuilt_missing_with_deletes = false;
    missing.may_include_deletes = false;
    PGLog child_log(cct);
    pg_t child_pg;
    split_into(child_pg, 6, &child_log);
    ASSERT_FALSE(child_log.get_missing().may_include_deletes);
    ASSERT_FALSE(child_log.get_rebuilt_missing_with_deletes());
  }
}

class PGLogTestRebuildMissing : public PGLogTest, public StoreTestFixture {
public:
  PGLogTestRebuildMissing() : PGLogTest(), StoreTestFixture("memstore") {}
  void SetUp() override {
    StoreTestFixture::SetUp();
    ObjectStore::Transaction t;
    test_coll = coll_t(spg_t(pg_t(1, 1)));
    ch = store->create_new_collection(test_coll);
    t.create_collection(test_coll, 0);
    store->queue_transaction(ch, std::move(t));
    existing_oid = mk_obj(0);
    nonexistent_oid = mk_obj(1);
    ghobject_t existing_ghobj(existing_oid);
    object_info_t existing_info;
    existing_info.version = eversion_t(6, 2);
    bufferlist enc_oi;
    encode(existing_info, enc_oi, 0);
    ObjectStore::Transaction t2;
    t2.touch(test_coll, ghobject_t(existing_oid));
    t2.setattr(test_coll, ghobject_t(existing_oid), OI_ATTR, enc_oi);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t2)));
    info.last_backfill = hobject_t::get_max();
    info.last_complete = eversion_t();
  }

  void TearDown() override {
    clear();
    missing.may_include_deletes = false;
    StoreTestFixture::TearDown();
  }

  pg_info_t info;
  coll_t test_coll;
  hobject_t existing_oid, nonexistent_oid;

  void run_rebuild_missing_test(const map<hobject_t, pg_missing_item> &expected_missing_items) {
    rebuild_missing_set_with_deletes(store.get(), ch, info);
    ASSERT_EQ(expected_missing_items, missing.get_items());
  }
};

TEST_F(PGLogTestRebuildMissing, EmptyLog) {
  missing.add(existing_oid, mk_evt(6, 2), mk_evt(6, 3), false);
  missing.add(nonexistent_oid, mk_evt(7, 4), mk_evt(0, 0), false);
  map<hobject_t, pg_missing_item> orig_missing = missing.get_items();
  run_rebuild_missing_test(orig_missing);
}

TEST_F(PGLogTestRebuildMissing, SameVersionMod) {
  missing.add(existing_oid, mk_evt(6, 2), mk_evt(6, 1), false);
  log.add(mk_ple_mod(existing_oid, mk_evt(6, 2), mk_evt(6, 1)));
  map<hobject_t, pg_missing_item> empty_missing;
  run_rebuild_missing_test(empty_missing);
}

TEST_F(PGLogTestRebuildMissing, DelExisting) {
  missing.add(existing_oid, mk_evt(6, 3), mk_evt(6, 2), false);
  log.add(mk_ple_dt(existing_oid, mk_evt(7, 5), mk_evt(7, 4)));
  map<hobject_t, pg_missing_item> expected;
  expected[existing_oid] = pg_missing_item(mk_evt(7, 5), mk_evt(6, 2), true);
  run_rebuild_missing_test(expected);
}

TEST_F(PGLogTestRebuildMissing, DelNonexistent) {
  log.add(mk_ple_dt(nonexistent_oid, mk_evt(7, 5), mk_evt(7, 4)));
  map<hobject_t, pg_missing_item> expected;
  expected[nonexistent_oid] = pg_missing_item(mk_evt(7, 5), mk_evt(0, 0), true);
  run_rebuild_missing_test(expected);
}

TEST_F(PGLogTestRebuildMissing, MissingNotInLog) {
  missing.add(mk_obj(10), mk_evt(8, 12), mk_evt(8, 10), false);
  log.add(mk_ple_dt(nonexistent_oid, mk_evt(7, 5), mk_evt(7, 4)));
  map<hobject_t, pg_missing_item> expected;
  expected[nonexistent_oid] = pg_missing_item(mk_evt(7, 5), mk_evt(0, 0), true);
  expected[mk_obj(10)] = pg_missing_item(mk_evt(8, 12), mk_evt(8, 10), false);
  run_rebuild_missing_test(expected);
}


class PGLogMergeDupsTest : protected PGLog, public StoreTestFixture {

public:

  PGLogMergeDupsTest() : PGLog(g_ceph_context), StoreTestFixture("memstore") { }

  void SetUp() override {
    StoreTestFixture::SetUp();
    ObjectStore::Transaction t;
    test_coll = coll_t(spg_t(pg_t(1, 1)));
    auto ch = store->create_new_collection(test_coll);
    t.create_collection(test_coll, 0);
    store->queue_transaction(ch, std::move(t));
  }

  void TearDown() override {
    test_disk_roundtrip();
    clear();
    StoreTestFixture::TearDown();
  }

  static pg_log_dup_t create_dup_entry(uint a, uint b) {
    // make each dup_entry unique by using different client id's
    static uint client_id = 777;
    return pg_log_dup_t(eversion_t(a, b),
			a,
			osd_reqid_t(entity_name_t::CLIENT(client_id++), 8, 1),
			0);
  }

  static std::vector<pg_log_dup_t> example_dups_1() {
    std::vector<pg_log_dup_t> result = {
      create_dup_entry(10, 11),
      create_dup_entry(10, 12),
      create_dup_entry(11, 1),
      create_dup_entry(12, 3),
      create_dup_entry(13, 99)
    };
    return result;
  }

  static std::vector<pg_log_dup_t> example_dups_2() {
    std::vector<pg_log_dup_t> result = {
      create_dup_entry(12, 3),
      create_dup_entry(13, 99),
      create_dup_entry(15, 11),
      create_dup_entry(16, 14),
      create_dup_entry(16, 32)
    };
    return result;
  }

  void add_dups(uint a, uint b) {
    log.dups.push_back(create_dup_entry(a, b));
    write_from_dups = std::min(write_from_dups, log.dups.back().version);
  }

  void add_dups(const std::vector<pg_log_dup_t>& l) {
    for (auto& i : l) {
      log.dups.push_back(i);
      write_from_dups = std::min(write_from_dups, log.dups.back().version);
    }
  }

  static void add_dups(IndexedLog& log, const std::vector<pg_log_dup_t>& dups) {
    for (auto& i : dups) {
      log.dups.push_back(i);
    }
  }

  void check_order() {
    eversion_t prev(0, 0);

    for (auto& i : log.dups) {
      EXPECT_LT(prev, i.version) << "verify versions monotonically increase";
      prev = i.version;
    }
  }

  void check_index() {
    EXPECT_EQ(log.dups.size(), log.dup_index.size());
    for (auto& i : log.dups) {
      EXPECT_EQ(1u, log.dup_index.count(i.reqid));
    }
  }

  void test_disk_roundtrip() {
    ObjectStore::Transaction t;
    hobject_t hoid;
    hoid.pool = 1;
    hoid.oid = "log";
    ghobject_t log_oid(hoid);
    map<string, bufferlist> km;
    write_log_and_missing(t, &km, test_coll, log_oid, false);
    if (!km.empty()) {
      t.omap_setkeys(test_coll, log_oid, km);
    }
    auto ch = store->open_collection(test_coll);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));

    auto orig_dups = log.dups;
    clear();
    ostringstream err;
    read_log_and_missing(store.get(), ch, log_oid,
			 pg_info_t(), err, false);
    ASSERT_EQ(orig_dups.size(), log.dups.size());
    ASSERT_EQ(orig_dups, log.dups);
    auto dups_it = log.dups.begin();
    for (auto orig_dup : orig_dups) {
      ASSERT_EQ(orig_dup, *dups_it);
      ++dups_it;
    }
  }

  coll_t test_coll;
};

TEST_F(PGLogMergeDupsTest, OtherEmpty) {
  log.tail = eversion_t(14, 5);

  IndexedLog olog;

  add_dups(example_dups_1());
  index();

  bool changed = merge_log_dups(olog);

  EXPECT_FALSE(changed);
  EXPECT_EQ(5u, log.dups.size());

  if (5 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);
    EXPECT_EQ(13u, log.dups.back().version.epoch);
    EXPECT_EQ(99u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}

TEST_F(PGLogMergeDupsTest, AmEmpty) {
  log.tail = eversion_t(14, 5);
  index();

  IndexedLog olog;

  add_dups(olog, example_dups_1());

  bool changed = merge_log_dups(olog);

  EXPECT_TRUE(changed);
  EXPECT_EQ(5u, log.dups.size());

  if (5 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);

    EXPECT_EQ(13u, log.dups.back().version.epoch);
    EXPECT_EQ(99u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}

TEST_F(PGLogMergeDupsTest, AmEmptyOverlap) {
  log.tail = eversion_t(12, 3);
  index();

  IndexedLog olog;

  add_dups(olog, example_dups_1());

  bool changed = merge_log_dups(olog);

  EXPECT_TRUE(changed);
  EXPECT_EQ(3u, log.dups.size());

  if (3 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);

    EXPECT_EQ(11u, log.dups.back().version.epoch);
    EXPECT_EQ(1u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}

TEST_F(PGLogMergeDupsTest, Same) {
  log.tail = eversion_t(14, 1);

  IndexedLog olog;

  add_dups(example_dups_1());
  index();
  add_dups(olog, example_dups_1());

  bool changed = merge_log_dups(olog);

  EXPECT_FALSE(changed);
  EXPECT_EQ(5u, log.dups.size());

  if (5 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);

    EXPECT_EQ(13u, log.dups.back().version.epoch);
    EXPECT_EQ(99u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}


TEST_F(PGLogMergeDupsTest, Later) {
  log.tail = eversion_t(16, 14);

  IndexedLog olog;

  add_dups(example_dups_1());
  index();
  add_dups(olog, example_dups_2());

  bool changed = merge_log_dups(olog);

  EXPECT_TRUE(changed);
  EXPECT_EQ(6u, log.dups.size());

  if (6 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);

    EXPECT_EQ(15u, log.dups.back().version.epoch);
    EXPECT_EQ(11u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}


TEST_F(PGLogMergeDupsTest, Earlier) {
  log.tail = eversion_t(17, 2);

  IndexedLog olog;

  add_dups(example_dups_2());
  index();
  add_dups(olog, example_dups_1());

  bool changed = merge_log_dups(olog);

  EXPECT_TRUE(changed);
  EXPECT_EQ(8u, log.dups.size());

  if (6 == log.dups.size()) {
    EXPECT_EQ(10u, log.dups.front().version.epoch);
    EXPECT_EQ(11u, log.dups.front().version.version);

    EXPECT_EQ(16u, log.dups.back().version.epoch);
    EXPECT_EQ(32u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}


TEST_F(PGLogMergeDupsTest, Superset) {
  log.tail = eversion_t(17, 2);

  IndexedLog olog;

  add_dups(example_dups_1());
  index();

  olog.dups.push_back(create_dup_entry(9, 5));
  olog.dups.push_back(create_dup_entry(15, 11));

  bool changed = merge_log_dups(olog);

  EXPECT_TRUE(changed);
  EXPECT_EQ(7u, log.dups.size());

  if (7 == log.dups.size()) {
    EXPECT_EQ(9u, log.dups.front().version.epoch);
    EXPECT_EQ(5u, log.dups.front().version.version);

    EXPECT_EQ(15u, log.dups.back().version.epoch);
    EXPECT_EQ(11u, log.dups.back().version.version);
  }

  check_order();
  check_index();
}


struct PGLogTrimTest :
  public ::testing::Test,
  public PGLogTestBase,
  public PGLog::IndexedLog
{
  CephContext *cct = g_ceph_context;

  using ::testing::Test::SetUp;
  void SetUp(unsigned min_entries, unsigned max_entries, unsigned dup_track) {
    constexpr size_t size = 10;

    char min_entries_s[size];
    char max_entries_s[size];
    char dup_track_s[size];

    snprintf(min_entries_s, size, "%u", min_entries);
    snprintf(max_entries_s, size, "%u", max_entries);
    snprintf(dup_track_s, size, "%u", dup_track);

    cct->_conf.set_val_or_die("osd_min_pg_log_entries", min_entries_s);
    cct->_conf.set_val_or_die("osd_max_pg_log_entries", max_entries_s);
    cct->_conf.set_val_or_die("osd_pg_log_dups_tracked", dup_track_s);
  }
}; // struct PGLogTrimTest


TEST_F(PGLogTrimTest, TestMakingCephContext)
{
  SetUp(1, 2, 5);

  EXPECT_EQ(1u, cct->_conf->osd_min_pg_log_entries);
  EXPECT_EQ(2u, cct->_conf->osd_max_pg_log_entries);
  EXPECT_EQ(5u, cct->_conf->osd_pg_log_dups_tracked);
}


TEST_F(PGLogTrimTest, TestPartialTrim)
{
  SetUp(1, 2, 20);
  PGLog::IndexedLog log;
  log.head = mk_evt(24, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(19, 160), mk_evt(25, 152)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166)));

  std::set<eversion_t> trimmed;
  std::set<std::string> trimmed_dups;
  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(19, 157), &trimmed, &trimmed_dups, &write_from_dups);

  EXPECT_EQ(eversion_t(15, 150), write_from_dups);
  EXPECT_EQ(3u, log.log.size());
  EXPECT_EQ(3u, trimmed.size());
  EXPECT_EQ(2u, log.dups.size());
  EXPECT_EQ(0u, trimmed_dups.size());

  SetUp(1, 2, 15);

  std::set<eversion_t> trimmed2;
  std::set<std::string> trimmed_dups2;
  eversion_t write_from_dups2 = eversion_t::max();

  log.trim(cct, mk_evt(20, 164), &trimmed2, &trimmed_dups2, &write_from_dups2);

  EXPECT_EQ(eversion_t(19, 160), write_from_dups2);
  EXPECT_EQ(2u, log.log.size());
  EXPECT_EQ(1u, trimmed2.size());
  EXPECT_EQ(2u, log.dups.size());
  EXPECT_EQ(1u, trimmed_dups2.size());
}


TEST_F(PGLogTrimTest, TestTrimNoTrimmed) {
  SetUp(1, 2, 20);
  PGLog::IndexedLog log;
  log.head = mk_evt(20, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(20, 160), mk_evt(25, 152)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166)));

  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(19, 157), nullptr, nullptr, &write_from_dups);

  EXPECT_EQ(eversion_t(15, 150), write_from_dups);
  EXPECT_EQ(3u, log.log.size());
  EXPECT_EQ(2u, log.dups.size());
}


TEST_F(PGLogTrimTest, TestTrimNoDups)
{
  SetUp(1, 2, 10);
  PGLog::IndexedLog log;
  log.head = mk_evt(20, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(20, 160), mk_evt(25, 152)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166)));

  std::set<eversion_t> trimmed;
  std::set<std::string> trimmed_dups;
  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(19, 157), &trimmed, &trimmed_dups, &write_from_dups);

  EXPECT_EQ(eversion_t::max(), write_from_dups);
  EXPECT_EQ(3u, log.log.size());
  EXPECT_EQ(3u, trimmed.size());
  EXPECT_EQ(0u, log.dups.size());
  EXPECT_EQ(0u, trimmed_dups.size());
}

TEST_F(PGLogTrimTest, TestNoTrim)
{
  SetUp(1, 2, 20);
  PGLog::IndexedLog log;
  log.head = mk_evt(24, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(19, 160), mk_evt(25, 152)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166)));

  std::set<eversion_t> trimmed;
  std::set<std::string> trimmed_dups;
  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(9, 99), &trimmed, &trimmed_dups, &write_from_dups);

  EXPECT_EQ(eversion_t::max(), write_from_dups);
  EXPECT_EQ(6u, log.log.size());
  EXPECT_EQ(0u, trimmed.size());
  EXPECT_EQ(0u, log.dups.size());
  EXPECT_EQ(0u, trimmed_dups.size());
}

TEST_F(PGLogTrimTest, TestTrimAll)
{
  SetUp(1, 2, 20);
  PGLog::IndexedLog log;
  EXPECT_EQ(0u, log.dup_index.size()); // Sanity check
  log.head = mk_evt(24, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(19, 160), mk_evt(25, 152)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166)));

  std::set<eversion_t> trimmed;
  std::set<std::string> trimmed_dups;
  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(22, 180), &trimmed, &trimmed_dups, &write_from_dups);

  EXPECT_EQ(eversion_t(15, 150), write_from_dups);
  EXPECT_EQ(0u, log.log.size());
  EXPECT_EQ(6u, trimmed.size());
  EXPECT_EQ(5u, log.dups.size());
  EXPECT_EQ(0u, trimmed_dups.size());
  EXPECT_EQ(0u, log.dup_index.size()); // dup_index entry should be trimmed
}


TEST_F(PGLogTrimTest, TestGetRequest) {
  SetUp(1, 2, 20);
  PGLog::IndexedLog log;
  log.head = mk_evt(20, 0);
  log.skip_can_rollback_to_to_head();
  log.head = mk_evt(9, 0);

  entity_name_t client = entity_name_t::CLIENT(777);

  log.add(mk_ple_mod(mk_obj(1), mk_evt(10, 100), mk_evt(8, 70),
		     osd_reqid_t(client, 8, 1)));
  log.add(mk_ple_dt(mk_obj(2), mk_evt(15, 150), mk_evt(10, 100),
		    osd_reqid_t(client, 8, 2)));
  log.add(mk_ple_mod_rb(mk_obj(3), mk_evt(15, 155), mk_evt(15, 150),
			osd_reqid_t(client, 8, 3)));
  log.add(mk_ple_mod(mk_obj(1), mk_evt(20, 160), mk_evt(25, 152),
		     osd_reqid_t(client, 8, 4)));
  log.add(mk_ple_mod(mk_obj(4), mk_evt(21, 165), mk_evt(26, 160),
		     osd_reqid_t(client, 8, 5)));
  log.add(mk_ple_dt_rb(mk_obj(5), mk_evt(21, 167), mk_evt(31, 166),
		       osd_reqid_t(client, 8, 6)));

  eversion_t write_from_dups = eversion_t::max();

  log.trim(cct, mk_evt(19, 157), nullptr, nullptr, &write_from_dups);

  EXPECT_EQ(eversion_t(15, 150), write_from_dups);
  EXPECT_EQ(3u, log.log.size());
  EXPECT_EQ(2u, log.dups.size());

  eversion_t version;
  version_t user_version;
  int return_code;

  osd_reqid_t log_reqid = osd_reqid_t(client, 8, 5);
  osd_reqid_t dup_reqid = osd_reqid_t(client, 8, 3);
  osd_reqid_t bad_reqid = osd_reqid_t(client, 8, 1);

  bool result;

  result = log.get_request(log_reqid, &version, &user_version, &return_code);
  EXPECT_EQ(true, result);
  EXPECT_EQ(mk_evt(21, 165), version);

  result = log.get_request(dup_reqid, &version, &user_version, &return_code);
  EXPECT_EQ(true, result);
  EXPECT_EQ(mk_evt(15, 155), version);

  result = log.get_request(bad_reqid, &version, &user_version, &return_code);
  EXPECT_FALSE(result);
}

TEST_F(PGLogTest, _merge_object_divergent_entries) {
  {
    // Test for issue 20843
    clear();
    hobject_t hoid(object_t(/*name*/"notify.7"),
                   /*key*/string(""),
                   /*snap*/7,
                   /*hash*/77,
                   /*pool*/5,
                   /*nspace*/string(""));
    mempool::osd_pglog::list<pg_log_entry_t> orig_entries;
    orig_entries.push_back(mk_ple_mod(hoid, eversion_t(8336, 957), eversion_t(8336, 952)));
    orig_entries.push_back(mk_ple_err(hoid, eversion_t(8336, 958)));
    orig_entries.push_back(mk_ple_err(hoid, eversion_t(8336, 959)));
    orig_entries.push_back(mk_ple_mod(hoid, eversion_t(8336, 960), eversion_t(8336, 957)));
    log.add(mk_ple_mod(hoid, eversion_t(8973, 1075), eversion_t(8971, 1070)));
    missing.add(hoid,
                /*need*/eversion_t(8971, 1070),
                /*have*/eversion_t(8336, 952),
                false);
    pg_info_t oinfo;
    LogHandler rollbacker;
    _merge_object_divergent_entries(log, hoid,
                                    orig_entries, oinfo,
                                    log.get_can_rollback_to(),
                                    log.get_can_rollback_to(),
                                    missing, &rollbacker,
                                    this);
    // No core dump
  }
  {
    // skip leading error entries
    clear();
    hobject_t hoid(object_t(/*name*/"notify.7"),
                   /*key*/string(""),
                   /*snap*/7,
                   /*hash*/77,
                   /*pool*/5,
                   /*nspace*/string(""));
    mempool::osd_pglog::list<pg_log_entry_t> orig_entries;
    orig_entries.push_back(mk_ple_err(hoid, eversion_t(8336, 956)));
    orig_entries.push_back(mk_ple_mod(hoid, eversion_t(8336, 957), eversion_t(8336, 952)));
    log.add(mk_ple_mod(hoid, eversion_t(8973, 1075), eversion_t(8971, 1070)));
    missing.add(hoid,
                /*need*/eversion_t(8971, 1070),
                /*have*/eversion_t(8336, 952),
                false);
    pg_info_t oinfo;
    LogHandler rollbacker;
    _merge_object_divergent_entries(log, hoid,
                                    orig_entries, oinfo,
                                    log.get_can_rollback_to(),
                                    log.get_can_rollback_to(),
                                    missing, &rollbacker,
                                    this);
    // No core dump
  }
}

TEST(eversion_t, get_key_name) {
  eversion_t a(1234, 5678);
  std::string a_key_name = a.get_key_name();
  EXPECT_EQ("0000001234.00000000000000005678", a_key_name);
}

TEST(pg_log_dup_t, get_key_name) {
  pg_log_dup_t a(eversion_t(1234, 5678),
		 13,
		 osd_reqid_t(entity_name_t::CLIENT(777), 8, 999),
		 15);
  std::string a_key_name = a.get_key_name();
  EXPECT_EQ("dup_0000001234.00000000000000005678", a_key_name);
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_pglog ; ./unittest_pglog --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* "
// End:
