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
#include <gtest/gtest.h>
#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "common/ceph_argparse.h"
#include "msg/Messenger.h"
#include "test/unit.h"

class TestOSDScrub: public OSD {

public:
  TestOSDScrub(CephContext *cct_,
      ObjectStore *store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev) :
      OSD(cct_, store_, id, internal, external, hb_client, hb_front_server, hb_back_server, osdc_messenger, mc, dev, jdev)
  {
  }

  bool scrub_time_permit(utime_t now) {
    return OSD::scrub_time_permit(now);
  }
};

TEST(TestOSDScrub, scrub_time_permit) {
  ObjectStore *store = ObjectStore::create(g_ceph_context,
             g_conf->osd_objectstore,
             g_conf->osd_data,
             g_conf->osd_journal);
  Messenger *ms = Messenger::create(g_ceph_context, g_conf->ms_type,
             entity_name_t::OSD(0), "make_checker",
             getpid());
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_default_policy(Messenger::Policy::stateless_server(0, 0));
  ms->bind(g_conf->public_addr);
  MonClient mc(g_ceph_context);
  mc.build_initial_monmap();
  TestOSDScrub* osd = new TestOSDScrub(g_ceph_context, store, 0, ms, ms, ms, ms, ms, ms, &mc, "", "");

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "0");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "24");
  g_ceph_context->_conf->apply_changes(NULL);
  tm tm;
  strptime("2015-01-16 12:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  utime_t now = utime_t(mktime(&tm), 0);
  bool ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "24");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "0");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 12:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "0");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "0");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 12:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 01:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 08:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 00:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf->set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf->set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf->apply_changes(NULL);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* "
// End:
