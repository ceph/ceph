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
#include "common/async/context_pool.h"
#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "common/ceph_argparse.h"
#include "msg/Messenger.h"

class TestOSDScrub: public OSD {

public:
  TestOSDScrub(CephContext *cct_,
      std::unique_ptr<ObjectStore> store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_front_client,
      Messenger *hb_back_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev,
      ceph::async::io_context_pool& ictx) :
      OSD(cct_, std::move(store_), id, internal, external,
	  hb_front_client, hb_back_client,
	  hb_front_server, hb_back_server,
	  osdc_messenger, mc, dev, jdev, ictx)
  {
  }

  bool scrub_time_permit(utime_t now) {
    return service.get_scrub_services().scrub_time_permit(now);
  }
};

TEST(TestOSDScrub, scrub_time_permit) {
  ceph::async::io_context_pool icp(1);
  std::unique_ptr<ObjectStore> store = ObjectStore::create(g_ceph_context,
             g_conf()->osd_objectstore,
             g_conf()->osd_data,
             g_conf()->osd_journal);
  std::string cluster_msgr_type = g_conf()->ms_cluster_type.empty() ? g_conf().get_val<std::string>("ms_type") : g_conf()->ms_cluster_type;
  Messenger *ms = Messenger::create(g_ceph_context, cluster_msgr_type,
				    entity_name_t::OSD(0), "make_checker",
				    getpid());
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_default_policy(Messenger::Policy::stateless_server(0));
  ms->bind(g_conf()->public_addr);
  MonClient mc(g_ceph_context, icp);
  mc.build_initial_monmap();
  TestOSDScrub* osd = new TestOSDScrub(g_ceph_context, std::move(store), 0, ms, ms, ms, ms, ms, ms, ms, &mc, "", "", icp);

  // These are now invalid
  int err = g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "24");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_begin_hour = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_begin_hour");

  err = g_ceph_context->_conf.set_val("osd_scrub_end_hour", "24");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_end_hour = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_end_hour");

  err = g_ceph_context->_conf.set_val("osd_scrub_begin_week_day", "7");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_begin_week_day = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_begin_week_day");

  err = g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "7");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_end_week_day = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_end_week_day");

  // Test all day
  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "0");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "0");
  g_ceph_context->_conf.apply_changes(nullptr);
  tm tm;
  tm.tm_isdst = -1;
  strptime("2015-01-16 12:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  utime_t now = utime_t(mktime(&tm), 0);
  bool ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 01:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 08:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 00:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Sun = 0, Mon = 1, Tue = 2, Wed = 3, Thu = 4m, Fri = 5, Sat = 6
  // Jan 16, 2015 is a Friday (5)
  // every day
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "0"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "0"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // test Sun - Thu
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "0"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "5"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  // test Fri - Sat
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "5"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "0"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Jan 14, 2015 is a Wednesday (3)
  // test Tue - Fri
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "2"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "6"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-14 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Test Sat - Sun
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "6"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "1"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-14 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);
  mc.shutdown();
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* "
// End:
