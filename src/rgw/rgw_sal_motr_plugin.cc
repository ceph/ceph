// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_motr.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "driver/rados/rgw_service.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "cls/rgw/cls_rgw_client.h"

#include "driver/rados/rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

extern "C" {

void *new_Driver(const DoutPrefixProvider *dpp,
                 CephContext *cct,
                 bool raw,
                 boost::asio::io_context& io_context,
                 const rgw::SiteConfig& site_config,
                 bool use_gc_thread,
                 bool use_lc_thread,
                 bool quota_threads,
                 bool run_sync_thread,
                 bool run_reshard_thread,
                 bool run_notification_thread,
                 bool use_cache,
                 bool use_gc,
                 optional_yield opt_yield)
{
  g_ceph_context = cct;

  rgw::sal::MotrStore* driver = new rgw::sal::MotrStore(cct);

  if (driver) {
    driver->init_metadata_cache(dpp, cct);

    /* XXX: temporary - create testid user */
    rgw_user testid_user("tenant", "tester", "ns");
    std::unique_ptr<rgw::sal::User> user = driver->get_user(testid_user);
    user->get_info().user_id = testid_user;
    user->get_info().display_name = "Motr Explorer";
    user->get_info().user_email = "tester@seagate.com";
    RGWAccessKey k1("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
    user->get_info().access_keys["0555b35654ad1656d804"] = k1;

    ldpp_dout(dpp, 20) << "Store testid and user for Motr. User = " << user->get_info().user_id.id << dendl;
    int r = user->store_user(dpp, null_yield, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed inserting testid user in dbstore error r=" << r << dendl;
    }
    // Read user info and compare.
    rgw_user ruser("", "tester", "");
    std::unique_ptr<rgw::sal::User> suser = driver->get_user(ruser);
    suser->get_info().user_id = ruser;
    auto rc = suser->load_user(dpp, null_yield);
    if (rc != 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load testid user from Motr: rc=" << rc << dendl;
    } else {
      ldpp_dout(dpp, 20) << "Read and compare user info: " << dendl;
      ldpp_dout(dpp, 20) << "User id = " << suser->get_info().user_id.id << dendl;
      ldpp_dout(dpp, 20) << "User display name = " << suser->get_info().display_name << dendl;
      ldpp_dout(dpp, 20) << "User email = " << suser->get_info().user_email << dendl;
    }
  }

  return driver;
}

}
