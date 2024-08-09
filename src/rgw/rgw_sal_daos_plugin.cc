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
#include "rgw_sal_daos.h"
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
                 rgw::SiteConfig& site_config,
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

  rgw::sal::DaosStore* driver = new rgw::sal::DaosStore(cct);

  if (driver->initialize(cct, dpp) < 0) {
    delete driver;
    return nullptr;
  }

  return driver;
}

}
