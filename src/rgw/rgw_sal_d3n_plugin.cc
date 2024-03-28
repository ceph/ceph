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
#include "rgw_sal_rados.h"
#include "rgw_d3n_datacache.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"

#include "rgw_rados.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "driver/rados/rgw_service.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "cls/rgw/cls_rgw_client.h"

#include "driver/rados/rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

extern "C" {

void* new_Driver(const DoutPrefixProvider *dpp,
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

  rgw::sal::RadosStore* driver = new rgw::sal::RadosStore(io_context);

  if (driver) {
    RGWRados* rados = new D3nRGWDataCache<RGWRados>;

    if (!rados) {
      delete driver; driver = nullptr;
    } else {
      driver->setRados(rados);
      rados->set_store(driver);
      rados->set_context(cct);

      if ((*rados).set_use_cache(use_cache)
                  .set_use_datacache(true)
                  .set_run_gc_thread(use_gc_thread)
                  .set_run_lc_thread(use_lc_thread)
                  .set_run_quota_threads(quota_threads)
                  .set_run_sync_thread(run_sync_thread)
                  .set_run_reshard_thread(run_reshard_thread)
                  .set_run_notification_thread(run_notification_thread)
                  .init_begin(cct, dpp, site_config) < 0) {
        delete driver;
        return nullptr;
      }
      if (driver->initialize(cct, dpp) < 0) {
        delete driver;
        return nullptr;
      }
      if (rados->init_complete(dpp, opt_yield) < 0) {
        delete driver;
        return nullptr;
      }
    }
  }
  return driver;
}

}
