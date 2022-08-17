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
#include "rgw_sal_dbstore.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

extern "C" {

void *new_Store(const DoutPrefixProvider *dpp,
                CephContext *cct,
                bool raw,
                bool use_gc_thread,
                bool use_lc_thread,
                bool quota_threads,
                bool run_sync_thread,
                bool run_reshard_thread,
                bool use_cache,
                bool use_gc)
{
  rgw::sal::DBStore* store = new rgw::sal::DBStore();
  DB *db = nullptr;

  if (store) {
    db = store->getDB();
    db->set_context(cct);

    if ((*(rgw::sal::DBStore*)store).set_run_lc_thread(use_lc_thread)
                                    .initialize(cct, dpp) < 0) {
      delete store;
      return nullptr;
    }
  }

  return store;
}

}
