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
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"

#include "rgw_rados.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
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
  rgw::sal::RadosStore* store = new rgw::sal::RadosStore();
  if (store) {
    RGWRados* rados = new RGWRados();

    if (!rados) {
      delete store; store = nullptr;
    } else {
      store->setRados(rados);
      rados->set_store(store);
      rados->set_context(cct);

      if (raw) {
        if (rados->init_svc(true, dpp) < 0) {
          delete store;
          return nullptr;
        }
        if (rados->init_rados() < 0) {
          delete store;
          return nullptr;
        }
      } else {
        if ((*rados).set_use_cache(use_cache)
                    .set_use_datacache(false)
                    .set_run_gc_thread(use_gc_thread)
                    .set_run_lc_thread(use_lc_thread)
                    .set_run_quota_threads(quota_threads)
                    .set_run_sync_thread(run_sync_thread)
                    .set_run_reshard_thread(run_reshard_thread)
                    .init_begin(cct, dpp) < 0) {
          delete store;
          return nullptr;
        }
        if (store->initialize(cct, dpp) < 0) {
          delete store;
          return nullptr;
        }
      }
    }
  }

  return store;
}

int initiateStorageProvider(void *st, const void *in_dpp, void *in_cct)
{
    rgw::sal::RadosStore *store = static_cast<rgw::sal::RadosStore *>(st);
    ceph::common::CephContext *cct = static_cast<ceph::common::CephContext *>(in_cct);
    const DoutPrefixProvider *dpp = static_cast<const DoutPrefixProvider *>(in_dpp);
    RGWRados *rados = store->getRados();
    int ret = 0;

    rados->set_context(cct);

    if (rados->init_svc(false, dpp) < 0) {
      ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }

    return 1;
}

int initiateRawStorageProvider(void *st, const void *in_dpp, void *in_cct)
{
    rgw::sal::RadosStore *store = static_cast<rgw::sal::RadosStore *>(st);
    ceph::common::CephContext *cct = static_cast<ceph::common::CephContext *>(in_cct);
    const DoutPrefixProvider *dpp = static_cast<const DoutPrefixProvider *>(in_dpp);
    RGWRados *rados = store->getRados();
    int ret = 0;

    rados->set_context(cct);
    if (rados->init_svc(true, dpp) < 0) {
      ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }

    if (rados->init_rados() < 0) {
      ldout(cct, 0) << "ERROR: failed to init rados (ret=" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }

    return 1;
}

}
