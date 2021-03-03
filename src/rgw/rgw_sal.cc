// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
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

#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

extern "C" {
extern rgw::sal::RGWStore* newRGWStore(void);
}

rgw::sal::RGWStore *RGWStoreManager::init_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache)
{
  rgw::sal::RGWStore *store = nullptr;
  if (svc.compare("rados") == 0) {
    store = newRGWStore();
    RGWRados *rados = static_cast<rgw::sal::RGWRadosStore *>(store)->getRados();

    if ((*rados).set_use_cache(use_cache)
                .set_run_gc_thread(use_gc_thread)
                .set_run_lc_thread(use_lc_thread)
                .set_run_quota_threads(quota_threads)
                .set_run_sync_thread(run_sync_thread)
                .set_run_reshard_thread(run_reshard_thread)
                .initialize(cct, dpp) < 0) {
      delete store; store = nullptr;
    }
  }

  return store;
}

rgw::sal::RGWStore *RGWStoreManager::init_raw_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct, const std::string svc)
{
  rgw::sal::RGWStore *store = nullptr;
  if (svc.compare("rados") == 0) {
    store = newRGWStore();
    RGWRados *rados = static_cast<rgw::sal::RGWRadosStore *>(store)->getRados();

    rados->set_context(cct);

    int ret = rados->init_svc(true, dpp);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
      delete store; store = nullptr;
      return store;
    }

    if (rados->init_rados() < 0) {
      delete store; store = nullptr;
    }
  }

  return store;
}

void RGWStoreManager::close_storage(rgw::sal::RGWStore *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
