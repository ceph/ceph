// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

RGWObject *RGWRadosBucket::create_object(const rgw_obj_key &key)
{
  if (!object) {
    object = new RGWRadosObject(store, key);
  }

  return object;
}

RGWUser *RGWRadosStore::get_user(const rgw_user &u)
{
  if (!user) {
    user = new RGWRadosUser(this, u);
  }

  return user;
}

RGWSalBucket *RGWRadosStore::create_bucket(RGWUser &u, const cls_user_bucket &b)
{
  if (!bucket) {
    bucket = new RGWRadosBucket(this, u, b);
  }

  return bucket;
}

void RGWRadosStore::finalize(void) {
  if (rados)
    rados->finalize();
}

} // namespace rgw::sal

rgw::sal::RGWRadosStore *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  if ((*rados).set_use_cache(use_cache)
              .set_run_gc_thread(use_gc_thread)
              .set_run_lc_thread(use_lc_thread)
              .set_run_quota_threads(quota_threads)
              .set_run_sync_thread(run_sync_thread)
              .set_run_reshard_thread(run_reshard_thread)
              .initialize(cct) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

rgw::sal::RGWRadosStore *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  rados->set_context(cct);

  int ret = rados->init_svc(true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    delete store;
    return nullptr;
  }

  if (rados->init_rados() < 0) {
    delete store;
    return nullptr;
  }

  return store;
}

void RGWStoreManager::close_storage(rgw::sal::RGWRadosStore *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
