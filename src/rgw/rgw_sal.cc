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
#include <dlfcn.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_d3n_datacache.h"

#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#endif

#ifdef WITH_RADOSGW_MOTR
#include "rgw_sal_motr.h"
#endif


RGWObjState::RGWObjState() {
}

RGWObjState::~RGWObjState() {
}

RGWObjState::RGWObjState(const RGWObjState& rhs) : obj (rhs.obj) {
  is_atomic = rhs.is_atomic;
  has_attrs = rhs.has_attrs;
  exists = rhs.exists;
  size = rhs.size;
  accounted_size = rhs.accounted_size;
  mtime = rhs.mtime;
  epoch = rhs.epoch;
  if (rhs.obj_tag.length()) {
    obj_tag = rhs.obj_tag;
  }
  if (rhs.tail_tag.length()) {
    tail_tag = rhs.tail_tag;
  }
  write_tag = rhs.write_tag;
  fake_tag = rhs.fake_tag;
  shadow_obj = rhs.shadow_obj;
  has_data = rhs.has_data;
  if (rhs.data.length()) {
    data = rhs.data;
  }
  prefetch_data = rhs.prefetch_data;
  keep_tail = rhs.keep_tail;
  is_olh = rhs.is_olh;
  objv_tracker = rhs.objv_tracker;
  pg_ver = rhs.pg_ver;
  compressed = rhs.compressed;
}

rgw::sal::Store* StoreManager::init_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache, bool use_gc)
{
  const char *dlname = "/usr/lib64/ceph/librgw_sal_rados.so";
  rgw::sal::Store* store = nullptr;
  void *dl = nullptr;
  rgw::sal::Store *(*newStore)(const DoutPrefixProvider *, CephContext *, bool, bool, bool, bool, bool, bool, bool, bool) = nullptr;
  if (svc.compare("d3n") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_d3n.so";
  }
#ifdef WITH_RADOSGW_DBSTORE
  else if (svc.compare("dbstore") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_dbstore.so";
  }
#endif
#ifdef WITH_RADOSGW_MOTR
  else if (svc.compare("motr") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_motr.so";
  }
#endif
  dl = dlopen(dlname, RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
  if (dl) {
    newStore = (rgw::sal::Store* (*)(const DoutPrefixProvider *, CephContext *, bool, bool, bool, bool, bool, bool, bool, bool))dlsym(dl, "new_Store");
    if (newStore)
      store = newStore(dpp, cct, false, use_gc_thread, use_lc_thread, quota_threads, run_sync_thread, run_reshard_thread, use_cache, use_gc);
  }
  if (dlclose(dl) < 0)
    ldpp_dout(dpp, 0) << "WARNING: dlclose() failed" << dendl;
  return store;
}

rgw::sal::Store* StoreManager::init_raw_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc)
{
  const char *dlname = "/usr/lib64/ceph/librgw_sal_rados.so";
  rgw::sal::Store* store = nullptr;
  void *dl = nullptr;
  rgw::sal::Store *(*newStore)(const DoutPrefixProvider *, CephContext *, bool, bool, bool, bool, bool, bool, bool, bool) = nullptr;
  if (svc.compare("d3n") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_d3n.so";
  }
#ifdef WITH_RADOSGW_DBSTORE
  else if (svc.compare("dbstore") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_dbstore.so";
  }
#endif
#ifdef WITH_RADOSGW_MOTR
  else if (svc.compare("motr") == 0) {
    dlname = "/usr/lib64/ceph/librgw_sal_motr.so";
  }
#endif
  dl = dlopen(dlname, RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
  if (dl) {
    newStore = (rgw::sal::Store* (*)(const DoutPrefixProvider *, CephContext *, bool, bool, bool, bool, bool, bool, bool, bool))dlsym(dl, "new_Store");
    if (newStore)
      store = newStore(dpp, cct, true, false, false, false, false, false, false, false);
  }
  if (dlclose(dl) < 0)
    ldpp_dout(dpp, 0) << "WARNING: dlclose() failed" << dendl;
  return store;
}

void StoreManager::close_storage(rgw::sal::Store* store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}

namespace rgw::sal {
int Object::range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end)
{
  if (ofs < 0) {
    ofs += obj_size;
    if (ofs < 0)
      ofs = 0;
    end = obj_size - 1;
  } else if (end < 0) {
    end = obj_size - 1;
  }

  if (obj_size > 0) {
    if (ofs >= (off_t)obj_size) {
      return -ERANGE;
    }
    if (end >= (off_t)obj_size) {
      end = obj_size - 1;
    }
  }
  return 0;
}
}
