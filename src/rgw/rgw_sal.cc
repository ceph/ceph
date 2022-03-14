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
#include "rgw_d3n_datacache.h"

#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#endif

#ifdef WITH_RADOSGW_MOTR
#include "rgw_sal_motr.h"
#endif


#define dout_subsys ceph_subsys_rgw

extern "C" {
extern rgw::sal::Store* newStore(void);
#ifdef WITH_RADOSGW_DBSTORE
extern rgw::sal::Store* newDBStore(CephContext *cct);
#endif
#ifdef WITH_RADOSGW_MOTR
extern rgw::sal::Store* newMotrStore(CephContext *cct);
#endif
}

rgw::sal::Store* StoreManager::init_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache, bool use_gc)
{
  if (svc.compare("rados") == 0) {
    rgw::sal::Store* store = newStore();
    RGWRados* rados = static_cast<rgw::sal::RadosStore* >(store)->getRados();

    if ((*rados).set_use_cache(use_cache)
                .set_use_datacache(false)
                .set_use_gc(use_gc)
                .set_run_gc_thread(use_gc_thread)
                .set_run_lc_thread(use_lc_thread)
                .set_run_quota_threads(quota_threads)
                .set_run_sync_thread(run_sync_thread)
                .set_run_reshard_thread(run_reshard_thread)
                .initialize(cct, dpp) < 0) {
      delete store; store = nullptr;
    }
    return store;
  }
  else if (svc.compare("d3n") == 0) {
    rgw::sal::RadosStore *store = new rgw::sal::RadosStore();
    RGWRados* rados = new D3nRGWDataCache<RGWRados>;
    store->setRados(rados);
    rados->set_store(static_cast<rgw::sal::RadosStore* >(store));

    if ((*rados).set_use_cache(use_cache)
                .set_use_datacache(true)
                .set_run_gc_thread(use_gc_thread)
                .set_run_lc_thread(use_lc_thread)
                .set_run_quota_threads(quota_threads)
                .set_run_sync_thread(run_sync_thread)
                .set_run_reshard_thread(run_reshard_thread)
                .initialize(cct, dpp) < 0) {
      delete store; store = nullptr;
    }
    return store;
  }

#ifdef WITH_RADOSGW_DBSTORE
  if (svc.compare("dbstore") == 0) {
    rgw::sal::Store* store = newDBStore(cct);

    if ((*(rgw::sal::DBStore*)store).set_run_lc_thread(use_lc_thread)
                                    .initialize(cct, dpp) < 0) {
      delete store; store = nullptr;
    }

    /* XXX: temporary - create testid user */
    rgw_user testid_user("", "testid", "");
    std::unique_ptr<rgw::sal::User> user = store->get_user(testid_user);
    user->get_info().display_name = "M. Tester";
    user->get_info().user_email = "tester@ceph.com";
    RGWAccessKey k1("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
    user->get_info().access_keys["0555b35654ad1656d804"] = k1;
    user->get_info().max_buckets = RGW_DEFAULT_MAX_BUCKETS;

    int r = user->store_user(dpp, null_yield, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed inserting testid user in dbstore error r=" << r << dendl;
    }
    return store;
  }
#endif

#ifdef WITH_RADOSGW_MOTR
  if (svc.compare("motr") == 0) {
    rgw::sal::Store* store = newMotrStore(cct);
    if (store == nullptr) {
      ldpp_dout(dpp, 0) << "newMotrStore() failed!" << dendl;
      return store;
    }
    ((rgw::sal::MotrStore *)store)->init_metadata_cache(dpp, cct, use_cache);

    return store;
  }
#endif

  return nullptr;
}

rgw::sal::Store* StoreManager::init_raw_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc)
{
  rgw::sal::Store* store = nullptr;
  if (svc.compare("rados") == 0) {
    store = newStore();
    RGWRados* rados = static_cast<rgw::sal::RadosStore* >(store)->getRados();

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

  if (svc.compare("dbstore") == 0) {
#ifdef WITH_RADOSGW_DBSTORE
    store = newDBStore(cct);
#else
    store = nullptr;
#endif
  }

  if (svc.compare("motr") == 0) {
#ifdef WITH_RADOSGW_MOTR
    store = newMotrStore(cct);
#else
    store = nullptr;
#endif
  }
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
