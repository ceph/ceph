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
  rgw::sal::MotrStore* store = new rgw::sal::MotrStore(cct);
  DB *db = nullptr;

  if (store) {
    db = store->getDB();
    db->set_context(cct);

    /* XXX: temporary - create testid user */
    rgw_user testid_user("", "testid", "");
    std::unique_ptr<rgw::sal::User> user = store->get_user(testid_user);
    user->get_info().display_name = "M. Tester";
    user->get_info().user_email = "tester@ceph.com";
    RGWAccessKey k1("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
    user->get_info().access_keys["0555b35654ad1656d804"] = k1;
    int r = user->store_user(dpp, null_yield, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed inserting testid user in dbstore error r=" << r << dendl;
    }
  }

  return store;
}

int initiateStorageProvider(void *st, const void *in_dpp, void *in_cct)
{
    rgw::sal::DBStore *store = static_cast<rgw::sal::DBStore *>(st);
    ceph::common::CephContext *cct = static_cast<ceph::common::CephContext *>(in_cct);
    int ret = 0;
    DBStoreManager *dbsm = new DBStoreManager(cct);
    if (dbsm) {
      DB *db = dbsm->getDB();
      if (!db) {
        delete dbsm;
      } else {
        store->setDBStoreManager(dbsm);
        store->setDB(db);
        db->set_context(cct);
        db->set_store((rgw::sal::Store*)store);
        ret = 1;
      }
    }

    if (!ret) {
      ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    }

    return ret;
}

}
