// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_rados.h"
#include "svc_zone_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

RGWSI_Zone_RADOS::RGWSI_Zone_RADOS(CephContext *cct) : RGWSI_Zone(cct)
{
}

void RGWSI_Zone_RADOS::init(RGWSI_SysObj *_sysobj_svc,
                            RGWSI_RADOS *_rados_svc,
                            RGWSI_SyncModules * _sync_modules_svc,
                            RGWSI_Bucket_Sync *_bucket_sync_svc)
{
  rados_svc = _rados_svc;

  RGWSI_Zone::init_base(_sysobj_svc,
                   _sync_modules_svc,
                   _bucket_sync_svc);
}

int RGWSI_Zone_RADOS::do_start_backend(optional_yield y, const DoutPrefixProvider *dpp)
{
  return rados_svc->start(y, dpp);
}

int RGWSI_Zone_RADOS::lookup_placement(const DoutPrefixProvider *dpp,
                                       const rgw_pool& pool)
{
  int ret = rados_svc->pool(pool).lookup();
  if (ret < 0) { // DNE, or something
    return ret;
  }

  return 0;
}

int RGWSI_Zone_RADOS::create_placement(const DoutPrefixProvider *dpp,
                                       const rgw_pool& pool)
{
  vector<rgw_pool> pools;
  pools.push_back(pool);
  vector<int> retcodes;
  int ret = rados_svc->pool().create(dpp, pools, &retcodes);
  if (ret < 0)
    return ret;

  return 0;
}

