// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"

#include "services/svc_zone.h"

class RGWSI_RADOS;

class RGWSI_Zone_RADOS : public RGWSI_Zone
{
  friend struct RGWServices_Def;

  RGWSI_RADOS *rados_svc{nullptr};

  void init(RGWSI_SysObj *_sysobj_svc,
            RGWSI_RADOS *_rados_svc,
	    RGWSI_SyncModules *_sync_modules_svc,
	    RGWSI_Bucket_Sync *_bucket_sync_svc);

protected:
  int do_start_backend(optional_yield y, const DoutPrefixProvider *dpp) override;
  int lookup_placement(const DoutPrefixProvider *dpp,
                       const rgw_pool& pool) override;
  int create_placement(const DoutPrefixProvider *dpp,
                       const rgw_pool& pool) override;

public:
  RGWSI_Zone_RADOS(CephContext *cct);
  ~RGWSI_Zone_RADOS() {}
};
