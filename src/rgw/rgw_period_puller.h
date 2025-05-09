// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_period_history.h"
#include "include/common_fwd.h"
#include "rgw/services/svc_sys_obj.h"

class RGWPeriod;

class RGWPeriodPuller : public RGWPeriodHistory::Puller {
  CephContext *cct;

  struct {
    RGWSI_Zone *zone;
    RGWSI_SysObj *sysobj;
  } svc;

 public:
  explicit RGWPeriodPuller(RGWSI_Zone *zone_svc, RGWSI_SysObj *sysobj_svc);

  int pull(const DoutPrefixProvider *dpp, const std::string& period_id, RGWPeriod& period, optional_yield y) override;
};
