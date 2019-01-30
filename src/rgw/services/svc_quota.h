// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class RGWSI_Quota : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};

public:
  RGWSI_Quota(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}

  void init(RGWSI_Zone *_zone_svc) {
    zone_svc = _zone_svc;
  }

  const RGWQuotaInfo& get_bucket_quota() const;
  const RGWQuotaInfo& get_user_quota() const;
};
