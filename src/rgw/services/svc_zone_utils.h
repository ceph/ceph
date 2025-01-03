// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_service.h"


class RGWSI_Zone;

class RGWSI_ZoneUtils : public RGWServiceInstance
{
  friend struct RGWServices_Def;

  librados::Rados* rados{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  std::string trans_id_suffix;

  void init(librados::Rados* rados_,
            RGWSI_Zone *_zone_svc) {
    rados = rados_;
    zone_svc = _zone_svc;
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

  void init_unique_trans_id_deps();

public:
  RGWSI_ZoneUtils(CephContext *cct): RGWServiceInstance(cct) {}

  std::string gen_host_id();
  std::string unique_id(uint64_t unique_num);

  std::string unique_trans_id(const uint64_t unique_num);
};
