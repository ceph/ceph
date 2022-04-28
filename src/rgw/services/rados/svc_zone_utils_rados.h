// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "svc_zone_utils.h"


class RGWSI_RADOS;
class RGWSI_Zone;

class RGWSI_ZoneUtils_RADOS : public RGWSI_ZoneUtils
{
  friend struct RGWServices_Def;

  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_Zone *zone_svc{nullptr};

  std::string trans_id_suffix;

  void init(RGWSI_RADOS *_rados_svc,
            RGWSI_Zone *_zone_svc) {
    rados_svc = _rados_svc;
    zone_svc = _zone_svc;
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

  void init_unique_trans_id_deps();

public:
  RGWSI_ZoneUtils_RADOS(CephContext *cct): RGWSI_ZoneUtils(cct) {}

  std::string gen_host_id() override;
  std::string unique_id(uint64_t unique_num) override;

  std::string unique_trans_id(const uint64_t unique_num) override;
};
