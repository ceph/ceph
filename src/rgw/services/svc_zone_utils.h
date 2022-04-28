// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class RGWSI_ZoneUtils : public RGWServiceInstance
{
public:
  RGWSI_ZoneUtils(CephContext *cct): RGWServiceInstance(cct) {}

  virtual std::string gen_host_id() = 0;
  virtual std::string unique_id(uint64_t unique_num) = 0;

  virtual std::string unique_trans_id(const uint64_t unique_num) = 0;
};
