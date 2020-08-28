// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_zone.h"

class RGWOp_ZoneGroupMap_Get : public RGWRESTOp {
  RGWZoneGroupMap zonegroup_map;
  bool old_format;
public:
  explicit RGWOp_ZoneGroupMap_Get(bool _old_format):old_format(_old_format) {}
  ~RGWOp_ZoneGroupMap_Get() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override;
  void send_response(const Span& parent_span = nullptr) override;
  const char* name() const override {
    if (old_format) {
      return "get_region_map";
    } else {
      return "get_zonegroup_map";
    }
  }
};

class RGWOp_ZoneConfig_Get : public RGWRESTOp {
  RGWZoneParams zone_params;
public:
  RGWOp_ZoneConfig_Get() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_READ);
  }
  int verify_permission(const Span& parent_span = nullptr) override {
    return check_caps(s->user->get_caps());
  }
  void execute(const Span& parent_span = nullptr) override {} /* store already has the info we need, just need to send response */
  void send_response(const Span& parent_span = nullptr) override ;
  const char* name() const override {
    return "get_zone_config";
  }
};

class RGWHandler_Config : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;

  int read_permissions(RGWOp*, const Span& parent_span = nullptr) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Config() override = default;
};


class RGWRESTMgr_Config : public RGWRESTMgr {
public:
  RGWRESTMgr_Config() = default;
  ~RGWRESTMgr_Config() override = default;

  RGWHandler_REST* get_handler(rgw::sal::RGWRadosStore *,
			       struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Config(auth_registry);
  }
};
