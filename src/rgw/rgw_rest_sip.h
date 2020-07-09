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

#pragma once

#include "rgw_rest_s3.h"
#include "rgw_sync_info.h"

class RGWOp_SIP_GetInfo : public RGWRESTOp {
  string provider;
  SIProviderRef sip;
public:
  RGWOp_SIP_GetInfo(string&& _provider) : provider(std::move(_provider)) {}
  ~RGWOp_SIP_GetInfo() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("sip", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->get_caps());
  }
  void send_response() override;
  void execute() override;
  const char* name() const override {
    return "get_sip_info";
  }
};

class RGWOp_SIP_GetStageStatus : public RGWRESTOp {
  string provider;
  string start_marker;
  string cur_marker;
public:
  RGWOp_SIP_GetStageStatus(string&& _provider) : provider(std::move(_provider)) {}
  ~RGWOp_SIP_GetStageStatus() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("sip", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->get_caps());
  }
  void send_response() override;
  void execute() override;
  const char* name() const override {
    return "get_sip_info";
  }
};

class RGWOp_SIP_List : public RGWRESTOp {
  /* result */
  std::vector<std::string> providers;
public:
  RGWOp_SIP_List() {}
  ~RGWOp_SIP_List() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("sip", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->get_caps());
  }
  void send_response() override;
  void execute() override;
  const char* name() const override {
    return "list_sip";
  }
};

class RGWOp_SIP_Fetch : public RGWRESTOp {
  static constexpr int default_max = 1000;

  string provider;
  string instance;
  string stage_id;
  string marker;
  int max;

  SIProviderRef sip;
  SIProvider::fetch_result result;
public:
  RGWOp_SIP_Fetch(string&& _provider) : provider(std::move(_provider)) {}
  ~RGWOp_SIP_Fetch() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("sip", RGW_CAP_READ);
  }
  int verify_permission() override {
    return check_caps(s->user->get_caps());
  }
  void send_response() override;
  void execute() override;
  const char* name() const override {
    return "sip_fetch";
  }
};

class RGWOp_SIP_Trim : public RGWRESTOp {
  string provider;
  string instance;
  string marker;
public:
  RGWOp_SIP_Trim(string&& _provider) : provider(std::move(_provider)) {}
  ~RGWOp_SIP_Trim() override {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("sip", RGW_CAP_WRITE);
  }
  void execute() override;
  const char* name() const override {
    return "sip_trim";
  }
};

class RGWHandler_SIP : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
  RGWOp *op_delete() override;

  int read_permissions(RGWOp*) override {
    return 0;
  }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_SIP() override = default;
};

class RGWRESTMgr_SIP : public RGWRESTMgr {
public:
  RGWRESTMgr_SIP() = default;
  ~RGWRESTMgr_SIP() override = default;

  RGWHandler_REST* get_handler(rgw::sal::RGWStore *store,
                               struct req_state* const,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefixs) override {
    return new RGWHandler_SIP(auth_registry);
  }
};
