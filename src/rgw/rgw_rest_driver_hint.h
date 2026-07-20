// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "common/config_proxy.h"

class RGWOp_Driver_Hint : public RGWRESTOp {
public:
  RGWOp_Driver_Hint() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("driver-hint", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    if (!s->cct->_conf.get_val<bool>("rgw_driver_debug_apis")) {
      op_ret = -EACCES;
      return;
    }

    std::string hint;
    RESTArgs::get_string(s, "hint", "", &hint);
    if (hint.empty()) {
      op_ret = -EINVAL;
      return;
    }

    std::map<std::string, std::string> params;
    const auto& args = s->info.args.get_params();
    for (const auto& [k, v] : args) {
      if (k != "hint") {
        params[k] = v;
      }
    }

    ldpp_dout(s, 10) << "driver_hint: hint=" << hint
      << " params=" << params.size() << dendl;
    op_ret = driver->driver_hint(s, hint, params);
    ldpp_dout(s, 10) << "driver_hint: hint=" << hint
      << " ret=" << op_ret << dendl;
  }

  const char* name() const override { return "driver_hint"; }
};

class RGWHandler_Driver_Hint : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_delete() override { return new RGWOp_Driver_Hint; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Driver_Hint() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_Driver_Hint : public RGWRESTMgr {
public:
  RGWRESTMgr_Driver_Hint() = default;
  ~RGWRESTMgr_Driver_Hint() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver*,
                               req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Driver_Hint(auth_registry);
  }
};
