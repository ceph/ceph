// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

/* GET /admin/features -- what this deployment implements.
 *
 * Named features rather than capabilities:  these are properties of the
 * driver, and "capability" is taken in this codebase by RGWUserCaps,
 * which is a permission model -- the very one this resource checks, since
 * it requires info=read exactly as /admin/info does.
 *
 * The answer comes from sal::Driver::get_features(), so every backend
 * can contribute and a client needs one query rather than one per driver.
 * See that method for the rules:  open namespace, added on demand, and
 * absent means unknown rather than false. */
class RGWHandler_Features : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() override;
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_Features() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_Features : public RGWRESTMgr {
public:
  RGWRESTMgr_Features() = default;
  ~RGWRESTMgr_Features() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string&) override {
    return new RGWHandler_Features(auth_registry);
  }
};
