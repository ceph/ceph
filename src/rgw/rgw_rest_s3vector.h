// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "rgw_rest.h"

class RGWHandler_REST_s3Vector : public RGWHandler_REST {
  const rgw::auth::StrategyRegistry& auth_registry;
  bufferlist bl_post_body;
  RGWOp *op_post() override;

public:
  RGWHandler_REST_s3Vector(const rgw::auth::StrategyRegistry& auth_registry,
                           const bufferlist& bl_post_body)
    : auth_registry(auth_registry), bl_post_body(bl_post_body) {}
  ~RGWHandler_REST_s3Vector() override = default;

  int init(rgw::sal::Driver* driver, req_state *s, rgw::io::BasicClient *cio) override;
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override;
  int postauth_init(optional_yield y) override { return 0; }
  int read_permissions(RGWOp* op, optional_yield y) override { return 0; }
};
