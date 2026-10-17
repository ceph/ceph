// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_rest.h"

/**
 * REST routes of the hipobj-rc-v2 control protocol:
 * POST /.hipobj-rc/{prepare,ready,cancel}.
 *
 * The routes authenticate like any S3 request (SigV4, every
 * x-amz-rdma-* header signed) and authorize like the S3 operation they
 * stand for: PREPARE and READY of a GET run the GetObject permission
 * checks against the session's target, a PUT the PutObject checks.
 * Object data never crosses HTTP; see rgw_rdma_rc_session.h.
 */
namespace rgw::rdma::rc {

class RESTMgr : public RGWRESTMgr {
 public:
  RGWHandler_REST* get_handler(rgw::sal::Driver* driver, req_state* s,
                               const rgw::auth::StrategyRegistry& auth,
                               const std::string& frontend_prefix) override;
};

} // namespace rgw::rdma::rc
