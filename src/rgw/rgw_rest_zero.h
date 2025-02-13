// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include <memory>
#include "rgw_rest.h"

namespace rgw {

class ZeroResource;

// a rest endpoint that's only useful for benchmarking the http frontend.
// requests are not authenticated, and do no reads/writes to the backend
class RESTMgr_Zero : public RGWRESTMgr {
  std::unique_ptr<ZeroResource> resource;
 public:
  RESTMgr_Zero();
  RGWHandler_REST* get_handler(sal::Driver* driver, req_state* s,
                               const auth::StrategyRegistry& auth,
                               const std::string& prefix) override;
};

} // namespace rgw
