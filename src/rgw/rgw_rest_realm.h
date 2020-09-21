// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_rest.h"

class RGWRESTMgr_Realm : public RGWRESTMgr {
public:
  RGWRESTMgr_Realm();

  RGWHandler_REST* get_handler(rgw::sal::RGWRadosStore *store,
			       struct req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override;
};
