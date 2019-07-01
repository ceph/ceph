// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "rgw_rest.h"

class RGWRESTMgr_PubSub_S3 : public RGWLinkedRESTMgr {
protected:
  virtual RGWHandler_REST *_get_handler(req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
public:
  RGWRESTMgr_PubSub_S3(RGWRESTMgr* _next) : RGWLinkedRESTMgr(_next) {}

};

