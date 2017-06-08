// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REST_MON_COMMAND_H
#define RGW_REST_MON_COMMAND_H

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

class RGWRESTMgr_MonCommand : public RGWRESTMgr {
public:
  RGWHandler_REST* get_handler(struct req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};

#endif /*!RGW_REST_MON_COMMAND_H*/
