// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_REALM_H
#define CEPH_RGW_REST_REALM_H

#include "rgw_rest.h"

class RGWRESTMgr_Realm : public RGWRESTMgr {
public:
  RGWRESTMgr_Realm();

  RGWHandler_REST* get_handler(struct req_state*,
                               const std::string&) override;
};

#endif
