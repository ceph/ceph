// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest_realm.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_config.h"

#define dout_subsys ceph_subsys_rgw

class RGWHandler_Period : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() { return new RGWOp_Period_Get; }
  RGWOp *op_post() { return new RGWOp_Period_Post; }
};

class RGWRESTMgr_Period : public RGWRESTMgr {
public:
  RGWHandler *get_handler(struct req_state *s) { return new RGWHandler_Period; }
};

RGWRESTMgr_Realm::RGWRESTMgr_Realm()
{
  // add the /admin/realm/period resource
  register_resource("period", new RGWRESTMgr_Period);
}
