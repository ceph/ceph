// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest_realm.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_config.h"

#define dout_subsys ceph_subsys_rgw

// base period op, shared between Get and Post
class RGWOp_Period_Base : public RGWRESTOp {
 protected:
  RGWPeriod period;
 public:
  int verify_permission() override { return 0; }
  void send_response() override;
};

// reply with the period object on success
void RGWOp_Period_Base::send_response()
{
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("period", period, s->formatter);
  flusher.flush();
}

// GET /admin/realm/period
class RGWOp_Period_Get : public RGWOp_Period_Base {
 public:
  void execute() override;
  const string name() override { return "get_period"; }
};

void RGWOp_Period_Get::execute()
{
  string realm_id, realm_name, period_id;
  epoch_t epoch = 0;
  RESTArgs::get_string(s, "realm_id", realm_id, &realm_id);
  RESTArgs::get_string(s, "realm_name", realm_name, &realm_name);
  RESTArgs::get_string(s, "period_id", period_id, &period_id);
  RESTArgs::get_uint32(s, "epoch", 0, &epoch);

  period.set_id(period_id);
  period.set_epoch(epoch);

  http_ret = period.init(store->ctx(), store, realm_id, realm_name);
  if (http_ret < 0)
    ldout(store->ctx(), 5) << "failed to read period" << dendl;
}

// POST /admin/realm/period
class RGWOp_Period_Post : public RGWOp_Period_Base {
 public:
  void execute() override;
  const string name() override { return "post_period"; }
};

void RGWOp_Period_Post::execute()
{
  // initialize the period without reading from rados
  period.init(store->ctx(), store, false);

  // decode the period from input
#define PERIOD_INPUT_MAX_LEN 4096
  bool empty;
  http_ret = rgw_rest_get_json_input(store->ctx(), s, period,
                                     PERIOD_INPUT_MAX_LEN, &empty);
  if (http_ret < 0) {
    dout(5) << "failed to decode period" << dendl;
    return;
  }

  period.store_info(false);
}

class RGWHandler_Period : public RGWHandler_Auth_S3 {
 protected:
  RGWOp *op_get() override { return new RGWOp_Period_Get; }
  RGWOp *op_post() override { return new RGWOp_Period_Post; }
};

class RGWRESTMgr_Period : public RGWRESTMgr {
 public:
  RGWHandler* get_handler(struct req_state*) override {
    return new RGWHandler_Period;
  }
};

RGWRESTMgr_Realm::RGWRESTMgr_Realm()
{
  // add the /admin/realm/period resource
  register_resource("period", new RGWRESTMgr_Period);
}
