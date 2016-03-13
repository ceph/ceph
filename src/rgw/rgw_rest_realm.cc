// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "rgw_rest_realm.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_config.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

// reject 'period push' if we would have to fetch too many intermediate periods
static const uint32_t PERIOD_HISTORY_FETCH_MAX = 64;

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
  auto cct = store->ctx();

  // initialize the period without reading from rados
  period.init(cct, store, false);

  // decode the period from input
#define PERIOD_MAX_LEN 4096
  bool empty;
  http_ret = rgw_rest_get_json_input(cct, s, period, PERIOD_MAX_LEN, &empty);
  if (http_ret < 0) {
    lderr(cct) << "failed to decode period" << dendl;
    return;
  }

  // require period.realm_id to match our realm
  if (period.get_realm() != store->realm.get_id()) {
    lderr(cct) << "period with realm id " << period.get_realm()
        << " doesn't match current realm " << store->realm.get_id() << dendl;
    http_ret = -EINVAL;
    return;
  }

  // load the realm and current period from rados; there may be a more recent
  // period that we haven't restarted with yet. we also don't want to modify
  // the objects in use by RGWRados
  RGWRealm realm(period.get_realm());
  http_ret = realm.init(cct, store);
  if (http_ret < 0) {
    lderr(cct) << "failed to read current realm: "
        << cpp_strerror(-http_ret) << dendl;
    return;
  }

  RGWPeriod current_period;
  http_ret = current_period.init(cct, store, realm.get_id());
  if (http_ret < 0) {
    lderr(cct) << "failed to read current period: "
        << cpp_strerror(-http_ret) << dendl;
    return;
  }

  // if period id is empty, handle as 'period commit'
  if (period.get_id().empty()) {
    http_ret = period.commit(realm, current_period);
    if (http_ret < 0) {
      lderr(cct) << "master zone failed to commit period" << dendl;
    }
    return;
  }

  // if it's not period commit, nobody is allowed to push to the master zone
  if (period.get_master_zone() == store->get_zone_params().get_id()) {
    ldout(cct, 10) << "master zone rejecting period id="
        << period.get_id() << " epoch=" << period.get_epoch() << dendl;
    http_ret = -EINVAL; // XXX: error code
    return;
  }

  // write the period to rados
  http_ret = period.store_info(false);
  if (http_ret < 0) {
    lderr(cct) << "failed to store period " << period.get_id() << dendl;
    return;
  }

  // decide whether we can set_current_period() or set_latest_epoch()
  if (period.get_id() != current_period.get_id()) {
    auto current_epoch = current_period.get_realm_epoch();
    // discard periods in the past
    if (period.get_realm_epoch() < current_epoch) {
      ldout(cct, 10) << "discarding period " << period.get_id()
          << " with realm epoch " << period.get_realm_epoch()
          << " older than current epoch " << current_epoch << dendl;
      // return success to ack that we have this period
      return;
    }
    // discard periods too far in the future
    if (period.get_realm_epoch() > current_epoch + PERIOD_HISTORY_FETCH_MAX) {
      lderr(cct) << "discarding period " << period.get_id()
          << " with realm epoch " << period.get_realm_epoch() << " too far in "
          "the future from current epoch " << current_epoch << dendl;
      http_ret = -ENOENT; // XXX: error code
      return;
    }
    // attach a copy of the period into the period history
    auto cursor = store->period_history->attach(RGWPeriod{period});
    if (!cursor) {
      // we're missing some history between the new period and current_period
      http_ret = cursor.get_error();
      lderr(cct) << "failed to collect the periods between current period "
          << current_period.get_id() << " (realm epoch " << current_epoch
          << ") and the new period " << period.get_id()
          << " (realm epoch " << period.get_realm_epoch()
          << "): " << cpp_strerror(-http_ret) << dendl;
      return;
    }
    if (cursor.has_next()) {
      // don't switch if we have a newer period in our history
      ldout(cct, 4) << "attached period " << period.get_id()
          << " to history, but the history contains newer periods" << dendl;
      return;
    }
    // set as current period
    http_ret = realm.set_current_period(period);
    if (http_ret < 0) {
      lderr(cct) << "failed to update realm's current period" << dendl;
      return;
    }
    ldout(cct, 4) << "period " << period.get_id()
        << " is newer than current period " << current_period.get_id()
        << ", updating realm's current period and notifying zone" << dendl;
    realm.notify_new_period(period);
    return;
  }

  if (period.get_epoch() <= current_period.get_epoch()) {
    lderr(cct) << "period epoch " << period.get_epoch() << " is not newer "
        "than current epoch " << current_period.get_epoch()
        << ", discarding update" << dendl;
    return;
  }
  // set as latest epoch
  http_ret = period.set_latest_epoch(period.get_epoch());
  if (http_ret < 0) {
    lderr(cct) << "failed to set latest epoch" << dendl;
    return;
  }
  // reflect the period into our local objects
  http_ret  = period.reflect();
  if (http_ret  < 0) {
    lderr(cct) << "failed to update local objects: "
        << cpp_strerror(-http_ret) << dendl;
    return;
  }
  ldout(cct, 4) << "period epoch " << period.get_epoch()
      << " is newer than current epoch " << current_period.get_epoch()
      << ", updating period's latest epoch and notifying zone" << dendl;
  realm.notify_new_period(period);
  // update the period history
  store->period_history->insert(RGWPeriod{period});
}

class RGWHandler_Period : public RGWHandler_Auth_S3 {
 protected:
  RGWOp *op_get() override { return new RGWOp_Period_Get; }
  RGWOp *op_post() override { return new RGWOp_Period_Post; }
};

class RGWRESTMgr_Period : public RGWRESTMgr {
 public:
  RGWHandler_REST* get_handler(struct req_state*) override {
    return new RGWHandler_Period;
  }
};


// GET /admin/realm
class RGWOp_Realm_Get : public RGWRESTOp {
  std::unique_ptr<RGWRealm> realm;
public:
  int verify_permission() override { return 0; }
  void execute() override;
  void send_response() override;
  const string name() { return "get_realm"; }
};

void RGWOp_Realm_Get::execute()
{
  string id;
  RESTArgs::get_string(s, "id", id, &id);
  string name;
  RESTArgs::get_string(s, "name", name, &name);

  // read realm
  realm.reset(new RGWRealm(id, name));
  http_ret = realm->init(g_ceph_context, store);
  if (http_ret < 0)
    lderr(store->ctx()) << "failed to read realm id=" << id
        << " name=" << name << dendl;
}

void RGWOp_Realm_Get::send_response()
{
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("realm", *realm, s->formatter);
  flusher.flush();
}

class RGWHandler_Realm : public RGWHandler_Auth_S3 {
protected:
  RGWOp *op_get() { return new RGWOp_Realm_Get; }
};

RGWRESTMgr_Realm::RGWRESTMgr_Realm()
{
  // add the /admin/realm/period resource
  register_resource("period", new RGWRESTMgr_Period);
}

RGWHandler_REST* RGWRESTMgr_Realm::get_handler(struct req_state*)
{
  return new RGWHandler_Realm;
}
