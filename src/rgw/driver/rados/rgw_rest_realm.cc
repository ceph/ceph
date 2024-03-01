// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "rgw_rest_realm.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_config.h"
#include "rgw_zone.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"
#include "services/svc_mdlog.h"

#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

// reject 'period push' if we would have to fetch too many intermediate periods
static const uint32_t PERIOD_HISTORY_FETCH_MAX = 64;

// base period op, shared between Get and Post
class RGWOp_Period_Base : public RGWRESTOp {
 protected:
  RGWPeriod period;
  std::ostringstream error_stream;
 public:
  int verify_permission(optional_yield) override { return 0; }
  void send_response() override;
};

// reply with the period object on success
void RGWOp_Period_Base::send_response()
{
  set_req_state_err(s, op_ret, error_stream.str());
  dump_errno(s);

  if (op_ret < 0) {
    if (!s->err.message.empty()) {
      ldpp_dout(this, 4) << "Request failed with " << op_ret
          << ": " << s->err.message << dendl;
    }
    end_header(s);
    return;
  }

  encode_json("period", period, s->formatter);
  end_header(s, NULL, "application/json", s->formatter->get_len());
  flusher.flush();
}

// GET /admin/realm/period
class RGWOp_Period_Get : public RGWOp_Period_Base {
 public:
  void execute(optional_yield y) override;
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_READ);
  }
  int verify_permission(optional_yield) override {
    return check_caps(s->user->get_caps());
  }
  const char* name() const override { return "get_period"; }
};

void RGWOp_Period_Get::execute(optional_yield y)
{
  string realm_id, period_id;
  epoch_t epoch = 0;
  RESTArgs::get_string(s, "realm_id", realm_id, &realm_id);
  RESTArgs::get_string(s, "period_id", period_id, &period_id);
  RESTArgs::get_uint32(s, "epoch", 0, &epoch);

  period.set_id(period_id);
  period.set_epoch(epoch);

  op_ret = period.init(this, driver->ctx(), static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
  if (op_ret < 0)
    ldpp_dout(this, 5) << "failed to read period" << dendl;
}

// POST /admin/realm/period
class RGWOp_Period_Post : public RGWOp_Period_Base {
 public:
  void execute(optional_yield y) override;
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_WRITE);
  }
  int verify_permission(optional_yield) override {
    return check_caps(s->user->get_caps());
  }
  const char* name() const override { return "post_period"; }
  RGWOpType get_type() override { return RGW_OP_PERIOD_POST; }
};

void RGWOp_Period_Post::execute(optional_yield y)
{
  auto cct = driver->ctx();

  // initialize the period without reading from rados
  period.init(this, cct, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, y, false);

  // decode the period from input
  const auto max_size = cct->_conf->rgw_max_put_param_size;
  bool empty;
  op_ret = get_json_input(cct, s, period, max_size, &empty);
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to decode period" << dendl;
    return;
  }

  // require period.realm_id to match our realm
  if (period.get_realm() != static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_realm().get_id()) {
    error_stream << "period with realm id " << period.get_realm()
        << " doesn't match current realm " << static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_realm().get_id() << std::endl;
    op_ret = -EINVAL;
    return;
  }

  // load the realm and current period from rados; there may be a more recent
  // period that we haven't restarted with yet. we also don't want to modify
  // the objects in use by RGWRados
  RGWRealm realm(period.get_realm());
  op_ret = realm.init(this, cct, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, y);
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to read current realm: "
        << cpp_strerror(-op_ret) << dendl;
    return;
  }

  RGWPeriod current_period;
  op_ret = current_period.init(this, cct, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm.get_id(), y);
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to read current period: "
        << cpp_strerror(-op_ret) << dendl;
    return;
  }

  // if period id is empty, handle as 'period commit'
  if (period.get_id().empty()) {
    op_ret = period.commit(this, driver, realm, current_period, error_stream, y);
    if (op_ret < 0) {
      ldpp_dout(this, -1) << "master zone failed to commit period" << dendl;
    }
    return;
  }

  // if it's not period commit, nobody is allowed to push to the master zone
  if (period.get_master_zone() == static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_params().get_id()) {
    ldpp_dout(this, 10) << "master zone rejecting period id="
        << period.get_id() << " epoch=" << period.get_epoch() << dendl;
    op_ret = -EINVAL; // XXX: error code
    return;
  }

  // write the period to rados
  op_ret = period.store_info(this, false, y);
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to store period " << period.get_id() << dendl;
    return;
  }
  // set as latest epoch
  op_ret = period.update_latest_epoch(this, period.get_epoch(), y);
  if (op_ret == -EEXIST) {
    // already have this epoch (or a more recent one)
    ldpp_dout(this, 4) << "already have epoch >= " << period.get_epoch()
        << " for period " << period.get_id() << dendl;
    op_ret = 0;
    return;
  }
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to set latest epoch" << dendl;
    return;
  }

  auto period_history = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_period_history();

  // decide whether we can set_current_period() or set_latest_epoch()
  if (period.get_id() != current_period.get_id()) {
    auto current_epoch = current_period.get_realm_epoch();
    // discard periods in the past
    if (period.get_realm_epoch() < current_epoch) {
      ldpp_dout(this, 10) << "discarding period " << period.get_id()
          << " with realm epoch " << period.get_realm_epoch()
          << " older than current epoch " << current_epoch << dendl;
      // return success to ack that we have this period
      return;
    }
    // discard periods too far in the future
    if (period.get_realm_epoch() > current_epoch + PERIOD_HISTORY_FETCH_MAX) {
      ldpp_dout(this, -1) << "discarding period " << period.get_id()
          << " with realm epoch " << period.get_realm_epoch() << " too far in "
          "the future from current epoch " << current_epoch << dendl;
      op_ret = -ENOENT; // XXX: error code
      return;
    }
    // attach a copy of the period into the period history
    auto cursor = period_history->attach(this, RGWPeriod{period}, y);
    if (!cursor) {
      // we're missing some history between the new period and current_period
      op_ret = cursor.get_error();
      ldpp_dout(this, -1) << "failed to collect the periods between current period "
          << current_period.get_id() << " (realm epoch " << current_epoch
          << ") and the new period " << period.get_id()
          << " (realm epoch " << period.get_realm_epoch()
          << "): " << cpp_strerror(-op_ret) << dendl;
      return;
    }
    if (cursor.has_next()) {
      // don't switch if we have a newer period in our history
      ldpp_dout(this, 4) << "attached period " << period.get_id()
          << " to history, but the history contains newer periods" << dendl;
      return;
    }
    // set as current period
    op_ret = realm.set_current_period(this, period, y);
    if (op_ret < 0) {
      ldpp_dout(this, -1) << "failed to update realm's current period" << dendl;
      return;
    }
    ldpp_dout(this, 4) << "period " << period.get_id()
        << " is newer than current period " << current_period.get_id()
        << ", updating realm's current period and notifying zone" << dendl;
    realm.notify_new_period(this, period, y);
    return;
  }
  // reflect the period into our local objects
  op_ret = period.reflect(this, y);
  if (op_ret < 0) {
    ldpp_dout(this, -1) << "failed to update local objects: "
        << cpp_strerror(-op_ret) << dendl;
    return;
  }
  ldpp_dout(this, 4) << "period epoch " << period.get_epoch()
      << " is newer than current epoch " << current_period.get_epoch()
      << ", updating period's latest epoch and notifying zone" << dendl;
  realm.notify_new_period(this, period, y);
  // update the period history
  period_history->insert(RGWPeriod{period});
}

class RGWHandler_Period : public RGWHandler_Auth_S3 {
 protected:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;

  RGWOp *op_get() override { return new RGWOp_Period_Get; }
  RGWOp *op_post() override { return new RGWOp_Period_Post; }
};

class RGWRESTMgr_Period : public RGWRESTMgr {
 public:
  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state*,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string&) override {
    return new RGWHandler_Period(auth_registry);
  }
};


// GET /admin/realm
class RGWOp_Realm_Get : public RGWRESTOp {
  std::unique_ptr<RGWRealm> realm;
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_READ);
  }
  int verify_permission(optional_yield) override {
    return check_caps(s->user->get_caps());
  }
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "get_realm"; }
};

void RGWOp_Realm_Get::execute(optional_yield y)
{
  string id;
  RESTArgs::get_string(s, "id", id, &id);
  string name;
  RESTArgs::get_string(s, "name", name, &name);

  // read realm
  realm.reset(new RGWRealm(id, name));
  op_ret = realm->init(this, g_ceph_context, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, y);
  if (op_ret < 0)
    ldpp_dout(this, -1) << "failed to read realm id=" << id
        << " name=" << name << dendl;
}

void RGWOp_Realm_Get::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);

  if (op_ret < 0) {
    end_header(s);
    return;
  }

  encode_json("realm", *realm, s->formatter);
  end_header(s, NULL, "application/json", s->formatter->get_len());
  flusher.flush();
}

// GET /admin/realm?list
class RGWOp_Realm_List : public RGWRESTOp {
  std::string default_id;
  std::list<std::string> realms;
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("zone", RGW_CAP_READ);
  }
  int verify_permission(optional_yield) override {
    return check_caps(s->user->get_caps());
  }
  void execute(optional_yield y) override;
  void send_response() override;
  const char* name() const override { return "list_realms"; }
};

void RGWOp_Realm_List::execute(optional_yield y)
{
  {
    // read default realm
    RGWRealm realm(driver->ctx(), static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj);
    [[maybe_unused]] int ret = realm.read_default_id(this, default_id, y);
  }
  op_ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->list_realms(this, realms);
  if (op_ret < 0)
    ldpp_dout(this, -1) << "failed to list realms" << dendl;
}

void RGWOp_Realm_List::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);

  if (op_ret < 0) {
    end_header(s);
    return;
  }

  s->formatter->open_object_section("realms_list");
  encode_json("default_info", default_id, s->formatter);
  encode_json("realms", realms, s->formatter);
  s->formatter->close_section();
  end_header(s, NULL, "application/json", s->formatter->get_len());
  flusher.flush();
}

class RGWHandler_Realm : public RGWHandler_Auth_S3 {
protected:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  RGWOp *op_get() override {
    if (s->info.args.sub_resource_exists("list"))
      return new RGWOp_Realm_List;
    return new RGWOp_Realm_Get;
  }
};

RGWRESTMgr_Realm::RGWRESTMgr_Realm()
{
  // add the /admin/realm/period resource
  register_resource("period", new RGWRESTMgr_Period);
}

RGWHandler_REST*
RGWRESTMgr_Realm::get_handler(rgw::sal::Driver* driver,
			      req_state*,
                              const rgw::auth::StrategyRegistry& auth_registry,
                              const std::string&)
{
  return new RGWHandler_Realm(auth_registry);
}
