// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_admin_remote.h"
#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "common/errno.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

namespace {

// pull the given period over the connection
int send_remote(RGWRESTConn* conn,
                const string& method,
                const string& path,
                const std::vector<std::pair<string, string> >& request_params,
                bufferlist *outbl,
                optional_yield y)
{
  rgw_user user;
  RGWEnv env;
  req_info info(conn->get_ctx(), &env);
  info.method = method.c_str();
  info.request_uri = path;

  auto& params = info.args.get_params();
  for (auto& pp : request_params) {
    params[pp.first] = pp.second;
  }

  bufferlist data;
#define MAX_REST_RESPONSE (128 * 1024)
  int r = conn->forward(user, info, nullptr, MAX_REST_RESPONSE, nullptr, outbl, y);
  if (r < 0) {
    return r;
  }

  return 0;
}

} // anonymous namespace

#if 0
int RGWPeriodPuller::pull(const std::string& period_id, RGWPeriod& period,
			  optional_yield y)
{
  // try to read the period from rados
  period.set_id(period_id);
  period.set_epoch(0);
  int r = period.init(cct, svc.sysobj, y);
  if (r < 0) {
    if (svc.zone->is_meta_master()) {
      // can't pull if we're the master
      ldout(cct, 1) << "metadata master failed to read period "
          << period_id << " from local storage: " << cpp_strerror(r) << dendl;
      return r;
    }
    ldout(cct, 14) << "pulling period " << period_id
        << " from master" << dendl;
    // request the period from the master zone
    r = pull_period(svc.zone->get_master_conn(), period_id,
                    svc.zone->get_realm().get_id(), period, y);
    if (r < 0) {
      lderr(cct) << "failed to pull period " << period_id << dendl;
      return r;
    }
    // write the period to rados
    r = period.store_info(true, y);
    if (r == -EEXIST) {
      r = 0;
    } else if (r < 0) {
      lderr(cct) << "failed to store period " << period_id << dendl;
      return r;
    }
    // update latest epoch
    r = period.update_latest_epoch(period.get_epoch(), y);
    if (r == -EEXIST) {
      // already have this epoch (or a more recent one)
      return 0;
    }
    if (r < 0) {
      lderr(cct) << "failed to update latest_epoch for period "
          << period_id << dendl;
      return r;
    }
    // reflect period objects if this is the latest version
    if (svc.zone->get_realm().get_current_period() == period_id) {
      r = period.reflect(y);
      if (r < 0) {
        return r;
      }
    }
    ldout(cct, 14) << "period " << period_id
        << " pulled and written to local storage" << dendl;
  } else {
    ldout(cct, 14) << "found period " << period_id
        << " in local storage" << dendl;
  }
  return 0;
}

  struct CreateUserParams {
    rgw_user user;
    std::string display_name;
    std::optional<std::string> email;
    std::optional<std::string> access_key;
    std::optional<std::string> secret_key;
    std::optional<std::string> key_type;
    std::optional<std::string> user_caps;
    bool generate_key{true};
    bool suspended{false};
    std::optional<int32_t> max_buckets;
    std::optional<bool> system;
    std::optional<bool> exclusive;
    std::optional<std::string> op_mask;
    std::optional<std::string> default_placement;
    std::optional<std::string> placement_tags;
  };

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "display-name", display_name, &display_name);
  RESTArgs::get_string(s, "email", email, &email);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_string(s, "user-caps", caps, &caps);
  RESTArgs::get_string(s, "tenant", tenant_name, &tenant_name);
  RESTArgs::get_bool(s, "generate-key", true, &gen_key);
  RESTArgs::get_bool(s, "suspended", false, &suspended);
  RESTArgs::get_int32(s, "max-buckets", default_max_buckets, &max_buckets);
  RESTArgs::get_bool(s, "system", false, &system);
  RESTArgs::get_bool(s, "exclusive", false, &exclusive);
  RESTArgs::get_string(s, "op-mask", op_mask_str, &op_mask_str);
  RESTArgs::get_string(s, "default-placement", default_placement_str, &default_placement_str);
  RESTArgs::get_string(s, "placement-tags", placement_tags_str, &placement_tags_str);
#endif

RGWRemoteAdminOp::RGWRemoteAdminOp(RGWSI_Zone *zone_svc) : cct(zone_svc->ctx())
{
  svc.zone = zone_svc;
}

static inline void add_param_str(std::vector<pair<string, string> >& params, const char *name, string val)
{
  params.push_back(make_pair(string(name), std::move(val)));
}

static inline void add_param_str(std::vector<pair<string, string> >& params, const char *name, std::optional<string> opt_val)
{
  if (!opt_val) {
    return;
  }
  add_param_str(params, name, *opt_val);
}

static inline void add_param_bool(std::vector<pair<string, string> >& params, const char *name, bool val)
{
  string v = (val ? "true" : "false");
  add_param_str(params, name, v);
}

static inline void add_param_bool(std::vector<pair<string, string> >& params, const char *name, std::optional<bool> opt_val)
{
  if (!opt_val) {
    return;
  }
  add_param_bool(params, name, *opt_val);
}

static inline void add_param_int(std::vector<pair<string, string> >& params, const char *name, int val)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", val);
  add_param_str(params, name, string(buf));
}

static inline void add_param_int(std::vector<pair<string, string> >& params, const char *name, std::optional<int> opt_val)
{
  if (!opt_val) {
    return;
  }
  add_param_int(params, name, *opt_val);
}


int RGWRemoteAdminOp::create_user(RGWRESTConn *conn,
                                  const CreateUserParams& params,
                                  optional_yield y)
{
  if (!conn) {
    ldout(cct, 1) << __func__ << "(): connection is not initialized" << dendl;
    return -EINVAL;
  }

  std::vector<pair<string, string> > req_params;
  add_param_str(req_params, "uid", params.user.id);
  add_param_str(req_params, "tenant", params.user.tenant);
  add_param_str(req_params, "display-name", params.display_name);
  add_param_str(req_params, "email", params.email);
  add_param_str(req_params, "access-key", params.access_key);
  add_param_str(req_params, "secret-key", params.secret_key);
  add_param_str(req_params, "key-type", params.key_type);
  add_param_str(req_params, "user-caps", params.user_caps);
  add_param_bool(req_params, "generate-key", params.generate_key);
  add_param_bool(req_params, "suspended", params.suspended);
  add_param_int(req_params, "max-buckets", params.max_buckets);
  add_param_int(req_params, "system", params.system);
  add_param_bool(req_params, "exclusive", params.exclusive);
  add_param_str(req_params, "op-mask", params.op_mask);
  add_param_str(req_params, "default-placement", params.default_placement);
  add_param_str(req_params, "placement-tags", params.placement_tags);

  bufferlist outbl;
  int r = send_remote(conn,
                      "PUT",
                      "/admin/user",
                      req_params,
                      &outbl,
                      y);
  if (r < 0) {
    ldout(cct, 5) << "remote request failed: r=" << r << dendl;
    return r;
  }
  return 0;
}

int RGWRemoteAdminOp::create_user(const CreateUserParams& params,
                                  optional_yield y)
{
  if (svc.zone->is_meta_master()) {
    // we are the master, don't hold connectioin to ourselves
    ldout(cct, 1) << "Zone is meta primary, no remote connection to ourselves to perform operation" << dendl;
    return -EINVAL;
  }

  return create_user(svc.zone->get_master_conn(), params, y);
}

