// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "common/ceph_json.h"
#include "common/errno.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw period puller: ")

RGWPeriodPuller::RGWPeriodPuller(RGWSI_Zone *zone_svc, RGWSI_SysObj *sysobj_svc)
{
  cct = zone_svc->ctx();
  svc.zone = zone_svc;
  svc.sysobj = sysobj_svc;
}

namespace {

// pull the given period over the connection
int pull_period(const DoutPrefixProvider *dpp, RGWRESTConn* conn, const std::string& period_id,
                const std::string& realm_id, RGWPeriod& period,
		optional_yield y)
{
  rgw_user user;
  RGWEnv env;
  req_info info(conn->get_ctx(), &env);
  info.method = "GET";
  info.request_uri = "/admin/realm/period";

  auto& params = info.args.get_params();
  params["realm_id"] = realm_id;
  params["period_id"] = period_id;

  bufferlist data;
#define MAX_REST_RESPONSE (128 * 1024)
  int r = conn->forward(dpp, user, info, nullptr, MAX_REST_RESPONSE, nullptr, &data, y);
  if (r < 0) {
    return r;
  }

  JSONParser parser;
  r = parser.parse(data.c_str(), data.length());
  if (r < 0) {
    ldpp_dout(dpp, -1) << "request failed: " << cpp_strerror(-r) << dendl;
    return r;
  }

  try {
    decode_json_obj(period, &parser);
  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, -1) << "failed to decode JSON input: "
        << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

} // anonymous namespace

int RGWPeriodPuller::pull(const DoutPrefixProvider *dpp, const std::string& period_id, RGWPeriod& period,
			  optional_yield y)
{
  // try to read the period from rados
  period.set_id(period_id);
  period.set_epoch(0);
  int r = period.init(dpp, cct, svc.sysobj, y);
  if (r < 0) {
    if (svc.zone->is_meta_master()) {
      // can't pull if we're the master
      ldpp_dout(dpp, 1) << "metadata master failed to read period "
          << period_id << " from local storage: " << cpp_strerror(r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 14) << "pulling period " << period_id
        << " from master" << dendl;
    // request the period from the master zone
    r = pull_period(dpp, svc.zone->get_master_conn(), period_id,
                    svc.zone->get_realm().get_id(), period, y);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "failed to pull period " << period_id << dendl;
      return r;
    }
    // write the period to rados
    r = period.store_info(dpp, true, y);
    if (r == -EEXIST) {
      r = 0;
    } else if (r < 0) {
      ldpp_dout(dpp, -1) << "failed to store period " << period_id << dendl;
      return r;
    }
    // update latest epoch
    r = period.update_latest_epoch(dpp, period.get_epoch(), y);
    if (r == -EEXIST) {
      // already have this epoch (or a more recent one)
      return 0;
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << "failed to update latest_epoch for period "
          << period_id << dendl;
      return r;
    }
    // reflect period objects if this is the latest version
    if (svc.zone->get_realm().get_current_period() == period_id) {
      r = period.reflect(dpp, y);
      if (r < 0) {
        return r;
      }
    }
    ldpp_dout(dpp, 14) << "period " << period_id
        << " pulled and written to local storage" << dendl;
  } else {
    ldpp_dout(dpp, 14) << "found period " << period_id
        << " in local storage" << dendl;
  }
  return 0;
}
