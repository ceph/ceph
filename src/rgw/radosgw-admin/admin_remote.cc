// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/admin_remote.h"
#include <iostream>
#include <map>
#include <optional>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_sal_rados.h"
#include "global/global_context.h"
#include "rgw_http_client.h"
#include "rgw_http_errors.h"
#include "rgw_rest_client.h"
#include "rgw_rest_conn.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_zone.h"

using ceph::Formatter;
using namespace std;

static const DoutPrefixProvider* g_admin_dpp;

namespace {

#undef dpp
#define dpp g_admin_dpp

#ifdef WITH_RADOSGW_RADOS
static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* rados_driver,
                                                    const RGWZoneGroup& zonegroup,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  if (remote == zonegroup.get_id()) {
    conn.emplace(rados_driver->ctx(), rados_driver, remote, zonegroup.endpoints, zonegroup.api_name);
  } else {
    for (const auto& z : zonegroup.zones) {
      const auto& zone = z.second;
      if (remote == zone.id) {
        conn.emplace(rados_driver->ctx(), rados_driver, remote, zone.endpoints, zonegroup.api_name);
        break;
      }
    }
  }
  return conn;
}

static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* rados_driver,
                                                    const RGWPeriodMap& period_map,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  for (const auto& zg : period_map.zonegroups) {
    conn = get_remote_conn(rados_driver, zg.second, remote);
    if (conn) {
      break;
    }
  }
  return conn;
}
#endif // WITH_RADOSGW_RADOS

static constexpr size_t MAX_REST_RESPONSE = 128 * 1024;

static int send_to_remote_gateway(RGWRESTConn* conn, req_info& info,
                                  bufferlist& in_data, JSONParser& parser)
{
  if (!conn) {
    return -EINVAL;
  }

  ceph::bufferlist response;
  rgw_user user;
  auto result = conn->forward(dpp, user, info, MAX_REST_RESPONSE, &in_data, &response, null_yield);
  if (!result) {
    return result.error();
  }
  int ret = rgw_http_error_to_errno(*result);
  if (ret < 0) {
    return ret;
  }

  ret = parser.parse(response.c_str(), response.length());
  if (ret < 0) {
    cerr << "failed to parse response" << std::endl;
    return ret;
  }
  return 0;
}

static int send_to_url(const string& url,
                       std::optional<string> opt_region,
                       const string& access,
                       const string& secret, req_info& info,
                       bufferlist& in_data, JSONParser& parser)
{
  if (access.empty() || secret.empty()) {
    cerr << "An --access-key and --secret must be provided with --url." << std::endl;
    return -EINVAL;
  }
  RGWAccessKey key;
  key.id = access;
  key.key = secret;

  param_vec_t params;
  RGWRESTSimpleRequest req(g_ceph_context, info.method, url, NULL, &params, opt_region);

  bufferlist response;
  auto result = req.forward_request(dpp, key, info, MAX_REST_RESPONSE, &in_data, &response, null_yield);
  if (!result) {
    return result.error();
  }
  int ret = rgw_http_error_to_errno(*result);
  if (ret < 0) {
    return ret;
  }

  ret = parser.parse(response.c_str(), response.length());
  if (ret < 0) {
    cout << "failed to parse response" << std::endl;
    return ret;
  }
  return 0;
}

} // anonymous namespace

int rgw_admin_send_to_remote_or_url(RGWRESTConn *conn, const string& url,
                                    std::optional<string> opt_region,
                                    const string& access, const string& secret,
                                    req_info& info, bufferlist& in_data,
                                    JSONParser& parser,
                                    const DoutPrefixProvider* dpp,
                                    rgw::sal::Driver* driver)
{
  g_admin_dpp = dpp;
  if (url.empty()) {
    return send_to_remote_gateway(conn, info, in_data, parser);
  }
  return send_to_url(url, opt_region, access, secret, info, in_data, parser);
}

int rgw_admin_commit_period(rgw::sal::ConfigStore* cfgstore,
                            RGWRealm& realm, rgw::sal::RealmWriter& realm_writer,
                            RGWPeriod& period, string remote, const string& url,
                            std::optional<string> opt_region,
                            const string& access, const string& secret,
                            bool force, rgw::SiteConfig* site,
                            const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver)
{
  g_admin_dpp = dpp;
  auto& master_zone = period.get_master_zone().id;
  if (master_zone.empty()) {
    cerr << "cannot commit period: period does not have a master zone of a master zonegroup" << std::endl;
    return -EINVAL;
  }
  if (driver->get_zone()->get_id() == master_zone) {
    RGWPeriod current_period;
    int ret = cfgstore->read_period(dpp, null_yield, realm.current_period,
                                    std::nullopt, current_period);
    if (ret < 0) {
      cerr << "failed to load current period: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rgw::commit_period(dpp, null_yield, cfgstore, driver,
                             realm, realm_writer, current_period,
                             period, cerr, force, *site);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    }
    (void) cfgstore->realm_notify_new_period(dpp, null_yield, period);
    return ret;
  }

  if (remote.empty() && url.empty()) {
    remote = master_zone;
    cerr << "Sending period to new master zone " << remote << std::endl;
  }
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (!remote.empty()) {
#ifdef WITH_RADOSGW_RADOS
    conn = get_remote_conn(static_cast<rgw::sal::RadosStore*>(driver), period.get_map(), remote);
    if (!conn) {
      cerr << "failed to find a zone or zonegroup for remote "
          << remote << std::endl;
      return -ENOENT;
    }
    remote_conn = &*conn;
#else
    cerr << "ERROR: sending the period to a remote zone by id (--remote) "
        "requires the RADOS backend; use --url instead" << std::endl;
    return -ENOTSUP;
#endif
  }

  period.set_id(string());

  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "POST";
  info.request_uri = "/admin/realm/period";

  JSONFormatter jf(false);
  encode_json("period", period, &jf);
  bufferlist bl;
  jf.flush(bl);

  JSONParser p;
  int ret = rgw_admin_send_to_remote_or_url(remote_conn, url, opt_region, access, secret,
                                            info, bl, p, dpp, driver);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    auto message = p.find_obj("Message");
    if (message) {
      cerr << "Reason: " << message->get_data() << std::endl;
    }
    return ret;
  }

  try {
    decode_json_obj(period, &p);
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
  if (period.get_id().empty()) {
    cerr << "Period commit got back an empty period id" << std::endl;
    return -EINVAL;
  }
  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp, null_yield, exclusive, period);
  if (ret < 0) {
    cerr << "Error storing committed period " << period.get_id() << ": "
        << cpp_strerror(ret) << std::endl;
    return ret;
  }

  ret = cfgstore->update_latest_epoch(dpp, null_yield, period.get_id(), period.get_epoch());
  if (ret == -EEXIST) {
    cerr << "already have epoch >= " << period.get_epoch()
        << " for period " << period.get_id() << std::endl;
    return 0;
  }
  if (ret < 0) {
    cerr << "Error updating latest epoch for period " << period.get_id() << ": " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  ret = rgw::reflect_period(dpp, null_yield, cfgstore, period);
  if (ret < 0) {
    cerr << "Error updating local objects: " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  (void) cfgstore->realm_notify_new_period(dpp, null_yield, period);
  return ret;
}

int rgw_admin_update_period(rgw::sal::ConfigStore* cfgstore,
                            const string& realm_id, const string& realm_name,
                            const string& period_epoch, bool commit,
                            const string& remote, const string& url,
                            std::optional<string> opt_region,
                            const string& access, const string& secret,
                            Formatter *formatter, bool force, rgw::SiteConfig* site,
                            const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver)
{
  g_admin_dpp = dpp;
  RGWRealm realm;
  std::unique_ptr<rgw::sal::RealmWriter> realm_writer;
  int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                            realm_id, realm_name,
                            realm, &realm_writer);
  if (ret < 0) {
    cerr << "failed to load realm " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  std::optional<epoch_t> epoch;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  RGWPeriod period;
  ret = cfgstore->read_period(dpp, null_yield, realm.current_period,
                              epoch, period);
  if (ret < 0) {
    cerr << "failed to load current period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  rgw::fork_period(dpp, period);
  ret = rgw::update_period(dpp, null_yield, cfgstore, period);
  if (ret < 0) {
    return ret;
  }

  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp, null_yield, exclusive, period);
  if (ret < 0) {
    cerr << "failed to driver period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  if (commit) {
    ret = rgw_admin_commit_period(cfgstore, realm, *realm_writer, period, remote, url,
                                   opt_region, access, secret, force, site, dpp, driver);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

int rgw_admin_do_period_pull(rgw::sal::ConfigStore* cfgstore,
                             RGWRESTConn *remote_conn, const string& url,
                             std::optional<string> opt_region,
                             const string& access_key, const string& secret_key,
                             const string& realm_id, const string& realm_name,
                             const string& period_id, const string& period_epoch,
                             RGWPeriod *period,
                             const DoutPrefixProvider* dpp,
                             rgw::sal::Driver* driver)
{
  g_admin_dpp = dpp;
  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "GET";
  info.request_uri = "/admin/realm/period";

  map<string, string> &params = info.args.get_params();
  if (!realm_id.empty())
    params["realm_id"] = realm_id;
  if (!realm_name.empty())
    params["realm_name"] = realm_name;
  if (!period_id.empty())
    params["period_id"] = period_id;
  if (!period_epoch.empty())
    params["epoch"] = period_epoch;

  bufferlist bl;
  JSONParser p;
  int ret = rgw_admin_send_to_remote_or_url(remote_conn, url, opt_region, access_key, secret_key,
                                            info, bl, p, dpp, driver);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  try {
    decode_json_obj(*period, &p);
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp, null_yield, exclusive, *period);
  if (ret < 0) {
    cerr << "Error storing period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
  }

  ret = cfgstore->update_latest_epoch(dpp, null_yield, period->get_id(), period->get_epoch());
  if (ret == -EEXIST) {
    cerr << "already have epoch >= " << period->get_epoch()
        << " for period " << period->get_id() << std::endl;
    return 0;
  }
  if (ret < 0) {
    cerr << "Error updating latest epoch for period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  return 0;
}
