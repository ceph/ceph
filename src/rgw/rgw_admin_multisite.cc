#include "rgw_admin_multisite.h"

#include "common/errno.h"
/// search for a matching zone/zonegroup id and return a connection if found
static boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store,
                                                    const RGWZoneGroup& zonegroup,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  if (remote == zonegroup.get_id()) {
    conn.emplace(store->ctx(), store, remote, zonegroup.endpoints);
  } else {
    for (const auto& z : zonegroup.zones) {
      const auto& zone = z.second;
      if (remote == zone.id) {
        conn.emplace(store->ctx(), store, remote, zone.endpoints);
        break;
      }
    }
  }
  return conn;
}

// we expect a very small response
static constexpr size_t MAX_REST_RESPONSE = 128 * 1024;

static int send_to_remote_gateway(RGWRESTConn* conn, req_info& info,
                                  bufferlist& in_data, JSONParser& parser)
{
  if (!conn) {
    return -EINVAL;
  }

  ceph::bufferlist response;
  rgw_user user;
  int ret = conn->forward(user, info, nullptr, MAX_REST_RESPONSE, &in_data, &response);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cerr << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store,
                                             const RGWPeriodMap& period_map,
                                             const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  for (const auto& zg : period_map.zonegroups) {
    conn = get_remote_conn(store, zg.second, remote);
    if (conn) {
      break;
    }
  }
  return conn;
}


int send_to_url(const string& url, const string& access,
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
  RGWRESTSimpleRequest req(g_ceph_context, url, nullptr, &params);

  bufferlist response;
  int ret = req.forward_request(key, info, MAX_REST_RESPONSE, &in_data, &response);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cout << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

int send_to_remote_or_url(RGWRESTConn *conn, const string& url,
                          const string& access, const string& secret,
                          req_info& info, bufferlist& in_data,
                          JSONParser& parser)
{
  if (url.empty()) {
    return send_to_remote_gateway(conn, info, in_data, parser);
  }
  return send_to_url(url, access, secret, info, in_data, parser);
}

int commit_period(RGWRados *store, RGWRealm& realm, RGWPeriod& period,
                  string remote, const string& url,
                  const string& access, const string& secret,
                  bool force)
{
  const string& master_zone = period.get_master_zone();
  if (master_zone.empty()) {
    cerr << "cannot commit period: period does not have a master zone of a master zonegroup" << std::endl;
    return -EINVAL;
  }
  // are we the period's master zone?
  if (store->get_zone_params().get_id() == master_zone) {
    // read the current period
    RGWPeriod current_period;
    int ret = current_period.init(g_ceph_context, store, realm.get_id());
    if (ret < 0) {
      cerr << "Error initializing current period: "
           << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    // the master zone can commit locally
    ret = period.commit(realm, current_period, cerr, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    }
    return ret;
  }

  if (remote.empty() && url.empty()) {
    // use the new master zone's connection
    remote = master_zone;
    cout << "Sending period to new master zone " << remote << std::endl;
  }
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (!remote.empty()) {
    conn = get_remote_conn(store, period.get_map(), remote);
    if (!conn) {
      cerr << "failed to find a zone or zonegroup for remote "
           << remote << std::endl;
      return -ENOENT;
    }
    remote_conn = &*conn;
  }

  // push period to the master with an empty period id
  period.set_id("");

  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "POST";
  info.request_uri = "/admin/realm/period";

  // json format into a bufferlist
  JSONFormatter jf(false);
  encode_json("period", period, &jf);
  bufferlist bl;
  jf.flush(bl);

  JSONParser p;
  int ret = send_to_remote_or_url(remote_conn, url, access, secret, info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;

    // did we parse an error message?
    auto message = p.find_obj("Message");
    if (message) {
      cerr << "Reason: " << message->get_data() << std::endl;
    }
    return ret;
  }

  // decode the response and store it back
  try {
    decode_json_obj(period, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  if (period.get_id().empty()) {
    cerr << "Period commit got back an empty period id" << std::endl;
    return -EINVAL;
  }
  // the master zone gave us back the period that it committed, so it's
  // safe to save it as our latest epoch
  ret = period.store_info(false);
  if (ret < 0) {
    cerr << "Error storing committed period " << period.get_id() << ": "
         << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.set_latest_epoch(period.get_epoch());
  if (ret < 0) {
    cerr << "Error updating period epoch: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.reflect();
  if (ret < 0) {
    cerr << "Error updating local objects: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  realm.notify_new_period(period);
  return ret;
}

int update_period(RGWRados *store, const string& realm_id, const string& realm_name,
                  const string& period_id, const string& period_epoch,
                  bool commit, const string& remote, const string& url,
                  const string& access, const string& secret,
                  Formatter *formatter, bool force)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(g_ceph_context, store);
  if (ret < 0 ) {
    cerr << "Error initializing realm " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  epoch_t epoch = 0;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  RGWPeriod period(period_id, epoch);
  ret = period.init(g_ceph_context, store, realm.get_id());
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  period.fork();
  ret = period.update();
  if(ret < 0) {
    // Dropping the error message here, as both the ret codes were handled in
    // period.update()
    return ret;
  }
  ret = period.store_info(false);
  if (ret < 0) {
    cerr << "failed to store period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  if (commit) {
    ret = commit_period(store, realm, period, remote, url, access, secret, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}