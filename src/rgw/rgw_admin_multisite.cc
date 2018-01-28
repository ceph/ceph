#include "rgw_admin_multisite.h"

#include "common/ceph_json.h"
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

int do_period_pull(RGWRados *store, RGWRESTConn *remote_conn, const string& url,
                   const string& access_key, const string& secret_key,
                   const string& realm_id, const string& realm_name,
                   const string& period_id, const string& period_epoch,
                   RGWPeriod *period)
{
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
  int ret = send_to_remote_or_url(remote_conn, url, access_key, secret_key,
                                  info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  ret = period->init(g_ceph_context, store, false);
  if (ret < 0) {
    cerr << "faile to init period " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  try {
    decode_json_obj(*period, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  ret = period->store_info(false);
  if (ret < 0) {
    cerr << "Error storing period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
  }
  // store latest epoch (ignore errors)
  period->update_latest_epoch(period->get_epoch());
  return 0;
}

int handle_opt_period_delete(const string& period_id, CephContext *context, RGWRados *store)
{
  if (period_id.empty()) {
    cerr << "missing period id" << std::endl;
    return EINVAL;
  }
  RGWPeriod period(period_id);
  int ret = period.init(context, store);
  if (ret < 0) {
    cerr << "period.init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = period.delete_obj();
  if (ret < 0) {
    cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_period_get(const string& period_epoch, string& period_id, bool staging, string& realm_id,
                          string& realm_name, CephContext *context, RGWRados *store, Formatter *formatter)
{
  epoch_t epoch = 0;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  if (staging) {
    RGWRealm realm(realm_id, realm_name);
    int ret = realm.init(context, store);
    if (ret < 0 ) {
      cerr << "Error initializing realm " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    realm_id = realm.get_id();
    realm_name = realm.get_name();
    period_id = RGWPeriod::get_staging_id(realm_id);
    epoch = 1;
  }
  RGWPeriod period(period_id, epoch);
  int ret = period.init(context, store, realm_id, realm_name);
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_period_get_current(const string& realm_id, const string& realm_name, RGWRados *store, Formatter *formatter)
{
  string period_id;
  int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
  if (ret < 0) {
    return -ret;
  }
  formatter->open_object_section("period_get_current");
  encode_json("current_period", period_id, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_period_list(RGWRados *store, Formatter *formatter)
{
  list<string> periods;
  int ret = store->list_periods(periods);
  if (ret < 0) {
    cerr << "failed to list periods: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_object_section("periods_list");
  encode_json("periods", periods, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_period_pull(const string& period_id, const string& period_epoch, const string& realm_id,
                           const string& realm_name, const string& url, const string& access_key, const string& secret_key,
                           string& remote, CephContext *context, RGWRados *store, Formatter *formatter)
{
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (url.empty()) {
    // load current period for endpoints
    RGWRealm realm(realm_id, realm_name);
    int ret = realm.init(context, store);
    if (ret < 0) {
      cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWPeriod current_period(realm.get_current_period());
    ret = current_period.init(context, store);
    if (ret < 0) {
      cerr << "failed to init current period: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    if (remote.empty()) {
      // use realm master zone as remote
      remote = current_period.get_master_zone();
    }
    conn = get_remote_conn(store, current_period.get_map(), remote);
    if (!conn) {
      cerr << "failed to find a zone or zonegroup for remote "
           << remote << std::endl;
      return -ENOENT;
    }
    remote_conn = &*conn;
  }

  RGWPeriod period;
  int ret = do_period_pull(store, remote_conn, url, access_key, secret_key,
                           realm_id, realm_name, period_id, period_epoch,
                           &period);
  if (ret < 0) {
    cerr << "period pull failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_period_push(const string& period_id, const string& period_epoch, const string& realm_id,
                           const string& realm_name, const string& url, const string& access_key, const string& secret_key,
                           CephContext *context, RGWRados *store)
{
  RGWEnv env;
  req_info r_info(context, &env);
  r_info.method = "POST";
  r_info.request_uri = "/admin/realm/period";

  map<string, string> &params = r_info.args.get_params();
  if (!realm_id.empty())
    params["realm_id"] = realm_id;
  if (!realm_name.empty())
    params["realm_name"] = realm_name;
  if (!period_id.empty())
    params["period_id"] = period_id;
  if (!period_epoch.empty())
    params["epoch"] = period_epoch;

  // load the period
  RGWPeriod period(period_id);
  int ret = period.init(context, store);
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  // json format into a bufferlist
  JSONFormatter jf(false);
  encode_json("period", period, &jf);
  bufferlist bl;
  jf.flush(bl);

  JSONParser p;
  ret = send_to_remote_or_url(nullptr, url, access_key, secret_key,
                              r_info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_period_commit(const string& period_id, const string& period_epoch, const string& realm_id,
                             const string& realm_name, const string& url, const string& access_key,
                             const string& secret_key, const string& remote, bool yes_i_really_mean_it,
                             CephContext *context, RGWRados *store, Formatter *formatter)
{
  // read realm and staging period
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  RGWPeriod period(RGWPeriod::get_staging_id(realm.get_id()), 1);
  ret = period.init(context, store, realm.get_id());
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = commit_period(store, realm, period, remote, url, access_key, secret_key,
                      yes_i_really_mean_it);
  if (ret < 0) {
    cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_create(const string& realm_name, bool set_default, CephContext *context, RGWRados *store,
                            Formatter *formatter)
{
  if (realm_name.empty()) {
    cerr << "missing realm name" << std::endl;
    return EINVAL;
  }

  RGWRealm realm(realm_name, context, store);
  int ret = realm.create();
  if (ret < 0) {
    cerr << "ERROR: couldn't create realm " << realm_name << ": " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (set_default) {
    ret = realm.set_as_default();
    if (ret < 0) {
      cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("realm", realm, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_delete(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store)
{
  RGWRealm realm(realm_id, realm_name);
  if (realm_name.empty() && realm_id.empty()) {
    cerr << "missing realm name or id" << std::endl;
    return EINVAL;
  }
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = realm.delete_obj();
  if (ret < 0) {
    cerr << "ERROR: couldn't : " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_realm_get(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store,
                         Formatter *formatter)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  if (ret < 0) {
    if (ret == -ENOENT && realm_name.empty() && realm_id.empty()) {
      cerr << "missing realm name or id, or default realm not found" << std::endl;
    } else {
      cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
    }
    return -ret;
  }
  encode_json("realm", realm, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_get_default(CephContext *context, RGWRados *store)
{
  RGWRealm realm(context, store);
  string default_id;
  int ret = realm.read_default_id(default_id);
  if (ret == -ENOENT) {
    cout << "No default realm is set" << std::endl;
    return -ret;
  } else if (ret < 0) {
    cerr << "Error reading default realm:" << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  cout << "default realm: " << default_id << std::endl;
  return 0;
}

int handle_opt_realm_list(CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWRealm realm(context, store);
  string default_id;
  int ret = realm.read_default_id(default_id);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
  }
  list<string> realms;
  ret = store->list_realms(realms);
  if (ret < 0) {
    cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_object_section("realms_list");
  encode_json("default_info", default_id, formatter);
  encode_json("realms", realms, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_list_periods(const string& realm_id, const string& realm_name, RGWRados *store, Formatter *formatter)
{
  string period_id;
  int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
  if (ret < 0) {
    return -ret;
  }
  list<string> periods;
  ret = store->list_periods(period_id, periods);
  if (ret < 0) {
    cerr << "list periods failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_object_section("realm_periods_list");
  encode_json("current_period", period_id, formatter);
  encode_json("periods", periods, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_rename(const string& realm_id, const string& realm_name, const string& realm_new_name,
                            CephContext *context, RGWRados *store)
{
  RGWRealm realm(realm_id, realm_name);
  if (realm_new_name.empty()) {
    cerr << "missing realm new name" << std::endl;
    return EINVAL;
  }
  if (realm_name.empty() && realm_id.empty()) {
    cerr << "missing realm name or id" << std::endl;
    return EINVAL;
  }
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = realm.rename(realm_new_name);
  if (ret < 0) {
    cerr << "realm.rename failed: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  cout << "Realm name updated. Note that this change only applies to "
      "the current cluster, so this command must be run separately "
      "on each of the realm's other clusters." << std::endl;
  return 0;
}

int handle_opt_realm_set(const string& realm_id, const string& realm_name, const string& infile,
                         bool set_default, CephContext *context, RGWRados *store, Formatter *formatter)
{
  if (realm_id.empty() && realm_name.empty()) {
    cerr << "no realm name or id provided" << std::endl;
    return EINVAL;
  }
  RGWRealm realm(realm_id, realm_name);
  bool new_realm = false;
  int ret = realm.init(context, store);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  } else if (ret == -ENOENT) {
    new_realm = true;
  }
  ret = read_decode_json(infile, realm);
  if (ret < 0) {
    return 1;
  }
  if (!realm_name.empty() && realm.get_name() != realm_name) {
    cerr << "mismatch between --rgw-realm " << realm_name << " and json input file name " <<
         realm.get_name() << std::endl;
    return EINVAL;
  }
  /* new realm */
  if (new_realm) {
    cout << "clearing period and epoch for new realm" << std::endl;
    realm.clear_current_period_and_epoch();
    ret = realm.create();
    if (ret < 0) {
      cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  } else {
    ret = realm.update();
    if (ret < 0) {
      cerr << "ERROR: couldn't store realm info: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (set_default) {
    ret = realm.set_as_default();
    if (ret < 0) {
      cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }
  encode_json("realm", realm, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_realm_default(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = realm.set_as_default();
  if (ret < 0) {
    cerr << "failed to set realm as default: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_realm_pull(const string& realm_id, const string& realm_name, const string& url, const string& access_key,
                          const string& secret_key, bool set_default, CephContext *context, RGWRados *store, Formatter *formatter)
{
  if (url.empty()) {
    cerr << "A --url must be provided." << std::endl;
    return EINVAL;
  }
  RGWEnv env;
  req_info r_info(context, &env);
  r_info.method = "GET";
  r_info.request_uri = "/admin/realm";

  map<string, string> &params = r_info.args.get_params();
  if (!realm_id.empty())
    params["id"] = realm_id;
  if (!realm_name.empty())
    params["name"] = realm_name;

  bufferlist bl;
  JSONParser p;
  int ret = send_to_url(url, access_key, secret_key, r_info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    if (ret == -EACCES) {
      cerr << "If the realm has been changed on the master zone, the "
          "master zone's gateway may need to be restarted to recognize "
          "this user." << std::endl;
    }
    return -ret;
  }
  RGWRealm realm;
  realm.init(context, store, false);
  try {
    decode_json_obj(realm, &p);
  } catch (JSONDecoder::err& e) {
    cerr << "failed to decode JSON response: " << e.message << std::endl;
    return EINVAL;
  }
  RGWPeriod period;
  auto& current_period = realm.get_current_period();
  if (!current_period.empty()) {
    // pull the latest epoch of the realm's current period
    ret = do_period_pull(store, nullptr, url, access_key, secret_key,
                         realm_id, realm_name, current_period, "",
                         &period);
    if (ret < 0) {
      cerr << "could not fetch period " << current_period << std::endl;
      return -ret;
    }
  }
  ret = realm.create(false);
  if (ret < 0 && ret != -EEXIST) {
    cerr << "Error storing realm " << realm.get_id() << ": "
         << cpp_strerror(ret) << std::endl;
    return -ret;
  } else if (ret ==-EEXIST) {
    ret = realm.update();
    if (ret < 0) {
      cerr << "Error storing realm " << realm.get_id() << ": "
           << cpp_strerror(ret) << std::endl;
    }
  }

  if (set_default) {
    ret = realm.set_as_default();
    if (ret < 0) {
      cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("realm", realm, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_add(const string& zonegroup_id, const string& zonegroup_name, const string& zone_id,
                             const string& zone_name, bool tier_type_specified, string *tier_type,
                             const map<string, string, ltstr_nocase>& tier_config_add, bool sync_from_all_specified,
                             bool *sync_from_all, bool redirect_zone_set, string *redirect_zone,
                             bool is_master_set, bool *is_master, bool is_read_only_set,
                             bool *read_only, const list<string>& endpoints, list<string>& sync_from,
                             list<string>& sync_from_rm, CephContext *context, RGWRados *store,
                             Formatter *formatter)
{
  if (zonegroup_id.empty() && zonegroup_name.empty()) {
    cerr << "no zonegroup name or id provided" << std::endl;
    return EINVAL;
  }

  RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to initialize zonegroup " << zonegroup_name << " id " << zonegroup_id << " :"
         << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  RGWZoneParams zone(zone_id, zone_name);
  ret = zone.init(context, store);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  if (zone.realm_id != zonegroup.realm_id) {
    zone.realm_id = zonegroup.realm_id;
    ret = zone.update();
    if (ret < 0) {
      cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  string *ptier_type = (tier_type_specified ? tier_type : nullptr);
  zone.tier_config = tier_config_add;

  bool *psync_from_all = (sync_from_all_specified ? sync_from_all : nullptr);
  string *predirect_zone = (redirect_zone_set ? redirect_zone : nullptr);

  ret = zonegroup.add_zone(zone,
                           (is_master_set ? is_master : nullptr),
                           (is_read_only_set ? read_only : nullptr),
                           endpoints, ptier_type,
                           psync_from_all, sync_from, sync_from_rm,
                           predirect_zone);
  if (ret < 0) {
    cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name() << ": "
         << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_create(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id,
                                const string& realm_name, const string& api_name, bool set_default, bool is_master,
                                const list<string>& endpoints, CephContext *context, RGWRados *store, Formatter *formatter)
{
  if (zonegroup_name.empty()) {
    cerr << "Missing zonegroup name" << std::endl;
    return EINVAL;
  }
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWZoneGroup zonegroup(zonegroup_name, is_master, context, store, realm.get_id(), endpoints);
  zonegroup.api_name = (api_name.empty() ? zonegroup_name : api_name);
  ret = zonegroup.create();
  if (ret < 0) {
    cerr << "failed to create zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (set_default) {
    ret = zonegroup.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_default(const string& zonegroup_id, const string& zonegroup_name, CephContext *context,
                                 RGWRados *store)
{
  if (zonegroup_id.empty() && zonegroup_name.empty()) {
    cerr << "no zonegroup name or id provided" << std::endl;
    return EINVAL;
  }

  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  ret = zonegroup.set_as_default();
  if (ret < 0) {
    cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_zonegroup_delete(const string& zonegroup_id, const string& zonegroup_name, CephContext *context,
                                RGWRados *store)
{
  if (zonegroup_id.empty() && zonegroup_name.empty()) {
    cerr << "no zonegroup name or id provided" << std::endl;
    return EINVAL;
  }
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = zonegroup.delete_obj();
  if (ret < 0) {
    cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_zonegroup_get(const string& zonegroup_id, const string& zonegroup_name, CephContext *context,
                             RGWRados *store, Formatter *formatter)
{
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_list(CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWZoneGroup zonegroup;
  int ret = zonegroup.init(context, store, false);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  list<string> zonegroups;
  ret = store->list_zonegroups(zonegroups);
  if (ret < 0) {
    cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  string default_zonegroup;
  ret = zonegroup.read_default_id(default_zonegroup);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
  }
  formatter->open_object_section("zonegroups_list");
  encode_json("default_info", default_zonegroup, formatter);
  encode_json("zonegroups", zonegroups, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_modify(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id,
                                const string& realm_name, const string& api_name, const string& master_zone,
                                bool is_master_set, bool is_master, bool set_default, const list<string>& endpoints,
                                CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  if (ret < 0) {
    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  bool need_update = false;

  if (!master_zone.empty()) {
    zonegroup.master_zone = master_zone;
    need_update = true;
  }

  if (is_master_set) {
    zonegroup.update_master(is_master);
    need_update = true;
  }

  if (!endpoints.empty()) {
    zonegroup.endpoints = endpoints;
    need_update = true;
  }

  if (!api_name.empty()) {
    zonegroup.api_name = api_name;
    need_update = true;
  }

  if (!realm_id.empty()) {
    zonegroup.realm_id = realm_id;
    need_update = true;
  } else if (!realm_name.empty()) {
    // get realm id from name
    RGWRealm realm{context, store};
    ret = realm.read_id(realm_name, zonegroup.realm_id);
    if (ret < 0) {
      cerr << "failed to find realm by name " << realm_name << std::endl;
      return -ret;
    }
    need_update = true;
  }

  if (need_update) {
    ret = zonegroup.update();
    if (ret < 0) {
      cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (set_default) {
    ret = zonegroup.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_set(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id,
                             const string& realm_name, const string& infile,  bool set_default, const list<string>& endpoints,
                             CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(context, store);
  bool default_realm_not_exist = (ret == -ENOENT && realm_id.empty() && realm_name.empty());

  if (ret < 0 && !default_realm_not_exist ) {
    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWZoneGroup zonegroup;
  ret = zonegroup.init(context, store, false);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = read_decode_json(infile, zonegroup);
  if (ret < 0) {
    return 1;
  }
  if (zonegroup.realm_id.empty() && !default_realm_not_exist) {
    zonegroup.realm_id = realm.get_id();
  }
  ret = zonegroup.create();
  if (ret < 0 && ret != -EEXIST) {
    cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
    return 1;
  } else if (ret == -EEXIST) {
    ret = zonegroup.update();
    if (ret < 0) {
      cerr << "ERROR: couldn't store zonegroup info: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (set_default) {
    ret = zonegroup.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_remove(const string& zonegroup_id, const string& zonegroup_name, string& zone_id,
                                const string& zone_name, CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (zone_id.empty()) {
    if (zone_name.empty()) {
      cerr << "no --zone-id or --rgw-zone name provided" << std::endl;
      return EINVAL;
    }
    // look up zone id by name
    for (auto& z : zonegroup.zones) {
      if (zone_name == z.second.name) {
        zone_id = z.second.id;
        break;
      }
    }
    if (zone_id.empty()) {
      cerr << "zone name " << zone_name << " not found in zonegroup "
           << zonegroup.get_name() << std::endl;
      return ENOENT;
    }
  }

  ret = zonegroup.remove_zone(zone_id);
  if (ret < 0) {
    cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("zonegroup", zonegroup, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zonegroup_rename(const string& zonegroup_id, const string& zonegroup_name,
                                const string& zonegroup_new_name, CephContext *context, RGWRados *store)
{
  if (zonegroup_new_name.empty()) {
    cerr << " missing zonegroup new name" << std::endl;
    return EINVAL;
  }
  if (zonegroup_id.empty() && zonegroup_name.empty()) {
    cerr << "no zonegroup name or id provided" << std::endl;
    return EINVAL;
  }
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(g_ceph_context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = zonegroup.rename(zonegroup_new_name);
  if (ret < 0) {
    cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_zone_create(const string& zone_id, const string& zone_name, const string& zonegroup_id,
                           const string& zonegroup_name, string& realm_id, const string& realm_name,
                           const string& access_key, const string& secret_key, bool tier_type_specified,
                           string *tier_type, const map<string, string, ltstr_nocase>& tier_config_add,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<string>& endpoints, list<string>& sync_from,
                           list<string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter)
{
  if (zone_name.empty()) {
    cerr << "zone name not provided" << std::endl;
    return EINVAL;
  }
  int ret;
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  /* if the user didn't provide zonegroup info , create stand alone zone */
  if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
    ret = zonegroup.init(context, store);
    if (ret < 0) {
      cerr << "unable to initialize zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    if (realm_id.empty() && realm_name.empty()) {
      realm_id = zonegroup.realm_id;
    }
  }

  RGWZoneParams zone(zone_id, zone_name);
  ret = zone.init(context, store, false);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  zone.system_key.id = access_key;
  zone.system_key.key = secret_key;
  zone.realm_id = realm_id;
  zone.tier_config = tier_config_add;

  ret = zone.create();
  if (ret < 0) {
    cerr << "failed to create zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
    string *ptier_type = (tier_type_specified ? tier_type : nullptr);
    bool *psync_from_all = (sync_from_all_specified ? sync_from_all : nullptr);
    string *predirect_zone = (redirect_zone_set ? redirect_zone : nullptr);
    ret = zonegroup.add_zone(zone,
                             (is_master_set ? is_master : nullptr),
                             (is_read_only_set ? read_only : nullptr),
                             endpoints,
                             ptier_type,
                             psync_from_all,
                             sync_from, sync_from_rm,
                             predirect_zone);
    if (ret < 0) {
      cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name()
           << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (set_default) {
    ret = zone.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zone", zone, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zone_default(const string& zone_id, const string& zone_name, const string& zonegroup_id,
                            const string& zonegroup_name, CephContext *context, RGWRados *store)
{
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  int ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
  }
  if (zone_id.empty() && zone_name.empty()) {
    cerr << "no zone name or id provided" << std::endl;
    return EINVAL;
  }
  RGWZoneParams zone(zone_id, zone_name);
  ret = zone.init(context, store);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = zone.set_as_default();
  if (ret < 0) {
    cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_zone_delete(const string& zone_id, const string& zone_name, const string& zonegroup_id,
                           const string& zonegroup_name, CephContext *context, RGWRados *store)
{
  if (zone_id.empty() && zone_name.empty()) {
    cerr << "no zone name or id provided" << std::endl;
    return EINVAL;
  }
  RGWZoneParams zone(zone_id, zone_name);
  int ret = zone.init(g_ceph_context, store);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  list<string> zonegroups;
  ret = store->list_zonegroups(zonegroups);
  if (ret < 0) {
    cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  for (string &zg : zonegroups) {
    RGWZoneGroup zonegroup(string(), zg);
    int ret = zonegroup.init(g_ceph_context, store);
    if (ret < 0) {
      cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
      continue;
    }
    ret = zonegroup.remove_zone(zone.get_id());
    if (ret < 0 && ret != -ENOENT) {
      cerr << "failed to remove zone " << zone_name << " from zonegroup " << zonegroup.get_name() << ": "
           << cpp_strerror(-ret) << std::endl;
    }
  }

  ret = zone.delete_obj();
  if (ret < 0) {
    cerr << "failed to delete zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_zone_get(const string& zone_id, const string& zone_name, CephContext *context, RGWRados *store,
                        Formatter *formatter)
{
  RGWZoneParams zone(zone_id, zone_name);
  int ret = zone.init(context, store);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  encode_json("zone", zone, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zone_set(string& zone_name, const string& realm_id, const string& realm_name, const string& infile,
                        bool set_default, CephContext *context, RGWRados *store, Formatter *formatter)
{
  RGWZoneParams zone(zone_name);
  int ret = zone.init(context, store, false);
  if (ret < 0) {
    return -ret;
  }

  ret = zone.read();
  if (ret < 0 && ret != -ENOENT) {
    cerr << "zone.read() returned ret=" << ret << std::endl;
    return -ret;
  }

  string orig_id = zone.get_id();

  ret = read_decode_json(infile, zone);
  if (ret < 0) {
    return 1;
  }

  if(zone.realm_id.empty()) {
    RGWRealm realm(realm_id, realm_name);
    ret = realm.init(context, store);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    zone.realm_id = realm.get_id();
  }

  if( !zone_name.empty() && !zone.get_name().empty() && zone.get_name() != zone_name) {
    cerr << "Error: zone name" << zone_name << " is different than the zone name " << zone.get_name() << " in the provided json " << std::endl;
    return EINVAL;
  }

  if (zone.get_name().empty()) {
    zone.set_name(zone_name);
    if (zone.get_name().empty()) {
      cerr << "no zone name specified" << std::endl;
      return EINVAL;
    }
  }

  zone_name = zone.get_name();

  if (zone.get_id().empty()) {
    zone.set_id(orig_id);
  }

  if (zone.get_id().empty()) {
    cerr << "no zone name id the json provided, assuming old format" << std::endl;
    if (zone_name.empty()) {
      cerr << "missing zone name"  << std::endl;
      return EINVAL;
    }
    zone.set_name(zone_name);
    zone.set_id(zone_name);
  }

  cerr << "zone id " << zone.get_id();
  ret = zone.fix_pool_names();
  if (ret < 0) {
    cerr << "ERROR: couldn't fix zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = zone.write(false);
  if (ret < 0) {
    cerr << "ERROR: couldn't create zone: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }

  if (set_default) {
    ret = zone.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zone", zone, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zone_list(CephContext *context, RGWRados *store, Formatter *formatter)
{
  list<string> zones;
  int ret = store->list_zones(zones);
  if (ret < 0) {
    cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWZoneParams zone;
  ret = zone.init(context, store, false);
  if (ret < 0) {
    cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  string default_zone;
  ret = zone.read_default_id(default_zone);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "could not determine default zone: " << cpp_strerror(-ret) << std::endl;
  }
  formatter->open_object_section("zones_list");
  encode_json("default_info", default_zone, formatter);
  encode_json("zones", zones, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_zone_modify(const string& zone_id, const string& zone_name, const string& zonegroup_id,
                           const string& zonegroup_name, string& realm_id, const string& realm_name,
                           const string& access_key, const string& secret_key, bool tier_type_specified,
                           string *tier_type, const map<string, string, ltstr_nocase>& tier_config_add,
                           const map<string, string, ltstr_nocase>& tier_config_rm,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<string>& endpoints, list<string>& sync_from,
                           list<string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter)
{
  RGWZoneParams zone(zone_id, zone_name);
  int ret = zone.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  bool need_zone_update = false;
  if (!access_key.empty()) {
    zone.system_key.id = access_key;
    need_zone_update = true;
  }

  if (!secret_key.empty()) {
    zone.system_key.key = secret_key;
    need_zone_update = true;
  }

  if (!realm_id.empty()) {
    zone.realm_id = realm_id;
    need_zone_update = true;
  } else if (!realm_name.empty()) {
    // get realm id from name
    RGWRealm realm{context, store};
    ret = realm.read_id(realm_name, zone.realm_id);
    if (ret < 0) {
      cerr << "failed to find realm by name " << realm_name << std::endl;
      return -ret;
    }
    need_zone_update = true;
  }

  if (!tier_config_add.empty()) {
    for (auto add : tier_config_add) {
      zone.tier_config[add.first] = add.second;
    }
    need_zone_update = true;
  }

  if (!tier_config_rm.empty()) {
    for (auto rm : tier_config_rm) {
      zone.tier_config.erase(rm.first);
    }
    need_zone_update = true;
  }

  if (need_zone_update) {
    ret = zone.update();
    if (ret < 0) {
      cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  string *ptier_type = (tier_type_specified ? tier_type : nullptr);

  bool *psync_from_all = (sync_from_all_specified ? sync_from_all : nullptr);
  string *predirect_zone = (redirect_zone_set ? redirect_zone : nullptr);

  ret = zonegroup.add_zone(zone,
                           (is_master_set ? is_master : nullptr),
                           (is_read_only_set ? read_only : nullptr),
                           endpoints, ptier_type,
                           psync_from_all, sync_from, sync_from_rm,
                           predirect_zone);
  if (ret < 0) {
    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  ret = zonegroup.update();
  if (ret < 0) {
    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (set_default) {
    ret = zone.set_as_default();
    if (ret < 0) {
      cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
    }
  }

  encode_json("zone", zone, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_zone_rename(const string& zone_id, const string& zone_name, const string& zone_new_name,
                           const string& zonegroup_id, const string& zonegroup_name,
                           CephContext *context, RGWRados *store)
{
  if (zone_new_name.empty()) {
    cerr << " missing zone new name" << std::endl;
    return EINVAL;
  }
  if (zone_id.empty() && zone_name.empty()) {
    cerr << "no zonegroup name or id provided" << std::endl;
    return EINVAL;
  }
  RGWZoneParams zone(zone_id,zone_name);
  int ret = zone.init(context, store);
  if (ret < 0) {
    cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = zone.rename(zone_new_name);
  if (ret < 0) {
    cerr << "failed to rename zone " << zone_name << " to " << zone_new_name << ": " << cpp_strerror(-ret)
         << std::endl;
    return -ret;
  }
  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
  ret = zonegroup.init(context, store);
  if (ret < 0) {
    cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
  } else {
    ret = zonegroup.rename_zone(zone);
    if (ret < 0) {
      cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  return 0;
}

int handle_opt_metadata_sync_status(RGWRados *store, Formatter *formatter)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0) {
    cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }

  formatter->open_object_section("summary");
  encode_json("sync_status", sync_status, formatter);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
  }

  formatter->open_object_section("full_sync");
  encode_json("total", full_total, formatter);
  encode_json("complete", full_complete, formatter);
  formatter->close_section();
  formatter->close_section();

  formatter->flush(cout);
  return 0;
}

int handle_opt_metadata_sync_init(RGWRados *store)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }
  ret = sync.init_sync_status();
  if (ret < 0) {
    cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_metadata_sync_run(RGWRados *store)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  ret = sync.run();
  if (ret < 0) {
    cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_data_sync_status(const string& source_zone, RGWRados *store, Formatter *formatter)
{
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }
  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

  int ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }

  formatter->open_object_section("summary");
  encode_json("sync_status", sync_status, formatter);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
  }

  formatter->open_object_section("full_sync");
  encode_json("total", full_total, formatter);
  encode_json("complete", full_complete, formatter);
  formatter->close_section();
  formatter->close_section();

  formatter->flush(cout);
  return 0;
}

int handle_opt_data_sync_init(const string& source_zone, const boost::intrusive_ptr<CephContext>& cct, RGWRados *store)
{
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }

  RGWSyncModuleInstanceRef sync_module;
  int ret = store->get_sync_modules_manager()->create_instance(g_ceph_context, store->get_zone().tier_type,
                                                               store->get_zone_params().tier_config, &sync_module);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
    return ret;
  }

  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone, sync_module);

  ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  ret = sync.init_sync_status();
  if (ret < 0) {
    cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_data_sync_run(const string& source_zone, RGWRados *store)
{
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }
  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

  int ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  ret = sync.run();
  if (ret < 0) {
    cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}