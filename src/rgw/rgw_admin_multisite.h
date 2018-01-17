#ifndef CEPH_RGW_ADMIN_MULTISITE_H
#define CEPH_RGW_ADMIN_MULTISITE_H

#include <boost/optional.hpp>
#include <common/errno.h>
#include "rgw_rest_conn.h"
#include "rgw_admin_common.h"

// Period, realm, zone , zonegroup , data sync, metadata sync

/// search each zonegroup for a connection
boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store, const RGWPeriodMap& period_map,
                                             const std::string& remote);

int send_to_url(const string& url, const string& access, const string& secret, req_info& info, bufferlist& in_data,
                JSONParser& parser);

int send_to_remote_or_url(RGWRESTConn *conn, const string& url, const string& access, const string& secret,
                          req_info& info, bufferlist& in_data, JSONParser& parser);

int commit_period(RGWRados *store, RGWRealm& realm, RGWPeriod& period, string remote, const string& url,
                  const string& access, const string& secret, bool force);

int update_period(RGWRados *store, const string& realm_id, const string& realm_name, const string& period_id,
                  const string& period_epoch, bool commit, const string& remote, const string& url,
                  const string& access, const string& secret, Formatter *formatter, bool force);

int do_period_pull(RGWRados *store, RGWRESTConn *remote_conn, const string& url,
                   const string& access_key, const string& secret_key,
                   const string& realm_id, const string& realm_name,
                   const string& period_id, const string& period_epoch,
                   RGWPeriod *period);

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
    int ret = realm.init(g_ceph_context, store);
    if (ret < 0) {
      cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWPeriod current_period(realm.get_current_period());
    ret = current_period.init(g_ceph_context, store);
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


#endif //CEPH_RGW_ADMIN_MULTISITE_H
