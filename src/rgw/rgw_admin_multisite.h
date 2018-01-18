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

int handle_opt_period_delete(const string& period_id, CephContext *context, RGWRados *store);

int handle_opt_period_get(const string& period_epoch, string& period_id, bool staging, string& realm_id,
                          string& realm_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_get_current(const string& realm_id, const string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_period_list(RGWRados *store, Formatter *formatter);

int handle_opt_period_pull(const string& period_id, const string& period_epoch, const string& realm_id,
                           const string& realm_name, const string& url, const string& access_key, const string& secret_key,
                           string& remote, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_push(const string& period_id, const string& period_epoch, const string& realm_id,
                           const string& realm_name, const string& url, const string& access_key, const string& secret_key,
                           CephContext *context, RGWRados *store);

int handle_opt_period_commit(const string& period_id, const string& period_epoch, const string& realm_id,
                             const string& realm_name, const string& url, const string& access_key,
                             const string& secret_key, const string& remote, bool yes_i_really_mean_it,
                             CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_create(const string& realm_name, bool set_default, CephContext *context, RGWRados *store,
                            Formatter *formatter);

int handle_opt_realm_delete(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_get(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store,
                         Formatter *formatter);

int handle_opt_realm_get_default(CephContext *context, RGWRados *store);

int handle_opt_realm_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_list_periods(const string& realm_id, const string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_realm_rename(const string& realm_id, const string& realm_name, const string& realm_new_name,
                            CephContext *context, RGWRados *store);

int handle_opt_realm_set(const string& realm_id, const string& realm_name, const string& infile,
                         bool set_default, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_default(const string& realm_id, const string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_pull(const string& realm_id, const string& realm_name, const string& url, const string& access_key,
                          const string& secret_key, bool set_default, CephContext *context, RGWRados *store,
                          Formatter *formatter);

#endif //CEPH_RGW_ADMIN_MULTISITE_H
