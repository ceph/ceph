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

int handle_opt_zonegroup_add(const string& zonegroup_id, const string& zonegroup_name, const string& zone_id,
                             const string& zone_name, bool tier_type_specified, string& tier_type,
                             const map<string, string, ltstr_nocase>& tier_config_add, bool sync_from_all_specified,
                             bool sync_from_all, bool redirect_zone_set,
                             string& redirect_zone, bool is_master_set, bool is_master, bool is_read_only_set,
                             bool read_only, const list<string>& endpoints, list<string>& sync_from,
                             list<string>& sync_from_rm, CephContext *context, RGWRados *store,
                             Formatter *formatter);

int handle_opt_zonegroup_create(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id, 
                                const string& realm_name, const string& api_name, bool set_default, bool is_master,
                                const list<string>& endpoints, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_default(const string& zonegroup_id, const string& zonegroup_name, CephContext *context,
                                 RGWRados *store);

int handle_opt_zonegroup_delete(const string& zonegroup_id, const string& zonegroup_name, CephContext *context,
                                RGWRados *store);

int handle_opt_zonegroup_get(const string& zonegroup_id, const string& zonegroup_name, CephContext *context, 
                             RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_modify(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id,
                                const string& realm_name, const string& api_name, const string& master_zone,
                                bool is_master_set, bool is_master, bool set_default, const list<string>& endpoints,
                                CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_set(const string& zonegroup_id, const string& zonegroup_name, const string& realm_id,
                                const string& realm_name, const string& infile,  bool set_default, const list<string>& endpoints,
                                CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_remove(const string& zonegroup_id, const string& zonegroup_name, string& zone_id, 
                                const string& zone_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_rename(const string& zonegroup_id, const string& zonegroup_name,
                                const string& zonegroup_new_name, CephContext *context, RGWRados *store);

#endif //CEPH_RGW_ADMIN_MULTISITE_H
