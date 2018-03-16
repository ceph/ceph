#ifndef CEPH_RGW_ADMIN_MULTISITE_H
#define CEPH_RGW_ADMIN_MULTISITE_H

#include <boost/optional.hpp>
#include "rgw_sync.h"
#include "rgw_data_sync.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// Period, realm, zone, zonegroup, data sync, metadata sync

/// search each zonegroup for a connection
boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store, const RGWPeriodMap& period_map,
                                             const std::string& remote);

int send_to_url(const std::string& url, const std::string& access, const std::string& secret, req_info& info, bufferlist& in_data,
                JSONParser& parser);

int send_to_remote_or_url(RGWRESTConn *conn, const std::string& url, const std::string& access, const std::string& secret,
                          req_info& info, bufferlist& in_data, JSONParser& parser);

int commit_period(RGWRados *store, RGWRealm& realm, RGWPeriod& period, std::string remote, const std::string& url,
                  const std::string& access, const std::string& secret, bool force);

int update_period(RGWRados *store, const std::string& realm_id, const std::string& realm_name, const std::string& period_id,
                  const std::string& period_epoch, bool commit, const std::string& remote, const std::string& url,
                  const std::string& access, const std::string& secret, Formatter *formatter, bool force);

int do_period_pull(RGWRados *store, RGWRESTConn *remote_conn, const std::string& url,
                   const std::string& access_key, const std::string& secret_key,
                   const std::string& realm_id, const std::string& realm_name,
                   const std::string& period_id, const std::string& period_epoch,
                   RGWPeriod *period);

int handle_opt_period_delete(const std::string& period_id, CephContext *context, RGWRados *store);

int handle_opt_period_get(const std::string& period_epoch, std::string& period_id, bool staging, std::string& realm_id,
                          std::string& realm_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_get_current(const std::string& realm_id, const std::string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_period_list(RGWRados *store, Formatter *formatter);

int handle_opt_period_pull(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                           const std::string& realm_name, const std::string& url, const std::string& access_key, const std::string& secret_key,
                           std::string& remote, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_push(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                           const std::string& realm_name, const std::string& url, const std::string& access_key, const std::string& secret_key,
                           CephContext *context, RGWRados *store);

int handle_opt_period_commit(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                             const std::string& realm_name, const std::string& url, const std::string& access_key,
                             const std::string& secret_key, const std::string& remote, bool yes_i_really_mean_it,
                             CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_create(const std::string& realm_name, bool set_default, CephContext *context, RGWRados *store,
                            Formatter *formatter);

int handle_opt_realm_delete(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_get(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store,
                         Formatter *formatter);

int handle_opt_realm_get_default(CephContext *context, RGWRados *store);

int handle_opt_realm_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_list_periods(const std::string& realm_id, const std::string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_realm_rename(const std::string& realm_id, const std::string& realm_name, const std::string& realm_new_name,
                            CephContext *context, RGWRados *store);

int handle_opt_realm_set(const std::string& realm_id, const std::string& realm_name, const std::string& infile,
                         bool set_default, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_default(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_pull(const std::string& realm_id, const std::string& realm_name, const std::string& url, const std::string& access_key,
                          const std::string& secret_key, bool set_default, CephContext *context, RGWRados *store,
                          Formatter *formatter);

int handle_opt_zonegroup_add(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& zone_id,
                             const std::string& zone_name, bool tier_type_specified, std::string *tier_type,
                             const map<std::string, std::string, ltstr_nocase>& tier_config_add, bool sync_from_all_specified,
                             bool *sync_from_all, bool redirect_zone_set, std::string *redirect_zone,
                             bool is_master_set, bool *is_master, bool is_read_only_set,
                             bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                             list<std::string>& sync_from_rm, CephContext *context, RGWRados *store,
                             Formatter *formatter);

int handle_opt_zonegroup_create(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                                const std::string& realm_name, const std::string& api_name, bool set_default, bool is_master,
                                const list<std::string>& endpoints, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_default(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                 RGWRados *store);

int handle_opt_zonegroup_delete(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                RGWRados *store);

int handle_opt_zonegroup_get(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                             RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_modify(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                                const std::string& realm_name, const std::string& api_name, const std::string& master_zone,
                                bool is_master_set, bool is_master, bool set_default, const list<std::string>& endpoints,
                                CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_set(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                             const std::string& realm_name, const std::string& infile,  bool set_default, const list<std::string>& endpoints,
                             CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_remove(const std::string& zonegroup_id, const std::string& zonegroup_name, std::string& zone_id,
                                const std::string& zone_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_rename(const std::string& zonegroup_id, const std::string& zonegroup_name,
                                const std::string& zonegroup_new_name, CephContext *context, RGWRados *store);

int handle_opt_zonegroup_placement_list(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                        RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_add(const std::string& placement_id, const std::string& zonegroup_id,
                                       const std::string& zonegroup_name, const list<std::string>& tags, CephContext *context,
                                       RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_modify(const std::string& placement_id, const std::string& zonegroup_id,
                                          const std::string& zonegroup_name, const list<std::string>& tags,
                                          const list<std::string> tags_add, const list<std::string>& tags_rm,
                                          CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_rm(const std::string& placement_id, const std::string& zonegroup_id,
                                      const std::string& zonegroup_name, CephContext *context,
                                      RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_default(const std::string& placement_id, const std::string& zonegroup_id,
                                           const std::string& zonegroup_name, CephContext *context, RGWRados *store,
                                           Formatter *formatter);

int handle_opt_zone_create(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, std::string& realm_id, const std::string& realm_name,
                           const std::string& access_key, const std::string& secret_key, bool tier_type_specified,
                           std::string *tier_type, const map<std::string, std::string, ltstr_nocase>& tier_config_add,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           std::string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                           list<std::string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter);

int handle_opt_zone_default(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                            const std::string& zonegroup_name, CephContext *context, RGWRados *store);

int handle_opt_zone_delete(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, CephContext *context, RGWRados *store);

int handle_opt_zone_get(const std::string& zone_id, const std::string& zone_name, CephContext *context, RGWRados *store,
                        Formatter *formatter);

int handle_opt_zone_set(std::string& zone_name, const std::string& realm_id, const std::string& realm_name, const std::string& infile,
                        bool set_default, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_modify(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, std::string& realm_id, const std::string& realm_name,
                           const std::string& access_key, const std::string& secret_key, bool tier_type_specified,
                           std::string *tier_type, const map<std::string, std::string, ltstr_nocase>& tier_config_add,
                           const map<std::string, std::string, ltstr_nocase>& tier_config_rm,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           std::string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                           list<std::string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter);

int handle_opt_zone_rename(const std::string& zone_id, const std::string& zone_name, const std::string& zone_new_name,
                           const std::string& zonegroup_id, const std::string& zonegroup_name,
                           CephContext *context, RGWRados *store);

int handle_opt_zone_placement_list(const std::string& zone_id, const std::string& zone_name, CephContext *context,
                                   RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_add(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                  const boost::optional<std::string>& compression_type, const boost::optional<std::string>& index_pool,
                                  const boost::optional<std::string>& data_pool, const boost::optional<std::string>& data_extra_pool,
                                  bool index_type_specified, RGWBucketIndexType placement_index_type,
                                  CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_modify(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                     const boost::optional<std::string>& compression_type, const boost::optional<std::string>& index_pool,
                                     const boost::optional<std::string>& data_pool, const boost::optional<std::string>& data_extra_pool,
                                     bool index_type_specified, RGWBucketIndexType placement_index_type,
                                     CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_rm(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                 const boost::optional<std::string>& compression_type, CephContext *context, RGWRados *store,
                                 Formatter *formatter);

int handle_opt_metadata_sync_status(RGWRados *store, Formatter *formatter);

int handle_opt_metadata_sync_init(RGWRados *store);

int handle_opt_metadata_sync_run(RGWRados *store);

int handle_opt_data_sync_status(const std::string& source_zone, RGWRados *store, Formatter *formatter);

int handle_opt_data_sync_init(const std::string& source_zone, const boost::intrusive_ptr<CephContext>& cct, RGWRados *store);

int handle_opt_data_sync_run(const std::string& source_zone, const boost::intrusive_ptr<CephContext>& cct, RGWRados *store);


#endif //CEPH_RGW_ADMIN_MULTISITE_H
