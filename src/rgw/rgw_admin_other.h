#ifndef CEPH_RGW_ADMIN_OTHER_H
#define CEPH_RGW_ADMIN_OTHER_H

#include <string>
#include <map>
#include "rgw_rados.h"
#include "rgw_admin_argument_parsing.h"
#include "rgw_usage.h"
#include "rgw_lc.h"
#include "rgw_sync.h"
#include "rgw_bucket.h"
#include "rgw_replica_log.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// Pool, log, usage, olh, gc, lc, metadata (except metadata sync), user list, mdlog, sync error,
// datalog, opstate, sync status.

int handle_opt_pool_add(const std::string& pool_name, rgw_pool& pool, RGWRados *store);

int handle_opt_pool_rm(const std::string& pool_name, rgw_pool& pool, RGWRados *store);

int handle_opt_pools_list(RGWRados *store, Formatter *formatter);

int handle_opt_log_list(const std::string& date, RGWRados *store, Formatter *formatter);

int handle_opt_log_show(const std::string& object, const std::string& date,
                        const std::string& bucket_id, const std::string& bucket_name, bool show_log_entries,
                        bool skip_zero_entries,  bool show_log_sum, RGWRados *store, Formatter *formatter);

int handle_opt_log_rm(const std::string& object, const std::string& date,
                      const std::string& bucket_id, const std::string& bucket_name, RGWRados *store);

int handle_opt_usage_show(rgw_user& user_id, const std::string& start_date, const std::string& end_date,
                          bool show_log_entries, bool show_log_sum, RGWFormatterFlusher& flusher,
                          std::map<std::string, bool> *categories, RGWRados *store);

int handle_opt_usage_trim(rgw_user& user_id, const std::string& start_date, const std::string& end_date,
                          bool yes_i_really_mean_it, RGWRados *store);

int handle_opt_usage_clear(bool yes_i_really_mean_it, RGWRados *store);

int handle_opt_olh_get(const std::string& tenant, const std::string& bucket_id, const std::string& bucket_name,
                       const std::string& object, rgw_bucket& bucket, RGWRados *store, Formatter *formatter);

int handle_opt_olh_readlog(const std::string& tenant, const std::string& bucket_id, const std::string& bucket_name,
                           const std::string& object, rgw_bucket& bucket, RGWRados *store, Formatter *formatter);

int handle_opt_gc_list(bool include_all, std::string& marker, RGWRados *store, Formatter *formatter);

int handle_opt_gc_process(bool include_all, RGWRados *store);

int handle_opt_lc_list(int max_entries, RGWRados *store, Formatter *formatter);

int handle_opt_lc_process(RGWRados *store);

int handle_opt_metadata_get(std::string& metadata_key, RGWRados *store, Formatter *formatter);

int handle_opt_metadata_put(std::string& metadata_key, std::string& infile, RGWRados *store, Formatter *formatter);

int handle_opt_metadata_rm(std::string& metadata_key, RGWRados *store, Formatter *formatter);

int handle_opt_metadata_list(const std::string& metadata_key, const std::string& marker, bool max_entries_specified,
                             int max_entries, RGWRados *store, Formatter *formatter);

int handle_opt_user_list(const std::string& marker, bool max_entries_specified, int max_entries,
                         RGWRados *store, Formatter *formatter);

int handle_opt_mdlog_list(const std::string& start_date, const std::string& end_date, bool specified_shard_id,
                          int shard_id, const std::string& realm_id, const std::string& realm_name,
                          std::string& marker, std::string& period_id, RGWRados *store, Formatter *formatter);

int handle_opt_mdlog_status(bool specified_shard_id, int shard_id, const std::string& realm_id,
                            const std::string& realm_name, std::string& marker, std::string& period_id,
                            RGWRados *store, Formatter *formatter);

int handle_opt_mdlog_autotrim(RGWRados *store);

int handle_opt_mdlog_trim(const std::string& start_date, const std::string& end_date, bool specified_shard_id,
                          int shard_id, const std::string& start_marker, const std::string& end_marker,
                          std::string& period_id, RGWRados *store);

int handle_opt_sync_error_list(int max_entries, const std::string& start_date, const std::string& end_date,
                               bool specified_shard_id, int shard_id, std::string& marker,
                               RGWRados *store, Formatter *formatter);

int handle_opt_sync_error_trim(const std::string& start_date, const std::string& end_date,
                               bool specified_shard_id, int shard_id, const std::string& start_marker,
                               const std::string& end_marker,
                               RGWRados *store);

int handle_opt_datalog_status(bool specified_shard_id, int shard_id, RGWRados *store, Formatter *formatter);

int handle_opt_datalog_list(int max_entries, const std::string& start_date, const std::string& end_date,
                            bool extra_info, RGWRados *store, Formatter *formatter);

int handle_opt_datalog_trim(const std::string& start_date, const std::string& end_date, const std::string& start_marker,
                            const std::string& end_marker, RGWRados *store);

int handle_opt_opstate_set(const std::string& client_id, const std::string& op_id, const std::string& object,
                           const std::string& state_str, RGWRados *store);

int handle_opt_opstate_renew(const std::string& client_id, const std::string& op_id, const std::string& object,
                             const std::string& state_str, RGWRados *store);

int handle_opt_opstate_list(const std::string& client_id, const std::string& op_id, const std::string& object,
                            RGWRados *store, Formatter *formatter);

int handle_opt_opstate_rm(const std::string& client_id, const std::string& op_id, const std::string& object,
                          RGWRados *store);

int handle_opt_replicalog_get(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                              bool specified_shard_id, int shard_id, const std::string& bucket_id,
                              const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                              rgw_bucket& bucket, RGWRados *store, Formatter *formatter);

int handle_opt_replicalog_delete(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                                 bool specified_shard_id, int shard_id, bool specified_daemon_id,
                                 const std::string& daemon_id, const std::string& bucket_id,
                                 const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                                 rgw_bucket& bucket, RGWRados *store);

int handle_opt_replicalog_update(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                                 const std::string& marker, const std::string& date, const std::string& infile,
                                 bool specified_shard_id, int shard_id, bool specified_daemon_id,
                                 const std::string& daemon_id, const std::string& bucket_id,
                                 const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                                 rgw_bucket& bucket, RGWRados *store);

void handle_opt_sync_status(RGWRados *store);

#endif //CEPH_RGW_ADMIN_OTHER_H
