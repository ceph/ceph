// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ADMIN_ARGUMENT_PARSING_H
#define CEPH_RGW_ADMIN_ARGUMENT_PARSING_H

#include "rgw_admin_common.h"
#include "rgw_user.h"

void usage();

int get_cmd(const char *cmd, const char *prev_cmd, const char *prev_prev_cmd, bool *need_more);

int parse_command(const string& access_key, int gen_access_key, const string& secret_key, int gen_secret_key,
                  vector<const char*>& args, int& opt_cmd, string& metadata_key, string& tenant, rgw_user& user_id);

int parse_commandline_parameters(vector<const char*>& args, rgw_user& user_id, string& tenant, string& access_key,
                                 string& subuser, string& secret_key, string& user_email, RGWUserAdminOpState& user_op,
                                 string& display_name, string& bucket_name, string& pool_name, rgw_pool& pool,
                                 string& object, string& object_version, string& client_id, string& op_id,
                                 string& state_str, string& op_mask_str, int& key_type, string& job_id,
                                 int& gen_access_key, int& gen_secret_key, int& show_log_entries, int& show_log_sum,
                                 int& skip_zero_entries, int& admin, bool& admin_specified, int& system,
                                 bool& system_specified, int& verbose, int& staging, int& commit,
                                 uint64_t& min_rewrite_size, uint64_t& max_rewrite_size,
                                 uint64_t& min_rewrite_stripe_size, int& max_buckets, bool& max_buckets_specified,
                                 int& max_entries, bool& max_entries_specified, int64_t& max_size, bool& have_max_size,
                                 int64_t& max_objects, bool& have_max_objects, string& date, string& start_date,
                                 string& end_date, int& num_shards, bool& num_shards_specified, int& max_concurrent_ios,
                                 uint64_t& orphan_stale_secs, int& shard_id, bool& specified_shard_id,
                                 string& daemon_id, bool& specified_daemon_id, string& access, uint32_t& perm_mask,
                                 bool& set_perm, map<int, string>& temp_url_keys, bool& set_temp_url_key,
                                 string& bucket_id, string& format, map<string, bool>& categories,
                                 int& delete_child_objects, int& pretty_format, int& purge_data, int& purge_keys,
                                 int& yes_i_really_mean_it, int& fix, int& remove_bad, int& check_head_obj_locator,
                                 int& check_objects, int& sync_stats, int& include_all, int& extra_info, int& bypass_gc,
                                 int& warnings_only, int& inconsistent_index, string& caps, string& infile,
                                 string& metadata_key, string& marker, string& start_marker, string& end_marker,
                                 string& quota_scope, string& replica_log_type_str, ReplicaLogType& replica_log_type,
                                 BIIndexType& bi_index_type, bool& is_master, bool& is_master_set, int& set_default,
                                 string& redirect_zone, bool& redirect_zone_set, bool& read_only, int& is_read_only_set,
                                 string& master_zone, string& period_id, string& period_epoch, string& remote,
                                 string& url, string& realm_id,string& realm_new_name, string& zonegroup_id,
                                 string& zonegroup_new_name, string& placement_id, list<string>& tags,
                                 list<string>& tags_add, list<string>& tags_rm, string& api_name, string& zone_id,
                                 string& zone_new_name, list<string>& endpoints, list<string>& sync_from,
                                 list<string>& sync_from_rm, bool& sync_from_all, int& sync_from_all_specified,
                                 string& source_zone_name, string& tier_type, bool& tier_type_specified,
                                 map<string, string, ltstr_nocase>& tier_config_add,
                                 map<string, string, ltstr_nocase>& tier_config_rm, boost::optional<string>& index_pool,
                                 boost::optional<string>& data_pool, boost::optional<string>& data_extra_pool,
                                 RGWBucketIndexType& placement_index_type, bool& index_type_specified,
                                 boost::optional<string>& compression_type, string& role_name, string& path,
                                 string& assume_role_doc, string& policy_name, string& perm_policy_doc,
                                 string& path_prefix);

#endif //CEPH_RGW_ADMIN_ARGUMENT_PARSING_H