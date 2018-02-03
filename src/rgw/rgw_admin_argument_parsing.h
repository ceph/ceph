// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ADMIN_ARGUMENT_PARSING_H
#define CEPH_RGW_ADMIN_ARGUMENT_PARSING_H

#include "rgw_admin_common.h"
#include "rgw_user.h"

void usage();

int parse_command(const std::string& access_key, int gen_access_key, const std::string& secret_key, int gen_secret_key,
                  vector<const char*>& args, RgwAdminCommand& opt_cmd, std::string& metadata_key, std::string& tenant, rgw_user& user_id);

int parse_commandline_parameters(vector<const char*>& args, rgw_user& user_id, std::string& tenant, std::string& access_key,
                                 std::string& subuser, std::string& secret_key, std::string& user_email, RGWUserAdminOpState& user_op,
                                 std::string& display_name, std::string& bucket_name, std::string& pool_name, rgw_pool& pool,
                                 std::string& object, std::string& object_version, std::string& client_id, std::string& op_id,
                                 std::string& state_str, std::string& op_mask_str, int& key_type, std::string& job_id,
                                 int& gen_access_key, int& gen_secret_key, int& show_log_entries, int& show_log_sum,
                                 int& skip_zero_entries, int& admin, bool& admin_specified, int& system,
                                 bool& system_specified, int& verbose, int& staging, int& commit,
                                 uint64_t& min_rewrite_size, uint64_t& max_rewrite_size,
                                 uint64_t& min_rewrite_stripe_size, int& max_buckets, bool& max_buckets_specified,
                                 int& max_entries, bool& max_entries_specified, int64_t& max_size, bool& have_max_size,
                                 int64_t& max_objects, bool& have_max_objects, std::string& date, std::string& start_date,
                                 std::string& end_date, int& num_shards, bool& num_shards_specified, int& max_concurrent_ios,
                                 uint64_t& orphan_stale_secs, int& shard_id, bool& specified_shard_id,
                                 std::string& daemon_id, bool& specified_daemon_id, std::string& access, uint32_t& perm_mask,
                                 bool& set_perm, map<int, std::string>& temp_url_keys, bool& set_temp_url_key,
                                 std::string& bucket_id, std::string& format, map<std::string, bool>& categories,
                                 int& delete_child_objects, int& pretty_format, int& purge_data, int& purge_keys,
                                 int& yes_i_really_mean_it, int& fix, int& remove_bad, int& check_head_obj_locator,
                                 int& check_objects, int& sync_stats, int& include_all, int& extra_info, int& bypass_gc,
                                 int& warnings_only, int& inconsistent_index, std::string& caps, std::string& infile,
                                 std::string& metadata_key, std::string& marker, std::string& start_marker, std::string& end_marker,
                                 std::string& quota_scope, std::string& replica_log_type_str, ReplicaLogType& replica_log_type,
                                 BIIndexType& bi_index_type, bool& is_master, bool& is_master_set, int& set_default,
                                 std::string& redirect_zone, bool& redirect_zone_set, bool& read_only, int& is_read_only_set,
                                 std::string& master_zone, std::string& period_id, std::string& period_epoch, std::string& remote,
                                 std::string& url, std::string& realm_id,std::string& realm_new_name, std::string& zonegroup_id,
                                 std::string& zonegroup_new_name, std::string& placement_id, list<std::string>& tags,
                                 list<std::string>& tags_add, list<std::string>& tags_rm, std::string& api_name, std::string& zone_id,
                                 std::string& zone_new_name, list<std::string>& endpoints, list<std::string>& sync_from,
                                 list<std::string>& sync_from_rm, bool& sync_from_all, int& sync_from_all_specified,
                                 std::string& source_zone_name, std::string& tier_type, bool& tier_type_specified,
                                 map<std::string, std::string, ltstr_nocase>& tier_config_add,
                                 map<std::string, std::string, ltstr_nocase>& tier_config_rm, boost::optional<std::string>& index_pool,
                                 boost::optional<std::string>& data_pool, boost::optional<std::string>& data_extra_pool,
                                 RGWBucketIndexType& placement_index_type, bool& index_type_specified,
                                 boost::optional<std::string>& compression_type, std::string& role_name, std::string& path,
                                 std::string& assume_role_doc, std::string& policy_name, std::string& perm_policy_doc,
                                 std::string& path_prefix);

#endif //CEPH_RGW_ADMIN_ARGUMENT_PARSING_H