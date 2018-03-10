// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ADMIN_ARGUMENT_PARSING_H
#define CEPH_RGW_ADMIN_ARGUMENT_PARSING_H

#include "rgw_admin_common.h"
#include "rgw_user.h"

void usage();

int parse_command(const std::string& access_key, int gen_access_key, const std::string& secret_key, int gen_secret_key,
                  vector<const char*>& args, RgwAdminCommand& opt_cmd, std::string& metadata_key, std::string& tenant, rgw_user& user_id);

int parse_common_commandline_params(std::vector<const char*>& args, rgw_user& user_id,
                                    std::string& access_key, int& gen_access_key,
                                    std::string& secret_key, int& gen_secret_key,
                                    std::string& metadata_key, std::string& tenant, std::string& date,
                                    std::string& start_date, std::string& end_date,
                                    std::string& infile, std::string& source_zone_name,
                                    std::string& bucket_id, std::string& bucket_name,
                                    std::string& start_marker, std::string& end_marker,
                                    std::string& marker, int& yes_i_really_mean_it,
                                    int& max_entries, bool& max_entries_specified,
                                    std::string& object, int& shard_id, bool& specified_shard_id,
                                    int& fix, std::string& period_id, std::string& realm_id,
                                    std::string& realm_name, std::string& format, int& pretty_format,
                                    int& purge_data, int& delete_child_objects, int& max_concurrent_ios);

int parse_bucket_commandline_params(std::vector<const char*>& args, int& num_shards,
                                    bool& num_shards_specified, std::string& object_version,
                                    BIIndexType& bi_index_type, int& verbose, int& warnings_only,
                                    int& bypass_gc, int& check_head_obj_locator, int& remove_bad,
                                    int& check_objects, int& inconsistent_index,
                                    uint64_t& min_rewrite_size, uint64_t& max_rewrite_size,
                                    uint64_t& min_rewrite_stripe_size);

int parse_multisite_commandline_params(std::vector<const char*>& args, int& set_default,
                                       std::string& url, int& commit, std::string& period_epoch,
                                       std::string& remote, int& staging, std::string& realm_new_name,
                                       std::string& api_name, boost::optional<std::string>& compression_type,
                                       boost::optional<std::string>& index_pool,
                                       boost::optional<std::string>& data_pool,
                                       boost::optional<std::string>& data_extra_pool,
                                       std::list<std::string>& endpoints,
                                       RGWBucketIndexType& placement_index_type, bool& index_type_specified,
                                       bool& is_master, bool& is_master_set, bool& read_only,
                                       int& is_read_only_set, std::string& master_zone,
                                       std::string& placement_id, std::string& redirect_zone,
                                       bool& redirect_zone_set, std::list<std::string>& sync_from,
                                       std::list<std::string>& sync_from_rm, bool& sync_from_all,
                                       int& sync_from_all_specified, std::list<std::string>& tags,
                                       std::list<std::string>& tags_add, std::list<std::string>& tags_rm,
                                       std::string& tier_type, bool& tier_type_specified,
                                       map<std::string, std::string, ltstr_nocase>& tier_config_add,
                                       map<std::string, std::string, ltstr_nocase>& tier_config_rm,
                                       std::string& zone_id, std::string& zone_name,
                                       std::string& zone_new_name, std::string& zonegroup_id,
                                       std::string& zonegroup_name, std::string& zonegroup_new_name);

int parse_quota_commandline_params(std::vector<const char*>& args, std::string& quota_scope,
                                   int64_t& max_size, bool& have_max_size,
                                   int64_t& max_objects, bool& have_max_objects);

int parse_role_commandline_params(std::vector<const char*>& args, std::string& role_name,
                                  std::string& policy_name, std::string& assume_role_doc,
                                  std::string& perm_policy_doc, std::string& path,
                                  std::string& path_prefix);

int parse_user_commandline_params(std::vector<const char*>& args,
                                  std::string& subuser, std::string& display_name,
                                  std::string& user_email, RGWUserAdminOpState& user_op,
                                  std::string& caps, std::string& op_mask_str, int& key_type,
                                  int& purge_keys, int& max_buckets, bool& max_buckets_specified,
                                  int& admin, bool& admin_specified, int& system, bool& system_specified,
                                  bool& set_temp_url_key, map<int, std::string>& temp_url_keys,
                                  std::string& access, uint32_t& perm_mask, bool& set_perm,
                                  int& sync_stats);

int parse_other_commandline_params(std::vector<const char*>& args, std::string& pool_name,
                                   rgw_pool& pool, std::string& daemon_id, bool& specified_daemon_id,
                                   std::string& replica_log_type_str, ReplicaLogType& replica_log_type,
                                   int& extra_info, int& include_all, std::string& client_id,
                                   std::string& op_id, std::string& state_str, int& show_log_entries,
                                   int& show_log_sum, int& skip_zero_entries,
                                   std::map<std::string, bool>& categories,
                                   uint64_t& orphan_stale_secs, std::string& job_id);

class RgwAdminCommandGroupHandlerFactory {
public:
  static RgwAdminCommandGroupHandler* get_command_group_handler(std::vector<const char*>& args);

private:
  static const std::unordered_map<std::string, RgwAdminCommandGroup> STR_TO_RGW_COMMAND_GROUP;
  static RgwAdminCommandGroup parse_command_group(std::vector<const char*>& args);
};

#endif //CEPH_RGW_ADMIN_ARGUMENT_PARSING_H