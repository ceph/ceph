// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ADMIN_ARGUMENT_PARSING_H
#define CEPH_RGW_ADMIN_ARGUMENT_PARSING_H

#include "rgw_admin_common.h"
#include "rgw_user.h"

namespace rgw_admin_params {
  struct commandline_parameter {
    const char* name;
    const char* description;
  };

  const commandline_parameter ACCESS_KEY = {"access-key", "S3 access key"};
  const commandline_parameter GEN_ACCESS_KEY = {"gen-access-key", "Generate random access key (for S3)"};
  const commandline_parameter SECRET_KEY = {"secret-key", "Secret key"};
  const commandline_parameter GEN_SECRET_KEY = {"gen-secret", "Generate random secret key"};
  const commandline_parameter BUCKET_ID = {"bucket-id", "Bucket id"};
  const commandline_parameter BUCKET_NAME = {"bucket", "Bucket name"};
  const commandline_parameter DELETE_CHILD_OBJECTS = {"purge-objects",
                                                      "Remove a bucket's objects before deleting it.\n(NOTE: required to delete a non-empty bucket)"};
  const commandline_parameter DATE = {"date", "Date in the format yyyy-mm-dd"};
  const commandline_parameter START_DATE = {"start-date", "Start date in the format yyyy-mm-dd"};
  const commandline_parameter END_DATE = {"end-date", "End date in the format yyyy-mm-dd"};
  const commandline_parameter ENDPOINTS = {"endpoints", "Zone endpoints"};
  const commandline_parameter INFILE = {"infile", "A file to read in when setting data"};
  const commandline_parameter IS_MASTER = {"master", "Set as master"};
  const commandline_parameter MARKER = {"marker", ""};
  const commandline_parameter START_MARKER = {"start-marker", ""};
  const commandline_parameter END_MARKER = {"end-marker", ""};
  const commandline_parameter MAX_ENTRIES = {"max-entries", ""};
  const commandline_parameter MAX_OBJECTS = {"max-objects", "Specify max objects (negative value to disable)"};
  const commandline_parameter MAX_SIZE = {"max-size", "Specify max size (in B/K/M/G/T, negative value to disable)"};
  const commandline_parameter MIN_REWRITE_STRIPE_SIZE = {"min-rewrite-stripe-size",
                                                         "Min stripe size for object rewrite (default 0)"};
  const commandline_parameter MAX_CONCURRENT_IOS = {"max-concurrent-ios",
                                                    "Maximum concurrent ios for orphans find (default: 32)"};
  const commandline_parameter NUM_SHARDS = {"num-shards", "Num of shards to use for keeping the temporary scan info"};
  const commandline_parameter OBJECT = {"object", "Object name"};
  const commandline_parameter OBJECT_VERSION = {"object-version", ""};
  const commandline_parameter PERIOD_ID = {"period", "Period id"};
  const commandline_parameter PLACEMENT_ID = {"placement-id", "Placement id for zonegroup placement commands"};
  const commandline_parameter POOL = {"pool", "Pool name. Also used to scan for leaked rados objects."};
  const commandline_parameter PURGE_DATA = {"purge-data",
                                            "When specified, user removal will also purge all the user data"};
  const commandline_parameter QUOTA_SCOPE = {"quota-scope", "Scope of quota (bucket, user)"};
  const commandline_parameter READ_ONLY = {"read-only", "Set zone as read-only (when adding to zonegroup)"};
  const commandline_parameter REALM_ID = {"realm-id", "Realm id"};
  const commandline_parameter REALM_NAME = {"rgw-realm", "Realm name"};
  const commandline_parameter REDIRECT_ZONE = {"redirect-zone",
                                               "Specify zone id to redirect when response is 404 (not found)"};
  const commandline_parameter ROLE_NAME = {"role-name", "Name of the role to create"};
  const commandline_parameter SET_DEFAULT = {"default", "Set entity (realm, zonegroup, zone) as default"};
  const commandline_parameter SHARD_ID = {"shard-id", ""};
  const commandline_parameter SHOW_LOG_ENTRIES = {"show-log-entries", "Enable/disable dump of log entries on log show"};
  const commandline_parameter SHOW_LOG_SUM = {"show-log-sum", "Enable/disable dump of log summation on log show"};
  const commandline_parameter SOURCE_ZONE = {"source-zone", "Specify the source zone for data sync"};
  const commandline_parameter SYNC_FROM = {"sync-from", "List of zones to sync from"};
  const commandline_parameter SYNC_FROM_ALL = {"sync-from-all",
                                               "Set/reset whether zone syncs from all zonegroup peers"};
  const commandline_parameter SYNC_FROM_RM = {"sync-from-rm", "Remove zones from list of zones to sync from"};
  const commandline_parameter TENANT = {"tenant", "Tenant name"};
  const commandline_parameter TIER_CONFIG_ADD = {"tier-config", "Set zone tier config keys, values"};
  const commandline_parameter TIER_TYPE = {"tier-type", "Zone tier type"};
  const commandline_parameter URL = {"url", ""};
  const commandline_parameter USER_ID = {"uid", "User id"};
  const commandline_parameter YES_I_REALLY_MEAN_IT = {"yes-i-really-mean-it", ""};
  const commandline_parameter ZONEGROUP_ID = {"zonegroup-id", "Zonegroup id"};
  const commandline_parameter ZONEGROUP_NAME = {"rgw-zonegroup", "Zonegroup name"};
  const commandline_parameter ZONE_ID = {"zone-id", "Zone id"};
  const commandline_parameter ZONE_NAME = {"rgw-zone", "Zone name"};
}

void usage();

BIIndexType get_bi_index_type(const std::string& type_str);

void parse_tier_config_param(const std::string& s, map<std::string, std::string, ltstr_nocase>& out);

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
  static const std::unordered_map<std::string, RgwAdminCommandGroup> str_to_rgw_command_group;

  static RgwAdminCommandGroup parse_command_group(std::vector<const char*>& args,
                                                  std::vector<std::string>& command_prefix);
};

#endif //CEPH_RGW_ADMIN_ARGUMENT_PARSING_H
