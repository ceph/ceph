// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cerrno>
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <string>

#include <boost/optional.hpp>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"

#include "cls/rgw/cls_rgw_client.h"

#include "global/global_init.h"


#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl_s3.h"
#include "rgw_lc.h"
#include "rgw_usage.h"
#include "rgw_replica_log.h"
#include "rgw_orphan.h"
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"
#include "rgw_role.h"
#include "rgw_reshard.h"

#include "rgw_admin_argument_parsing.h"
#include "rgw_admin_common.h"
#include "rgw_admin_multisite.h"
#include "rgw_admin_opt_bucket.h"
#include "rgw_admin_opt_role.h"
#include "rgw_admin_opt_quota.h"
#include "rgw_admin_other.h"
#include "rgw_admin_opt_user.h"

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

static RGWRados *store = nullptr;


static void show_user_info(RGWUserInfo& info, Formatter *formatter)
{
  encode_json("user_info", info, formatter);
  formatter->flush(cout);
  cout << std::endl;
}

class StoreDestructor {
  RGWRados *store;
public:
  explicit StoreDestructor(RGWRados *_s) : store(_s) {}
  ~StoreDestructor() {
    RGWStoreManager::close_storage(store);
  }
};

static int check_pool_support_omap(const rgw_pool& pool)
{
  librados::IoCtx io_ctx;
  int ret = store->get_rados_handle()->ioctx_create(pool.to_str().c_str(), io_ctx);
  if (ret < 0) {
     // the pool may not exist at this moment, we have no way to check if it supports omap.
     return 0;
  }

  ret = io_ctx.omap_clear("__omap_test_not_exist_oid__");
  if (ret == -EOPNOTSUPP) {
    io_ctx.close();
    return ret;
  }
  io_ctx.close();
  return 0;
}


#ifdef BUILDING_FOR_EMBEDDED
extern "C" int cephd_rgw_admin(int argc, const char **argv)
#else
int main(int argc, const char **argv)
#endif
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  // for region -> zonegroup conversion (must happen before common_init_finish())
  if (!g_conf->rgw_region.empty() && g_conf->rgw_zonegroup.empty()) {
    g_conf->set_val_or_die("rgw_zonegroup", g_conf->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  rgw_user user_id;
  string tenant;
  std::string access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object;
  rgw_pool pool;
  std::string date, subuser, access, format;
  std::string start_date, end_date;
  std::string period_id, period_epoch, remote, url;
  std::string master_zone;
  std::string realm_name, realm_id, realm_new_name;
  std::string zone_name, zone_id, zone_new_name;
  std::string zonegroup_name, zonegroup_id, zonegroup_new_name;
  std::string api_name;
  std::string role_name, path, assume_role_doc, policy_name, perm_policy_doc, path_prefix;
  std::string redirect_zone;
  bool redirect_zone_set = false;
  list<string> endpoints;
  int sync_from_all_specified = false;
  bool sync_from_all = false;
  list<string> sync_from;
  list<string> sync_from_rm;
  int set_default = 0;
  bool is_master = false;
  bool is_master_set = false;
  bool read_only = false;
  int is_read_only_set = false;
  int commit = false;
  int staging = false;
  int key_type = KEY_TYPE_UNDEFINED;
  rgw_bucket bucket;
  uint32_t perm_mask = 0;
  RGWUserInfo info;
  int opt_cmd = OPT_NO_CMD;
  int gen_access_key = 0;
  int gen_secret_key = 0;
  bool set_perm = false;
  bool set_temp_url_key = false;
  map<int, string> temp_url_keys;
  string bucket_id;
  Formatter *formatter = nullptr;
  int purge_data = false;
  int pretty_format = false;
  int show_log_entries = true;
  int show_log_sum = true;
  int skip_zero_entries = false;  // log show
  int purge_keys = false;
  int yes_i_really_mean_it = false;
  int delete_child_objects = false;
  int fix = false;
  int remove_bad = false;
  int check_head_obj_locator = false;
  int max_buckets = -1;
  bool max_buckets_specified = false;
  map<string, bool> categories;
  string caps;
  int check_objects = false;
  RGWUserAdminOpState user_op;
  RGWBucketAdminOpState bucket_op;
  string infile;
  string metadata_key;
  RGWObjVersionTracker objv_tracker;
  string marker;
  string start_marker;
  string end_marker;
  int max_entries = -1;
  bool max_entries_specified = false;
  int admin = false;
  bool admin_specified = false;
  int system = false;
  bool system_specified = false;
  int shard_id = -1;
  bool specified_shard_id = false;
  string daemon_id;
  bool specified_daemon_id = false;
  string client_id;
  string op_id;
  string state_str;
  string replica_log_type_str;
  ReplicaLogType replica_log_type = ReplicaLog_Invalid;
  string op_mask_str;
  string quota_scope;
  string object_version;
  string placement_id;
  list<string> tags;
  list<string> tags_add;
  list<string> tags_rm;

  int64_t max_objects = -1;
  int64_t max_size = -1;
  bool have_max_objects = false;
  bool have_max_size = false;
  int include_all = false;

  int sync_stats = false;
  int bypass_gc = false;
  int warnings_only = false;
  int inconsistent_index = false;

  int verbose = false;

  int extra_info = false;

  uint64_t min_rewrite_size = 4 * 1024 * 1024;
  uint64_t max_rewrite_size = ULLONG_MAX;
  uint64_t min_rewrite_stripe_size = 0;

  BIIndexType bi_index_type = PlainIdx;

  string job_id;
  int num_shards = 0;
  bool num_shards_specified = false;
  int max_concurrent_ios = 32;
  uint64_t orphan_stale_secs = (24 * 3600);

  string err;

  string source_zone_name;
  string source_zone; /* zone id */

  string tier_type;
  bool tier_type_specified = false;

  map<string, string, ltstr_nocase> tier_config_add;
  map<string, string, ltstr_nocase> tier_config_rm;

  boost::optional<string> index_pool;
  boost::optional<string> data_pool;
  boost::optional<string> data_extra_pool;
  RGWBucketIndexType placement_index_type = RGWBIType_Normal;
  bool index_type_specified = false;

  boost::optional<std::string> compression_type;

  int ret = parse_commandline_parameters(args, user_id, tenant, access_key, subuser, secret_key, user_email,user_op,
                                         display_name, bucket_name, pool_name,pool, object, object_version, client_id,
                                         op_id, state_str, op_mask_str, key_type, job_id, gen_access_key,
                                         gen_secret_key, show_log_entries, show_log_sum, skip_zero_entries, admin,
                                         admin_specified, system, system_specified, verbose, staging, commit,
                                         min_rewrite_size, max_rewrite_size, min_rewrite_stripe_size, max_buckets,
                                         max_buckets_specified, max_entries, max_entries_specified, max_size,
                                         have_max_size, max_objects, have_max_objects, date, start_date, end_date,
                                         num_shards, num_shards_specified, max_concurrent_ios, orphan_stale_secs,
                                         shard_id, specified_shard_id, daemon_id, specified_daemon_id, access,
                                         perm_mask, set_perm, temp_url_keys, set_temp_url_key, bucket_id, format,
                                         categories, delete_child_objects, pretty_format, purge_data, purge_keys,
                                         yes_i_really_mean_it, fix, remove_bad, check_head_obj_locator, check_objects,
                                         sync_stats, include_all, extra_info, bypass_gc, warnings_only,
                                         inconsistent_index, caps, infile, metadata_key, marker, start_marker,
                                         end_marker, quota_scope, replica_log_type_str, replica_log_type, bi_index_type,
                                         is_master, is_master_set, set_default, redirect_zone, redirect_zone_set,
                                         read_only, is_read_only_set, master_zone, period_id, period_epoch, remote, url,
                                         realm_id, realm_new_name, zonegroup_id, zonegroup_new_name, placement_id, tags,
                                         tags_add, tags_rm, api_name, zone_id, zone_new_name, endpoints, sync_from,
                                         sync_from_rm, sync_from_all, sync_from_all_specified, source_zone_name,
                                         tier_type, tier_type_specified, tier_config_add, tier_config_rm, index_pool,
                                         data_pool, data_extra_pool, placement_index_type, index_type_specified,
                                         compression_type, role_name, path, assume_role_doc, policy_name,
                                         perm_policy_doc, path_prefix);
  if (ret != 0)
    return ret;

  ret = parse_command(access_key, gen_access_key, secret_key, gen_secret_key, args, opt_cmd, metadata_key, tenant,
                      user_id);
  if (ret != 0)
    return ret;


  // default to pretty json
  if (format.empty()) {
    format = "json";
    pretty_format = true;
  }

  if (format ==  "xml")
    formatter = new XMLFormatter(pretty_format);
  else if (format == "json")
    formatter = new JSONFormatter(pretty_format);
  else {
    cerr << "unrecognized format: " << format << std::endl;
    usage();
    ceph_abort();
  }

  realm_name = g_conf->rgw_realm;
  zone_name = g_conf->rgw_zone;
  zonegroup_name = g_conf->rgw_zonegroup;

  RGWStreamFlusher f(formatter, cout);

  // not a raw op if 'period update' needs to commit to master
  bool raw_period_update = opt_cmd == OPT_PERIOD_UPDATE && !commit;
  std::set<int> raw_storage_ops_list = {OPT_ZONEGROUP_ADD, OPT_ZONEGROUP_CREATE, OPT_ZONEGROUP_DELETE,
			 OPT_ZONEGROUP_GET, OPT_ZONEGROUP_LIST,
                         OPT_ZONEGROUP_SET, OPT_ZONEGROUP_DEFAULT,
			 OPT_ZONEGROUP_RENAME, OPT_ZONEGROUP_MODIFY,
			 OPT_ZONEGROUP_REMOVE,
			 OPT_ZONEGROUP_PLACEMENT_ADD, OPT_ZONEGROUP_PLACEMENT_RM,
			 OPT_ZONEGROUP_PLACEMENT_MODIFY, OPT_ZONEGROUP_PLACEMENT_LIST,
			 OPT_ZONEGROUP_PLACEMENT_DEFAULT,
			 OPT_ZONE_CREATE, OPT_ZONE_DELETE,
                         OPT_ZONE_GET, OPT_ZONE_SET, OPT_ZONE_RENAME,
                         OPT_ZONE_LIST, OPT_ZONE_MODIFY, OPT_ZONE_DEFAULT,
			 OPT_ZONE_PLACEMENT_ADD, OPT_ZONE_PLACEMENT_RM,
			 OPT_ZONE_PLACEMENT_MODIFY, OPT_ZONE_PLACEMENT_LIST,
			 OPT_REALM_CREATE,
			 OPT_PERIOD_DELETE, OPT_PERIOD_GET,
			 OPT_PERIOD_PULL,
			 OPT_PERIOD_GET_CURRENT, OPT_PERIOD_LIST,
			 OPT_GLOBAL_QUOTA_GET, OPT_GLOBAL_QUOTA_SET,
			 OPT_GLOBAL_QUOTA_ENABLE, OPT_GLOBAL_QUOTA_DISABLE,
			 OPT_REALM_DELETE, OPT_REALM_GET, OPT_REALM_LIST,
			 OPT_REALM_LIST_PERIODS,
			 OPT_REALM_GET_DEFAULT,
			 OPT_REALM_RENAME, OPT_REALM_SET,
			 OPT_REALM_DEFAULT, OPT_REALM_PULL};


  bool raw_storage_op = (raw_storage_ops_list.find(opt_cmd) != raw_storage_ops_list.end() ||
                         raw_period_update);

  if (raw_storage_op) {
    store = RGWStoreManager::get_raw_storage(g_ceph_context);
  } else {
    store = RGWStoreManager::get_storage(g_ceph_context, false, false, false, false, false);
  }
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  if (!source_zone_name.empty()) {
    if (!store->find_zone_id_by_name(source_zone_name, &source_zone)) {
      cerr << "WARNING: cannot find source zone id for name=" << source_zone_name << std::endl;
      source_zone = source_zone_name;
    }
  }

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);

  StoreDestructor store_destructor(store);

  if (raw_storage_op) {
    switch (opt_cmd) {
    case OPT_PERIOD_DELETE:
      ret = handle_opt_period_delete(period_id, g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
      break;
    case OPT_PERIOD_GET:
      ret = handle_opt_period_get(period_epoch, period_id, staging, realm_id, realm_name, g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
      break;
    case OPT_PERIOD_GET_CURRENT:
      ret = handle_opt_period_get_current(realm_id, realm_name, store, formatter);
      if (ret != 0) {
        return ret;
      }
      break;
    case OPT_PERIOD_LIST:
      {
        ret = handle_opt_period_list(store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_PERIOD_UPDATE:
      {
        ret = update_period(store, realm_id, realm_name, period_id, period_epoch,
                                commit, remote, url, access_key, secret_key,
                                formatter, yes_i_really_mean_it);
	if (ret < 0) {
	  return -ret;
	}
      }
      break;
    case OPT_PERIOD_PULL:
      {
        ret = handle_opt_period_pull(period_id, period_epoch, realm_id, realm_name, url, access_key, secret_key,
                                     remote, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_GLOBAL_QUOTA_GET:
    case OPT_GLOBAL_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_DISABLE: {
      ret = handle_opt_global_quota(realm_id, realm_name, have_max_size, max_size, have_max_objects, max_objects,
                                    opt_cmd, quota_scope, store, formatter);
      if (ret != 0) return ret;
      break;
    }
    case OPT_REALM_CREATE:
      {
        ret = handle_opt_realm_create(realm_name, set_default, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_REALM_DELETE:
    {
      ret = handle_opt_realm_delete(realm_id, realm_name, g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_GET:
    {
      ret = handle_opt_realm_get(realm_id, realm_name, g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_GET_DEFAULT:
    {
      ret = handle_opt_realm_get_default(g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_LIST:
    {
      ret = handle_opt_realm_list(g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_LIST_PERIODS:
    {
      ret = handle_opt_realm_list_periods(realm_id, realm_name, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;

    case OPT_REALM_RENAME:
    {
      ret = handle_opt_realm_rename(realm_id, realm_name, realm_new_name, g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_SET:
    {
      ret = handle_opt_realm_set(realm_id, realm_name, infile, set_default, g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;

    case OPT_REALM_DEFAULT:
    {
      ret = handle_opt_realm_default(realm_id, realm_name, g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_REALM_PULL:
      return handle_opt_realm_pull(realm_id, realm_name, url, access_key, secret_key, set_default, g_ceph_context,
                                   store, formatter);
    case OPT_ZONEGROUP_ADD:
      {
	ret = handle_opt_zonegroup_add(zonegroup_id, zonegroup_name, zone_id, zone_name, tier_type_specified, tier_type,
                                       tier_config_add, sync_from_all_specified, sync_from_all, redirect_zone_set,
                                       redirect_zone, is_master_set, is_master, is_read_only_set, read_only, endpoints,
                                       sync_from, sync_from_rm, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_CREATE:
      {
        ret = handle_opt_zonegroup_create(zonegroup_id, zonegroup_name, realm_id, realm_name, api_name, set_default,
                                          is_master, endpoints, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_DEFAULT:
      {
        ret = handle_opt_zonegroup_default(zonegroup_id, zonegroup_name, g_ceph_context, store);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_DELETE:
    {
      ret = handle_opt_zonegroup_delete(zonegroup_id, zonegroup_name, g_ceph_context, store);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_ZONEGROUP_GET:
    {
      ret = handle_opt_zonegroup_get(zonegroup_id, zonegroup_name, g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_ZONEGROUP_LIST:
    {
      ret = handle_opt_zonegroup_list(g_ceph_context, store, formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_ZONEGROUP_MODIFY:
    {
      ret = handle_opt_zonegroup_modify(zonegroup_id, zonegroup_name, realm_id, realm_name, api_name, master_zone,
                                        is_master_set, is_master, set_default, endpoints, g_ceph_context, store,
                                        formatter);
      if (ret != 0) {
        return ret;
      }
    }
      break;
    case OPT_ZONEGROUP_SET:
      {
        ret = handle_opt_zonegroup_set(zonegroup_id, zonegroup_name, realm_id, realm_name, infile, set_default,
                                       endpoints, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_REMOVE:
      {
        ret = handle_opt_zonegroup_remove(zonegroup_id, zonegroup_name, zone_id, zone_name, g_ceph_context, store, formatter);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_RENAME:
      {
	ret = handle_opt_zonegroup_rename(zonegroup_id, zonegroup_name, zonegroup_new_name, g_ceph_context, store);
        if (ret != 0) {
          return ret;
        }
      }
      break;
    case OPT_ZONEGROUP_PLACEMENT_LIST:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("placement_targets", zonegroup.placement_targets, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_PLACEMENT_ADD:
    case OPT_ZONEGROUP_PLACEMENT_MODIFY:
    case OPT_ZONEGROUP_PLACEMENT_RM:
    case OPT_ZONEGROUP_PLACEMENT_DEFAULT:
      {
        if (placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_ADD) {
          RGWZoneGroupPlacementTarget target;
          target.name = placement_id;
          for (auto& t : tags) {
            target.tags.insert(t);
          }
          zonegroup.placement_targets[placement_id] = target;
        } else if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_MODIFY) {
          RGWZoneGroupPlacementTarget& target = zonegroup.placement_targets[placement_id];
          if (!tags.empty()) {
            target.tags.clear();
            for (auto& t : tags) {
              target.tags.insert(t);
            }
          }
          target.name = placement_id;
          for (auto& t : tags_rm) {
            target.tags.erase(t);
          }
          for (auto& t : tags_add) {
            target.tags.insert(t);
          }
        } else if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_RM) {
          zonegroup.placement_targets.erase(placement_id);
        } else if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_DEFAULT) {
          if (!zonegroup.placement_targets.count(placement_id)) {
            cerr << "failed to find a zonegroup placement target named '"
                << placement_id << "'" << std::endl;
            return -ENOENT;
          }
          zonegroup.default_placement = placement_id;
        }

        zonegroup.post_process_params();
        ret = zonegroup.update();
        if (ret < 0) {
          cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("placement_targets", zonegroup.placement_targets, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONE_CREATE:
      {
        if (zone_name.empty()) {
	  cerr << "zone name not provided" << std::endl;
	  return EINVAL;
        }
	int ret;
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
	  ret = zonegroup.init(g_ceph_context, store);
	  if (ret < 0) {
	    cerr << "unable to initialize zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (realm_id.empty() && realm_name.empty()) {
	    realm_id = zonegroup.realm_id;
	  }
	}

	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store, false);
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
          string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
          bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
          string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);
	  ret = zonegroup.add_zone(zone,
                                   (is_master_set ? &is_master : nullptr),
                                   (is_read_only_set ? &read_only : nullptr),
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
      }
      break;
    case OPT_ZONE_DEFAULT:
      {
	RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
	}
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONE_DELETE:
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

        for (auto iter = zonegroups.begin(); iter != zonegroups.end(); ++iter) {
          RGWZoneGroup zonegroup(string(), *iter);
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
      }
      break;
    case OPT_ZONE_GET:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("zone", zone, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONE_SET:
      {
	RGWZoneParams zone(zone_name);
	int ret = zone.init(g_ceph_context, store, false);
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
	  int ret = realm.init(g_ceph_context, store);
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
      }
      break;
    case OPT_ZONE_LIST:
      {
	list<string> zones;
	int ret = store->list_zones(zones);
	if (ret < 0) {
	  cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneParams zone;
	ret = zone.init(g_ceph_context, store, false);
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
      }
      break;
    case OPT_ZONE_MODIFY:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store);
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
          RGWRealm realm{g_ceph_context, store};
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
	ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        string *ptier_type = (tier_type_specified ? &tier_type : nullptr);

        bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        ret = zonegroup.add_zone(zone,
                                 (is_master_set ? &is_master : nullptr),
                                 (is_read_only_set ? &read_only : nullptr),
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
      }
      break;
    case OPT_ZONE_RENAME:
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
	int ret = zone.init(g_ceph_context, store);
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
	ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
	} else {
	  ret = zonegroup.rename_zone(zone);
	  if (ret < 0) {
	    cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}
      }
      break;
    case OPT_ZONE_PLACEMENT_ADD:
    case OPT_ZONE_PLACEMENT_MODIFY:
    case OPT_ZONE_PLACEMENT_RM:
      {
        if (placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }
        // validate compression type
        if (compression_type && *compression_type != "random"
            && !Compressor::get_comp_alg_type(*compression_type)) {
          std::cerr << "Unrecognized compression type" << std::endl;
          return EINVAL;
        }

	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT_ZONE_PLACEMENT_ADD) {
          // pool names are required
          if (!index_pool || index_pool->empty() ||
              !data_pool || data_pool->empty()) {
            cerr << "ERROR: need to specify both --index-pool and --data-pool" << std::endl;
            return EINVAL;
          }

          RGWZonePlacementInfo& placement_info = zone.placement_pools[placement_id];

          placement_info.index_pool = *index_pool;
          placement_info.data_pool = *data_pool;
          if (data_extra_pool) {
            placement_info.data_extra_pool = *data_extra_pool;
          }
          if (index_type_specified) {
            placement_info.index_type = placement_index_type;
          }
          if (compression_type) {
            placement_info.compression_type = *compression_type;
          }

          ret = check_pool_support_omap(placement_info.get_data_extra_pool());
          if (ret < 0) {
             cerr << "ERROR: the data extra (non-ec) pool '" << placement_info.get_data_extra_pool()
                 << "' does not support omap" << std::endl;
             return ret;
          }
        } else if (opt_cmd == OPT_ZONE_PLACEMENT_MODIFY) {
          auto p = zone.placement_pools.find(placement_id);
          if (p == zone.placement_pools.end()) {
            cerr << "ERROR: zone placement target '" << placement_id
                << "' not found" << std::endl;
            return -ENOENT;
          }
          auto& info = p->second;
          if (index_pool && !index_pool->empty()) {
            info.index_pool = *index_pool;
          }
          if (data_pool && !data_pool->empty()) {
            info.data_pool = *data_pool;
          }
          if (data_extra_pool) {
            info.data_extra_pool = *data_extra_pool;
          }
          if (index_type_specified) {
            info.index_type = placement_index_type;
          }
          if (compression_type) {
            info.compression_type = *compression_type;
          }

          ret = check_pool_support_omap(info.get_data_extra_pool());
          if (ret < 0) {
             cerr << "ERROR: the data extra (non-ec) pool '" << info.get_data_extra_pool()
                 << "' does not support omap" << std::endl;
             return ret;
          }
        } else if (opt_cmd == OPT_ZONE_PLACEMENT_RM) {
          zone.placement_pools.erase(placement_id);
        }

        ret = zone.update();
        if (ret < 0) {
          cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zone", zone, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONE_PLACEMENT_LIST:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("placement_pools", zone.placement_pools, formatter);
	formatter->flush(cout);
      }
      break;
    }
    return 0;
  }

  if (!user_id.empty()) {
    user_op.set_user_id(user_id);
    bucket_op.set_user_id(user_id);
  }

  if (!display_name.empty())
    user_op.set_display_name(display_name);

  if (!user_email.empty())
    user_op.set_user_email(user_email);

  if (!access_key.empty())
    user_op.set_access_key(access_key);

  if (!secret_key.empty())
    user_op.set_secret_key(secret_key);

  if (!subuser.empty())
    user_op.set_subuser(subuser);

  if (!caps.empty())
    user_op.set_caps(caps);

  user_op.set_purge_data(purge_data);

  if (purge_keys)
    user_op.set_purge_keys();

  if (gen_access_key)
    user_op.set_generate_key();

  if (gen_secret_key)
    user_op.set_gen_secret(); // assume that a key pair should be created

  if (max_buckets_specified)
    user_op.set_max_buckets(max_buckets);

  if (admin_specified)
     user_op.set_admin(admin);

  if (system_specified)
    user_op.set_system(system);

  if (set_perm)
    user_op.set_perm(perm_mask);

  if (set_temp_url_key) {
    auto iter = temp_url_keys.begin();
    for (; iter != temp_url_keys.end(); ++iter) {
      user_op.set_temp_url_key(iter->second, iter->first);
    }
  }

  if (!op_mask_str.empty()) {
    uint32_t op_mask;
    int ret = rgw_parse_op_type_list(op_mask_str, &op_mask);
    if (ret < 0) {
      cerr << "failed to parse op_mask: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    user_op.set_op_mask(op_mask);
  }

  if (key_type != KEY_TYPE_UNDEFINED)
    user_op.set_key_type(key_type);

  // set suspension operation parameters
  if (opt_cmd == OPT_USER_ENABLE)
    user_op.set_suspension(false);
  else if (opt_cmd == OPT_USER_SUSPEND)
    user_op.set_suspension(true);

  // RGWUser to use for user operations
  RGWUser user;
  ret = 0;
  if (!user_id.empty() || !subuser.empty()) {
    ret = user.init(store, user_op);
    if (ret < 0) {
      cerr << "user.init failed: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  /* populate bucket operation */
  bucket_op.set_bucket_name(bucket_name);
  bucket_op.set_object(object);
  bucket_op.set_check_objects(check_objects);
  bucket_op.set_delete_children(delete_child_objects);
  bucket_op.set_fix_index(fix);
  bucket_op.set_max_aio(max_concurrent_ios);

  // required to gather errors from operations
  std::string err_msg;

  bool output_user_info = true;

  switch (opt_cmd) {
  case OPT_USER_INFO:
    break;
  case OPT_USER_CREATE:
    ret = handle_opt_user_create(subuser, user_op, user);
    if (ret != 0) {
      return ret;
    }
    break;
  case OPT_USER_RM:
    ret = user.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove user: " << err_msg << std::endl;
      return -ret;
    }

    output_user_info = false;
    break;
  case OPT_USER_ENABLE:
  case OPT_USER_SUSPEND:
  case OPT_USER_MODIFY:
    ret = user.modify(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not modify user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_CREATE:
    ret = user.subusers.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_MODIFY:
    ret = user.subusers.modify(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not modify subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_RM:
    ret = user.subusers.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_CAPS_ADD:
    ret = user.caps.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not add caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_CAPS_RM:
    ret = user.caps.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_KEY_CREATE:
    ret = user.keys.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create key: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_KEY_RM:
    ret = user.keys.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove key: " << err_msg << std::endl;
      return -ret;
    }
    break;
  case OPT_PERIOD_PUSH:
    return handle_opt_period_push(period_id, period_epoch, realm_id, realm_name, url, access_key, secret_key,
                                 g_ceph_context, store);
  case OPT_PERIOD_UPDATE:
      return update_period(store, realm_id, realm_name, period_id, period_epoch,
                              commit, remote, url, access_key, secret_key,
                              formatter, yes_i_really_mean_it);
  case OPT_PERIOD_COMMIT:
    return handle_opt_period_commit(period_id, period_epoch, realm_id, realm_name, url, access_key, secret_key,
                                   remote, yes_i_really_mean_it, g_ceph_context, store, formatter);
  case OPT_ROLE_CREATE: return handle_opt_role_create(role_name, assume_role_doc, path, tenant,
                                                      g_ceph_context, store, formatter);
  case OPT_ROLE_DELETE: return handle_opt_role_delete(role_name, tenant, g_ceph_context, store);
  case OPT_ROLE_GET: return handle_opt_role_get(role_name, tenant, g_ceph_context, store, formatter);
  case OPT_ROLE_MODIFY: return handle_opt_role_modify(role_name, tenant, assume_role_doc,
                                                      g_ceph_context, store);
  case OPT_ROLE_LIST: return handle_opt_role_list(path_prefix, tenant, g_ceph_context, store, formatter);
  case OPT_ROLE_POLICY_PUT: return handle_opt_role_policy_put(role_name, policy_name, perm_policy_doc,
                                                              tenant, g_ceph_context, store);
  case OPT_ROLE_POLICY_LIST: return handle_opt_role_policy_list(role_name, tenant, g_ceph_context, store, formatter);
  case OPT_ROLE_POLICY_GET: return handle_opt_role_policy_get(role_name, policy_name, tenant, g_ceph_context,
                                                              store, formatter);
  case OPT_ROLE_POLICY_DELETE: return handle_opt_role_policy_delete(role_name, policy_name, tenant,
                                                                    g_ceph_context, store);
  default:
    output_user_info = false;
  }

  // output the result of a user operation
  if (output_user_info) {
    ret = user.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }
    show_user_info(info, formatter);
  }

  if (opt_cmd == OPT_POLICY) {
    if (format == "xml") {
      int ret = RGWBucketAdminOp::dump_s3_policy(store, bucket_op, cout);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = RGWBucketAdminOp::get_policy(store, bucket_op, f);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (opt_cmd == OPT_BUCKET_LIMIT_CHECK) {
    return handle_opt_bucket_limit_check(user_id, warnings_only, bucket_op, f, store);
  }

  if (opt_cmd == OPT_BUCKETS_LIST) {
    ret = handle_opt_buckets_list(bucket_name, tenant, bucket_id, marker, max_entries, bucket, bucket_op,
                                   f, store, formatter);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_BUCKET_STATS) {
    ret = handle_opt_bucket_stats(bucket_op, f, store);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
    ret = handle_opt_bucket_link(bucket_id, bucket_op, store);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    ret = handle_opt_bucket_unlink(bucket_op, store);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_LOG_LIST) {
    ret = handle_opt_log_list(date, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_LOG_SHOW || opt_cmd == OPT_LOG_RM) {
    ret = handle_opt_log_show_rm(opt_cmd, object, date, bucket_id, bucket_name, show_log_entries, skip_zero_entries,
                                 show_log_sum, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_POOL_ADD) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      usage();
      ceph_abort();
    }

    int ret = store->add_bucket_placement(pool);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOL_RM) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      usage();
      ceph_abort();
    }

    int ret = store->remove_bucket_placement(pool);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOLS_LIST) {
    set<rgw_pool> pools;
    int ret = store->list_placement_set(pools);
    if (ret < 0) {
      cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->reset();
    formatter->open_array_section("pools");
    for (const auto &pool : pools) {
      formatter->open_object_section("pool");
      formatter->dump_string("name", pool.to_str());
      formatter->close_section();
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_USAGE_SHOW) {
    ret = handle_opt_usage_show(user_id, start_date, end_date, show_log_entries, show_log_sum, f, &categories, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_USAGE_TRIM) {
    ret = handle_opt_usage_trim(user_id, start_date, end_date, yes_i_really_mean_it, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OLH_GET) {
    ret = handle_opt_olh_get(tenant, bucket_id, bucket_name, object, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OLH_READLOG) {
    ret = handle_opt_olh_readlog(tenant, bucket_id, bucket_name, object, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BI_GET) {
    ret = handle_opt_bi_get(object, bucket_id, bucket_name, tenant, bi_index_type, object_version, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BI_PUT) {
    ret = handle_opt_bi_put(bucket_id, bucket_name, tenant, infile, object_version, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BI_LIST) {
    ret = handle_opt_bi_list(bucket_id, bucket_name, tenant, max_entries, object, marker, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BI_PURGE) {
    ret = handle_opt_bi_purge(bucket_id, bucket_name, tenant, yes_i_really_mean_it, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_RM) {
    ret = handle_opt_object_rm(bucket_id, bucket_name, tenant, object, object_version, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_REWRITE) {
    ret = handle_opt_object_rewrite(bucket_id, bucket_name, tenant, object, object_version, min_rewrite_stripe_size,
                                    bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECTS_EXPIRE) {
    ret = handle_opt_object_expire(store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BUCKET_REWRITE) {
    ret = handle_opt_bucket_rewrite(bucket_name, tenant, bucket_id, start_date, end_date, min_rewrite_size,
                                    max_rewrite_size, min_rewrite_stripe_size, bucket, store, formatter);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_BUCKET_RESHARD) {
    return handle_opt_bucket_reshard(bucket_name, tenant, bucket_id, num_shards_specified, num_shards,
                                     yes_i_really_mean_it, max_entries, verbose, store, formatter);
  }

  if (opt_cmd == OPT_RESHARD_ADD) {
    return handle_opt_reshard_add(bucket_id, bucket_name, tenant, num_shards_specified, num_shards,
                                  yes_i_really_mean_it, store);
  }

  if (opt_cmd == OPT_RESHARD_LIST) {
    return handle_opt_reshard_list(max_entries, store, formatter);
  }


  if (opt_cmd == OPT_RESHARD_STATUS) {
    ret = handle_opt_reshard_status(bucket_id, bucket_name, tenant, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_RESHARD_PROCESS) {
    ret = handle_opt_reshard_process(store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_RESHARD_CANCEL) {
    ret = handle_opt_reshard_cancel(bucket_name, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_UNLINK) {
    ret = handle_opt_object_unlink(bucket_id, bucket_name, tenant, object, object_version, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_STAT) {
    ret = handle_opt_object_stat(bucket_id, bucket_name, tenant, object, object_version, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BUCKET_CHECK) {
    ret = handle_opt_bucket_check(check_head_obj_locator, bucket_name, tenant, fix, remove_bad,
                                  bucket_op, f, store, formatter);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_BUCKET_RM) {
    ret = handle_opt_bucket_rm(inconsistent_index, bypass_gc, yes_i_really_mean_it, bucket_op, store);
    if (ret != 0)
      return ret;
  }

  if (opt_cmd == OPT_GC_LIST) {
    ret = handle_opt_gc_list(include_all, marker, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_GC_PROCESS) {
    ret = handle_opt_gc_process(include_all, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_LC_LIST) {
    ret = handle_opt_lc_list(max_entries, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }


  if (opt_cmd == OPT_LC_PROCESS) {
    ret = handle_opt_lc_process(store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_ORPHANS_FIND) {
    RGWOrphanSearch search(store, max_concurrent_ios, orphan_stale_secs);

    if (job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    if (pool_name.empty()) {
      cerr << "ERROR: --pool not specified" << std::endl;
      return EINVAL;
    }

    RGWOrphanSearchInfo info;

    info.pool = pool;
    info.job_name = job_id;
    info.num_shards = num_shards;

    int ret = search.init(job_id, &info);
    if (ret < 0) {
      cerr << "could not init search, ret=" << ret << std::endl;
      return -ret;
    }
    ret = search.run();
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT_ORPHANS_FINISH) {
    RGWOrphanSearch search(store, max_concurrent_ios, orphan_stale_secs);

    if (job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    int ret = search.init(job_id, nullptr);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "job not found" << std::endl;
      }
      return -ret;
    }
    ret = search.finish();
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT_ORPHANS_LIST_JOBS){
    RGWOrphanStore orphan_store(store);
    int ret = orphan_store.init();
    if (ret < 0){
      cerr << "connection to cluster failed!" << std::endl;
      return -ret;
    }

    map <string,RGWOrphanSearchState> m;
    ret = orphan_store.list_jobs(m);
    if (ret < 0) {
      cerr << "job list failed" << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    for (const auto &it: m){
      if (!extra_info){
	formatter->dump_string("job-id",it.first);
      } else {
	encode_json("orphan_search_state", it.second, formatter);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_USER_CHECK) {
    check_bad_user_bucket_mapping(store, user_id, fix);
  }

  if (opt_cmd == OPT_USER_STATS) {
    ret = handle_opt_user_stats(sync_stats, bucket_name, tenant, user_id, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_METADATA_GET) {
    ret = handle_opt_metadata_get(metadata_key, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_METADATA_PUT) {
    ret = handle_opt_metadata_put(metadata_key, infile, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_METADATA_RM) {
    ret = handle_opt_metadata_rm(metadata_key, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_METADATA_LIST || opt_cmd == OPT_USER_LIST) {
    if (opt_cmd == OPT_USER_LIST) {
      metadata_key = "user";
    }
    void *handle;
    int max = 1000;
    int ret = store->meta_mgr->list_keys_init(metadata_key, marker, &handle);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated;
    uint64_t count = 0;

    if (max_entries_specified) {
      formatter->open_object_section("result");
    }
    formatter->open_array_section("keys");

    uint64_t left;
    do {
      list<string> keys;
      left = (max_entries_specified ? max_entries - count : max);
      ret = store->meta_mgr->list_keys_next(handle, left, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      } if (ret != -ENOENT) {
	for (auto iter = keys.begin(); iter != keys.end(); ++iter) {
	  formatter->dump_string("key", *iter);
          ++count;
	}
	formatter->flush(cout);
      }
    } while (truncated && left > 0);

    formatter->close_section();

    if (max_entries_specified) {
      encode_json("truncated", truncated, formatter);
      encode_json("count", count, formatter);
      if (truncated) {
        encode_json("marker", store->meta_mgr->get_marker(handle), formatter);
      }
      formatter->close_section();
    }
    formatter->flush(cout);

    store->meta_mgr->list_keys_complete(handle);
  }

  if (opt_cmd == OPT_MDLOG_LIST) {
    ret = handle_opt_mdlog_list(start_date, end_date, specified_shard_id, shard_id, realm_id, realm_name, marker,
                                period_id, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_MDLOG_STATUS) {
    ret = handle_opt_mdlog_status(specified_shard_id, shard_id, realm_id, realm_name, marker, period_id, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_MDLOG_AUTOTRIM) {
    ret = handle_opt_mdlog_autotrim(store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_MDLOG_TRIM) {
    ret = handle_opt_mdlog_trim(start_date, end_date, specified_shard_id, shard_id, start_marker, end_marker, period_id, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_SYNC_STATUS) {
    handle_opt_sync_status(store);
  }

  if (opt_cmd == OPT_METADATA_SYNC_STATUS) {
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

  }

  if (opt_cmd == OPT_METADATA_SYNC_INIT) {
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
  }


  if (opt_cmd == OPT_METADATA_SYNC_RUN) {
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
  }

  if (opt_cmd == OPT_DATA_SYNC_STATUS) {
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
  }

  if (opt_cmd == OPT_DATA_SYNC_INIT) {
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
  }

  if (opt_cmd == OPT_DATA_SYNC_RUN) {
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
  }

  if (opt_cmd == OPT_BUCKET_SYNC_INIT) {
    ret = handle_opt_bucket_sync_init(source_zone, bucket_name, bucket_id, tenant, bucket_op, store);
    if (ret != 0)
      return ret;
  }

  if ((opt_cmd == OPT_BUCKET_SYNC_DISABLE) || (opt_cmd == OPT_BUCKET_SYNC_ENABLE)) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    if (ret < 0) {
      cerr << "could not init realm " << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    RGWPeriod period;
    ret = period.init(g_ceph_context, store, realm_id, realm_name, true);
    if (ret < 0) {
      cerr << "failed to init period " << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!store->is_meta_master()) {
      cerr << "failed to update bucket sync: only allowed on meta master zone "  << std::endl;
      cerr << period.get_master_zone() << " | " << period.get_realm() << std::endl;
      return EINVAL;
    }

    rgw_obj obj(bucket, object);
    ret = set_bucket_sync_enabled(store, opt_cmd, tenant, bucket_name);
    if (ret < 0)
      return -ret;
}

  if (opt_cmd == OPT_BUCKET_SYNC_STATUS) {
    ret = handle_opt_bucket_sync_status(source_zone, bucket_name, bucket_id, tenant, bucket_op, store, formatter);
    if (ret != 0)
      return ret;
  }

 if (opt_cmd == OPT_BUCKET_SYNC_RUN) {
   ret = handle_opt_bucket_sync_run(source_zone, bucket_name, bucket_id, tenant, bucket_op, store);
   if (ret != 0)
     return ret;
 }

  if (opt_cmd == OPT_BILOG_LIST) {
    ret = handle_opt_bilog_list(bucket_id, bucket_name, tenant, max_entries, shard_id, marker, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_SYNC_ERROR_LIST) {
    ret = handle_opt_sync_error_list(max_entries, start_date, end_date, specified_shard_id, shard_id, marker, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_SYNC_ERROR_TRIM) {
    ret = handle_opt_sync_error_trim(start_date, end_date, specified_shard_id, shard_id, start_marker, end_marker, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BILOG_TRIM) {
    ret = handle_opt_bilog_trim(bucket_id, bucket_name, tenant, shard_id, start_marker, end_marker, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BILOG_STATUS) {
    ret = handle_opt_bilog_status(bucket_id, bucket_name, tenant, shard_id, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_BILOG_AUTOTRIM) {
    ret = handle_opt_bilog_autotrim(store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_DATALOG_LIST) {
    ret = handle_opt_datalog_list(max_entries, start_date, end_date, extra_info, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_DATALOG_STATUS) {
    RGWDataChangesLog *log = store->data_log;
    int i = (specified_shard_id ? shard_id : 0);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_data_log_num_shards; i++) {
      list<cls_log_entry> entries;

      RGWDataChangesLogInfo info;
      log->get_info(i, &info);

      ::encode_json("info", info, formatter);

      if (specified_shard_id)
        break;
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_DATALOG_TRIM) {
    ret = handle_opt_datalog_trim(start_date, end_date, start_marker, end_marker, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OPSTATE_LIST) {
    ret = handle_opt_opstate_list(client_id, op_id, object, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_OPSTATE_SET || opt_cmd == OPT_OPSTATE_RENEW) {
    RGWOpState oc(store);

    RGWOpState::OpState state;
    if (object.empty() || client_id.empty() || op_id.empty()) {
      cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
      return EINVAL;
    }
    if (state_str.empty()) {
      cerr << "ERROR: state was not specified" << std::endl;
      return EINVAL;
    }
    int ret = oc.state_from_str(state_str, &state);
    if (ret < 0) {
      cerr << "ERROR: invalid state: " << state_str << std::endl;
      return -ret;
    }

    if (opt_cmd == OPT_OPSTATE_SET) {
      ret = oc.set_state(client_id, op_id, object, state);
      if (ret < 0) {
        cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ret = oc.renew_state(client_id, op_id, object, state);
      if (ret < 0) {
        cerr << "ERROR: failed to renew state: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }
  if (opt_cmd == OPT_OPSTATE_RM) {
    ret = handle_opt_opstate_rm(client_id, op_id, object, store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_GET) {
    ret = handle_opt_replicalog_get(replica_log_type_str, replica_log_type, specified_shard_id, shard_id, bucket_id,
                                    bucket_name, tenant, pool, bucket, store, formatter);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_DELETE) {
    ret = handle_opt_replicalog_delete(replica_log_type_str, replica_log_type, specified_shard_id, shard_id,
                                       specified_daemon_id, daemon_id, bucket_id, bucket_name, tenant, pool, bucket,
                                       store);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_UPDATE) {
    ret = handle_opt_replicalog_update(replica_log_type_str, replica_log_type, marker, date, infile,
                                       specified_shard_id, shard_id, specified_daemon_id, daemon_id, bucket_id,
                                       bucket_name, tenant, pool, bucket, store);
    if (ret != 0) {
      return ret;
    }
  }

  bool quota_op = (opt_cmd == OPT_QUOTA_SET || opt_cmd == OPT_QUOTA_ENABLE || opt_cmd == OPT_QUOTA_DISABLE);

  if (quota_op)
    return handle_opt_quota(user_id, bucket_name, tenant, have_max_size, max_size, have_max_objects, max_objects,
                            opt_cmd, quota_scope, user, user_op, store);

  return 0;
}
