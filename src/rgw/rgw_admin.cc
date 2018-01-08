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
#include "rgw_admin_multisite.h"
#include "rgw_admin_opt_bucket.h"
#include "rgw_admin_opt_policy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

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

template <class T>
static int read_decode_json(const string& infile, T& t)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}

static int parse_date_str(const string& date_str, utime_t& ut)
{
  uint64_t epoch = 0;
  uint64_t nsec = 0;

  if (!date_str.empty()) {
    int ret = utime_t::parse_date(date_str, &epoch, &nsec);
    if (ret < 0) {
      cerr << "ERROR: failed to parse date: " << date_str << std::endl;
      return -EINVAL;
    }
  }

  ut = utime_t(epoch, nsec);

  return 0;
}

template <class T>
static bool decode_dump(const char *field_name, bufferlist& bl, Formatter *f)
{
  T t;

  bufferlist::iterator iter = bl.begin();

  try {
    ::decode(t, iter);
  } catch (buffer::error& err) {
    return false;
  }

  encode_json(field_name, t, f);

  return true;
}

static bool dump_string(const char *field_name, bufferlist& bl, Formatter *f)
{
  string val;
  if (bl.length() > 0) {
    val.assign(bl.c_str());
  }
  f->dump_string(field_name, val);

  return true;
}

void set_quota_info(RGWQuotaInfo& quota, int opt_cmd, int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects)
{
  switch (opt_cmd) {
    case OPT_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_SET:
      if (have_max_objects) {
        if (max_objects < 0) {
          quota.max_objects = -1;
        } else {
          quota.max_objects = max_objects;
        }
      }
      if (have_max_size) {
        if (max_size < 0) {
          quota.max_size = -1;
        } else {
          quota.max_size = rgw_rounded_kb(max_size) * 1024;
        }
      }
      break;
    case OPT_QUOTA_DISABLE:
    case OPT_GLOBAL_QUOTA_DISABLE:
      quota.enabled = false;
      break;
  }
}

int set_bucket_quota(RGWRados *store, int opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_size, int64_t max_objects,
                     bool have_max_size, bool have_max_objects)
{
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket_info.quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

   r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_user_bucket_quota(int opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                          bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.bucket_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.bucket_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int set_user_quota(int opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                   bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.user_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.user_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int check_min_obj_stripe_size(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, uint64_t min_stripe_size, bool *need_rewrite)
{
  map<string, bufferlist> attrs;
  uint64_t obj_size;

  RGWObjectCtx obj_ctx(store);
  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare();
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_MANIFEST);
  if (iter == attrs.end()) {
    *need_rewrite = (obj_size >= min_stripe_size);
    return 0;
  }

  RGWObjManifest manifest;

  try {
    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    ::decode(manifest, biter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode manifest" << dendl;
    return -EIO;
  }

  map<uint64_t, RGWObjManifestPart>& objs = manifest.get_explicit_objs();
  map<uint64_t, RGWObjManifestPart>::iterator oiter;
  for (oiter = objs.begin(); oiter != objs.end(); ++oiter) {
    RGWObjManifestPart& part = oiter->second;

    if (part.size >= min_stripe_size) {
      *need_rewrite = true;
      return 0;
    }
  }
  *need_rewrite = false;

  return 0;
}

static int read_current_period_id(RGWRados* store, const std::string& realm_id,
                                  const std::string& realm_name,
                                  std::string* period_id)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(g_ceph_context, store);
  if (ret < 0) {
    std::cerr << "failed to read realm: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  *period_id = realm.get_current_period();
  return 0;
}

void flush_ss(stringstream& ss, list<string>& l)
{
  if (!ss.str().empty()) {
    l.push_back(ss.str());
  }
  ss.str("");
}

stringstream& push_ss(stringstream& ss, list<string>& l, int tab = 0)
{
  flush_ss(ss, l);
  if (tab > 0) {
    ss << setw(tab) << "" << setw(1);
  }
  return ss;
}

static void get_md_sync_status(list<string>& status)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    status.push_back(string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0) {
    status.push_back(string("failed to read sync status: ") + cpp_strerror(-ret));
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_meta_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  status.push_back(status_str);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  stringstream ss;
  push_ss(ss, status) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status) << "full sync: " << full_total - full_complete << " entries to sync";
  }

  push_ss(ss, status) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  rgw_mdlog_info log_info;
  ret = sync.read_log_info(&log_info);
  if (ret < 0) {
    status.push_back(string("failed to fetch local sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, RGWMetadataLogInfo> master_shards_info;
  string master_period = store->get_current_period_id();

  ret = sync.read_master_log_shards_info(master_period, &master_shards_info);
  if (ret < 0) {
    status.push_back(string("failed to fetch master sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, string> shards_behind;
  if (sync_status.sync_info.period != master_period) {
    status.emplace_back("master is on a different period: master_period=" +
                        master_period + " local_period=" + sync_status.sync_info.period);
  } else {
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);

      if (iter == master_shards_info.end()) {
        /* huh? */
        derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind[shard_id] = local_iter.second.marker;
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";

    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const string& source_zone, list<string>& status, int tab)
{
  stringstream ss;

  auto ziter = store->zone_by_id.find(source_zone);
  if (ziter == store->zone_by_id.end()) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }
  RGWZone& sz = ziter->second;

  if (!store->zone_syncs_from(store->get_zone(), sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

  int ret = sync.init();
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_data_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  push_ss(ss, status, tab) << status_str;

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  push_ss(ss, status, tab) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status, tab) << "full sync: " << full_total - full_complete << " buckets to sync";
  }

  push_ss(ss, status, tab) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  rgw_datalog_info log_info;
  ret = sync.read_log_info(&log_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to fetch local sync status: ") + cpp_strerror(-ret);
    return;
  }


  map<int, RGWDataChangesLogInfo> source_shards_info;

  ret = sync.read_source_log_shards_info(&source_shards_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    return;
  }

  map<int, string> shards_behind;

  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);

    if (iter == source_shards_info.end()) {
      /* huh? */
      derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind[shard_id] = local_iter.second.marker;
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";

    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status, tab) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  flush_ss(ss, status);
}

static void tab_dump(const string& header, int width, const list<string>& entries)
{
  string s = header;

  for (auto e : entries) {
    cout << std::setw(width) << s << std::setw(1) << " " << e << std::endl;
    s.clear();
  }
}


static void sync_status()
{
  RGWRealm& realm = store->realm;
  RGWZoneGroup& zonegroup = store->get_zonegroup();
  RGWZone& zone = store->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << realm.get_id() << " (" << realm.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone.id << " (" << zone.name << ")" << std::endl;

  list<string> md_status;

  if (store->is_meta_master()) {
    md_status.emplace_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  for (auto iter : store->zone_conn_map) {
    const string& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id;
    auto siter = store->zone_by_id.find(source_id);
    if (siter != store->zone_by_id.end()) {
      s += string(" (") + siter->second.name + ")";
    }
    data_status.push_back(s);
    get_data_sync_status(source_id, data_status, source_str.size());
  }

  tab_dump("data sync", width, data_status);
}

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

int check_reshard_bucket_params(RGWRados *store,
				const string& bucket_name,
				const string& tenant,
				const string& bucket_id,
				bool num_shards_specified,
				int num_shards,
				int yes_i_really_mean_it,
				rgw_bucket& bucket,
				RGWBucketInfo& bucket_info,
				map<string, bufferlist>& attrs)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return -EINVAL;
  }

  if (!num_shards_specified) {
    cerr << "ERROR: --num-shards not specified" << std::endl;
    return -EINVAL;
  }

  if (num_shards > (int)store->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << store->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }

  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  if (num_shards <= num_source_shards && !yes_i_really_mean_it) {
    cerr << "num shards is less or equal to current shards count" << std::endl
	 << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return -EINVAL;
  }
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
      {
	if (period_id.empty()) {
	  cerr << "missing period id" << std::endl;
	  return EINVAL;
	}
	RGWPeriod period(period_id);
	int ret = period.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "period.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = period.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT_PERIOD_GET:
      {
	epoch_t epoch = 0;
	if (!period_epoch.empty()) {
	  epoch = atoi(period_epoch.c_str());
	}
        if (staging) {
          RGWRealm realm(realm_id, realm_name);
          int ret = realm.init(g_ceph_context, store);
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
	int ret = period.init(g_ceph_context, store, realm_id, realm_name);
	if (ret < 0) {
	  cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("period", period, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_PERIOD_GET_CURRENT:
      {
        int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	formatter->open_object_section("period_get_current");
	encode_json("current_period", period_id, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_PERIOD_LIST:
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
      }
      break;
    case OPT_PERIOD_UPDATE:
      {
        int ret = update_period(store, realm_id, realm_name, period_id, period_epoch,
                                commit, remote, url, access_key, secret_key,
                                formatter, yes_i_really_mean_it);
	if (ret < 0) {
	  return -ret;
	}
      }
      break;
    case OPT_PERIOD_PULL:
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
      }
      break;
    case OPT_GLOBAL_QUOTA_GET:
    case OPT_GLOBAL_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_DISABLE:
      {
        if (realm_id.empty()) {
          RGWRealm realm(g_ceph_context, store);
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = realm.read_id(realm_name, realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = realm.read_default_id(realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = period_config.read(store, realm_id);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        formatter->open_object_section("period_config");
        if (quota_scope == "bucket") {
          set_quota_info(period_config.bucket_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("bucket quota", period_config.bucket_quota, formatter);
        } else if (quota_scope == "user") {
          set_quota_info(period_config.user_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("user quota", period_config.user_quota, formatter);
        } else if (quota_scope.empty() && opt_cmd == OPT_GLOBAL_QUOTA_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket quota", period_config.bucket_quota, formatter);
          encode_json("user quota", period_config.user_quota, formatter);
        } else {
          cerr << "ERROR: invalid quota scope specification. Please specify "
              "either --quota-scope=bucket, or --quota-scope=user" << std::endl;
          return EINVAL;
        }
        formatter->close_section();

        if (opt_cmd != OPT_GLOBAL_QUOTA_GET) {
          // write the modified period config
          ret = period_config.write(store, realm_id);
          if (ret < 0) {
            cerr << "ERROR: failed to write period config: "
                << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (!realm_id.empty()) {
            cout << "Global quota changes saved. Use 'period update' to apply "
                "them to the staging period, and 'period commit' to commit the "
                "new period." << std::endl;
          } else {
            cout << "Global quota changes saved. They will take effect as "
                "the gateways are restarted." << std::endl;
          }
        }

        formatter->flush(cout);
      }
      break;
    case OPT_REALM_CREATE:
      {
	if (realm_name.empty()) {
	  cerr << "missing realm name" << std::endl;
	  return EINVAL;
	}

	RGWRealm realm(realm_name, g_ceph_context, store);
	int ret = realm.create();
	if (ret < 0) {
	  cerr << "ERROR: couldn't create realm " << realm_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_DELETE:
      {
	RGWRealm realm(realm_id, realm_name);
	if (realm_name.empty() && realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't : " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT_REALM_GET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  if (ret == -ENOENT && realm_name.empty() && realm_id.empty()) {
	    cerr << "missing realm name or id, or default realm not found" << std::endl;
	  } else {
	    cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
          }
	  return -ret;
	}
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_GET_DEFAULT:
      {
	RGWRealm realm(g_ceph_context, store);
	string default_id;
	int ret = realm.read_default_id(default_id);
	if (ret == -ENOENT) {
	  cout << "No default realm is set" << std::endl;
	  return -ret;
	} else if (ret < 0) {
	  cerr << "Error reading default realm:" << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	cout << "default realm: " << default_id << std::endl;
      }
      break;
    case OPT_REALM_LIST:
      {
	RGWRealm realm(g_ceph_context, store);
	string default_id;
	int ret = realm.read_default_id(default_id);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
	}
	list<string> realms;
	ret = store->list_realms(realms);
	if (ret < 0) {
	  cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realms_list");
	encode_json("default_info", default_id, formatter);
	encode_json("realms", realms, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_LIST_PERIODS:
      {
        int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	list<string> periods;
	ret = store->list_periods(period_id, periods);
	if (ret < 0) {
	  cerr << "list periods failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realm_periods_list");
	encode_json("current_period", period_id, formatter);
	encode_json("periods", periods, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;

    case OPT_REALM_RENAME:
      {
	RGWRealm realm(realm_id, realm_name);
	if (realm_new_name.empty()) {
	  cerr << "missing realm new name" << std::endl;
	  return EINVAL;
	}
	if (realm_name.empty() && realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.rename(realm_new_name);
	if (ret < 0) {
	  cerr << "realm.rename failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        cout << "Realm name updated. Note that this change only applies to "
            "the current cluster, so this command must be run separately "
            "on each of the realm's other clusters." << std::endl;
      }
      break;
    case OPT_REALM_SET:
      {
	if (realm_id.empty() && realm_name.empty()) {
	  cerr << "no realm name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	bool new_realm = false;
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	} else if (ret == -ENOENT) {
	  new_realm = true;
	}
	ret = read_decode_json(infile, realm);
	if (ret < 0) {
	  return 1;
	}
	if (!realm_name.empty() && realm.get_name() != realm_name) {
	  cerr << "mismatch between --rgw-realm " << realm_name << " and json input file name " <<
	    realm.get_name() << std::endl;
	  return EINVAL;
	}
	/* new realm */
	if (new_realm) {
	  cout << "clearing period and epoch for new realm" << std::endl;
	  realm.clear_current_period_and_epoch();
	  ret = realm.create();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	} else {
	  ret = realm.update();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store realm info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;

    case OPT_REALM_DEFAULT:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set realm as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_REALM_PULL:
      {
        if (url.empty()) {
          cerr << "A --url must be provided." << std::endl;
          return EINVAL;
        }
        RGWEnv env;
        req_info r_info(g_ceph_context, &env);
        r_info.method = "GET";
        r_info.request_uri = "/admin/realm";

        map<string, string> &params = r_info.args.get_params();
        if (!realm_id.empty())
          params["id"] = realm_id;
        if (!realm_name.empty())
          params["name"] = realm_name;

        bufferlist bl;
        JSONParser p;
        int ret = send_to_url(url, access_key, secret_key, r_info, bl, p);
        if (ret < 0) {
          cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
          if (ret == -EACCES) {
            cerr << "If the realm has been changed on the master zone, the "
                "master zone's gateway may need to be restarted to recognize "
                "this user." << std::endl;
          }
          return -ret;
        }
        RGWRealm realm;
        realm.init(g_ceph_context, store, false);
        try {
          decode_json_obj(realm, &p);
        } catch (JSONDecoder::err& e) {
          cerr << "failed to decode JSON response: " << e.message << std::endl;
          return EINVAL;
        }
        RGWPeriod period;
        auto& current_period = realm.get_current_period();
        if (!current_period.empty()) {
          // pull the latest epoch of the realm's current period
          ret = do_period_pull(store, nullptr, url, access_key, secret_key,
                               realm_id, realm_name, current_period, "",
                               &period);
          if (ret < 0) {
            cerr << "could not fetch period " << current_period << std::endl;
            return -ret;
          }
        }
        ret = realm.create(false);
        if (ret < 0 && ret != -EEXIST) {
          cerr << "Error storing realm " << realm.get_id() << ": "
            << cpp_strerror(ret) << std::endl;
          return -ret;
        } else if (ret ==-EEXIST) {
	  ret = realm.update();
	  if (ret < 0) {
	    cerr << "Error storing realm " << realm.get_id() << ": "
		 << cpp_strerror(ret) << std::endl;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("realm", realm, formatter);
        formatter->flush(cout);
      }
      return 0;

    case OPT_ZONEGROUP_ADD:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to initialize zonegroup " << zonegroup_name << " id " << zonegroup_id << " :"
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        if (zone.realm_id != zonegroup.realm_id) {
          zone.realm_id = zonegroup.realm_id;
          ret = zone.update();
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

        string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
        zone.tier_config = tier_config_add;

        bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        ret = zonegroup.add_zone(zone,
                                 (is_master_set ? &is_master : nullptr),
                                 (is_read_only_set ? &read_only : nullptr),
                                 endpoints, ptier_type,
                                 psync_from_all, sync_from, sync_from_rm,
                                 predirect_zone);
	if (ret < 0) {
	  cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name() << ": "
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_CREATE:
      {
	if (zonegroup_name.empty()) {
	  cerr << "Missing zonegroup name" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup(zonegroup_name, is_master, g_ceph_context, store, realm.get_id(), endpoints);
        zonegroup.api_name = (api_name.empty() ? zonegroup_name : api_name);
	ret = zonegroup.create();
	if (ret < 0) {
	  cerr << "failed to create zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_DEFAULT:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zonegroup.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONEGROUP_DELETE:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONEGROUP_GET:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_LIST:
      {
	RGWZoneGroup zonegroup;
	int ret = zonegroup.init(g_ceph_context, store, false);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	list<string> zonegroups;
	ret = store->list_zonegroups(zonegroups);
	if (ret < 0) {
	  cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	string default_zonegroup;
	ret = zonegroup.read_default_id(default_zonegroup);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
	}
	formatter->open_object_section("zonegroups_list");
	encode_json("default_info", default_zonegroup, formatter);
	encode_json("zonegroups", zonegroups, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_MODIFY:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_update = false;

        if (!master_zone.empty()) {
          zonegroup.master_zone = master_zone;
          need_update = true;
        }

	if (is_master_set) {
	  zonegroup.update_master(is_master);
          need_update = true;
        }

        if (!endpoints.empty()) {
          zonegroup.endpoints = endpoints;
          need_update = true;
        }

        if (!api_name.empty()) {
          zonegroup.api_name = api_name;
          need_update = true;
        }

        if (!realm_id.empty()) {
          zonegroup.realm_id = realm_id;
          need_update = true;
        } else if (!realm_name.empty()) {
          // get realm id from name
          RGWRealm realm{g_ceph_context, store};
          ret = realm.read_id(realm_name, zonegroup.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_update = true;
        }

        if (need_update) {
	  ret = zonegroup.update();
	  if (ret < 0) {
	    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_SET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store);
       bool default_realm_not_exist = (ret == -ENOENT && realm_id.empty() && realm_name.empty());

	if (ret < 0 && !default_realm_not_exist ) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
	ret = zonegroup.init(g_ceph_context, store, false);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = read_decode_json(infile, zonegroup);
	if (ret < 0) {
	  return 1;
	}
	if (zonegroup.realm_id.empty() && !default_realm_not_exist) {
	  zonegroup.realm_id = realm.get_id();
	}
	ret = zonegroup.create();
	if (ret < 0 && ret != -EEXIST) {
	  cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	} else if (ret == -EEXIST) {
	  ret = zonegroup.update();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store zonegroup info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_REMOVE:
      {
        RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
        int ret = zonegroup.init(g_ceph_context, store);
        if (ret < 0) {
          cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        if (zone_id.empty()) {
          if (zone_name.empty()) {
            cerr << "no --zone-id or --rgw-zone name provided" << std::endl;
            return EINVAL;
          }
          // look up zone id by name
          for (auto& z : zonegroup.zones) {
            if (zone_name == z.second.name) {
              zone_id = z.second.id;
              break;
            }
          }
          if (zone_id.empty()) {
            cerr << "zone name " << zone_name << " not found in zonegroup "
                << zonegroup.get_name() << std::endl;
            return ENOENT;
          }
        }

        ret = zonegroup.remove_zone(zone_id);
        if (ret < 0) {
          cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_RENAME:
      {
	if (zonegroup_new_name.empty()) {
	  cerr << " missing zonegroup new name" << std::endl;
	  return EINVAL;
	}
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.rename(zonegroup_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
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
    if (!user_op.has_existing_user()) {
      user_op.set_generate_key(); // generate a new key by default
    }
    ret = user.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      if (ret == -ERR_INVALID_TENANT_NAME)
	ret = -EINVAL;

      return -ret;
    }
    if (!subuser.empty()) {
      ret = user.subusers.add(user_op, &err_msg);
      if (ret < 0) {
        cerr << "could not create subuser: " << err_msg << std::endl;
        return -ret;
      }
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
    {
      RGWEnv env;
      req_info r_info(g_ceph_context, &env);
      r_info.method = "POST";
      r_info.request_uri = "/admin/realm/period";

      map<string, string> &params = r_info.args.get_params();
      if (!realm_id.empty())
        params["realm_id"] = realm_id;
      if (!realm_name.empty())
        params["realm_name"] = realm_name;
      if (!period_id.empty())
        params["period_id"] = period_id;
      if (!period_epoch.empty())
        params["epoch"] = period_epoch;

      // load the period
      RGWPeriod period(period_id);
      int ret = period.init(g_ceph_context, store);
      if (ret < 0) {
        cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      // json format into a bufferlist
      JSONFormatter jf(false);
      encode_json("period", period, &jf);
      bufferlist bl;
      jf.flush(bl);

      JSONParser p;
      ret = send_to_remote_or_url(nullptr, url, access_key, secret_key,
                                  r_info, bl, p);
      if (ret < 0) {
        cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    return 0;
  case OPT_PERIOD_UPDATE:
    {
      int ret = update_period(store, realm_id, realm_name, period_id, period_epoch,
                              commit, remote, url, access_key, secret_key,
                              formatter, yes_i_really_mean_it);
      if (ret < 0) {
	return -ret;
      }
    }
    return 0;
  case OPT_PERIOD_COMMIT:
    {
      // read realm and staging period
      RGWRealm realm(realm_id, realm_name);
      int ret = realm.init(g_ceph_context, store);
      if (ret < 0) {
        cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      RGWPeriod period(RGWPeriod::get_staging_id(realm.get_id()), 1);
      ret = period.init(g_ceph_context, store, realm.get_id());
      if (ret < 0) {
        cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      ret = commit_period(store, realm, period, remote, url, access_key, secret_key,
                          yes_i_really_mean_it);
      if (ret < 0) {
        cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("period", period, formatter);
      formatter->flush(cout);
    }
    return 0;
  case OPT_ROLE_CREATE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }
      /* The following two calls will be replaced by read_decode_json or something
         similar when the code for AWS Policies is in places */
      bufferlist bl;
      int ret = read_input(assume_role_doc, bl);
      if (ret < 0) {
        cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
        return ret;
      }
      JSONParser p;
      if (!p.parse(bl.c_str(), bl.length())) {
        cout << "ERROR: failed to parse JSON: " << assume_role_doc << std::endl;
        return -EINVAL;
      }
      string trust_policy = bl.to_str();
      RGWRole role(g_ceph_context, store, role_name, path, trust_policy, tenant);
      ret = role.create(true);
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role, formatter);
      return 0;
    }
  case OPT_ROLE_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.delete_obj();
      if (ret < 0) {
        return -ret;
      }
      cout << "role: " << role_name << " successfully deleted" << std::endl;
      return 0;
    }
  case OPT_ROLE_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role, formatter);
      return 0;
    }
  case OPT_ROLE_MODIFY:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }

      /* The following two calls will be replaced by read_decode_json or something
         similar when the code for AWS Policies is in place */
      bufferlist bl;
      int ret = read_input(assume_role_doc, bl);
      if (ret < 0) {
        cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
        return ret;
      }
      JSONParser p;
      if (!p.parse(bl.c_str(), bl.length())) {
        cout << "ERROR: failed to parse JSON: " << assume_role_doc << std::endl;
        return -EINVAL;
      }
      string trust_policy = bl.to_str();
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      role.update_trust_policy(trust_policy);
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Assume role policy document updated successfully for role: " << role_name << std::endl;
      return 0;
    }
  case OPT_ROLE_LIST:
    {
      vector<RGWRole> result;
      ret = RGWRole::get_roles_by_path_prefix(store, g_ceph_context, path_prefix, tenant, result);
      if (ret < 0) {
        return -ret;
      }
      show_roles_info(result, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_PUT:
    {
      if (role_name.empty()) {
        cerr << "role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "policy name is empty" << std::endl;
        return -EINVAL;
      }

      if (perm_policy_doc.empty()) {
        cerr << "permission policy document is empty" << std::endl;
        return -EINVAL;
      }

      /* The following two calls will be replaced by read_decode_json or something
         similar, when code for AWS Policies is in place.*/
      bufferlist bl;
      int ret = read_input(perm_policy_doc, bl);
      if (ret < 0) {
        cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
        return ret;
      }
      JSONParser p;
      if (!p.parse(bl.c_str(), bl.length())) {
        cout << "ERROR: failed to parse JSON: " << std::endl;
        return -EINVAL;
      }
      string perm_policy;
      perm_policy = bl.c_str();

      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      role.set_perm_policy(policy_name, perm_policy);
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Permission policy attached successfully" << std::endl;
      return 0;
    }
  case OPT_ROLE_POLICY_LIST:
    {
      if (role_name.empty()) {
        cerr << "ERROR: Role name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      std::vector<string> policy_names = role.get_role_policy_names();
      show_policy_names(policy_names, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      int ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      string perm_policy;
      ret = role.get_role_policy(policy_name, perm_policy);
      if (ret < 0) {
        return -ret;
      }
      show_perm_policy(perm_policy, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      ret = role.delete_policy(policy_name);
      if (ret < 0) {
        return -ret;
      }
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Policy: " << policy_name << " successfully deleted for role: "
           << role_name << std::endl;
      return 0;
  }
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
    void *handle;
    std::list<std::string> user_ids;
    metadata_key = "user";
    int max = 1000;

    bool truncated;

    if (! user_id.empty()) {
      user_ids.push_back(user_id.id);
      ret =
	RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, f,
	  warnings_only);
    } else {
      /* list users in groups of max-keys, then perform user-bucket
       * limit-check on each group */
     ret = store->meta_mgr->list_keys_init(metadata_key, &handle);
      if (ret < 0) {
	cerr << "ERROR: buckets limit check can't get user metadata_key: "
	     << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      do {
	ret = store->meta_mgr->list_keys_next(handle, max, user_ids,
					      &truncated);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "ERROR: buckets limit check lists_keys_next(): "
	       << cpp_strerror(-ret) << std::endl;
	  break;
	} else {
	  /* ok, do the limit checks for this group */
	  ret =
	    RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, f,
	      warnings_only);
	  if (ret < 0)
	    break;
	}
	user_ids.clear();
      } while (truncated);
      store->meta_mgr->list_keys_complete(handle);
    }
    return -ret;
  } /* OPT_BUCKET_LIMIT_CHECK */

  if (opt_cmd == OPT_BUCKETS_LIST) {
    if (bucket_name.empty()) {
      RGWBucketAdminOp::info(store, bucket_op, f);
    } else {
      RGWBucketInfo bucket_info;
      int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      formatter->open_array_section("entries");
      bool truncated;
      int count = 0;
      if (max_entries < 0)
        max_entries = 1000;

      string prefix;
      string delim;
      vector<rgw_bucket_dir_entry> result;
      map<string, bool> common_prefixes;
      string ns;

      RGWRados::Bucket target(store, bucket_info);
      RGWRados::Bucket::List list_op(&target);

      list_op.params.prefix = prefix;
      list_op.params.delim = delim;
      list_op.params.marker = rgw_obj_key(marker);
      list_op.params.ns = ns;
      list_op.params.enforce_ns = false;
      list_op.params.list_versions = true;

      do {
        ret = list_op.list_objects(max_entries - count, &result, &common_prefixes, &truncated);
        if (ret < 0) {
          cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += result.size();

        for (auto iter = result.begin(); iter != result.end(); ++iter) {
          rgw_bucket_dir_entry& entry = *iter;
          encode_json("entry", entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->flush(cout);
    } /* have bucket_name */
  } /* OPT_BUCKETS_LIST */

  if (opt_cmd == OPT_BUCKET_STATS) {
    bucket_op.set_fetch_stats(true);

    int r = RGWBucketAdminOp::info(store, bucket_op, f);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
    bucket_op.set_bucket_id(bucket_id);
    int r = RGWBucketAdminOp::link(store, bucket_op, &err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    int r = RGWBucketAdminOp::unlink(store, bucket_op);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_LOG_LIST) {
    // filter by date?
    if (!date.empty() && date.size() != 10) {
      cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
      return EINVAL;
    }

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = store->log_list_init(date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
        cerr << "log list: error " << r << std::endl;
        return -r;
      }
      while (true) {
        string name;
        int r = store->log_list_next(h, &name);
        if (r == -ENOENT)
          break;
        if (r < 0) {
          cerr << "log list: error " << r << std::endl;
          return -r;
        }
        formatter->dump_string("object", name);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_LOG_SHOW || opt_cmd == OPT_LOG_RM) {
    if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
      cerr << "specify an object or a date, bucket and bucket-id" << std::endl;
      usage();
      ceph_abort();
    }

    string oid;
    if (!object.empty()) {
      oid = object;
    } else {
      oid = date;
      oid += "-";
      oid += bucket_id;
      oid += "-";
      oid += bucket_name;
    }

    if (opt_cmd == OPT_LOG_SHOW) {
      RGWAccessHandle h;

      int r = store->log_show_init(oid, &h);
      if (r < 0) {
	cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;

      // peek at first entry to get bucket metadata
      r = store->log_show_next(h, &entry);
      if (r < 0) {
	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      formatter->dump_string("bucket_id", entry.bucket_id);
      formatter->dump_string("bucket_owner", entry.bucket_owner.to_str());
      formatter->dump_string("bucket", entry.bucket);

      uint64_t agg_time = 0;
      uint64_t agg_bytes_sent = 0;
      uint64_t agg_bytes_received = 0;
      uint64_t total_entries = 0;

      if (show_log_entries)
        formatter->open_array_section("log_entries");

      do {
	uint64_t total_time =  entry.total_time.to_msec();

        agg_time += total_time;
        agg_bytes_sent += entry.bytes_sent;
        agg_bytes_received += entry.bytes_received;
        total_entries++;

        if (skip_zero_entries && entry.bytes_sent == 0 &&
            entry.bytes_received == 0)
          goto next;

        if (show_log_entries) {

	  rgw_format_ops_log_entry(entry, formatter);
	  formatter->flush(cout);
        }
next:
	r = store->log_show_next(h, &entry);
      } while (r > 0);

      if (r < 0) {
      	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      if (show_log_entries)
        formatter->close_section();

      if (show_log_sum) {
        formatter->open_object_section("log_sum");
	formatter->dump_int("bytes_sent", agg_bytes_sent);
	formatter->dump_int("bytes_received", agg_bytes_received);
	formatter->dump_int("total_time", agg_time);
	formatter->dump_int("total_entries", total_entries);
        formatter->close_section();
      }
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    if (opt_cmd == OPT_LOG_RM) {
      int r = store->log_remove(oid);
      if (r < 0) {
	cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
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
    uint64_t start_epoch = 0;
    auto end_epoch = (uint64_t)-1;

    int ret;

    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }
    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }


    ret = RGWUsage::show(store, user_id, start_epoch, end_epoch,
			 show_log_entries, show_log_sum, &categories,
			 f);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_USAGE_TRIM) {
    if (user_id.empty() && !yes_i_really_mean_it) {
      cerr << "usage trim without user specified will remove *all* users data" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }
    int ret;
    uint64_t start_epoch = 0;
    auto end_epoch = (uint64_t)-1;


    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }

    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    ret = RGWUsage::trim(store, user_id, start_epoch, end_epoch);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_OLH_GET || opt_cmd == OPT_OLH_READLOG) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
  }

  if (opt_cmd == OPT_OLH_GET) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWOLHInfo olh;
    rgw_obj obj(bucket, object);
    ret = store->get_olh(bucket_info, obj, &olh);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_OLH_READLOG) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    RGWObjectCtx rctx(store);
    rgw_obj obj(bucket, object);

    RGWObjState *state;

    ret = store->get_obj_state(&rctx, bucket_info, obj, &state, false); /* don't follow olh */
    if (ret < 0) {
      return -ret;
    }

    ret = store->bucket_index_read_olh_log(bucket_info, *state, obj, 0, &log, &is_truncated);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("is_truncated", is_truncated, formatter);
    encode_json("log", log, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket, object);
    if (!object_version.empty()) {
      obj.key.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;

    ret = store->bi_get(bucket, obj, bi_index_type, &entry);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_cls_bi_entry entry;
    cls_rgw_obj_key key;
    ret = read_decode_json(infile, entry, &key);
    if (ret < 0) {
      return 1;
    }

    rgw_obj obj(bucket, key);

    ret = store->bi_put(bucket, obj, entry);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BI_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    formatter->open_array_section("entries");

    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(store);
      int shard_id = (bucket_info.num_shards > 0  ? i : -1);
      int ret = bs.init(bucket, shard_id);
      marker.clear();

      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      do {
        entries.clear();
        ret = store->bi_list(bs, object, marker, max_entries, &entries, &is_truncated);
        if (ret < 0) {
          cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        list<rgw_cls_bi_entry>::iterator iter;
        for (iter = entries.begin(); iter != entries.end(); ++iter) {
          rgw_cls_bi_entry& entry = *iter;
          encode_json("entry", entry, formatter);
          marker = entry.idx;
        }
        formatter->flush(cout);
      } while (is_truncated);
      formatter->flush(cout);
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketInfo cur_bucket_info;
    rgw_bucket cur_bucket;
    ret = init_bucket(store, tenant, bucket_name, string(), cur_bucket_info, cur_bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init current bucket info for bucket_name=" << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    if (cur_bucket_info.bucket.bucket_id == bucket_info.bucket.bucket_id && !yes_i_really_mean_it) {
      cerr << "specified bucket instance points to a current bucket instance" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
    }

    int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(store);
      int shard_id = (bucket_info.num_shards > 0  ? i : -1);
      int ret = bs.init(bucket, shard_id);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      ret = store->bi_remove(bs);
      if (ret < 0) {
        cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (opt_cmd == OPT_OBJECT_RM) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj_key key(object, object_version);
    ret = rgw_remove_object(store, bucket_info, bucket, key);

    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_obj obj(bucket, object);
    obj.key.set_instance(object_version);
    bool need_rewrite = true;
    if (min_rewrite_stripe_size > 0) {
      ret = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
      }
    }
    if (need_rewrite) {
      ret = store->rewrite_obj(bucket_info, obj);
      if (ret < 0) {
        cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ldout(store->ctx(), 20) << "skipped object" << dendl;
    }
  }

  if (opt_cmd == OPT_OBJECTS_EXPIRE) {
    int ret = store->process_expire_objects();
    if (ret < 0) {
      cerr << "ERROR: process_expire_objects() processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_BUCKET_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    uint64_t start_epoch = 0;
    uint64_t end_epoch = 0;

    if (!end_date.empty()) {
      int ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return EINVAL;
      }
    }
    if (!start_date.empty()) {
      int ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }

    bool is_truncated = true;

    rgw_obj_index_key marker;
    string prefix;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", bucket_name);
    formatter->open_array_section("objects");
    while (is_truncated) {
      map<string, rgw_bucket_dir_entry> result;
      int r = store->cls_bucket_list(bucket_info, RGW_NO_SHARD, marker, prefix, 1000, true,
                                     result, &is_truncated, &marker,
                                     bucket_object_check_filter);

      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      }

      if (r == -ENOENT)
        break;

      map<string, rgw_bucket_dir_entry>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
        rgw_obj_key key = iter->second.key;
        rgw_bucket_dir_entry& entry = iter->second;

        formatter->open_object_section("object");
        formatter->dump_string("name", key.name);
        formatter->dump_string("instance", key.instance);
        formatter->dump_int("size", entry.meta.size);
        utime_t ut(entry.meta.mtime);
        ut.gmtime(formatter->dump_stream("mtime"));

        if ((entry.meta.size < min_rewrite_size) ||
            (entry.meta.size > max_rewrite_size) ||
            (start_epoch > 0 && start_epoch > (uint64_t)ut.sec()) ||
            (end_epoch > 0 && end_epoch < (uint64_t)ut.sec())) {
          formatter->dump_string("status", "Skipped");
        } else {
          rgw_obj obj(bucket, key);

          bool need_rewrite = true;
          if (min_rewrite_stripe_size > 0) {
            r = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            r = store->rewrite_obj(bucket_info, obj);
            if (r == 0) {
              formatter->dump_string("status", "Success");
            } else {
              formatter->dump_string("status", cpp_strerror(-r));
            }
          }
        }
        formatter->dump_int("flags", entry.flags);

        formatter->close_section();
        formatter->flush(cout);
      }
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BUCKET_RESHARD) {
    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;

    int ret = check_reshard_bucket_params(store,
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  bucket,
					  bucket_info,
					  attrs);
    if (ret < 0) {
      return ret;
    }

    RGWBucketReshard br(store, bucket_info, attrs);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (max_entries < 1) {
      max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    return br.execute(num_shards, max_entries,
                      verbose, &cout, formatter);
  }

  if (opt_cmd == OPT_RESHARD_ADD) {
    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;

    int ret = check_reshard_bucket_params(store,
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  bucket,
					  bucket_info,
					  attrs);
    if (ret < 0) {
      return ret;
    }

    int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    RGWReshard reshard(store);
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    entry.bucket_id = bucket_info.bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = num_shards;

    return reshard.add(entry);
  }

  if (opt_cmd == OPT_RESHARD_LIST) {
    list<cls_rgw_reshard_entry> entries;
    int ret;
    int count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int num_logshards = store->ctx()->_conf->rgw_reshard_num_logs;

    RGWReshard reshard(store);

    formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      string marker;
      do {
        entries.clear();
        ret = reshard.list(i, marker, max_entries, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (auto &entry : entries) {
          encode_json("entry", entry, formatter);
          entry.get_key(&marker);
        }
        count += entries.size();
        formatter->flush(cout);
      } while (is_truncated && count < max_entries);

      if (count >= max_entries) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
    return 0;
  }


  if (opt_cmd == OPT_RESHARD_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;
    ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(store, bucket_info, attrs);
    list<cls_rgw_bucket_instance_entry> status;
    int r = br.get_status(&status);
    if (r < 0) {
      cerr << "ERROR: could not get resharding status for bucket " << bucket_name << std::endl;
      return -r;
    }

    encode_json("status", status, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_RESHARD_PROCESS) {
    RGWReshard reshard(store, true, &cout);

    int ret = reshard.process_all_logshards();
    if (ret < 0) {
      cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_RESHARD_CANCEL) {
    RGWReshard reshard(store);

    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    cls_rgw_reshard_entry entry;
    //entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    //entry.bucket_id = bucket_id;
    int ret = reshard.get(entry);
    if (ret < 0) {
      cerr << "Error in getting bucket " << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    /* TBD stop running resharding */

    ret =reshard.remove(entry);
    if (ret < 0) {
      cerr << "Error removing bucket " << bucket_name << " for resharding queue: " << cpp_strerror(-ret) <<
	std::endl;
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_UNLINK) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_index_key> oid_list;
    rgw_obj_key key(object, object_version);
    rgw_obj_index_key index_key;
    key.get_index_key(&index_key);
    oid_list.push_back(index_key);
    ret = store->remove_objs_from_index(bucket_info, oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_OBJECT_STAT) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket, object);
    obj.key.set_instance(object_version);

    uint64_t obj_size;
    map<string, bufferlist> attrs;
    RGWObjectCtx obj_ctx(store);
    RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
    RGWRados::Object::Read read_op(&op_target);

    read_op.params.attrs = &attrs;
    read_op.params.obj_size = &obj_size;

    ret = read_op.prepare();
    if (ret < 0) {
      cerr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
    formatter->open_object_section("object_metadata");
    formatter->dump_string("name", object);
    formatter->dump_unsigned("size", obj_size);

    map<string, bufferlist>::iterator iter;
    map<string, bufferlist> other_attrs;
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      bufferlist& bl = iter->second;
      bool handled = false;
      if (iter->first == RGW_ATTR_MANIFEST) {
        handled = decode_dump<RGWObjManifest>("manifest", bl, formatter);
      } else if (iter->first == RGW_ATTR_ACL) {
        handled = decode_dump<RGWAccessControlPolicy>("policy", bl, formatter);
      } else if (iter->first == RGW_ATTR_ID_TAG) {
        handled = dump_string("tag", bl, formatter);
      } else if (iter->first == RGW_ATTR_ETAG) {
        handled = dump_string("etag", bl, formatter);
      }

      if (!handled)
        other_attrs[iter->first] = bl;
    }

    formatter->open_object_section("attrs");
    for (iter = other_attrs.begin(); iter != other_attrs.end(); ++iter) {
      dump_string(iter->first.c_str(), iter->second, formatter);
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BUCKET_CHECK) {
    if (check_head_obj_locator) {
      if (bucket_name.empty()) {
        cerr << "ERROR: need to specify bucket name" << std::endl;
        return EINVAL;
      }
      do_check_object_locator(store, tenant, bucket_name, fix, remove_bad, formatter);
    } else {
      RGWBucketAdminOp::check_index(store, bucket_op, f);
    }
  }

  if (opt_cmd == OPT_BUCKET_RM) {
    if (!inconsistent_index) {
      RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, true);
    } else {
      if (!yes_i_really_mean_it) {
	cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
	<< "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
	return 1;
      }
      RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, false);
    }
  }

  if (opt_cmd == OPT_GC_LIST) {
    int index = 0;
    bool truncated;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      int ret = store->list_gc_objs(&index, marker, 1000, !include_all, result, &truncated);
      if (ret < 0) {
	cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
	return 1;
      }


      list<cls_rgw_gc_obj_info>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
	cls_rgw_gc_obj_info& info = *iter;
	formatter->open_object_section("chain_info");
	formatter->dump_string("tag", info.tag);
	formatter->dump_stream("time") << info.time;
	formatter->open_array_section("objs");
        list<cls_rgw_obj>::iterator liter;
	cls_rgw_obj_chain& chain = info.chain;
	for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
	  cls_rgw_obj& obj = *liter;
          encode_json("obj", obj, formatter);
	}
	formatter->close_section(); // objs
	formatter->close_section(); // obj_chain
	formatter->flush(cout);
      }
    } while (truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_GC_PROCESS) {
    int ret = store->process_gc(!include_all);
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_LC_LIST) {
    formatter->open_array_section("lifecycle_list");
    map<string, int> bucket_lc_map;
    string marker;
#define MAX_LC_LIST_ENTRIES 100
    if (max_entries < 0) {
      max_entries = MAX_LC_LIST_ENTRIES;
    }
    do {
      int ret = store->list_lc_progress(marker, max_entries, &bucket_lc_map);
      if (ret < 0) {
        cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }
      map<string, int>::iterator iter;
      for (iter = bucket_lc_map.begin(); iter != bucket_lc_map.end(); ++iter) {
        formatter->open_object_section("bucket_lc_info");
        formatter->dump_string("bucket", iter->first);
        string lc_status = LC_STATUS[iter->second];
        formatter->dump_string("status", lc_status);
        formatter->close_section(); // objs
        formatter->flush(cout);
        marker = iter->first;
      }
    } while (!bucket_lc_map.empty());

    formatter->close_section(); //lifecycle list
    formatter->flush(cout);
  }


  if (opt_cmd == OPT_LC_PROCESS) {
    int ret = store->process_lc();
    if (ret < 0) {
      cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
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
    if (sync_stats) {
      if (!bucket_name.empty()) {
        int ret = rgw_bucket_sync_user_stats(store, tenant, bucket_name);
        if (ret < 0) {
          cerr << "ERROR: could not sync bucket stats: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        int ret = rgw_user_sync_all_stats(store, user_id);
        if (ret < 0) {
          cerr << "ERROR: failed to sync user stats: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
    }

    if (user_id.empty()) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    cls_user_header header;
    string user_str = user_id.to_str();
    int ret = store->cls_user_get_header(user_str, &header);
    if (ret < 0) {
      if (ret == -ENOENT) { /* in case of ENOENT */
        cerr << "User has not been initialized or user does not exist" << std::endl;
      } else {
        cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
      }
      return -ret;
    }

    encode_json("header", header, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_METADATA_GET) {
    int ret = store->meta_mgr->get(metadata_key, formatter);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->flush(cout);
  }

  if (opt_cmd == OPT_METADATA_PUT) {
    bufferlist bl;
    int ret = read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = store->meta_mgr->put(metadata_key, bl, RGWMetadataHandler::APPLY_ALWAYS);
    if (ret < 0) {
      cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_METADATA_RM) {
    int ret = store->meta_mgr->remove(metadata_key);
    if (ret < 0) {
      cerr << "ERROR: can't remove key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
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
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    int i = (specified_shard_id ? shard_id : 0);

    if (period_id.empty()) {
      int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      void *handle;
      list<cls_log_entry> entries;


      meta_log->init_list_entries(i, start_time.to_real_time(), end_time.to_real_time(), marker, &handle);
      bool truncated;
      do {
	  int ret = meta_log->list_entries(handle, 1000, entries, nullptr, &truncated);
        if (ret < 0) {
          cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
          cls_log_entry& entry = *iter;
          store->meta_mgr->dump_log_entry(entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated);

      meta_log->complete_list_entries(handle);

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_MDLOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    if (period_id.empty()) {
      int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    formatter->open_array_section("entries");

    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLogInfo info;
      meta_log->get_info(i, &info);

      ::encode_json("info", info, formatter);

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_MDLOG_AUTOTRIM) {
    // need a full history for purging old mdlog periods
    store->meta_mgr->init_oldest_log_period();

    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.set_threaded();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf->rgw_md_log_max_shards;
    ret = crs.run(create_admin_meta_log_trim_cr(store, &http, num_shards));
    if (ret < 0) {
      cerr << "automated mdlog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_MDLOG_TRIM) {
    utime_t start_time, end_time;

    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    if (period_id.empty()) {
      std::cerr << "missing --period argument" << std::endl;
      return EINVAL;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    ret = meta_log->trim(shard_id, start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_SYNC_STATUS) {
    sync_status();
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
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

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
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

    ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.read_sync_status();
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    map<int, rgw_bucket_shard_sync_info>& sync_status = sync.get_sync_status();

    encode_json("sync_status", sync_status, formatter);
    formatter->flush(cout);
  }

 if (opt_cmd == OPT_BUCKET_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

    ret = sync.init();
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

  if (opt_cmd == OPT_BILOG_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    do {
      list<rgw_bi_log_entry> entries;
      ret = store->list_bi_log_entries(bucket_info, shard_id, marker, max_entries - count, entries, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_bi_log_entry& entry = *iter;
        encode_json("entry", entry, formatter);

        marker = entry.id;
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_SYNC_ERROR_LIST) {
    if (max_entries < 0) {
      max_entries = 1000;
    }

    bool truncated;
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    if (shard_id < 0) {
      shard_id = 0;
    }

    formatter->open_array_section("entries");

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      formatter->open_object_section("shard");
      encode_json("shard_id", shard_id, formatter);
      formatter->open_array_section("entries");

      int count = 0;
      string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);

      do {
        list<cls_log_entry> entries;
        ret = store->time_log_list(oid, start_time.to_real_time(), end_time.to_real_time(),
                                   max_entries - count, entries, marker, &marker, &truncated);
        if (ret == -ENOENT) {
          break;
        }
        if (ret < 0) {
          cerr << "ERROR: store->time_log_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += entries.size();

        for (auto& cls_entry : entries) {
          rgw_sync_error_info log_entry;

          auto iter = cls_entry.data.begin();
          try {
            ::decode(log_entry, iter);
          } catch (buffer::error& err) {
            cerr << "ERROR: failed to decode log entry" << std::endl;
            continue;
          }
          formatter->open_object_section("entry");
          encode_json("id", cls_entry.id, formatter);
          encode_json("section", cls_entry.section, formatter);
          encode_json("name", cls_entry.name, formatter);
          encode_json("timestamp", cls_entry.timestamp, formatter);
          encode_json("info", log_entry, formatter);
          formatter->close_section();
          formatter->flush(cout);
        }
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->close_section();

      if (specified_shard_id) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_SYNC_ERROR_TRIM) {
    utime_t start_time, end_time;
    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    if (shard_id < 0) {
      shard_id = 0;
    }

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);
      ret = store->time_log_trim(oid, start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
      if (ret < 0 && ret != -ENODATA) {
        cerr << "ERROR: sync error trim: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (specified_shard_id) {
        break;
      }
    }
  }

  if (opt_cmd == OPT_BILOG_TRIM) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = store->trim_bi_log_entries(bucket_info, shard_id, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BILOG_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<int, string> markers;
    ret = store->get_bi_log_status(bucket_info, shard_id, markers);
    if (ret < 0) {
      cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("entries");
    encode_json("markers", markers, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BILOG_AUTOTRIM) {
    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.set_threaded();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    rgw::BucketTrimConfig config;
    configure_bucket_trim(store->ctx(), config);

    rgw::BucketTrimManager trim(store, config);
    ret = trim.init();
    if (ret < 0) {
      cerr << "trim manager init failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
    ret = crs.run(trim.create_admin_bucket_trim_cr(&http));
    if (ret < 0) {
      cerr << "automated bilog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_DATALOG_LIST) {
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    RGWDataChangesLog *log = store->data_log;
    RGWDataChangesLog::LogMarker marker;

    do {
      list<rgw_data_change_log_entry> entries;
      ret = log->list_entries(start_time.to_real_time(), end_time.to_real_time(), max_entries - count, entries, marker, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_data_change_log_entry& entry = *iter;
        if (!extra_info) {
          encode_json("entry", entry.entry, formatter);
        } else {
          encode_json("entry", entry, formatter);
        }
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
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
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    RGWDataChangesLog *log = store->data_log;
    ret = log->trim_entries(start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OPSTATE_LIST) {
    RGWOpState oc(store);

    int max = 1000;

    void *handle;
    oc.init_list_entries(client_id, op_id, object, &handle);
    list<cls_statelog_entry> entries;
    bool done;
    formatter->open_array_section("entries");
    do {
      int ret = oc.list_entries(handle, max, entries, &done);
      if (ret < 0) {
        cerr << "oc.list_entries returned " << cpp_strerror(-ret) << std::endl;
        oc.finish_list_entries(handle);
        return -ret;
      }

      for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
        oc.dump_entry(*iter, formatter);
      }

      formatter->flush(cout);
    } while (!done);
    formatter->close_section();
    formatter->flush(cout);
    oc.finish_list_entries(handle);
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
    RGWOpState oc(store);

    if (object.empty() || client_id.empty() || op_id.empty()) {
      cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
      return EINVAL;
    }
    ret = oc.remove_entry(client_id, op_id, object);
    if (ret < 0) {
      cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_GET || opt_cmd == OPT_REPLICALOG_UPDATE ||
      opt_cmd == OPT_REPLICALOG_DELETE) {
    if (replica_log_type_str.empty()) {
      cerr << "ERROR: need to specify --replica-log-type=<metadata | data | bucket>" << std::endl;
      return EINVAL;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_GET) {
    RGWReplicaBounds bounds;
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }

      RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.get_bounds(shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.get_bounds(shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.get_bounds(bucket, shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else { // shouldn't get here
      ceph_abort();
    }
    encode_json("bounds", bounds, formatter);
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_REPLICALOG_DELETE) {
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      if (!specified_daemon_id) {
        cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.delete_bound(shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      if (!specified_daemon_id) {
        cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.delete_bound(shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.delete_bound(bucket, shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_UPDATE) {
    if (marker.empty()) {
      cerr << "ERROR: marker was not specified" <<std::endl;
      return EINVAL;
    }
    utime_t time = ceph_clock_now();
    if (!date.empty()) {
      ret = parse_date_str(date, time);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }
    list<RGWReplicaItemMarker> entries;
    int ret = read_decode_json(infile, entries);
    if (ret < 0) {
      cerr << "ERROR: failed to decode entries" << std::endl;
      return EINVAL;
    }
    RGWReplicaBounds bounds;
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }

      RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.update_bound(bucket, shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  bool quota_op = (opt_cmd == OPT_QUOTA_SET || opt_cmd == OPT_QUOTA_ENABLE || opt_cmd == OPT_QUOTA_DISABLE);

  if (quota_op) {
    if (bucket_name.empty() && user_id.empty()) {
      cerr << "ERROR: bucket name or uid is required for quota operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!quota_scope.empty() && quota_scope != "bucket") {
        cerr << "ERROR: invalid quota scope specification." << std::endl;
        return EINVAL;
      }
      set_bucket_quota(store, opt_cmd, tenant, bucket_name,
                       max_size, max_objects, have_max_size, have_max_objects);
    } else if (!user_id.empty()) {
      if (quota_scope == "bucket") {
        return set_user_bucket_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else if (quota_scope == "user") {
        return set_user_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else {
        cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }

  return 0;
}
