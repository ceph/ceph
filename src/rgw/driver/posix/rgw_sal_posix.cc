// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_posix.h"
#include "rgw_rest_user.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_s3_filter.h"
#include <cstdint>
#include "rgw_multi.h"
#include "include/scope_guard.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/errno.h"
#include "rgw_lc.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

template <typename T>
static bool decode_raw_attr(rgw::sal::Attrs& attrs, const char* name, T& val) {
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    return false;
  }
  bufferlist bl = it->second;
  try {
    auto it = bl.cbegin();
    decode(val, it);
  } catch (buffer::error&) {
    return false;
  }
  return true;
}

template <typename T>
static void encode_attr(rgw::sal::Attrs& attrs, const char* name, const T& val) {
  bufferlist bl;
  encode(val, bl);
  attrs[name] = std::move(bl);
}

namespace rgw { namespace sal {

static inline std::string bucket_fname(std::string name, std::optional<std::string>& ns)
{
  std::string bname;

  if (ns)
    bname = "." + *ns + "_" + url_encode(name, true);
  else
    bname = url_encode(name, true);

  return bname;
}

static inline int copy_dir_fd(int old_fd)
{
  return openat(old_fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
}

bool POSIXZoneGroup::placement_target_exists(std::string& target) const {
  return !!group->placement_targets.count(target);
}

void POSIXZoneGroup::get_placement_target_names(std::set<std::string>& names) const {
  for (const auto& target : group->placement_targets) {
    names.emplace(target.second.name);
  }
}

ZoneGroup& POSIXZone::get_zonegroup() {
  return *zonegroup;
}

const RGWZoneParams& POSIXZone::get_rgw_params() {
  return *zone_params;
}

const std::string& POSIXZone::get_id() {
  return zone_params->get_id();
}

const std::string& POSIXZone::get_name() const {
  return zone_params->get_name();
}

bool POSIXZone::is_writeable() {
  return true;
}

bool POSIXZone::get_redirect_endpoint(std::string* endpoint) {
  return false;
}

const std::string& POSIXZone::get_current_period_id() {
  return current_period->get_id();
}

const RGWAccessKey& POSIXZone::get_system_key() {
  return zone_params->system_key;
}

const std::string& POSIXZone::get_realm_name() {
  return realm->get_name();
}

const std::string& POSIXZone::get_realm_id() {
  return realm->get_id();
}

RGWBucketSyncPolicyHandlerRef POSIXZone::get_sync_policy_handler() {
  return nullptr;
}

int POSIXLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
{
  return -ENOENT;
}

std::tuple<rgw::lua::LuaCodeType, int> POSIXLuaManager::get_script_or_bytecode(const DoutPrefixProvider* dpp, optional_yield y,
                                                                               const std::string& key)
{
  return std::make_tuple("", -ENOENT);
}

int POSIXLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
{
  return -ENOENT;
}

int POSIXLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
{
  return -ENOENT;
}

int POSIXLuaManager::add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int POSIXLuaManager::remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int POSIXLuaManager::list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages)
{
  return -ENOENT;
}

int POSIXLuaManager::reload_packages(const DoutPrefixProvider* dpp, optional_yield y)
{
  return -ENOENT;
}

int POSIXDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  int ret = -1;
  base_path = g_conf().get_val<std::string>("rgw_posix_base_path");

  ldpp_dout(dpp, 20) << "Initializing POSIX driver: " << base_path << dendl;

  /* ordered listing cache */
  bucket_cache.reset(
    new posix::BucketCache(
      this, base_path,
      g_conf().get_val<std::string>("rgw_posix_database_root"),
      g_conf().get_val<int64_t>("rgw_posix_cache_max_buckets"),
      g_conf().get_val<int64_t>("rgw_posix_cache_lanes"),
      g_conf().get_val<int64_t>("rgw_posix_cache_partitions"),
      g_conf().get_val<int64_t>("rgw_posix_cache_lmdb_count"),
      g_conf().get_val<bool>("rgw_posix_inotify")));

  /* user info cache */
  user_cache.set_max_size(dpp, g_conf().get_val<uint64_t>("rgw_posix_cache_max_users"));

  root_dir = std::make_unique<posix::Directory>(base_path, nullptr, ctx());
  ret = root_dir->open(dpp);
  if (ret < 0) {
    if (ret == -ENOTDIR) {
      ldpp_dout(dpp, 0) << " ERROR: base path (" << base_path
	<< "): was not a directory." << dendl;
      return ret;
    } else if (ret == -ENOENT) {
      ret = root_dir->create(dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not create base path ("
	  << base_path << "): " << cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
  }

  lc = new RGWLC();
  lc->initialize(cct, this);

  if (use_lc_thread) {
    ret = userDB->createLCTables(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Failed to create LC tables, ret=" << ret << dendl;
      return ret;
    }
    lc->start_processor();
  }

  ldpp_dout(dpp, 20) << "root_fd: " << root_dir->get_fd() << dendl;
  quota_handler = RGWQuotaHandler::generate_handler(dpp, this, true);

  if (!RGWPubSubEndpoint::init_all(cct)) {
    ldpp_dout(dpp, 1) << "WARNING: failed to init notification endpoints" << dendl;
  }

  ldpp_dout(dpp, 20) << "SUCCESS" << dendl;
  return 0;
}

void POSIXDriver::finalize()
{
  RGWPubSubEndpoint::shutdown_all();
  RGWQuotaHandler::free_handler(quota_handler);
}

std::unique_ptr<User> POSIXDriver::get_user(const rgw_user &u)
{
  return std::make_unique<POSIXUser>(this, u);
}

int POSIXDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  {
    UserCacheEntry ce;
    if (user_cache.lookup_user_by_access_key(dpp, key, ce)) {
      auto u = new POSIXUser(this, ce.info);
      u->get_attrs() = ce.attrs;
      u->get_version_tracker() = ce.objv_tracker;
      user->reset(u);
      return 0;
    }
  }

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("access_key"), key, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new POSIXUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int POSIXDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("email"), email, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new POSIXUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int POSIXDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  /* Swift keys and subusers are not supported by DBStore for now */
  return -ENOTSUP;
}

int POSIXDriver::load_account_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view id,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("account_id"), std::string(id), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int POSIXDriver::load_account_by_name(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view name,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("name"), std::string(name), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int POSIXDriver::load_account_by_email(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view email,
				  RGWAccountInfo& info,
				  Attrs& attrs,
				  RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("email"), std::string(email), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int POSIXDriver::store_account(const DoutPrefixProvider* dpp,
			  optional_yield y, bool exclusive,
			  const RGWAccountInfo& info,
			  const RGWAccountInfo* old_info,
			  const Attrs& attrs,
			  RGWObjVersionTracker& objv)
{
  int ret = userDB->store_account(dpp, info, exclusive, &attrs, &objv);

  if (ret < 0)
    return ret;

  return 0;
}

int POSIXDriver::delete_account(const DoutPrefixProvider* dpp,
			     optional_yield y,
			     const RGWAccountInfo& info,
			     RGWObjVersionTracker& objv)
{
  int ret = userDB->remove_account(dpp, info, &objv);

  if (ret < 0)
    return ret;

  return 0;
}



int POSIXDriver::load_owner_by_email(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view email,
				    rgw_owner& owner)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_user(dpp, "email", std::string{email},
				   uinfo, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }
  owner = std::move(uinfo.user_id);
  return 0;
}

std::unique_ptr<Object> POSIXDriver::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(this, k);
}

int POSIXDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  *bucket = std::make_unique<POSIXBucket>(this, root_dir.get(), b);
  return (*bucket)->load_bucket(dpp, y);
}

std::unique_ptr<Bucket> POSIXDriver::get_bucket(const RGWBucketInfo& i)
{
  /* Don't need to fetch the bucket info, use the provided one */
  return std::make_unique<POSIXBucket>(this, root_dir.get(), i);
}

std::string POSIXDriver::zone_unique_trans_id(const uint64_t unique_num)
{
  char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
  time_t timestamp = time(NULL);

  snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
           (unsigned long long)unique_num,
           (unsigned long long)timestamp);

  return std::string(buf);
}

int POSIXDriver::get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zg)
{
  /* XXX: for now only one zonegroup supported */
  std::unique_ptr<RGWZoneGroup> rzg =
      std::make_unique<RGWZoneGroup>("default", "default");
  rzg->api_name = "default";
  rzg->is_master = true;
  ZoneGroup* group = new POSIXZoneGroup(this, std::move(rzg));
  if (!group)
    return -ENOMEM;

  zg->reset(group);
  return 0;
}

int POSIXDriver::list_all_zones(const DoutPrefixProvider* dpp,
			    std::list<std::string>& zone_ids)
{
  zone_ids.push_back(zone.get_id());
  return 0;
}

int POSIXDriver::cluster_stat(RGWClusterStat& stats)
{
  return 0;
}

std::unique_ptr<Lifecycle> POSIXDriver::get_lifecycle(void)
{
  return std::make_unique<POSIXLifecycle>(this);
}

std::unique_ptr<Writer> POSIXDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  return nullptr;
}

std::unique_ptr<Writer> POSIXDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  if (_head_obj->get_bucket()->get_info().versioning_enabled() &&
      !_head_obj->have_instance()) {
    _head_obj->gen_rand_obj_instance_name();
  }
  return std::make_unique<POSIXAtomicWriter>(dpp, y, _head_obj, this, owner, ptail_placement_rule, olh_epoch, unique_tag);
}

const std::string& POSIXDriver::get_compression_type(const rgw_placement_rule& rule) {
  return zone.get_rgw_params().get_compression_type(rule);
}

std::unique_ptr<Notification> POSIXDriver::get_notification(rgw::sal::Object* obj,
			      rgw::sal::Object* src_obj, struct req_state* s,
			      rgw::notify::EventType event_type, optional_yield y,
			      const std::string* object_name)
{
  rgw::notify::EventTypeList event_types = {event_type};
  auto notif = std::make_unique<POSIXNotification>(this, obj, src_obj, event_types,
      s->bucket.get(),
      to_string(s->owner.id),
      s->owner.id.index() == 0 ? std::get<rgw_user>(s->owner.id).tenant : "",
      s->req_id);
  notif->x_meta_map = s->info.x_meta_map;
  return notif;
}

std::unique_ptr<Notification> POSIXDriver::get_notification(
    const DoutPrefixProvider* dpp,
    rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    const rgw::notify::EventTypeList& event_types,
    rgw::sal::Bucket* _bucket,
    std::string& _user_id,
    std::string& _user_tenant,
    std::string& _req_id,
    optional_yield y) {
  return std::make_unique<POSIXNotification>(this, obj, src_obj, event_types,
      _bucket, _user_id, _user_tenant, _req_id);
}

// TODO: marker and other params
int POSIXDriver::list_buckets(const DoutPrefixProvider* dpp, const rgw_owner& owner,
			     const std::string& tenant, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &result, optional_yield y)
{
  DIR* dir;
  struct dirent* entry;
  int dfd;
  int ret;

  result.buckets.clear();

  /* it's not sufficient to dup(root_fd), as as the new fd would share
   * the file position of root_fd */
  dfd = copy_dir_fd(get_root_fd());
  if (dfd == -1) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    return -errno;
  }

  dir = fdopendir(dfd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    ::close(dfd);
    return -ret;
  }

  auto cleanup_guard = make_scope_guard(
    [&dir]
      {
	closedir(dir);
	// dfd is also closed
      }
    );

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    struct statx stx;

    ret = statx(get_root_fd(), entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      if (ret == ENOENT) {
	errno = 0;
	continue;
      }
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    if (!S_ISDIR(stx.stx_mode)) {
      /* Not a bucket, skip it */
      errno = 0;
      continue;
    }
    if (entry->d_name[0] == '.') {
      /* Skip dotfiles */
      errno = 0;
      continue;
    }
    std::unique_ptr<Bucket> bucket;
    ret = load_bucket(dpp, rgw_bucket("", entry->d_name), &bucket, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT) {
	errno = 0;
	continue;
      }
      return ret;
    }
    if (bucket->get_owner() != owner) {
      continue;
    }
    RGWBucketEnt ent;
    ent.bucket.name = url_decode(entry->d_name);
    ent.creation_time = ceph::real_clock::from_time_t(stx.stx_btime.tv_sec);
    // TODO: ent.size and ent.count

    result.buckets.push_back(std::move(ent));
    errno = 0;
    if (result.buckets.size() == max){
      result.next_marker = ent.bucket.marker;
      break;
    }
  }
  ret = errno;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list buckets for " << owner << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int POSIXBucket::create(const DoutPrefixProvider* dpp,
			const CreateParams& params,
			optional_yield y)
{
  info.owner = params.owner;

  if (params.marker.empty()) {
    char buf[17];
    gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
    buf[16] = '\0';
    info.bucket.marker = info.bucket.name + "." + buf;
    info.bucket.bucket_id = info.bucket.marker;
  } else {
    info.bucket.marker = params.marker;
    info.bucket.bucket_id = params.bucket_id;
  }

  info.zonegroup = params.zonegroup_id;
  info.placement_rule = params.placement_rule;
  info.swift_versioning = params.swift_ver_location.has_value();
  if (params.swift_ver_location) {
    info.swift_ver_location = *params.swift_ver_location;
  }
  if (params.obj_lock_enabled) {
    info.flags |= BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
  }
  info.requester_pays = false;
  if (params.creation_time) {
    info.creation_time = *params.creation_time;
  } else {
    info.creation_time = ceph::real_clock::now();
  }
  if (params.quota) {
    info.quota = *params.quota;
  }

  int ret = set_attrs(params.attrs);
  if (ret < 0) {
    return ret;
  }

  bool existed = false;
  ret = create(dpp, y, &existed);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int POSIXUser::load_user_from_cache_or_db(const DoutPrefixProvider* dpp, bool& cache_hit)
{
  cache_hit = false;
  UserCacheEntry ce;
  if (driver->get_user_cache().lookup_user_by_uid(dpp, this->get_id().id, ce)) {
    this->get_info() = std::move(ce.info);
    this->get_attrs() = std::move(ce.attrs);
    this->get_version_tracker() = std::move(ce.objv_tracker);
    cache_hit = true;
    return 0;
  }

  int ret = driver->get_user_db()->get_user(dpp, std::string("user_id"), this->get_id().id, this->get_info(), &(this->get_attrs()),
        &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  return ret;
}

int POSIXUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: read_attrs: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int POSIXUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  auto attrs = this->get_attrs();
  for(auto& it : new_attrs) {
	attrs[it.first] = it.second;
  }
  this->get_attrs() = std::move(attrs);

  return store_user(dpp, y, false);
}

int POSIXUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: load_user: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int POSIXUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  int ret = driver->get_user_db()->store_user(dpp, this->get_info(), exclusive, &(this->get_attrs()), &(this->get_version_tracker()), old_info);
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  return ret;
}

int POSIXUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = driver->get_user_db()->remove_user(dpp, this->get_info(), &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to remove user uid=" << this->get_id().id << " ret=" << ret << dendl;
  }
  return ret;
}

int POSIXUser::list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                           std::string_view marker, uint32_t max_items,
                           GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = driver->get_user_db()->list_user_groups(dpp,
      get_id().id, std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int POSIXUser::verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider *dpp, optional_yield y)
{
  *verified = false;
  return 0;
}

std::unique_ptr<Object> POSIXBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(driver, k, this);
}

int POSIXObject::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  return ent->fill_cache(dpp, y, cb, posix::FSEnt::FLAG_NONE);
}

int POSIXDriver::mint_listing_entry(const std::string &bname,
                                    rgw_bucket_dir_entry &bde) {
    std::unique_ptr<Bucket> b;
    std::unique_ptr<Object> obj;
    POSIXObject *pobj;
    int ret;

    ret = load_bucket(nullptr, rgw_bucket(std::string(), bname),
                      &b, null_yield);
    if (ret < 0)
      return ret;

    obj = b->get_object(posix::decode_obj_key(bde.key.name));
    pobj = static_cast<POSIXObject *>(obj.get());

    if (!pobj->check_exists(nullptr)) {
      ret = errno;
      return -ret;
    }

    ret = pobj->get_obj_attrs(null_yield, nullptr);
    if (ret < 0)
      return ret;

    ret = pobj->fill_cache(nullptr, null_yield,
        [&bde](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &nbde) -> int {
	  bde = nbde;
	  return 0;
        });

    return ret;
}

std::unique_ptr<LuaManager> POSIXDriver::get_lua_manager(const std::string& luarocks_path)
{
  return std::make_unique<POSIXLuaManager>(this);
}

std::unique_ptr<RGWRole> POSIXDriver::get_role(std::string name,
    std::string tenant,
    rgw_account_id account_id,
    std::string path,
    std::string trust_policy,
    std::string description,
    std::string max_session_duration_str,
    std::multimap<std::string,std::string> tags)
{
  return std::make_unique<DBStoreRole>(
      get_user_db(), std::move(name), std::move(tenant),
      std::move(account_id), std::move(path), std::move(trust_policy),
      std::move(description), std::move(max_session_duration_str),
      std::move(tags));
}

std::unique_ptr<RGWRole> POSIXDriver::get_role(std::string id)
{
  return std::make_unique<DBStoreRole>(get_user_db(), std::move(id));
}

std::unique_ptr<RGWRole> POSIXDriver::get_role(const RGWRoleInfo& info)
{
  return std::make_unique<DBStoreRole>(get_user_db(), info);
}

int POSIXDriver::count_account_roles(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     uint32_t& count)
{
  return get_user_db()->count_account_roles(dpp,
      std::string(account_id), count);
}

int POSIXDriver::list_account_roles(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view path_prefix,
				    std::string_view marker,
				    uint32_t max_items,
				    RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "account",
      "", std::string(account_id),
      std::string(path_prefix), std::string(marker),
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int POSIXDriver::list_roles(const DoutPrefixProvider *dpp,
			    optional_yield y,
			    const std::string& tenant,
			    const std::string& path_prefix,
			    const std::string& marker,
			    uint32_t max_items,
			    RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "tenant",
      tenant, "",
      path_prefix, marker,
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int POSIXDriver::load_account_user_by_name(const DoutPrefixProvider* dpp,
					   optional_yield y,
					   std::string_view account_id,
					   std::string_view tenant,
					   std::string_view username,
					   std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_account_user_by_name(dpp,
      std::string(account_id), std::string(username), uinfo);
  if (ret < 0) {
    return ret;
  }
  if (user) {
    *user = get_user(uinfo.user_id);
    (*user)->get_info() = uinfo;
    ret = (*user)->load_user(dpp, y);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int POSIXDriver::count_account_users(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     uint32_t& count)
{
  return get_user_db()->count_account_users(dpp,
      std::string(account_id), count);
}

int POSIXDriver::list_account_users(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view tenant,
				    std::string_view path_prefix,
				    std::string_view marker,
				    uint32_t max_items,
				    UserList& listing)
{
  std::vector<RGWUserInfo> users;
  int ret = get_user_db()->list_account_users(dpp,
      std::string(account_id), std::string(marker),
      max_items + 1, users);
  if (ret < 0) {
    return ret;
  }

  if (!path_prefix.empty()) {
    std::string pp(path_prefix);
    users.erase(
        std::remove_if(users.begin(), users.end(),
            [&pp](const RGWUserInfo& u) {
              return u.path.substr(0, pp.size()) != pp;
            }),
        users.end());
  }

  if (users.size() > max_items) {
    listing.next_marker = users[max_items].display_name;
    users.resize(max_items);
  }
  listing.users = std::move(users);
  return 0;
}

int POSIXDriver::load_group_by_id(const DoutPrefixProvider* dpp,
				  optional_yield y, std::string_view id,
				  RGWGroupInfo& info, Attrs& attrs,
				  RGWObjVersionTracker& objv)
{
  info.id = std::string(id);
  return get_user_db()->get_group(dpp, "group_id", info, attrs);
}

int POSIXDriver::load_group_by_name(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view name,
				    RGWGroupInfo& info, Attrs& attrs,
				    RGWObjVersionTracker& objv)
{
  info.account_id = std::string(account_id);
  info.name = std::string(name);
  return get_user_db()->get_group(dpp, "name", info, attrs);
}

int POSIXDriver::store_group(const DoutPrefixProvider* dpp, optional_yield y,
			     const RGWGroupInfo& info, const Attrs& attrs,
			     RGWObjVersionTracker& objv, bool exclusive,
			     const RGWGroupInfo* old_info)
{
  return get_user_db()->store_group(dpp, info, attrs, exclusive);
}

int POSIXDriver::remove_group(const DoutPrefixProvider* dpp, optional_yield y,
			      const RGWGroupInfo& info,
			      RGWObjVersionTracker& objv)
{
  return get_user_db()->remove_group(dpp, info);
}

int POSIXDriver::list_group_users(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view tenant,
				  std::string_view id,
				  std::string_view marker,
				  uint32_t max_items,
				  UserList& listing)
{
  std::vector<std::string> user_ids;
  int ret = get_user_db()->list_group_users(dpp,
      std::string(id), std::string(marker), max_items + 1, user_ids);
  if (ret < 0) {
    return ret;
  }

  if (user_ids.size() > max_items) {
    listing.next_marker = user_ids[max_items];
    user_ids.resize(max_items);
  }

  for (auto& uid : user_ids) {
    RGWUserInfo uinfo;
    uinfo.user_id.id = uid;
    ret = get_user_db()->get_user(dpp, std::string("user_id"), uid,
                                  uinfo, nullptr, nullptr);
    if (ret < 0) {
      continue;
    }
    listing.users.push_back(std::move(uinfo));
  }
  return 0;
}

int POSIXDriver::count_account_groups(const DoutPrefixProvider* dpp,
				      optional_yield y,
				      std::string_view account_id,
				      uint32_t& count)
{
  return get_user_db()->count_account_groups(dpp,
      std::string(account_id), count);
}

int POSIXDriver::list_account_groups(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     std::string_view path_prefix,
				     std::string_view marker,
				     uint32_t max_items,
				     GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = get_user_db()->list_account_groups(dpp,
      std::string(account_id), std::string(path_prefix),
      std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int POSIXDriver::store_oidc_provider(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     const RGWOIDCProviderInfo& info,
				     bool exclusive,
				     RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->store_oidc_provider(dpp, info, exclusive);
}

int POSIXDriver::load_oidc_provider(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view tenant,
				    std::string_view url,
				    RGWOIDCProviderInfo& info,
				    RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->load_oidc_provider(dpp,
      std::string(tenant), std::string(url), info);
}

int POSIXDriver::delete_oidc_provider(const DoutPrefixProvider* dpp,
				      optional_yield y,
				      std::string_view tenant,
				      std::string_view url)
{
  return get_user_db()->delete_oidc_provider(dpp,
      std::string(tenant), std::string(url));
}

int POSIXDriver::get_oidc_providers(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view tenant,
				    std::vector<RGWOIDCProviderInfo>& providers)
{
  return get_user_db()->list_oidc_providers(dpp,
      std::string(tenant), providers);
}

/* --- Notification publish methods --- */

int POSIXNotification::publish_reserve(const DoutPrefixProvider *dpp,
				       RGWObjTags* obj_tags)
{
  obj_tags_ptr = obj_tags;

  if (!bucket) {
    return 0;
  }

  int ret = get_bucket_notifications(dpp, bucket, bucket_topics);
  if (ret < 0) {
    return ret;
  }

  const std::string obj_name = obj ? obj->get_name() : "";

  for (auto& [name, filter] : bucket_topics.topics) {
    bool event_match = false;
    for (auto req_type : event_types) {
      for (auto cfg_type : filter.events) {
	if (static_cast<uint64_t>(req_type) & static_cast<uint64_t>(cfg_type)) {
	  event_match = true;
	  break;
	}
      }
      if (event_match) break;
    }
    if (!event_match) continue;

    if (!match(filter.s3_filter.key_filter, obj_name)) {
      continue;
    }

    if (!filter.s3_filter.metadata_filter.kv.empty()) {
      if (!match(filter.s3_filter.metadata_filter, x_meta_map)) {
	continue;
      }
    }

    if (!filter.s3_filter.tag_filter.kv.empty()) {
      KeyMultiValueMap tags;
      if (obj_tags) {
	tags = obj_tags->get_tags();
      }
      if (!match(filter.s3_filter.tag_filter, tags)) {
	continue;
      }
    }

    matched.push_back(filter);
  }

  return 0;
}

int POSIXNotification::publish_commit(const DoutPrefixProvider* dpp,
				      uint64_t size,
				      const ceph::real_time& mtime,
				      const std::string& etag,
				      const std::string& version)
{
  if (matched.empty()) {
    return 0;
  }

  for (auto& filter : matched) {
    const auto& dest = filter.topic.dest;
    if (dest.push_endpoint.empty()) {
      continue;
    }

    rgw_pubsub_s3_event event;
    event.eventTime = mtime;
    event.eventName = rgw::notify::to_string(event_types.front());
    event.userIdentity = user_id;
    event.x_amz_request_id = req_id;
    event.configurationId = filter.s3_id;
    if (bucket) {
      event.bucket_name = bucket->get_name();
      event.bucket_ownerIdentity = to_string(bucket->get_owner());
      event.bucket_id = bucket->get_bucket_id();
    }
    if (obj) {
      event.object_key = obj->get_name();
    }
    event.object_size = size;
    event.object_etag = etag;
    event.object_versionId = version;

    try {
      RGWHTTPArgs args(dest.push_endpoint_args, dpp);
      auto endpoint = RGWPubSubEndpoint::create(
	  dest.push_endpoint, filter.topic.name, args,
	  dpp->get_cct());
      int r = endpoint->send(dpp, event, null_yield);
      if (r < 0) {
	ldpp_dout(dpp, 1) << "ERROR: notification endpoint send failed: "
			  << dest.push_endpoint << " ret=" << r << dendl;
      }
    } catch (const RGWPubSubEndpoint::configuration_error& e) {
      ldpp_dout(dpp, 1) << "ERROR: notification endpoint config error: "
			<< e.what() << dendl;
    }
  }

  return 0;
}

/* --- Topic SAL methods --- */

int POSIXDriver::read_topic_v2(const std::string& topic_name,
			       const std::string& tenant,
			       rgw_pubsub_topic& topic,
			       RGWObjVersionTracker* objv_tracker,
			       optional_yield y,
			       const DoutPrefixProvider* dpp)
{
  obj_version objv;
  int ret = get_user_db()->load_topic(dpp, topic_name, tenant, topic, objv);
  if (!ret && objv_tracker) {
    objv_tracker->read_version = objv;
  }
  return ret;
}

int POSIXDriver::write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
				RGWObjVersionTracker& objv_tracker,
				optional_yield y,
				const DoutPrefixProvider* dpp)
{
  return get_user_db()->store_topic(dpp, topic, exclusive, objv_tracker.write_version);
}

int POSIXDriver::remove_topic_v2(const std::string& topic_name,
				 const std::string& tenant,
				 RGWObjVersionTracker& objv_tracker,
				 optional_yield y,
				 const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_topic(dpp, topic_name, tenant);
}

int POSIXDriver::update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					     const std::string& bucket_key,
					     bool add_mapping,
					     optional_yield y,
					     const DoutPrefixProvider* dpp)
{
  if (add_mapping) {
    return get_user_db()->add_bucket_topic_mapping(dpp, topic.name, bucket_key);
  } else {
    return get_user_db()->remove_bucket_topic_mapping(dpp, topic.name, bucket_key);
  }
}

int POSIXDriver::get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					  std::set<std::string>& bucket_keys,
					  optional_yield y,
					  const DoutPrefixProvider* dpp)
{
  return get_user_db()->get_bucket_topic_mapping(dpp, topic.name, bucket_keys);
}

int POSIXDriver::remove_bucket_mapping_from_topics(
    const rgw_pubsub_bucket_topics& bucket_topics,
    const std::string& bucket_key,
    optional_yield y,
    const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_bucket_from_topic_mappings(dpp, bucket_key);
}

int POSIXDriver::list_account_topics(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     std::string_view marker,
				     uint32_t max_items,
				     TopicList& listing)
{
  rgw_owner owner = rgw_account_id(std::string(account_id));
  std::vector<rgw_pubsub_topic> topics;
  int ret = get_user_db()->list_topics(dpp, "owner", owner,
      std::string(marker), max_items, topics);
  if (ret) {
    return ret;
  }
  for (auto& t : topics) {
    listing.topics.push_back(std::move(t.name));
  }
  if (!listing.topics.empty()) {
    listing.next_marker = listing.topics.back();
  }
  return 0;
}

struct meta_list_handle {
  std::string marker;
  std::string section;

  DIR *dir = nullptr;
  long dpos = -1;

  meta_list_handle(const std::string& _section, const std::string& _marker) {
    marker = _marker;
    section = _section;
  }
};

int POSIXDriver::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                     const std::string& section,
                                     const std::string& marker, void** phandle)
{
  meta_list_handle* stuff = new meta_list_handle(section, marker);
  *phandle = (void *)stuff;
  if (section == "bucket") {
    int ret;
    int dfd = copy_dir_fd(get_root_fd());
    if (dfd == -1) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
                        << cpp_strerror(errno) << dendl;
      return -ret;
    }

    stuff->dir = fdopendir(dfd);
    if (stuff->dir == NULL) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
                        << cpp_strerror(ret) << dendl;
      ::close(dfd);
      return -ret;
    }
  }
  return 0;
  }

int POSIXDriver::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
                                     int max, std::list<std::string>& keys,
                                     bool* truncated)
{
  meta_list_handle *h = static_cast<meta_list_handle *>(handle);
  *truncated = false;
  int ret;
  keys.clear();
  if (h->section == "user") {
    ret = get_user_db()->list_users(dpp, h->marker, max, keys, truncated);
    if (ret < 0) {
      return ret;
    }
    if (keys.size() > 0) {
      h->marker = *keys.rbegin();
      if (std::cmp_equal(keys.size(),max)) {
        *truncated = true;
      }
    }
  } else if (h->section == "bucket") {
    if (h->dpos != -1) {
      seekdir(h->dir, h->dpos);
    }
    struct dirent* entry;
    while ((entry = readdir(h->dir)) != NULL) {
      if (entry->d_type == DT_UNKNOWN) {
        struct statx stx;

        ret = statx(get_root_fd(), entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
        if (ret < 0) {
          ret = errno;
          ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	                    << cpp_strerror(ret) << dendl;
          return -ret;
        }
        if (!S_ISDIR(stx.stx_mode)) {
        /* Not a bucket, skip it */
          continue;
        }
      } else if (entry->d_type != DT_DIR) {
        continue;
      }
      if (entry->d_name[0] == '.') {
        /* Skip dotfiles */
        continue;
     }
      keys.push_back(entry->d_name);
      if (std::cmp_equal(keys.size(),max)) {
        h->dpos = telldir(h->dir);
        *truncated = true;
        break;
      }
    }
  }
  return 0;
}

void POSIXDriver::meta_list_keys_complete(void* handle)
{
  if (handle) {
    meta_list_handle *h = static_cast<meta_list_handle *>(handle);
    if (h->section == "bucket") {
      closedir(h->dir);
    }
    delete h;
  }
  return;
}

int POSIXBucket::fill_cache(const DoutPrefixProvider* dpp, optional_yield y,
                            fill_cache_cb_t& cb)
{
  return dir->fill_cache(dpp, y, cb, posix::FSEnt::FLAG_NONE);
}

int POSIXBucket::list(const DoutPrefixProvider* dpp, ListParams& params,
		    int max, ListResults& results, optional_yield y)
{
  /* multipart namespace: incomplete uploads live in staging directories
   * that aren't in the LMDB cache — delegate to list_multiparts() and
   * format results using RADOS naming conventions so the LC processor
   * can parse them via rgw_obj_key::parse_index_key().
   *
   * Skip this when we ARE a shadow (staging) bucket (ns == mp_ns) —
   * list_parts() uses shadow->list() to enumerate part files inside
   * the staging directory, which must fall through to the normal
   * LMDB/directory listing below. */
  if (params.ns == mp_ns && ns != mp_ns) {
    std::vector<std::unique_ptr<MultipartUpload>> uploads;
    std::string marker;
    int ret = list_multiparts(dpp, "", marker, "",
			      max, uploads, nullptr,
			      &results.is_truncated, y);
    if (ret < 0)
      return ret;
    for (auto& upload : uploads) {
      const auto& obj_name = upload->get_key();
      if (!params.prefix.empty() &&
	  !obj_name.starts_with(params.prefix)) {
	continue;
      }
      rgw_bucket_dir_entry bde{};
      bde.key.name = fmt::format("_{}_{}",
				 mp_ns,
				 upload->get_meta());
      bde.meta.mtime = upload->get_mtime();
      bde.meta.category = RGWObjCategory::MultiMeta;
      bde.exists = true;
      results.objs.push_back(std::move(bde));
    }
    return 0;
  }

int count{0};
bool in_prefix{false};
// Names in the cache are in OID format
rgw_obj_key marker_key(params.marker);
params.marker = marker_key.get_oid();
{
  rgw_obj_key key(params.prefix);
  params.prefix = key.name;
}
if (max <= 0) {
    return 0;
  }

  //params.list_versions
  int ret = driver->get_bucket_cache()->list_bucket(
    dpp, y, this, params.marker.name, [&](const rgw_bucket_dir_entry& bde) -> bool
      {
	std::string ns;
	// bde.key can be encoded with the namespace.  Decode it here
	rgw_obj_key bde_key{bde.key};
	if (!params.list_versions && !bde.is_visible()) {
	  return true;
	}
	if (params.list_versions && versioned() && bde_key.instance.empty()) {
	  return true;
	}
        if (bde_key.ns != params.ns) {
          // Namespace must match
          return true;
        }
        if (!marker_key.empty() && marker_key == bde_key.name) {
	  // Skip marker
	  return true;
	}
	if (!params.prefix.empty()) {
	  // We have a prefix, only match
          if (!bde_key.name.starts_with(params.prefix)) {
            // Prefix doesn't match; skip
	    if (in_prefix) {
              return false;
            }
            return true;
          }
	  // Prefix matches
	  if (params.delim.empty()) {
	    // No delimiter, add matches
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          auto delim_pos = bde_key.name.find(params.delim, params.prefix.size());
          if (delim_pos == std::string_view::npos) {
	    // Straight prefix match
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
	  }
          results.next_marker =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!results.common_prefixes.contains(results.next_marker.name)) {
            results.common_prefixes[results.next_marker.name] = true;
            count++; // Count will be checked when we exit prefix
            if (in_prefix) {
              // We've hit the next prefix entry.  Check count
              if (count >= max) {
                results.is_truncated = true;
                // Time to stop
                return false;
	      }
            }
          }
          in_prefix = true;
          return true;
        }
        if (!params.delim.empty()) {
	  // Delimiter, but no prefix
	  auto delim_pos = bde_key.name.find(params.delim) ;
          if (delim_pos == std::string_view::npos) {
	    // Delimiter doesn't match, insert
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          std::string prefix_key =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!marker_key.empty() && marker_key == prefix_key) {
            // Skip marker
            return true;
          }
	  std::string decoded_key;
	  rgw_obj_key::parse_index_key(prefix_key, &decoded_key, &ns);
          if (!results.common_prefixes.contains(decoded_key)) {
	    if (in_prefix) {
	      // New prefix, check the count
	      count++;
              if (count >= max) {
                results.is_truncated = true;
                return false;
              }
            }
	    in_prefix = true;
            results.common_prefixes[decoded_key] = true;
	    // Fallthrough
          }
	  results.next_marker.name = decoded_key;
	  return true;
        }

        results.next_marker.set(bde.key);
        results.objs.push_back(bde);
        count++;
        if (count >= max) {
          results.is_truncated = true;
          return false;
        }
        return true;
    });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    results.objs.clear();
    return ret;
  }

  return 0;
}

int POSIXBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  for (auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }

  return write_attrs(dpp, y);
}

int POSIXBucket::remove(const DoutPrefixProvider* dpp,
			bool delete_children,
			optional_yield y)
{
  int ret = dir->remove(dpp, y, delete_children, nullptr);
  if (ret < 0) {
    return ret;
  }

  driver->get_bucket_cache()->invalidate_bucket(dpp, get_name());

  return ret;
}

int POSIXBucket::remove_bypass_gc(int concurrent_max,
				  bool keep_index_consistent,
				  optional_yield y,
				  const DoutPrefixProvider *dpp)
{
  return remove(dpp, true, y);
}

int POSIXBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  if (get_name()[0] == '.') {
    /* Skip dotfiles */
    return -ERR_INVALID_OBJECT_NAME;
  }
  ret = dir->stat(dpp);
  if (ret < 0) {
    return ret;
  }

  mtime = ceph::real_clock::from_time_t(dir->get_stx().stx_mtime.tv_sec);
  info.creation_time = ceph::real_clock::from_time_t(dir->get_stx().stx_btime.tv_sec);

  ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = dir->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  RGWBucketInfo bak_info = info;;
  ret = posix::decode_attr(attrs, RGW_POSIX_ATTR_BUCKET_INFO, info);
  if (ret < 0) {
    // TODO dang: fake info up (UID to owner conversion?)
    info = bak_info;
  } else {
    // Don't leave info visible in attributes
    attrs.erase(RGW_POSIX_ATTR_BUCKET_INFO);
  }

  return 0;
}

int POSIXBucket::set_acl(const DoutPrefixProvider* dpp,
			 RGWAccessControlPolicy& acl,
			 optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  info.owner = acl.get_owner().id;

  return write_attrs(dpp, y);
}

int POSIXBucket::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
			    const bucket_index_layout_generation& idx_layout,
			    int shard_id, std::string* bucket_ver, std::string* master_ver,
			    std::map<RGWObjCategory, RGWStorageStats>& stats,
			    std::string* max_marker, bool* syncstopped)
{
  auto& main = stats[RGWObjCategory::Main];

  // TODO: bucket stats shouldn't have to list all objects
  return dir->for_each(dpp, [this, dpp, y, &main] (const char* name) {
    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    std::unique_ptr<posix::FSEnt> dent;
    int ret = dir->get_ent(dpp, y, name, std::string(), dent);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get ent for object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    ret = dent->stat(dpp);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    struct statx& lstx = dent->get_stx();

    if (S_ISREG(lstx.stx_mode) || S_ISDIR(lstx.stx_mode)) {
      main.num_objects++;
      main.size += lstx.stx_size;
      main.size_rounded += lstx.stx_size;
      main.size_utilized += lstx.stx_size;
    }

    return 0;
  });
  return 0;
}

int POSIXBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return 0;
}

int POSIXBucket::sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                  RGWBucketEnt* ent)
{
  return 0;
}

int POSIXBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                     uint64_t num_objs, optional_yield y)
{
  return 0;
}

int POSIXBucket::chown(const DoutPrefixProvider* dpp,
                       const rgw_owner& new_owner,
                       const std::string& new_owner_name,
                       optional_yield y) {
  /* TODO map user to UID/GID, and change it */
  return 0;
}

int POSIXBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
{
  mtime = _mtime;

  struct timespec ts[2];
  ts[0].tv_nsec = UTIME_OMIT;
  ts[1] = ceph::real_clock::to_timespec(mtime);
  int ret = utimensat(dir->get_parent()->get_fd(), get_fname().c_str(), ts, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not set mtime on bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return write_attrs(dpp, y);
}

int POSIXBucket::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  encode(info, bl);
  Attrs extra_attrs;
  extra_attrs[RGW_POSIX_ATTR_BUCKET_INFO] = bl;

  return dir->write_attrs(dpp, y, attrs, &extra_attrs);
}

int POSIXBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return dir->for_each(dpp, [](const char* name) {
    /* for_each filters out "." and "..", so reaching here is not empty */
    std::string_view check_name = name;
    if (!check_name.starts_with(".multipart")) { // incomplete uploads can be deleted
      return -ENOTEMPTY;
    }
    return 0;
  });
}

int POSIXBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
  return driver->get_quota_handler()->check_quota(dpp, info.owner, get_key(),
                                                  quota, (check_size_only ? 0 : 1),
                                                  obj_size, y);
}

int POSIXBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y)
{
  *pmtime = mtime;

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return dir->read_attrs(dpp, y, attrs);
}

int POSIXBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			    uint64_t end_epoch, uint32_t max_entries,
			    bool* is_truncated, RGWUsageIter& usage_iter,
			    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int POSIXBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  return 0;
}

int POSIXBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return 0;
}

int POSIXBucket::check_index(const DoutPrefixProvider *dpp, optional_yield y,
                             std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                             std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return 0;
}

int POSIXBucket::rebuild_index(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int POSIXBucket::set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout)
{
  return 0;
}

int POSIXBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

std::unique_ptr<MultipartUpload> POSIXBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<POSIXMultipartUpload>(driver, this, oid, upload_id, owner, mtime);
}

int POSIXBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated, optional_yield y)
{
  int count = 0;
  int ret;

  ret = dir->for_each(dpp, [this, dpp, y, &count, &max_uploads, &is_truncated, &uploads] (const char* name) {
    std::string_view d_name = name;
    static std::string mp_pre{"." + mp_ns + "_"};
    if (!d_name.starts_with(mp_pre)) {
      /* Skip non-uploads */
      return 0;
    }

    if (count >= max_uploads) {
      if (is_truncated) {
	*is_truncated = true;
      }

      return -EAGAIN;
    }

    d_name.remove_prefix(mp_pre.size());

    /* d_name is the URL-encoded meta string (oid.upload_id) from
     * the staging directory name — decode it so from_meta() can
     * parse out the object key and upload_id */
    std::string decoded_meta = url_decode(std::string(d_name));

    /* use the staging directory's mtime as the upload creation time */
    struct statx stx;
    if (statx(dir->get_fd(), name, AT_SYMLINK_NOFOLLOW, STATX_MTIME, &stx) < 0) {
      return 0;
    }
    auto mtime = from_statx_timestamp(stx.stx_mtime);

    ACLOwner owner;
    std::unique_ptr<MultipartUpload> upload =
        std::make_unique<POSIXMultipartUpload>(
            driver, this, decoded_meta, std::nullopt, owner,
            mtime);
    rgw_placement_rule* rule{nullptr};
    int ret = upload->get_info(dpp, y, &rule, nullptr);
    if (ret < 0)
      return 0;
    uploads.emplace(uploads.end(), std::move(upload));
    count++;

    return 0;
  });

  return ret;
}

int POSIXBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y)
{
  return 0;
}

int POSIXBucket::create(const DoutPrefixProvider* dpp, optional_yield y, bool* existed)
{
  int ret = dir->create(dpp, existed);
  if (ret < 0) {
    return ret;
  }

  return write_attrs(dpp, y);
}

std::string POSIXBucket::get_fname()
{
  return bucket_fname(get_name(), ns);
}

int POSIXBucket::rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj)
{
  int ret;
  posix::Directory* dst_dir = dir->get_parent();

  info.bucket.name = target_obj->get_key().get_oid();
  ns.reset();

  if (!target_obj->get_instance().empty()) {
    /* This is a versioned object.  Need to handle versioneddirectory */
    POSIXObject *to = static_cast<POSIXObject *>(target_obj);
    ret = to->open(dpp, true, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open target obj " << to->get_name() << dendl;
      return ret;
    }
    dst_dir = static_cast<posix::Directory *>(to->get_fsent());
  }

  return dir->rename(dpp, y, dst_dir, get_fname());
}

int POSIXObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				uint32_t flags,
                                std::list<rgw_obj_index_key>* remove_objs,
				RGWObjVersionTracker* objv)
{
  POSIXBucket *b = static_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = stat(dpp);
  if (ret < 0) {
      if (ret == -ENOENT) {
	// Nothing to do
	return 0;
      }
      return ret;
  }
  ret = ent->remove(dpp, y, /*delete_children=*/false, &del_result);

  cls_rgw_obj_key key;
  get_key().get_index_key(&key);

  driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);

  if (!key.instance.empty() && !ent->exists()) {
    /* Remove the non-versioned key as well */
    key.instance.clear();
    driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);
  }

  /* after removing a versioned entry, the current version may have
   * changed — if the symlink now points to a delete marker, update
   * its cache entry to add FLAG_CURRENT so the LC can expire it */
  if (!get_key().instance.empty() && ent->get_parent()) {
    auto* vdir = dynamic_cast<posix::VersionedDirectory*>(ent->get_parent());
    if (vdir) {
      std::unique_ptr<posix::Symlink> sl =
	std::make_unique<posix::Symlink>(vdir->get_name(), vdir, driver->ctx());
      if (sl->stat(dpp) >= 0 && sl->exists()) {
	auto* target = sl->get_target();
	std::string cur_name = target->get_name();
	if (!cur_name.empty()) {
	  std::unique_ptr<posix::FSEnt> cur_ent;
	  ret = vdir->get_ent(dpp, y, cur_name, std::string(), cur_ent);
	  if (ret == 0) {
	    cur_ent->stat(dpp);
	    rgw_bucket_dir_entry bde{};
	    rgw_obj_key cur_key = posix::decode_obj_key(cur_name);
	    cur_key.get_index_key(&bde.key);
	    bde.flags = rgw_bucket_dir_entry::FLAG_VER
	      | rgw_bucket_dir_entry::FLAG_CURRENT;
	    bde.ver.pool = 1;
	    bde.ver.epoch = 1;
	    bde.exists = true;
	    bde.meta.mtime = from_statx_timestamp(cur_ent->get_stx().stx_mtime);
	    bde.meta.size = cur_ent->get_stx().stx_size;
	    bde.meta.accounted_size = bde.meta.size;
	    if (bde.meta.size == 0) {
	      Attrs attrs;
	      bufferlist bl;
	      if (cur_ent->read_attrs(dpp, y, attrs) >= 0 &&
		  posix::get_attr(attrs, RGW_POSIX_ATTR_VERSION, bl)) {
		bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
	      }
	    }
	    driver->get_bucket_cache()->add_entry(dpp, b->get_name(), bde);
	  }
	}
      }
    }
  }

  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            -1, 0, state.accounted_size);
  return 0;
}

int POSIXObject::copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              rgw::sal::DataProcessorFactory* dp_factory,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  int ret;
  POSIXBucket *db = static_cast<POSIXBucket*>(dest_bucket);
  POSIXBucket *sb = static_cast<POSIXBucket*>(src_bucket);
  POSIXObject *dobj = static_cast<POSIXObject*>(dest_object);

  if (!db || !sb) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket to copy " << get_name()
                      << dendl;
    return -EINVAL;
  }
  if (db->get_info().versioning_enabled() &&
      !dest_object->have_instance()) {
    dest_object->gen_rand_obj_instance_name();
  }
  bool has_instance = !get_key().instance.empty();

  // Source must exist, and we need to know if it's a shadow obj
  if (!check_exists(dpp)) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  /* check copy-source preconditions against the source object */
  if (if_match) {
    std::string if_match_str = rgw_string_unquote(if_match);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (mod_ptr && state.mtime <= *mod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }
  if (unmod_ptr && state.mtime > *unmod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }

  if (!get_key().instance.empty() && !has_instance) {
    /* For copy, no instance meance copy all instances.  Clear intance id if it
     * was passed in clear. */
    get_key().instance.clear();
  }

  if (state.obj != dobj->state.obj) {
    /* An actual copy, copy the data */
    ret = copy(dpp, y, sb, db, dobj);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to copy object " << get_key()
                          << dendl;
        return ret;
    }
  }
  dobj->make_ent(ent->get_type());

  /* Set up attributes for destination */
  Attrs src_attrs = state.attrset;
  /* Come attrs are never copied */
  src_attrs.erase(RGW_ATTR_DELETE_AT);
  src_attrs.erase(RGW_ATTR_OBJECT_RETENTION);
  src_attrs.erase(RGW_ATTR_OBJECT_LEGAL_HOLD);
  /* Some attrs, if they exist, always come from the call */
  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  bufferlist rt;
  if (get_attr(RGW_ATTR_OBJECT_RETENTION, rt)) {
    src_attrs[RGW_ATTR_OBJECT_RETENTION] = rt;
  }
  bufferlist lh;
  if (get_attr(RGW_ATTR_OBJECT_LEGAL_HOLD, lh)) {
    src_attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = lh;
  }

  bufferlist tt;
  switch (attrs_mod) {
  case ATTRSMOD_REPLACE:
    /* Keep tags if not set */
    if (!attrs[RGW_ATTR_ETAG].length()) {
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];
    }
    if (!attrs[RGW_ATTR_TAIL_TAG].length() &&
	posix::get_attr(src_attrs, RGW_ATTR_TAIL_TAG, tt)) {
      attrs[RGW_ATTR_TAIL_TAG] = tt;
    }
    break;

  case ATTRSMOD_MERGE:
    for (auto it = src_attrs.begin(); it != src_attrs.end(); ++it) {
      if (attrs.find(it->first) == attrs.end()) {
	attrs[it->first] = it->second;
      }
    }
    break;
  case ATTRSMOD_NONE:
    {
      auto tags = attrs.extract(RGW_ATTR_TAGS);
      attrs = src_attrs;
      if (!tags.empty()) {
        attrs[RGW_ATTR_TAGS] = std::move(tags.mapped());
      }
    }
    ret = 0;
    break;
  }

  /* Some attrs always come from the source */
  bufferlist com;
  if (posix::get_attr(src_attrs, RGW_ATTR_COMPRESSION, com)) {
    attrs[RGW_ATTR_COMPRESSION] = com;
  }
  bufferlist mpu;
  if (posix::get_attr(src_attrs, RGW_POSIX_ATTR_MPUPLOAD, mpu)) {
    attrs[RGW_POSIX_ATTR_MPUPLOAD] = mpu;
  }
  bufferlist pot;
  if (posix::get_attr(src_attrs, RGW_POSIX_ATTR_OBJECT_TYPE, pot)) {
    attrs[RGW_POSIX_ATTR_OBJECT_TYPE] = pot;
  }
  return dobj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
}

int POSIXObject::list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			    int max_parts, int marker, int* next_marker,
			    bool* truncated, list_parts_each_t&& each_func,
			    optional_yield y)
{
  if (ent->get_type() != posix::ObjectType::MULTIPART) {
    return 0;
  }

  uint16_t pc = 0;
  if (!decode_raw_attr(state.attrset, RGW_POSIX_ATTR_MULTIPART_PART_COUNT, pc) || pc == 0) {
    return 0;
  }

  auto* mpdir = static_cast<posix::MPDirectory*>(ent.get());
  int start = marker + 1;
  int end = std::min((int)pc, marker + max_parts);
  int count = 0;

  for (int pn = start; pn <= end; ++pn) {
    auto part_file = mpdir->get_part_file(pn);
    int ret = part_file->open(dpp);
    if (ret < 0) {
      continue;
    }

    Object::Part obj_part;
    obj_part.part_number = pn;

    Attrs part_attrs;
    ret = part_file->read_attrs(dpp, y, part_attrs);
    if (ret == 0) {
      POSIXUploadPartInfo info;
      if (decode_raw_attr(part_attrs, RGW_POSIX_ATTR_MPUPLOAD, info)) {
        obj_part.part_size = info.size;
        if (info.cksum) {
          obj_part.cksum = *info.cksum;
        }
      }
    }

    if (obj_part.part_size == 0) {
      ret = part_file->stat(dpp);
      if (ret == 0) {
        obj_part.part_size = part_file->get_size();
      }
    }

    ret = each_func(obj_part);
    if (ret < 0) {
      return ret;
    }
    ++count;
  }

  *next_marker = marker + count;
  *truncated = (*next_marker < (int)pc);
  return 0;
}

bool POSIXObject::is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                                    const ceph::real_time& obj_mtime)
{
  return false;
}

int POSIXObject::load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh)
{
  int ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = get_obj_attrs(y, dpp);

  return ret;
}

int POSIXObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  if (delattrs) {
    for (auto& it : *delattrs) {
      if (it.first == RGW_POSIX_ATTR_OBJECT_TYPE) {
	// Don't delete type
	continue;
      }
      state.attrset.erase(it.first);
    }
  }
  if (setattrs) {
    for (auto& it : *setattrs) {
      if (it.first == RGW_POSIX_ATTR_OBJECT_TYPE) {
	// Don't overwrite type
	continue;
      }
      state.attrset[it.first] = it.second;
    }
  }

  write_attrs(dpp, y);
  return 0;
}

int POSIXObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp)
{
  //int fd;

  int ret = open(dpp, false);
  if (ret < 0) {
    return ret;
  }

  ret = ent->read_attrs(dpp, y, state.attrset);
  if (ret == 0)
    state.has_attrs = true;
  else
    state.has_attrs = false;

  return ret;
}

int POSIXObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp, uint32_t flags)
{
  state.attrset[attr_name] = attr_val;
  return write_attrs(dpp, y);
}

int POSIXObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  state.attrset.erase(attr_name);

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = posix::remove_x_attr(dpp, y, ent->get_fd(), attr_name, get_name());
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remover attribute " << attr_name << " for " << get_name() << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

bool POSIXObject::is_expired()
{
  utime_t delete_at;
  if (!posix::decode_attr(state.attrset, RGW_ATTR_DELETE_AT, delete_at)) {
    ldout(driver->ctx(), 0)
        << "ERROR: " << __func__
        << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
    return false;
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

void POSIXObject::gen_rand_obj_instance_name()
{
  state.obj.key.set_instance(gen_rand_instance_name());
}

std::unique_ptr<MPSerializer> POSIXObject::get_serializer(const DoutPrefixProvider *dpp, optional_yield y, const std::string& lock_name)
{
  return std::make_unique<MPPOSIXSerializer>(dpp, driver, this, lock_name);
}

int MPPOSIXSerializer::try_lock(const DoutPrefixProvider *dpp, ceph::timespan dur, optional_yield y)
{
  if (!obj->check_exists(dpp)) {
    return -ENOENT;
  }

  POSIXBucket* b = static_cast<POSIXBucket*>(obj->get_bucket());
  if (b->get_dir()->get_type() == posix::ObjectType::MULTIPART && b->get_dir_fd(dpp) > 0) {
    locked = true;
    return 0;
  }

  return -ENOENT;
}

int MPPOSIXSerializer::unlock(const DoutPrefixProvider *dpp, optional_yield y)
{
  clear_locked();
  return 0;
}

int POSIXObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y,
                            uint32_t flags)
{
  return -ERR_NOT_IMPLEMENTED;
}

int POSIXObject::transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

int POSIXObject::restore_obj_from_cloud(Bucket* bucket,
          rgw::sal::PlacementTier* tier,
	  CephContext* cct,
          std::optional<uint64_t> days,
          bool& in_progress,
	  uint64_t& size,
          const DoutPrefixProvider* dpp,
          optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

bool POSIXObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return (r1 == r2);
}

int POSIXObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
    return 0;
}

int POSIXObject::swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  POSIXBucket *b = static_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }
  /* TODO Get UID from user */
  int uid = 0;
  int gid = 0;

  int ret = fchownat(b->get_dir_fd(dpp), get_fname(/*use_version=*/true).c_str(), uid, gid, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  return 0;
}

int POSIXObject::get_cur_version(const DoutPrefixProvider* dpp, rgw_obj_key& key)
{
  return 0;
}

int POSIXObject::set_cur_version(const DoutPrefixProvider *dpp)
{
  if (!ent) {
    int ret = open(dpp, true, false);
    if (ret < 0) {
      return ret;
    }
  }
  if (ent->get_type() != posix::ObjectType::VERSIONED) {
    return -EINVAL;
  }
  posix::VersionedDirectory* vdir = static_cast<posix::VersionedDirectory*>(ent.get());
  std::unique_ptr<posix::FSEnt> child;
  int ret = vdir->get_ent(dpp, null_yield, get_fname(true), std::string(), child);
  if (ret < 0) {
    return ret;
  }

  ret = vdir->set_cur_version_ent(dpp, child.get());
  return ret;
}

int POSIXObject::stat(const DoutPrefixProvider* dpp)
{
  int ret;

  if (!ent) {
    ret = static_cast<POSIXBucket *>(bucket)->get_dir()->get_ent(
        dpp, null_yield, get_fname(/*use_version=*/false), state.obj.key.instance, ent);
    if (ret < 0) {
      state.exists = false;
      return ret;
    }
  }

  ret = ent->stat(dpp);
  if (ret < 0) {
    state.exists = false;
    return ret;
  }

  if (state.obj.key.instance.empty()) {
    state.obj.key.instance = ent->get_cur_version();
  }

  state.exists = ent->exists();
  if (!state.exists) {
    return 0;
  }

  state.accounted_size = state.size = ent->get_stx().stx_size;
  state.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);

  return 0;
}

int POSIXObject::make_ent(posix::ObjectType type)
{
  if (ent)
    return 0;

  switch (type.type) {
    case posix::ObjectType::UNKNOWN:
      return -EINVAL;
    case posix::ObjectType::FILE:
      ent = std::make_unique<posix::File>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case posix::ObjectType::DIRECTORY:
      ent = std::make_unique<posix::Directory>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case posix::ObjectType::SYMLINK:
      ent = std::make_unique<posix::Symlink>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case posix::ObjectType::MULTIPART:
      ent = std::make_unique<posix::MPDirectory>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case posix::ObjectType::VERSIONED:
      ent = std::make_unique<posix::VersionedDirectory>(
          get_fname(/*use_version=*/false), static_cast<POSIXBucket *>(bucket)->get_dir(), get_instance(), driver->ctx());
      break;
  }

  return 0;
}

int POSIXObject::get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner)
{
  ACLOwner acl_owner;
  int ret = posix::decode_acl_owner(get_attrs(), acl_owner);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__
        << ": No " RGW_ATTR_ACL " attr" << dendl;
    return ret;
  }

  if (const auto* u = std::get_if<rgw_user>(&acl_owner.id)) {
    *owner = driver->get_user(*u);
  } else {
    *owner = driver->get_user(rgw_user(std::get<rgw_account_id>(acl_owner.id)));
  }
  (*owner)->load_user(dpp, y);
  return 0;
}

std::unique_ptr<Object::ReadOp> POSIXObject::get_read_op()
{
  return std::make_unique<POSIXReadOp>(this);
}

std::unique_ptr<Object::DeleteOp> POSIXObject::get_delete_op()
{
  return std::make_unique<POSIXDeleteOp>(this);
}

int POSIXObject::open(const DoutPrefixProvider* dpp, bool create, bool temp_file)
{
  int ret{0};

  if (!ent) {
    ret = stat(dpp);
    if (ret < 0) {
      if (!create) {
	return ret;
      }
      if (versioned()) {
        ret = make_ent(posix::ObjectType::VERSIONED);
      } else {
        ret = make_ent(posix::ObjectType::FILE);
      }
    }
  }
  if (ret < 0) {
    return ret;
  }

  if (create) {
    ret = ent->create(dpp, nullptr, temp_file);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not create " << ent->get_name() << dendl;
      return ret;
    }
  }

  return ent->open(dpp);
}

int POSIXObject::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string temp_fname = gen_temp_fname();
  int ret = ent->link_temp_file(dpp, y, temp_fname);
  if (ret < 0)
    return ret;

  POSIXBucket *b = static_cast<POSIXBucket *>(get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name()
		      << dendl;
    return -EINVAL;
  }

  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: POSIXAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: POSIXAtomicWriter failed closing file" << dendl;
    return ret;
  }

  fill_cache( nullptr, null_yield,
      [&](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &bde) -> int {
	driver->get_bucket_cache()->add_entry(dpp, b->get_name(), bde);
	return 0;
      });
  return 0;
}


int POSIXObject::close()
{
  if (ent)
    return ent->close();

  return 0;
}

int POSIXObject::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!ent)
    return -ENOENT;
  return ent->read(ofs, left, bl, dpp, y);
}

int POSIXObject::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  return ent->write(ofs, bl, dpp, y);
}

int POSIXObject::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return ent->write_attrs(dpp, y, state.attrset, nullptr);
}

int POSIXObject::POSIXReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = source->stat(dpp);
  if (ret < 0)
    return ret;

  ret = source->get_obj_attrs(y, dpp);
  if (ret < 0)
    return ret;

  bufferlist etag_bl;
  if (!source->get_attr(RGW_ATTR_ETAG, etag_bl)) {
    /* Sideloaded file.  Generate necessary attributes. Only done once. */
    int ret = source->generate_attrs(dpp, y);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not generate attrs for " << source->get_name() << " error: " << cpp_strerror(ret) << dendl;
	return ret;
    }
  }

  if (!source->get_attr(RGW_ATTR_ETAG, etag_bl)) {
    return -EINVAL;
  }

  {
    uint16_t pc = 0;
    if (decode_raw_attr(source->get_attrs(), RGW_POSIX_ATTR_MULTIPART_PART_COUNT, pc) && pc > 0) {
      params.parts_count = pc;
    }
  }

  if (params.part_num) {
    int pn = *params.part_num;
    if (source->ent->get_type() != posix::ObjectType::MULTIPART) {
      if (pn == 1) {
        params.parts_count = 1;
      } else {
        return -ERR_INVALID_PART;
      }
    } else {
      auto* mpdir = static_cast<posix::MPDirectory*>(source->ent.get());
      const auto& pmap = mpdir->get_parts();
      std::string pname = MP_OBJ_PART_PFX + fmt::format("{:0>5}", pn);
      auto it = pmap.find(pname);
      if (it == pmap.end()) {
        return -ERR_INVALID_PART;
      }
      int64_t ofs = 0;
      for (int i = 1; i < pn; ++i) {
        std::string prev = MP_OBJ_PART_PFX + fmt::format("{:0>5}", i);
        auto pit = pmap.find(prev);
        if (pit != pmap.end()) {
          ofs += pit->second;
        }
      }
      part_ofs = ofs;
      source->set_obj_size(it->second);
    }
  }

#if 0 // WIP
  if (params.mod_ptr || params.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);
    src_weight.high_precision = params.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = params.high_precision_time;

    if (params.mod_ptr && !params.if_nomatch) {
      dest_weight.init(*params.mod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      dest_weight.init(*params.unmod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
#endif

  if (params.mod_ptr || params.unmod_ptr) {
    if (params.mod_ptr && !params.if_nomatch) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.mod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (!(*params.mod_ptr < source->get_mtime())) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.unmod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (*params.unmod_ptr < source->get_mtime()) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (params.if_match) {
    std::string if_match_str = rgw_string_unquote(params.if_match);
    ldpp_dout(dpp, 10) << "If-Match: " << if_match_str << " ETAG: " << etag_bl.c_str() << dendl;

    if (if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (params.if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
    ldpp_dout(dpp, 10) << "If-No-Match: " << if_nomatch_str << " ETAG: " << etag_bl.c_str() << dendl;
    if (if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_NOT_MODIFIED;
    }
  }

  if (params.lastmod) {
    *params.lastmod = source->get_mtime();
  }

  return 0;
}

int POSIXObject::POSIXReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  return source->read(ofs + part_ofs, end + 1, bl, dpp, y);
}

int POSIXObject::generate_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  ret = generate_etag(dpp, y);
  return ret;
}

int POSIXObject::generate_mp_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::generate_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t left = get_size();
  int64_t cur_ofs = 0;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

  while (left > 0) {
    bufferlist bl;
    int len = read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }
    hash.Update((const unsigned char *)bl.c_str(), bl.length());

    left -= len;
    cur_ofs += len;
  }

  hash.Final(m);
  bufferlist etag_bl;
  append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1, [&](auto iter) {
    iter = buf_to_hex(m, iter);
    *iter++ = '\0';
    return iter;
  });
  get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));
  return write_attrs(dpp, y);
}

const std::string POSIXObject::get_fname(bool use_version)
{
  return posix::get_key_fname(state.obj.key, use_version);
}

std::string POSIXObject::gen_temp_fname()
{
  std::string temp_fname;
  enum { RAND_SUFFIX_SIZE = 8 };
  char buf[RAND_SUFFIX_SIZE + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, RAND_SUFFIX_SIZE);
  temp_fname = "." + get_fname(/*use_version=*/true) + ".";
  temp_fname.append(buf);

  return temp_fname;
}

int POSIXObject::POSIXReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int64_t left;
  int64_t cur_ofs = ofs + part_ofs;
  end += part_ofs;

  if (end < 0)
    left = 0;
  else
    left = end - ofs + 1;

  while (left > 0) {
    bufferlist bl;
    int len = source->read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << source->get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }

    /* Read some */
    int ret = cb->handle_data(bl, 0, len);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: callback failed on " << source->get_name() << ": " << ret << dendl;
	return ret;
    }

    left -= len;
    cur_ofs += len;
  }

  /* Doesn't seem to be anything needed from params */
  return 0;
}

int POSIXObject::POSIXReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  if (!source->check_exists(dpp)) {
    return -ENOENT;
  }
  if (source->get_obj_attrs(y, dpp) < 0) {
    return -ENODATA;
  }
  if (!source->get_attr(name, dest)) {
    return -ENODATA;
  }

  return 0;
}

int POSIXObject::POSIXDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y, uint32_t flags)
{
  bool has_cond = params.if_match ||
    !real_clock::is_zero(params.last_mod_time_match) ||
    params.size_match.has_value();

  if (has_cond) {
    int ret = source->stat(dpp);
    if (ret == -ENOENT) {
      return 0;
    }
    if (ret < 0) {
      return ret;
    }

    if (params.if_match && strcmp(params.if_match, "*") != 0) {
      auto it = source->get_attrs().find(RGW_ATTR_ETAG);
      if (it == source->get_attrs().end()) {
        return -ERR_PRECONDITION_FAILED;
      }
      bufferlist& bl = it->second;
      std::string if_match_str = rgw_string_unquote(params.if_match);
      if (if_match_str.compare(0, bl.length(), bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }

    if (!real_clock::is_zero(params.last_mod_time_match)) {
      if (params.last_mod_time_match_precise) {
        if (params.last_mod_time_match != source->get_mtime()) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        if (real_clock::to_time_t(params.last_mod_time_match) !=
            real_clock::to_time_t(source->get_mtime())) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }

    if (params.size_match.has_value()) {
      if (*params.size_match != source->get_size()) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  int ret = source->delete_object(dpp, y, flags, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }
  result = source->get_result();
  return 0;
}

int POSIXObject::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      POSIXBucket *sb, POSIXBucket *db, POSIXObject *dobj)
{
  rgw_obj_key dst_key = dobj->get_key();
  if (!get_key().instance.empty())
    dst_key.instance = get_key().instance;

  return ent->copy(dpp, y, db->get_dir(), posix::get_key_fname(dst_key, /*use_version=*/true));
}

void POSIXMPObj::init_gen(POSIXDriver* driver, const std::string& _oid, ACLOwner& _owner)
{
  char buf[33];
  std::string new_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
  /* Generate an upload ID */

  gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
  new_id.append(buf);
  init(_oid, new_id, _owner);
}

int POSIXMultipartPart::load(const DoutPrefixProvider* dpp, optional_yield y,
			     POSIXDriver* driver, rgw_obj_key& key)
{
  if (part_file) {
    /* Already loaded */
    return 0;
  }

  part_file = std::make_unique<posix::File>(posix::get_key_fname(key, false), upload->get_shadow()->get_dir(), driver->ctx());

  // Stat the part_file object to get things like size
  int ret = part_file->stat(dpp, y);
  if (ret < 0) {
    return ret;
  }

  Attrs attrs;
  ret = part_file->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  ret = posix::decode_attr(attrs, RGW_POSIX_ATTR_MPUPLOAD, info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": failed to decode part info: " << key << dendl;
    return ret;
  }

  return 0;
}

int POSIXMultipartUpload::load(const DoutPrefixProvider *dpp, bool create)
{
  int ret = 0;
  if (!shadow) {
    POSIXBucket* pb = static_cast<POSIXBucket*>(bucket);
    std::optional<std::string> ns{mp_ns};

    std::unique_ptr<posix::Directory> mpdir = std::make_unique<posix::MPDirectory>(bucket_fname(get_meta(), ns), pb->get_dir(), driver->ctx());

    shadow = std::make_unique<POSIXBucket>(driver, std::move(mpdir), rgw_bucket(std::string(), get_meta()), mp_ns);

    ret = shadow->load_bucket(dpp, null_yield);
    if (ret == -ENOENT && create) {
      ret = shadow->create(dpp, null_yield, nullptr);
    }
  }

  return ret;
}

std::unique_ptr<rgw::sal::Object> POSIXMultipartUpload::get_meta_obj()
{
  std::unique_ptr<rgw::sal::Object> meta_obj{nullptr};

  load(nullptr);

  if (!shadow) {
    meta_obj = bucket->get_object(rgw_obj_key(get_meta(), std::string(), mp_ns));
  } else {
    meta_obj = shadow->get_object(rgw_obj_key(get_meta(), std::string()));
  }

  auto posix_meta_obj = static_cast<POSIXObject*>(meta_obj.get());
  if (shadow) {
    posix_meta_obj->pin_bucket(shadow->clone());
  }
  rgw::sal::Attrs attrs;
  if (obj_retention) {
    buffer::list obj_retention_bl;
    obj_retention->encode(obj_retention_bl);
    attrs[RGW_ATTR_OBJECT_RETENTION] = std::move(obj_retention_bl);
  }
  if (obj_legal_hold) {
    buffer::list obj_legal_hold_bl;
    obj_legal_hold->encode(obj_legal_hold_bl);
    attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = std::move(obj_legal_hold_bl);
  }
  posix_meta_obj->set_attrs(attrs);

  return meta_obj;
}

int POSIXMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  int ret;

  /* Create the shadow bucket */
  ret = load(dpp, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << " ERROR: could not get shadow dir for mp upload "
      << get_key() << dendl;
    return ret;
  }

  /* Now create the meta object */
  std::unique_ptr<rgw::sal::Object> meta_obj;

  meta_obj = get_meta_obj();

  ret = static_cast<POSIXObject*>(meta_obj.get())->open(dpp, true);
  if (ret < 0) {
    return ret;
  }

  mp_obj.upload_info.cksum_type = cksum_type;
  mp_obj.upload_info.cksum_flags = cksum_flags;

  if (obj_retention) {
    mp_obj.upload_info.obj_retention_exist = true;
    mp_obj.upload_info.obj_retention = *obj_retention;
  }
  if (obj_legal_hold) {
    mp_obj.upload_info.obj_legal_hold_exist = true;
    mp_obj.upload_info.obj_legal_hold = *obj_legal_hold;
  }

  mp_obj.upload_info.dest_placement = dest_placement;
  mp_obj.owner = owner;

  bufferlist bl;
  encode(mp_obj, bl);

  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

  return meta_obj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
}

int POSIXMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated, optional_yield y,
				      bool assume_unsorted)
{
  int ret;
  int last_num = 0;

  ret = load(dpp);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = MP_OBJ_PART_PFX;
  params.marker = MP_OBJ_PART_PFX + fmt::format("{:0>5}", marker);
  params.marker.ns = mp_ns;
  params.ns = mp_ns;

  ret = shadow->list(dpp, params, num_parts + 1, results, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: list_parts: shadow->list failed ret="
      << ret << " upload=" << get_upload_id() << dendl;
    return ret;
  }
  if (results.objs.empty()) {
    ldpp_dout(dpp, 0) << "WARNING: list_parts: 0 results for upload="
      << get_upload_id() << " shadow=" << shadow->get_name()
      << " marker=" << params.marker.name << dendl;
  }
  for (rgw_bucket_dir_entry& ent : results.objs) {
    std::unique_ptr<MultipartPart> part = std::make_unique<POSIXMultipartPart>(this);
    POSIXMultipartPart* ppart = static_cast<POSIXMultipartPart*>(part.get());

    rgw_obj_key key(ent.key);
    // Parts are namespaced in the bucket listing
    key.ns.clear();
    ret = ppart->load(dpp, y, driver, key);
    if (ret == 0) {
      /* Skip anything that's not a part */
      last_num = part->get_num();
      parts[part->get_num()] = std::move(part);
    }
    if (parts.size() == (ulong)num_parts)
      break;
  }

  if (truncated)
    *truncated = results.is_truncated;

  if (next_marker)
    *next_marker = last_num;

  return 0;
}

int POSIXMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
{
  int ret;

  ret = load(dpp);
  if (ret < 0) {
    if (ret == -ENOENT)
      ret = ERR_NO_SUCH_UPLOAD;
    return ret;
  }

  driver->get_bucket_cache()->invalidate_bucket(dpp, shadow->get_name(), true);
  shadow->remove(dpp, true, y);

  return 0;
}

int POSIXMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
				    prefix_map_t& processed_prefixes,
            const char *if_match,
            const char *if_nomatch)
{
  if (bucket->get_info().versioning_enabled() &&
      !target_obj->have_instance()) {
    target_obj->gen_rand_obj_instance_name();
  }
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs& attrs = target_obj->get_attrs();

  ofs = accounted_size = 0;

  do {
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated, y);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret < 0)
      return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }

    for (auto obj_iter = parts.begin(); etags_iter != part_etags.end() && obj_iter != parts.end(); ++etags_iter, ++obj_iter, ++handled_parts) {
      POSIXMultipartPart* part = static_cast<POSIXMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
			 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      std::string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      hex_to_buf(part->get_etag().c_str(), petag,
		CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));

      // Compression is not supported yet
#if 0
      RGWUploadPartInfo& obj_part = part->info;

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
      }

      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }
#endif

      ofs += part->get_size();
      accounted_size += part->get_size();
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  bufferlist etag_bl;
  append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16, [&](auto iter) {
    iter = buf_to_hex(final_etag, iter);
    iter = fmt::format_to(iter, "-{}", part_etags.size());
    return iter;
  });

  attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  {
    uint16_t pc = total_parts;
    encode_attr(attrs, RGW_POSIX_ATTR_MULTIPART_PART_COUNT, pc);
    encode_attr(attrs, RGW_POSIX_ATTR_MULTIPART_TOTAL_SIZE, accounted_size);

    ret = shadow->merge_and_store_attrs(dpp, attrs, y);
    if (ret < 0) {
      return ret;
    }
  }

  /* conditional write checks against existing target object */
  if (if_match || if_nomatch) {
    POSIXObject *tobj = static_cast<POSIXObject*>(target_obj);
    bool target_exists = tobj->check_exists(dpp);

    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        if (!target_exists) {
          return -ENOENT;
        }
      } else {
        if (!target_exists) {
          return -ENOENT;
        }
        bufferlist bl;
        if (!posix::get_attr(tobj->get_attrs(), RGW_ATTR_ETAG, bl)) {
          return -ERR_PRECONDITION_FAILED;
        }
        std::string if_match_str = rgw_string_unquote(if_match);
        if (if_match_str != bl.to_str()) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        if (target_exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else if (target_exists) {
        bufferlist bl;
        if (posix::get_attr(tobj->get_attrs(), RGW_ATTR_ETAG, bl)) {
          std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
          if (if_nomatch_str == bl.to_str()) {
            return -ERR_PRECONDITION_FAILED;
          }
        }
      }
    }
  }

  // save shadow name before rename changes info.bucket.name
  std::string shadow_cache_name = shadow->get_name();

  // Rename to target_obj
  ret = shadow->rename(dpp, y, target_obj);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to rename to final name " << target_obj->get_name()
		      << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  POSIXObject *to = static_cast<POSIXObject*>(target_obj);
  POSIXBucket *sb = static_cast<POSIXBucket*>(target_obj->get_bucket());
  if (sb->versioned()) {
    ret = to->set_cur_version(dpp);
    if (ret < 0) {
      return ret;
    }
  }

  // remove staging directory listing cache entry (frees LMDB DBI slot)
  driver->get_bucket_cache()->invalidate_bucket(dpp, shadow_cache_name, true);

  return 0;
}

int POSIXMultipartUpload::cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
    CephContext *cct, optional_yield y,
    const rgw_obj& obj,
    std::list<rgw_obj_index_key>& remove_objs,
    prefix_map_t& processed_prefixes)
{
  return -ENOTSUP;
}

int POSIXMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y,
				   rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
{
  std::unique_ptr<rgw::sal::Object> meta_obj;
  int ret;

  if (!rule && !attrs) {
    return 0;
  }

  if (attrs) {
      meta_obj = get_meta_obj();
      int ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
	  << get_key() << dendl;
	return ret;
      }
      *attrs = meta_obj->get_attrs();
  }

  if (rule) {
    if (mp_obj.upload_info.dest_placement.name.empty()) {
      if (!meta_obj) {
	meta_obj = get_meta_obj();
      }
      ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
                          << get_key() << dendl;
        return ret;
      }
      ret = posix::decode_attr(meta_obj->get_attrs(), RGW_POSIX_ATTR_MPUPLOAD, mp_obj);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object attrs for mp upload "
	  << get_key() << dendl;
	return ret;
      }
    }
    *rule = &mp_obj.upload_info.dest_placement;

    if (mp_obj.upload_info.obj_retention_exist) {
      obj_retention = mp_obj.upload_info.obj_retention;
    }
    if (mp_obj.upload_info.obj_legal_hold_exist) {
      obj_legal_hold = mp_obj.upload_info.obj_legal_hold;
    }

    /* no te olvides los cksum */
    cksum_type = mp_obj.upload_info.cksum_type;
    cksum_flags = mp_obj.upload_info.cksum_flags;
  }

  return 0;
}

std::string POSIXMultipartUpload::get_fname()
{
  std::string name;

  name = "." + mp_ns + "_" + url_encode(get_meta(), true);

  return name;
}

std::unique_ptr<Writer> POSIXMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  std::string fname = MP_OBJ_PART_PFX + fmt::format("{:0>5}", part_num);
  rgw_obj_key part_key(fname);

  load(dpp);

  return std::make_unique<POSIXMultipartWriter>(dpp, y, shadow.get(), part_key,
                                                driver, owner,
                                                ptail_placement_rule, part_num);
}

int POSIXMultipartWriter::prepare(optional_yield y)
{
  int ret = part_file->create(dpp, /*existed=*/nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }

  return part_file->open(dpp);
}

int POSIXMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  return part_file->write(offset, data, dpp, null_yield);
}

int POSIXMultipartWriter::complete(
		       size_t accounted_size,
		       const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  int ret;
  POSIXUploadPartInfo info;

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      // test the object is existing
      if (!part_file->exists()) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      Attrs attrs;
      bufferlist bl;
      ret = part_file->read_attrs(rctx.dpp, rctx.y, attrs);
      if (ret < 0) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (!posix::get_attr(attrs, RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  info.num = part_num;
  info.size = accounted_size;
  info.etag = etag;
  info.cksum = cksum;
  info.mtime = set_mtime;

  bufferlist bl;
  encode(info, bl);
  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

  ret = part_file->write_attrs(rctx.dpp, rctx.y, attrs, /*extra_attrs=*/nullptr);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: failed writing attrs for " << part_file->get_name() << dendl;
    return ret;
  }

  ret = part_file->close();
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: failed closing file" << dendl;
    return ret;
  }

  return 0;
}

int POSIXAtomicWriter::prepare(optional_yield y)
{
  int ret;

  if (obj->versioned()) {
    ret = obj->make_ent(posix::ObjectType::VERSIONED);
  } else {
    ret = obj->make_ent(posix::ObjectType::FILE);
  }
  if (ret < 0) {
    return ret;
  }
  obj->get_obj_attrs(y, dpp);
  obj->close();
  return obj->open(dpp, true, true);
}

int POSIXAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  return obj->write(offset, data, dpp, null_yield);
}

int POSIXAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  int ret;
  uint64_t orig_size = 0;
  auto exists = obj->check_exists(dpp);
  if (exists) {
    orig_size = obj->get_size();
  }

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      if (!exists) {
	return -ENOENT;
      }
    } else {
      if (!exists) {
	return -ENOENT;
      }
      bufferlist bl;
      if (!posix::get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      std::string if_match_str = rgw_string_unquote(if_match);
      if (if_match_str.compare(0, bl.length(), bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (if_nomatch) {
    if (strcmp(if_nomatch, "*") == 0) {
      // test the object is not existing
      if (exists) {
	return -ERR_PRECONDITION_FAILED;
      }
    } else {
      bufferlist bl;
      if (posix::get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
        if (if_nomatch_str.compare(0, bl.length(), bl.c_str(), bl.length()) == 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  /* owner is already in attrs[RGW_ATTR_ACL] from the generic layer */

  obj->set_attrs(attrs);
  ret = obj->write_attrs(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: POSIXAtomicWriter failed writing attrs for "
                       << obj->get_name() << dendl;
    return ret;
  }

  ret = obj->link_temp_file(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed writing temp file" << dendl;
    return ret;
  }

  POSIXBucket *b = static_cast<POSIXBucket*>(obj->get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << obj->get_name() << dendl;
      return -EINVAL;
  }
  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            (exists ? 0 : 1), orig_size, accounted_size);

  return 0;
}

int POSIXLifecycle::get_entry(const DoutPrefixProvider* dpp, optional_yield y,
                              const std::string& oid, const std::string& marker,
                              LCEntry& entry)
{
  return driver->get_user_db()->get_entry(oid, marker, entry);
}

int POSIXLifecycle::get_next_entry(const DoutPrefixProvider* dpp, optional_yield y,
				   const std::string& oid, const std::string& marker,
                                   LCEntry& entry)
{
  return driver->get_user_db()->get_next_entry(oid, marker, entry);
}

int POSIXLifecycle::set_entry(const DoutPrefixProvider* dpp, optional_yield y,
                              const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->set_entry(oid, entry);
}

int POSIXLifecycle::list_entries(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& oid, const std::string& marker,
                                uint32_t max_entries, std::vector<LCEntry>& entries)
{
  return driver->get_user_db()->list_entries(oid, marker, max_entries, entries);
}

int POSIXLifecycle::rm_entry(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->rm_entry(oid, entry);
}

int POSIXLifecycle::get_head(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, LCHead& head)
{
  return driver->get_user_db()->get_head(oid, head);
}

int POSIXLifecycle::put_head(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const LCHead& head)
{
  return driver->get_user_db()->put_head(oid, head);
}

std::unique_ptr<LCSerializer> POSIXLifecycle::get_serializer(const std::string& lock_name,
                                                             const std::string& oid,
                                                             const std::string& cookie)
{
  return std::make_unique<LCPOSIXSerializer>(driver, oid, lock_name, cookie);

}

void POSIXDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  mgr->register_resource("user", new RGWRESTMgr_User);
  /* TODO: register "bucket" once rgw_rest_bucket is decoupled from rados */
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newPOSIXDriver(CephContext *cct)
{
  rgw::sal::POSIXDriver* driver = new rgw::sal::POSIXDriver(cct);

  int ret = -1;
  const static std::string tenant = "default_ns";
  if ((ret = driver->get_user_db()->Initialize("", -1)) < 0) {
    ldout(cct, 0) << "DB initialization failed for tenant("<<tenant<<")" << dendl;
    return nullptr;
  }

  driver->set_context(cct);

  return driver;
}

}
