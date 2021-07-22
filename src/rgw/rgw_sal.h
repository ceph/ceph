// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_user.h"
#include "rgw_notify_event_type.h"
#include "rgw_putobj.h"

class RGWGetDataCB;
struct RGWObjState;
class RGWAccessListFilter;
class RGWLC;
class RGWObjManifest;
struct RGWZoneGroup;
struct RGWZoneParams;
struct RGWRealm;
struct RGWCtl;
struct rgw_user_bucket;
class RGWUsageBatch;
class RGWCoroutinesManagerRegistry;
class RGWListRawObjsCtx;
class RGWBucketSyncPolicyHandler;
using RGWBucketSyncPolicyHandlerRef = std::shared_ptr<RGWBucketSyncPolicyHandler>;
class RGWDataSyncStatusManager;
class RGWSyncModuleInstance;
typedef std::shared_ptr<RGWSyncModuleInstance> RGWSyncModuleInstanceRef;
namespace rgw {
  class Aio;
  namespace IAM { struct Policy; }
}

struct RGWUsageIter {
  std::string read_iter;
  uint32_t index;

  RGWUsageIter() : index(0) {}
};

/**
 * @struct RGWClusterStat
 * Cluster-wide usage information
 */
struct RGWClusterStat {
  /// total device size
  uint64_t kb;
  /// total used
  uint64_t kb_used;
  /// total available/free
  uint64_t kb_avail;
  /// number of objects
  uint64_t num_objects;
};

class RGWGetBucketStats_CB : public RefCountedObject {
protected:
  rgw_bucket bucket;
  map<RGWObjCategory, RGWStorageStats>* stats;
public:
  explicit RGWGetBucketStats_CB(const rgw_bucket& _bucket) : bucket(_bucket), stats(NULL) {}
  ~RGWGetBucketStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(map<RGWObjCategory, RGWStorageStats>* _stats) {
    stats = _stats;
  }
};

class RGWGetUserStats_CB : public RefCountedObject {
protected:
  rgw_user user;
  RGWStorageStats stats;
public:
  explicit RGWGetUserStats_CB(const rgw_user& _user) : user(_user) {}
  ~RGWGetUserStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(RGWStorageStats& _stats) {
    stats = _stats;
  }
};


namespace rgw { namespace sal {

#define RGW_SAL_VERSION 1

class User;
class Bucket;
class Object;
class BucketList;
struct MPSerializer;
class Lifecycle;
class Notification;
class GCChain;
class Writer;
class Zone;
class LuaScriptManager;
class RGWOIDCProvider;
class RGWRole;

enum AttrsMod {
  ATTRSMOD_NONE    = 0,
  ATTRSMOD_REPLACE = 1,
  ATTRSMOD_MERGE   = 2
};

/**
 * Base class for AIO completions
 */
class Completions {
  public:
    Completions() {}
    virtual ~Completions() = default;
    virtual int drain() = 0;
};

using Attrs = std::map<std::string, ceph::buffer::list>;

class Store {
  public:
    Store() {}
    virtual ~Store() = default;

    /* This one does not query the cluster for info */
    virtual std::unique_ptr<User> get_user(const rgw_user& u) = 0;
    /* These three do query the cluster for info */
    virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) = 0;
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) = 0;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) = 0;
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            User* u, const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
			    Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
			    bool exclusive,
			    bool obj_lock_enabled,
			    bool* existed,
			    req_info& req_info,
			    std::unique_ptr<Bucket>* bucket,
			    optional_yield y) = 0;
    virtual bool is_meta_master() = 0;
    virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
					  bufferlist& in_data, JSONParser* jp, req_info& info,
					  optional_yield y) = 0;
    virtual int defer_gc(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Bucket* bucket, Object* obj,
			 optional_yield y) = 0;
    virtual Zone* get_zone() = 0;
    virtual std::string zone_unique_id(uint64_t unique_num) = 0;
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) = 0;
    virtual int cluster_stat(RGWClusterStat& stats) = 0;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) = 0;
    virtual std::unique_ptr<Completions> get_completions(void) = 0;
    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, struct req_state* s, 
        rgw::notify::EventType event_type, const std::string* object_name=nullptr) = 0;
    virtual std::unique_ptr<GCChain> get_gc_chain(rgw::sal::Object* obj) = 0;
    virtual std::unique_ptr<Writer> get_writer(Aio* aio, rgw::sal::Bucket* bucket,
              RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::Object> _head_obj,
              const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual RGWLC* get_rgwlc(void) = 0;
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() = 0;
    virtual int delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj) = 0;
    virtual int delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, Completions* aio) = 0;
    virtual void get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj) = 0;
    virtual int get_raw_chunk_size(const DoutPrefixProvider* dpp, const rgw_raw_obj& obj, uint64_t* chunk_size) = 0;

    virtual int log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info) = 0;
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) = 0;
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
					const map<std::string, std::string>& meta) = 0;
    virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) = 0;
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, vector<rgw_bucket>& buckets, bool enabled) = 0;
    virtual uint64_t get_new_req_id() = 0;
    virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
					std::optional<rgw_zone_id> zone,
					std::optional<rgw_bucket> bucket,
					RGWBucketSyncPolicyHandlerRef* phandler,
					optional_yield y) = 0;
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) = 0;
    virtual void wakeup_meta_sync_shards(set<int>& shard_ids) = 0;
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, map<int, set<std::string> >& shard_ids) = 0;
    virtual int clear_usage(const DoutPrefixProvider *dpp) = 0;
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    virtual int get_config_key_val(std::string name, bufferlist* bl) = 0;
    virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) = 0;
    virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<std::string>& keys, bool* truncated) = 0;
    virtual void meta_list_keys_complete(void* handle) = 0;
    virtual std::string meta_get_marker(void* handle) = 0;
    virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) = 0;
    virtual const RGWSyncModuleInstanceRef& get_sync_module() = 0;
    virtual std::string get_host_id() = 0;
    virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() = 0;
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="") = 0;
    virtual std::unique_ptr<RGWRole> get_role(std::string id) = 0;
    virtual int get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  vector<std::unique_ptr<RGWRole>>& roles) = 0;
    virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() = 0;
    virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
				   const std::string& tenant,
				   vector<std::unique_ptr<RGWOIDCProvider>>& providers) = 0;

    virtual void finalize(void) = 0;

    virtual CephContext* ctx(void) = 0;
    
    // get the location of where lua packages are installed
    virtual const std::string& get_luarocks_path() const = 0;
    // set the location of where lua packages are installed
    virtual void set_luarocks_path(const std::string& path) = 0;
};

class User {
  protected:
    RGWUserInfo info;
    RGWObjVersionTracker objv_tracker;
    Attrs attrs;

  public:
    User() : info() {}
    User(const rgw_user& _u) : info() { info.user_id = _u; }
    User(const RGWUserInfo& _i) : info(_i) {}
    User(User& _o) = default;
    virtual ~User() = default;

    virtual std::unique_ptr<User> clone() = 0;
    virtual int list_buckets(const DoutPrefixProvider* dpp,
			     const std::string& marker, const std::string& end_marker,
			     uint64_t max, bool need_stats, BucketList& buckets,
			     optional_yield y) = 0;
    virtual Bucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) = 0;
    friend class Bucket;
    virtual std::string& get_display_name() { return info.display_name; }

    const std::string& get_tenant() { return info.user_id.tenant; }
    void set_tenant(std::string& _t) { info.user_id.tenant = _t; }
    const std::string& get_ns() { return info.user_id.ns; }
    void set_ns(std::string& _ns) { info.user_id.ns = _ns; }
    void clear_ns() { info.user_id.ns.clear(); }
    const rgw_user& get_id() const { return info.user_id; }
    uint32_t get_type() const { return info.type; }
    int32_t get_max_buckets() const { return info.max_buckets; }
    const RGWUserCaps& get_caps() const { return info.caps; }
    static bool empty(User* u) { return (!u || u->info.user_id.id.empty()); }
    static bool empty(std::unique_ptr<User>& u) { return (!u || u->info.user_id.id.empty()); }
    virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int read_stats(const DoutPrefixProvider *dpp,
                           optional_yield y, RGWStorageStats* stats,
			   ceph::real_time* last_stats_sync = nullptr,
			   ceph::real_time* last_stats_update = nullptr) = 0;
    virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) = 0;
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    virtual RGWObjVersionTracker& get_version_tracker() { return objv_tracker; }
    virtual Attrs& get_attrs() { return attrs; }
    virtual void set_attrs(Attrs& _attrs) { attrs = _attrs; }

    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) = 0;
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;

    /* dang temporary; will be removed when User is complete */
    RGWUserInfo& get_info() { return info; }

    friend inline ostream& operator<<(ostream& out, const User& u) {
      out << u.info.user_id;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const User* u) {
      if (!u)
	out << "<NULL>";
      else
	out << u->info.user_id;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const std::unique_ptr<User>& p) {
      out << p.get();
      return out;
    }

};

class Bucket {
  protected:
    RGWBucketEnt ent;
    RGWBucketInfo info;
    User* owner = nullptr;
    Attrs attrs;
    obj_version bucket_version;
    ceph::real_time mtime;

  public:

    struct ListParams {
      std::string prefix;
      std::string delim;
      rgw_obj_key marker;
      rgw_obj_key end_marker;
      std::string ns;
      bool enforce_ns{true};
      RGWAccessListFilter* filter{nullptr};
      bool list_versions{false};
      bool allow_unordered{false};
      int shard_id{RGW_NO_SHARD};
    };
    struct ListResults {
      vector<rgw_bucket_dir_entry> objs;
      map<std::string, bool> common_prefixes;
      bool is_truncated{false};
      rgw_obj_key next_marker;
    };

    Bucket() = default;
    Bucket(User* _u) :
      owner(_u) { }
    Bucket(const rgw_bucket& _b) { ent.bucket = _b; info.bucket = _b; }
    Bucket(const RGWBucketEnt& _e) : ent(_e) {
      info.bucket = ent.bucket;
      info.placement_rule = ent.placement_rule;
      info.creation_time = ent.creation_time;
    }
    Bucket(const RGWBucketInfo& _i) : info(_i) {
      ent.bucket = info.bucket;
      ent.placement_rule = info.placement_rule;
      ent.creation_time = info.creation_time;
    }
    Bucket(const rgw_bucket& _b, User* _u) :
      owner(_u) { ent.bucket = _b; info.bucket = _b; }
    Bucket(const RGWBucketEnt& _e, User* _u) : ent(_e), owner(_u) {
      info.bucket = ent.bucket;
      info.placement_rule = ent.placement_rule;
      info.creation_time = ent.creation_time;
    }
    Bucket(const RGWBucketInfo& _i, User* _u) : info(_i), owner(_u) {
      ent.bucket = info.bucket;
      ent.placement_rule = info.placement_rule;
      ent.creation_time = info.creation_time;
    }
    virtual ~Bucket() = default;

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) = 0;
    virtual int list(const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y) = 0;
    virtual Object* create_object(const rgw_obj_key& key /* Attributes */) = 0;
    virtual Attrs& get_attrs(void) { return attrs; }
    virtual int set_attrs(Attrs a) { attrs = a; return 0; }
    virtual int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y) = 0;
    virtual int get_bucket_info(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int get_bucket_stats(const DoutPrefixProvider *dpp, int shard_id,
				 std::string* bucket_ver, std::string* master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string* max_marker = nullptr,
				 bool* syncstopped = nullptr) = 0;
    virtual int get_bucket_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) = 0;
    virtual int read_bucket_stats(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    virtual int update_container_stats(const DoutPrefixProvider* dpp) = 0;
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp) = 0;
    virtual int chown(const DoutPrefixProvider* dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) = 0;
    virtual int put_instance_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) = 0;
    virtual int remove_metadata(const DoutPrefixProvider* dpp, RGWObjVersionTracker* objv, optional_yield y) = 0;
    virtual bool is_owner(User* user) = 0;
    virtual User* get_owner(void) { return owner; };
    virtual ACLOwner get_acl_owner(void) { return ACLOwner(info.owner); };
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) = 0;
    virtual int set_instance_attrs(const DoutPrefixProvider* dpp, Attrs& attrs, optional_yield y) = 0;
    virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime) = 0;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) = 0;
    virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) = 0;
    virtual int rebuild_index(const DoutPrefixProvider *dpp) = 0;
    virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) = 0;
    virtual int purge_instance(const DoutPrefixProvider* dpp) = 0;

    bool empty() const { return info.bucket.name.empty(); }
    const std::string& get_name() const { return info.bucket.name; }
    const std::string& get_tenant() const { return info.bucket.tenant; }
    const std::string& get_marker() const { return info.bucket.marker; }
    const std::string& get_bucket_id() const { return info.bucket.bucket_id; }
    size_t get_size() const { return ent.size; }
    size_t get_size_rounded() const { return ent.size_rounded; }
    uint64_t get_count() const { return ent.count; }
    rgw_placement_rule& get_placement_rule() { return info.placement_rule; }
    ceph::real_time& get_creation_time() { return info.creation_time; }
    ceph::real_time& get_modification_time() { return mtime; }
    obj_version& get_version() { return bucket_version; }
    void set_version(obj_version &ver) { bucket_version = ver; }
    bool versioned() { return info.versioned(); }
    bool versioning_enabled() { return info.versioning_enabled(); }

    void convert(cls_user_bucket_entry* b) const {
      ent.convert(b);
    }

    static bool empty(Bucket* b) { return (!b || b->empty()); }
    virtual std::unique_ptr<Bucket> clone() = 0;

    /* dang - This is temporary, until the API is completed */
    rgw_bucket& get_key() { return info.bucket; }
    RGWBucketInfo& get_info() { return info; }

    friend inline ostream& operator<<(ostream& out, const Bucket& b) {
      out << b.info.bucket;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const Bucket* b) {
      if (!b)
	out << "<NULL>";
      else
	out << b->info.bucket;
      return out;
    }

    friend inline ostream& operator<<(ostream& out, const std::unique_ptr<Bucket>& p) {
      out << p.get();
      return out;
    }

    bool operator==(const Bucket& b) const {
      return (info.bucket.tenant == b.info.bucket.tenant) &&
	     (info.bucket.name == b.info.bucket.name) &&
	     (info.bucket.bucket_id == b.info.bucket.bucket_id);
    }
    bool operator!=(const Bucket& b) const {
      return (info.bucket.tenant != b.info.bucket.tenant) ||
	     (info.bucket.name != b.info.bucket.name) ||
	     (info.bucket.bucket_id != b.info.bucket.bucket_id);
    }

    friend class BucketList;
  protected:
    virtual void set_ent(RGWBucketEnt& _ent) { ent = _ent; info.bucket = ent.bucket; info.placement_rule = ent.placement_rule; }
};


class BucketList {
  std::map<std::string, std::unique_ptr<Bucket>> buckets;
  bool truncated;

public:
  BucketList() : buckets(), truncated(false) {}
  BucketList(BucketList&& _bl) :
    buckets(std::move(_bl.buckets)),
    truncated(_bl.truncated)
    { }
  BucketList& operator=(const BucketList&) = delete;
  BucketList& operator=(BucketList&& _bl) {
    for (auto& ent : _bl.buckets) {
      buckets.emplace(ent.first, std::move(ent.second));
    }
    truncated = _bl.truncated;
    return *this;
  };

  map<std::string, std::unique_ptr<Bucket>>& get_buckets() { return buckets; }
  bool is_truncated(void) const { return truncated; }
  void set_truncated(bool trunc) { truncated = trunc; }
  void add(std::unique_ptr<Bucket> bucket) {
    buckets.emplace(bucket->info.bucket.name, std::move(bucket));
  }
  size_t count() const { return buckets.size(); }
  void clear(void) {
    buckets.clear();
    truncated = false;
  }
};

class Object {
  protected:
    rgw_obj_key key;
    Bucket* bucket;
    std::string index_hash_source;
    uint64_t obj_size;
    Attrs attrs;
    ceph::real_time mtime;
    bool delete_marker{false};
    bool in_extra_data{false};

  public:

    struct ReadOp {
      struct Params {
        const ceph::real_time* mod_ptr{nullptr};
        const ceph::real_time* unmod_ptr{nullptr};
        bool high_precision_time{false};
        uint32_t mod_zone_id{0};
        uint64_t mod_pg_ver{0};
        const char* if_match{nullptr};
        const char* if_nomatch{nullptr};
        ceph::real_time* lastmod{nullptr};
        rgw_obj* target_obj{nullptr}; // XXX dang remove?
      } params;

      struct Result {
        rgw_raw_obj head_obj;

        Result() : head_obj() {}
      } result;

      virtual ~ReadOp() = default;

      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) = 0;
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) = 0;
      virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) = 0;
      virtual int get_manifest(const DoutPrefixProvider* dpp, RGWObjManifest **pmanifest, optional_yield y) = 0;
      virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) = 0;
    };

    struct WriteOp {
      struct Params {
	bool versioning_disabled{false};
	ceph::real_time* mtime{nullptr};
	Attrs* rmattrs{nullptr};
	const bufferlist* data{nullptr};
	RGWObjManifest* manifest{nullptr};
	const std::string* ptag{nullptr};
	std::list<rgw_obj_index_key>* remove_objs{nullptr};
	ceph::real_time set_mtime;
	ACLOwner owner;
	RGWObjCategory category{RGWObjCategory::Main};
	int flags{0};
	const char* if_match{nullptr};
	const char* if_nomatch{nullptr};
	std::optional<uint64_t> olh_epoch;
	ceph::real_time delete_at;
	bool canceled{false};
	const std::string* user_data{nullptr};
	rgw_zone_set* zones_trace{nullptr};
	bool modify_tail{false};
	bool completeMultipart{false};
	bool appendable{false};
	Attrs* attrs{nullptr};
	// In MultipartObjectProcessor::complete, we need this parameter
	// to tell the exact placement rule since it may be different from
	// bucket.placement_rule when Storage Class is specified explicitly
	const rgw_placement_rule *pmeta_placement_rule{nullptr};
      } params;

      virtual ~WriteOp() = default;

      virtual int prepare(optional_yield y) = 0;
      virtual int write_meta(const DoutPrefixProvider* dpp, uint64_t size, uint64_t accounted_size, optional_yield y) = 0;
      //virtual int write_data(const char* data, uint64_t ofs, uint64_t len, bool exclusive) = 0;
    };

    struct DeleteOp {
      struct Params {
        ACLOwner bucket_owner;
        ACLOwner obj_owner;
        int versioning_status{0};
        uint64_t olh_epoch{0};
	std::string marker_version_id;
        uint32_t bilog_flags{0};
        list<rgw_obj_index_key>* remove_objs{nullptr};
        ceph::real_time expiration_time;
        ceph::real_time unmod_since;
        ceph::real_time mtime;
        bool high_precision_time{false};
        rgw_zone_set* zones_trace{nullptr};
	bool abortmp{false};
	uint64_t parts_accounted_size{0};
      } params;

      struct Result {
        bool delete_marker{false};
	std::string version_id;
      } result;

      virtual ~DeleteOp() = default;

      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    };

    struct StatOp {
      struct Result {
	Object* obj;
        RGWObjManifest* manifest;
      } result;

      virtual ~StatOp() = default;

      virtual int stat_async(const DoutPrefixProvider *dpp) = 0;
      virtual int wait(const DoutPrefixProvider *dpp) = 0;
    };

    Object()
      : key(),
      bucket(nullptr),
      index_hash_source(),
      obj_size(),
      attrs(),
      mtime() {}
    Object(const rgw_obj_key& _k)
      : key(_k),
      bucket(),
      index_hash_source(),
      obj_size(),
      attrs(),
      mtime() {}
    Object(const rgw_obj_key& _k, Bucket* _b)
      : key(_k),
      bucket(_b),
      index_hash_source(),
      obj_size(),
      attrs(),
      mtime() {}
    Object(Object& _o) = default;

    virtual ~Object() = default;

    virtual int delete_object(const DoutPrefixProvider* dpp,
			      RGWObjectCtx* obj_ctx,
			      optional_yield y,
			      bool prevent_versioning = false) = 0;
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
			       bool keep_index_consistent, optional_yield y) = 0;
    virtual int copy_object(RGWObjectCtx& obj_ctx, User* user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
	       boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    virtual int set_acl(const RGWAccessControlPolicy& acl) = 0;
    virtual void set_atomic(RGWObjectCtx* rctx) const = 0;
    virtual void set_prefetch_data(RGWObjectCtx* rctx) = 0;

    bool empty() const { return key.empty(); }
    const std::string &get_name() const { return key.name; }

    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh = false) = 0;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj = NULL) = 0;
    virtual int get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) = 0;
    virtual int modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) = 0;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y) = 0;
    virtual int copy_obj_data(RGWObjectCtx& rctx, Bucket* dest_bucket, Object* dest_obj, uint16_t olh_epoch, std::string* petag, const DoutPrefixProvider* dpp, optional_yield y) = 0;
    virtual bool is_expired() = 0;
    virtual void gen_rand_obj_instance_name() = 0;
    virtual void raw_obj_to_obj(const rgw_raw_obj& raw_obj) = 0;
    virtual void get_raw_obj(rgw_raw_obj* raw_obj) = 0;
    virtual MPSerializer* get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name) = 0;
    virtual int transition(RGWObjectCtx& rctx,
			   Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) = 0;
    virtual int get_max_chunk_size(const DoutPrefixProvider* dpp,
                                   rgw_placement_rule placement_rule,
				   uint64_t* max_chunk_size,
				   uint64_t* alignment = nullptr) = 0;
    virtual void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t* max_size) = 0;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) = 0;

    Attrs& get_attrs(void) { return attrs; }
    const Attrs& get_attrs(void) const { return attrs; }
    ceph::real_time get_mtime(void) const { return mtime; }
    uint64_t get_obj_size(void) const { return obj_size; }
    Bucket* get_bucket(void) const { return bucket; }
    void set_bucket(Bucket* b) { bucket = b; }
    std::string get_hash_source(void) { return index_hash_source; }
    void set_hash_source(std::string s) { index_hash_source = s; }
    std::string get_oid(void) const { return key.get_oid(); }
    bool get_delete_marker(void) { return delete_marker; }
    bool get_in_extra_data(void) { return in_extra_data; }
    void set_in_extra_data(bool i) { in_extra_data = i; }
    int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
    void set_obj_size(uint64_t s) { obj_size = s; }
    virtual void set_name(const std::string& n) { key = n; }
    virtual void set_key(const rgw_obj_key& k) { key = k; }
    virtual rgw_obj get_obj(void) const {
      rgw_obj obj(bucket->get_key(), key);
      obj.set_in_extra_data(in_extra_data);
      obj.index_hash_source = index_hash_source;
      return obj;
    }

    /* Swift versioning */
    virtual int swift_versioning_restore(RGWObjectCtx* obj_ctx,
					 bool& restored,   /* out */
					 const DoutPrefixProvider* dpp) = 0;
    virtual int swift_versioning_copy(RGWObjectCtx* obj_ctx,
				      const DoutPrefixProvider* dpp,
				      optional_yield y) = 0;

    /* OPs */
    virtual std::unique_ptr<ReadOp> get_read_op(RGWObjectCtx*) = 0;
    virtual std::unique_ptr<WriteOp> get_write_op(RGWObjectCtx*) = 0;
    virtual std::unique_ptr<DeleteOp> get_delete_op(RGWObjectCtx*) = 0;
    virtual std::unique_ptr<StatOp> get_stat_op(RGWObjectCtx*) = 0;

    /* OMAP */
    virtual int omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
			      std::map<std::string, bufferlist>* m,
			      bool* pmore, optional_yield y) = 0;
    virtual int omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>* m,
			     optional_yield y) = 0;
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
			      const std::set<std::string>& keys,
			      Attrs* vals) = 0;
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) = 0;

    static bool empty(Object* o) { return (!o || o->empty()); }
    virtual std::unique_ptr<Object> clone() = 0;

    /* dang - Not sure if we want this, but it simplifies things a lot */

    /* dang - This is temporary, until the API is completed */
    rgw_obj_key& get_key() { return key; }
    void set_instance(const std::string &i) { key.set_instance(i); }
    const std::string &get_instance() const { return key.instance; }
    bool have_instance(void) { return key.have_instance(); }

    friend inline ostream& operator<<(ostream& out, const Object& o) {
      if (o.bucket)
	out << o.bucket << ":";
      out << o.key;
      return out;
    }
    friend inline ostream& operator<<(ostream& out, const Object* o) {
      if (!o)
	out << "<NULL>";
      else
	out << *o;
      return out;
    }
    friend inline ostream& operator<<(ostream& out, const std::unique_ptr<Object>& p) {
      out << p.get();
      return out;
    }
};

struct Serializer {
  Serializer() = default;
  virtual ~Serializer() = default;

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) = 0;
  virtual int unlock()  = 0;
};

struct MPSerializer : Serializer {
  bool locked;
  std::string oid;
  MPSerializer() : locked(false) {}
  virtual ~MPSerializer() = default;

  void clear_locked() {
    locked = false;
  }
};

struct LCSerializer : Serializer {
  LCSerializer() {}
  virtual ~LCSerializer() = default;
};

class Lifecycle {
public:
  struct LCHead {
    time_t start_date{0};
    std::string marker;

    LCHead() = default;
    LCHead(time_t _date, std::string& _marker) : start_date(_date), marker(_marker) {}
  };

  struct LCEntry {
    std::string bucket;
    uint64_t start_time{0};
    uint32_t status{0};

    LCEntry() = default;
    LCEntry(std::string& _bucket, uint64_t _time, uint32_t _status) : bucket(_bucket), start_time(_time), status(_status) {}
  };

  Lifecycle() = default;
  virtual ~Lifecycle() = default;

  virtual int get_entry(const std::string& oid, const std::string& marker, LCEntry& entry) = 0;
  virtual int get_next_entry(const std::string& oid, std::string& marker, LCEntry& entry) = 0;
  virtual int set_entry(const std::string& oid, const LCEntry& entry) = 0;
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries, vector<LCEntry>& entries) = 0;
  virtual int rm_entry(const std::string& oid, const LCEntry& entry) = 0;
  virtual int get_head(const std::string& oid, LCHead& head) = 0;
  virtual int put_head(const std::string& oid, const LCHead& head) = 0;

  virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) = 0;
};

class Notification {
protected:
  Object* obj;
  rgw::notify::EventType event_type;

  public:
    Notification(Object* _obj, rgw::notify::EventType _type) : obj(_obj), event_type(_type) {}
    virtual ~Notification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) = 0;
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) = 0;
};

class GCChain {
protected:
  Object* obj;

  public:
    GCChain(Object* _obj) : obj(_obj) {}
    virtual ~GCChain() = default;

    virtual void update(const DoutPrefixProvider *dpp, RGWObjManifest* manifest) = 0;
    virtual int send(const std::string& tag) = 0;
    virtual void delete_inline(const DoutPrefixProvider *dpp, const std::string& tag) = 0;
};

using RawObjSet = std::set<rgw_raw_obj>;

class Writer : public rgw::putobj::DataProcessor {
protected:
  Aio* const aio;
  rgw::sal::Bucket* bucket;
  RGWObjectCtx& obj_ctx;
  std::unique_ptr<rgw::sal::Object> head_obj;
  RawObjSet written; // set of written objects for deletion
  const DoutPrefixProvider* dpp;
  optional_yield y;

 public:
  Writer(Aio* aio,
	      rgw::sal::Bucket* bucket,
              RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::Object> _head_obj,
              const DoutPrefixProvider* dpp, optional_yield y)
    : aio(aio), bucket(bucket),
      obj_ctx(obj_ctx), head_obj(std::move(_head_obj)), dpp(dpp), y(y)
  {}
  Writer(Writer&& r)
    : aio(r.aio), bucket(r.bucket),
      obj_ctx(r.obj_ctx), head_obj(std::move(r.head_obj)), dpp(r.dpp), y(r.y)
  {}

  ~Writer() = default;

  // change the current stripe object
  virtual int set_stripe_obj(const rgw_raw_obj& obj) = 0;

  // write the data as an exclusive create and wait for it to complete
  virtual int write_exclusive(const bufferlist& data) = 0;

  virtual int drain() = 0;

  // when the operation completes successfully, clear the set of written objects
  // so they aren't deleted on destruction
  virtual void clear_written() { written.clear(); }

};

class Zone {
  public:
    virtual ~Zone() = default;

    virtual const RGWZoneGroup& get_zonegroup() = 0;
    virtual int get_zonegroup(const std::string& id, RGWZoneGroup& zonegroup) = 0;
    virtual const RGWZoneParams& get_params() = 0;
    virtual const rgw_zone_id& get_id() = 0;
    virtual const RGWRealm& get_realm() = 0;
    virtual const std::string& get_name() const = 0;
    virtual bool is_writeable() = 0;
    virtual bool get_redirect_endpoint(std::string* endpoint) = 0;
    virtual bool has_zonegroup_api(const std::string& api) const = 0;
    virtual const std::string& get_current_period_id() = 0;
};

class LuaScriptManager {
protected:
  std::string script;

public:
  virtual ~LuaScriptManager() = default;

  virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) = 0;
  virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) = 0;
  virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) = 0;
};

} } // namespace rgw::sal

class StoreManager {
public:
  StoreManager() {}
  static rgw::sal::Store* get_storage(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
                               bool run_sync_thread, bool run_reshard_thread, bool use_cache = true, bool use_gc = true) {
    rgw::sal::Store* store = init_storage_provider(dpp, cct, svc, use_gc_thread, use_lc_thread,
        quota_threads, run_sync_thread, run_reshard_thread, use_cache, use_gc);
    return store;
  }
  static rgw::sal::Store* get_raw_storage(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc) {
    rgw::sal::Store* store = init_raw_storage_provider(dpp, cct, svc);
    return store;
  }
  static rgw::sal::Store* init_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache, bool use_gc);
  static rgw::sal::Store* init_raw_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc);
  static void close_storage(rgw::sal::Store* store);

};
