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
#include "common/tracer.h"

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
class RGWCompressionInfo;


using RGWBucketListNameFilter = std::function<bool (const std::string&)>;


namespace rgw {
  class Aio;
  namespace IAM { struct Policy; }
}

class RGWGetDataCB {
public:
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) = 0;
  RGWGetDataCB() {}
  virtual ~RGWGetDataCB() {}
};

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
  std::map<RGWObjCategory, RGWStorageStats>* stats;
public:
  explicit RGWGetBucketStats_CB(const rgw_bucket& _bucket) : bucket(_bucket), stats(NULL) {}
  ~RGWGetBucketStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(std::map<RGWObjCategory, RGWStorageStats>* _stats) {
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

/**
 * @defgroup RGWSAL RGW Store Abstraction Layer
 *
 * The Store Abstraction Layer is an API that separates the top layer of RGW that
 * handles client protocols (such as S3 or Swift) from the bottom layer of RGW that
 * interacts with a backing store.  It allows the creation of multiple backing stores
 * that can co-exist with a single RGW instance, and allows the creation of stacking
 * layers of translators that can modify operations as they pass down the stack.
 * Examples of translators might be a cache layer, a duplication layer that copies
 * operations to multiple stores, or a policy layer that sends some operations to one
 * store and some to another.
 *
 * The basic unit of a SAL implementation is the Store.  Whether an actual backing store
 * or a translator, there will be a Store implementation that represents it.  Examples
 * are the RadosStore that communicates via RADOS with a Ceph cluster, and the DBStore
 * that uses a SQL db (such as SQLite3) as a backing store.  There is a singleton
 * instance of each Store.
 *
 * Data within RGW is owned by a User.  The User is the unit of authentication and
 * access control.
 *
 * Data within RGW is stored as an Object.  Each Object is a single chunk of data, owned
 * by a single User, contained within a single Bucket.  It has metadata associated with
 * it, such as size, owner, and so on, and a set of key-value attributes that can
 * contain anything needed by the top half.
 *
 * Data with RGW is organized into Buckets.  Each Bucket is owned by a User, and
 * contains Objects.  There is a single, flat layer of Buckets, there is no hierarchy,
 * and each Object is contained in a single Bucket.
 *
 * Instantiations of SAL classes are done as unique pointers, using std::unique_ptr.
 * Instances of these classes are acquired via getters, and it's up to the caller to
 * manage the lifetime.
 *
 * @note Anything using RGWObjContext is subject to change, as that type will not be
 * used in the final API.
 * @{
 */

/**
 * @file rgw_sal.h
 * @brief Base abstractions and API for SAL
 */

namespace rgw { namespace sal {

/**
 * @addtogroup RGWSAL
 * @{
 */

#define RGW_SAL_VERSION 1

class User;
class Bucket;
class Object;
class BucketList;
class MultipartUpload;
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

// a simple streaming data processing abstraction
/**
 * @brief A simple streaming data processing abstraction
 */
class DataProcessor {
 public:
  virtual ~DataProcessor() {}

  /**
   * @brief Consume a bufferlist in its entirety at the given object offset.
   *
   * An empty bufferlist is given to request that any buffered data be flushed, though this doesn't
   * wait for completions
   */
  virtual int process(bufferlist&& data, uint64_t offset) = 0;
};

/**
 * @brief a data consumer that writes an object in a bucket
 */
class ObjectProcessor : public DataProcessor {
 public:
  /** prepare to start processing object data */
  virtual int prepare(optional_yield y) = 0;

  /** complete the operation and make its result visible to clients */
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) = 0;
};

/** Base class for AIO completions */
class Completions {
  public:
    Completions() {}
    virtual ~Completions() = default;
    virtual int drain() = 0;
};

/** A list of key-value attributes */
using Attrs = std::map<std::string, ceph::buffer::list>;

/**
 * @brief Base singleton representing a Store or stacking layer
 *
 * The Store is the base abstraction of the SAL layer.  It represents a base storage
 * mechanism, or a intermediate stacking layer.  There is a single instance of a given
 * Store per RGW, and this Store mediates all access to it's backing.
 *
 * A store contains, loosely, @a User, @a Bucket, and @a Object entities.  The @a Object
 * contains data, and it's associated metadata.  The @a Bucket contains Objects, and
 * metadata about the bucket.  Both Buckets and Objects are owned by a @a User, which is
 * the basic unit of access control.
 *
 * A store also has metadata and some global responsibilities.  For example, a store is
 * responsible for managing the LifeCycle activities for it's data.
 */
class Store {
  public:
    Store() {}
    virtual ~Store() = default;

    /** Name of this store provider (e.g., "rados") */
    virtual const char* get_name() const = 0;
    /** Get cluster unique identifier */
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) = 0;
    /** Get a User from a rgw_user.  Does not query store for user info, so quick */
    virtual std::unique_ptr<User> get_user(const rgw_user& u) = 0;
    /** Lookup a User by access key.  Queries store for user info. */
    virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Lookup a User by email address.  Queries store for user info. */
    virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Lookup a User by swift username.  Queries store for user info. */
    virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Get a basic Object.  This Object is not looked up, and is incomplete, since is
     * does not have a bucket.  This should only be used when an Object is needed before
     * there is a Bucket, otherwise use the get_object() in the Bucket class. */
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) = 0;
    /** Get a Bucket by info.  Does not query the store, just uses the give bucket info. */
    virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) = 0;
    /** Lookup a Bucket by key.  Queries store for bucket info. */
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    /** Lookup a Bucket by name.  Queries store for bucket info. */
    virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    /** For multisite, this Store is the zone's master */
    virtual bool is_meta_master() = 0;
    /** For multisite, forward an OP to the zone's master */
    virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
					  bufferlist& in_data, JSONParser* jp, req_info& info,
					  optional_yield y) = 0;
    /** Get zone info for this store */
    virtual Zone* get_zone() = 0;
    /** Get a unique ID specific to this zone. */
    virtual std::string zone_unique_id(uint64_t unique_num) = 0;
    /** Get a unique Swift transaction ID specific to this zone */
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) = 0;
    /** Get statistics about the cluster represented by this Store */
    virtual int cluster_stat(RGWClusterStat& stats) = 0;
    /** Get a @a Lifecycle object. Used to manage/run lifecycle transitions */
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) = 0;
    /** Get a @a Completions object.  Used for Async I/O tracking */
    virtual std::unique_ptr<Completions> get_completions(void) = 0;

     /** Get a @a Notification object.  Used to communicate with non-RGW daemons, such as
      * management/tracking software */
    /** RGWOp variant */
    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
        rgw::notify::EventType event_type, const std::string* object_name=nullptr) = 0;
    /** No-req_state variant (e.g., rgwlc) */
    virtual std::unique_ptr<Notification> get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, RGWObjectCtx* rctx,
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y) = 0;

    /** Get access to the lifecycle management thread */
    virtual RGWLC* get_rgwlc(void) = 0;
    /** Get access to the coroutine registry.  Used to create new coroutine managers */
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() = 0;

    /** Log usage data to the store.  Usage data is things like bytes sent/received and
     * op count */
    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) = 0;
    /** Log OP data to the store.  Data is opaque to SAL */
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) = 0;
    /** Register this Store to the service map.  Somewhat Rados specific; may be removed*/
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
					const std::map<std::string, std::string>& meta) = 0;
    /** Get default quota info.  Used as fallback if a user or bucket has no quota set*/
    virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) = 0;
    /** Get global rate limit configuration*/
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) = 0;
    /** Enable or disable a set of bucket.  e.g. if a User is suspended */
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled) = 0;
    /** Get a new request ID */
    virtual uint64_t get_new_req_id() = 0;
    /** Get a handler for bucket sync policy. */
    virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
					std::optional<rgw_zone_id> zone,
					std::optional<rgw_bucket> bucket,
					RGWBucketSyncPolicyHandlerRef* phandler,
					optional_yield y) = 0;
    /** Get a status manager for bucket sync */
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) = 0;
    /** Wake up sync threads for bucket metadata sync */
    virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) = 0;
    /** Wake up sync threads for bucket data sync */
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, std::map<int, std::set<std::string> >& shard_ids) = 0;
    /** Clear all usage statistics globally */
    virtual int clear_usage(const DoutPrefixProvider *dpp) = 0;
    /** Get usage statistics for all users and buckets */
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim usage log for all users and buckets */
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    /** Get a configuration value for the given name */
    virtual int get_config_key_val(std::string name, bufferlist* bl) = 0;
    /** Start a metadata listing of the given section */
    virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) = 0;
    /** Get the next key from a metadata list */
    virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) = 0;
    /** Complete a metadata listing */
    virtual void meta_list_keys_complete(void* handle) = 0;
    /** Get the marker associated with the current metadata listing */
    virtual std::string meta_get_marker(void* handle) = 0;
    /** Remove a specific metadata key */
    virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) = 0;
    /** Get list of users */
    virtual int list_users(const DoutPrefixProvider* dpp, const std::string& metadata_key,
                        std::string& marker, int max_entries, void *&handle,
                        bool* truncated, std::list<std::string>& users) = 0;
    /** Get an instance of the Sync module for bucket sync */
    virtual const RGWSyncModuleInstanceRef& get_sync_module() = 0;
    /** Get the ID of the current host */
    virtual std::string get_host_id() = 0;
    /** Get a Lua script manager for running lua scripts */
    virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() = 0;
    /** Get an IAM Role by name etc. */
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) = 0;
    /** Get an IAM Role by ID */
    virtual std::unique_ptr<RGWRole> get_role(std::string id) = 0;
    /** Get all IAM Roles optionally filtered by path */
    virtual int get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  std::vector<std::unique_ptr<RGWRole>>& roles) = 0;
    /** Get an empty Open ID Connector provider */
    virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() = 0;
    /** Get all Open ID Connector providers, optionally filtered by tenant  */
    virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
				   const std::string& tenant,
				   std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) = 0;
    /** Get a Writer that appends to an object */
    virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) = 0;
    /** Get a Writer that atomically writes an entire object */
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) = 0;

    /** Clean up a store for termination */
    virtual void finalize(void) = 0;

    /** Get the Ceph context associated with this store.  May be removed. */
    virtual CephContext* ctx(void) = 0;

    /** Get the location of where lua packages are installed */
    virtual const std::string& get_luarocks_path() const = 0;
    /** Set the location of where lua packages are installed */
    virtual void set_luarocks_path(const std::string& path) = 0;
};

/**
 * @brief User abstraction
 *
 * This represents a user.  In general, there will be a @a User associated with an OP
 * (the user performing the OP), and potentially several others acting as owners.
 * Lifetime of a User is a bit tricky , since it must last as long as any Buckets
 * associated with it.  A User has associated metadata, including a set of key/value
 * attributes, and statistics (including usage) about the User.
 */
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

    /** Clone a copy of this user.  Used when modification is necessary of the copy */
    virtual std::unique_ptr<User> clone() = 0;
    /** List the buckets owned by a user */
    virtual int list_buckets(const DoutPrefixProvider* dpp,
			     const std::string& marker, const std::string& end_marker,
			     uint64_t max, bool need_stats, BucketList& buckets,
			     optional_yield y) = 0;
    /** Create a new bucket owned by this user.  Creates in the backing store, not just the instantiation. */
    virtual int create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
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

    /** Get the display name for this User */
    virtual std::string& get_display_name() { return info.display_name; }
    /** Get the tenant name for this User */
    const std::string& get_tenant() { return info.user_id.tenant; }
    /** Set the tenant name for this User */
    void set_tenant(std::string& _t) { info.user_id.tenant = _t; }
    /** Get the namespace for this User */
    const std::string& get_ns() { return info.user_id.ns; }
    /** Set the namespace for this User */
    void set_ns(std::string& _ns) { info.user_id.ns = _ns; }
    /** Clear the namespace for this User */
    void clear_ns() { info.user_id.ns.clear(); }
    /** Get the full ID for this User */
    const rgw_user& get_id() const { return info.user_id; }
    /** Get the type of this User */
    uint32_t get_type() const { return info.type; }
    /** Get the inline policy size of this User of this User */
    uint32_t get_user_policy_size() const { return info.inline_policy_size; }
    /** Set the inline policy size of this User */
    void set_user_policy_size(uint32_t pol_size) { info.inline_policy_size = pol_size; }
    /** Get the maximum number of buckets allowed for this User */
    int32_t get_max_buckets() const { return info.max_buckets; }
    /** Get the capabilities for this User */
    const RGWUserCaps& get_caps() const { return info.caps; }
    /** Get the version tracker for this User */
    virtual RGWObjVersionTracker& get_version_tracker() { return objv_tracker; }
    /** Get the cached attributes for this User */
    virtual Attrs& get_attrs() { return attrs; }
    /** Set the cached attributes fro this User */
    virtual void set_attrs(Attrs& _attrs) { attrs = _attrs; }
    /** Check if a User pointer is empty */
    static bool empty(User* u) { return (!u || u->info.user_id.id.empty()); }
    /** Check if a User unique_pointer is empty */
    static bool empty(std::unique_ptr<User>& u) { return (!u || u->info.user_id.id.empty()); }
    /** Read the User attributes from the backing Store */
    virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Set the attributes in attrs, leaving any other existing attrs set, and
     * write them to the backing store; a merge operation */
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) = 0;
    /** Read the User stats from the backing Store, synchronous */
    virtual int read_stats(const DoutPrefixProvider *dpp,
                           optional_yield y, RGWStorageStats* stats,
			   ceph::real_time* last_stats_sync = nullptr,
			   ceph::real_time* last_stats_update = nullptr) = 0;
    /** Read the User stats from the backing Store, asynchronous */
    virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) = 0;
    /** Flush accumulated stat changes for this User to the backing store */
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    /** Read detailed usage stats for this User from the backing store */
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim User usage stats to the given epoch range */
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;

    /** Load this User from the backing store.  requires ID to be set, fills all other fields. */
    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Store this User to the backing store */
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) = 0;
    /** Remove this User from the backing store */
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;

    /* dang temporary; will be removed when User is complete */
    RGWUserInfo& get_info() { return info; }

    friend inline std::ostream& operator<<(std::ostream& out, const User& u) {
      out << u.info.user_id;
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const User* u) {
      if (!u)
	out << "<NULL>";
      else
	out << u->info.user_id;
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<User>& p) {
      out << p.get();
      return out;
    }

    friend class Bucket;
};

/**
 * @brief Bucket abstraction
 *
 * This represents a bucket.  A bucket is a container for objects.  It is owned by a user, and has
 * it's own set of metadata, including a set of key/value attributes.  A bucket may not contain
 * other buckets, only objects.  Buckets have Access Control Lists (ACLs) that control what users
 * can access the contents of the bucket, and in what ways.
 */
class Bucket {
  protected:
    RGWBucketEnt ent;
    RGWBucketInfo info;
    User* owner = nullptr;
    Attrs attrs;
    obj_version bucket_version;
    ceph::real_time mtime;

  public:

    /**
     * @brief Parameters for a bucket list operation
     */
    struct ListParams {
      std::string prefix;
      std::string delim;
      rgw_obj_key marker;
      rgw_obj_key end_marker;
      std::string ns;
      bool enforce_ns{true};
      RGWAccessListFilter* access_list_filter{nullptr};
      RGWBucketListNameFilter force_check_filter;
      bool list_versions{false};
      bool allow_unordered{false};
      int shard_id{RGW_NO_SHARD};

      friend std::ostream& operator<<(std::ostream& out, const ListParams& p) {
	out << "rgw::sal::Bucket::ListParams{ prefix=\"" << p.prefix <<
	  "\", delim=\"" << p.delim <<
	  "\", marker=\"" << p.marker <<
	  "\", end_marker=\"" << p.end_marker <<
	  "\", ns=\"" << p.ns <<
	  "\", enforce_ns=" << p.enforce_ns <<
	  ", list_versions=" << p.list_versions <<
	  ", allow_unordered=" << p.allow_unordered <<
	  ", shard_id=" << p.shard_id <<
	  " }";
	return out;
      }
    };
    /**
     * @brief Results from a bucket list operation
     */
    struct ListResults {
      std::vector<rgw_bucket_dir_entry> objs;
      std::map<std::string, bool> common_prefixes;
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

    /** Get an @a Object belonging to this bucket */
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) = 0;
    /** List the contents of this bucket */
    virtual int list(const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y) = 0;
    /** Get the cached attributes associated with this bucket */
    virtual Attrs& get_attrs(void) { return attrs; }
    /** Set the cached attributes on this bucket */
    virtual int set_attrs(Attrs a) { attrs = a; return 0; }
    /** Remove this bucket from the backing store */
    virtual int remove_bucket(const DoutPrefixProvider* dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y) = 0;
    /** Remove this bucket, bypassing garbage collection.  May be removed */
    virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) = 0;
    /** Get then ACL for this bucket */
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    /** Set the ACL for this bucket */
    virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y) = 0;

    // XXXX hack
    void set_owner(rgw::sal::User* _owner) {
      owner = _owner;
    }

    /** Load this bucket from the backing store.  Requires the key to be set, fills other fields.
     * If @a get_stats is true, then statistics on the bucket are also looked up. */
    virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y, bool get_stats = false) = 0;
    /** Read the bucket stats from the backing Store, synchronous */
    virtual int read_stats(const DoutPrefixProvider *dpp, int shard_id,
				 std::string* bucket_ver, std::string* master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string* max_marker = nullptr,
				 bool* syncstopped = nullptr) = 0;
    /** Read the bucket stats from the backing Store, asynchronous */
    virtual int read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) = 0;
    /** Sync this bucket's stats to the owning user's stats in the backing store */
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    /** Refresh the metadata stats (size, count, and so on) from the backing store */
    virtual int update_container_stats(const DoutPrefixProvider* dpp) = 0;
    /** Check if this bucket needs resharding, and schedule it if it does */
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp) = 0;
    /** Change the owner of this bucket in the backing store */
    virtual int chown(const DoutPrefixProvider* dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) = 0;
    /** Store the cached bucket info into the backing store */
    virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime) = 0;
    /** Check to see if the given user is the owner of this bucket */
    virtual bool is_owner(User* user) = 0;
    /** Get the owner of this bucket */
    virtual User* get_owner(void) { return owner; };
    /** Get the owner of this bucket in the form of an ACLOwner object */
    virtual ACLOwner get_acl_owner(void) { return ACLOwner(info.owner); };
    /** Check in the backing store if this bucket is empty */
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Chec k if the given size fits within the quota */
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) = 0;
    /** Set the attributes in attrs, leaving any other existing attrs set, and
     * write them to the backing store; a merge operation */
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) = 0;
    /** Try to refresh the cached bucket info from the backing store.  Used in
     * read-modify-update loop. */
    virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime) = 0;
    /** Read usage information about this bucket from the backing store */
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim the usage information to the given epoch range */
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) = 0;
    /** Remove objects from the bucket index of this bucket.  May be removed from API */
    virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) = 0;
    /** Check the state of the bucket index, and get stats from it.  May be removed from API */
    virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) = 0;
    /** Rebuild the bucket index.  May be removed from API */
    virtual int rebuild_index(const DoutPrefixProvider *dpp) = 0;
    /** Set a timeout on the check_index() call.  May be removed from API */
    virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) = 0;
    /** Remove this specific bucket instance from the backing store.  May be removed from API */
    virtual int purge_instance(const DoutPrefixProvider* dpp) = 0;

    /** Check if this instantiation is empty */
    bool empty() const { return info.bucket.name.empty(); }
    /** Get the cached name of this bucket */
    const std::string& get_name() const { return info.bucket.name; }
    /** Get the cached tenant of this bucket */
    const std::string& get_tenant() const { return info.bucket.tenant; }
    /** Get the cached marker of this bucket */
    const std::string& get_marker() const { return info.bucket.marker; }
    /** Get the cached ID of this bucket */
    const std::string& get_bucket_id() const { return info.bucket.bucket_id; }
    /** Get the cached size of this bucket */
    size_t get_size() const { return ent.size; }
    /** Get the cached rounded size of this bucket */
    size_t get_size_rounded() const { return ent.size_rounded; }
    /** Get the cached object count of this bucket */
    uint64_t get_count() const { return ent.count; }
    /** Get the cached placement rule of this bucket */
    rgw_placement_rule& get_placement_rule() { return info.placement_rule; }
    /** Get the cached creation time of this bucket */
    ceph::real_time& get_creation_time() { return info.creation_time; }
    /** Get the cached modification time of this bucket */
    ceph::real_time& get_modification_time() { return mtime; }
    /** Get the cached version of this bucket */
    obj_version& get_version() { return bucket_version; }
    /** Set the cached version of this bucket */
    void set_version(obj_version &ver) { bucket_version = ver; }
    /** Check if this bucket is versioned */
    bool versioned() { return info.versioned(); }
    /** Check if this bucket has versioning enabled */
    bool versioning_enabled() { return info.versioning_enabled(); }

    /** Check if a Bucket pointer is empty */
    static bool empty(Bucket* b) { return (!b || b->empty()); }
    /** Clone a copy of this bucket.  Used when modification is necessary of the copy */
    virtual std::unique_ptr<Bucket> clone() = 0;

    /** Create a multipart upload in this bucket */
    virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) = 0;
    /** List multipart uploads currently in this bucket */
    virtual int list_multiparts(const DoutPrefixProvider *dpp,
				const std::string& prefix,
				std::string& marker,
				const std::string& delim,
				const int& max_uploads,
				std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				std::map<std::string, bool> *common_prefixes,
				bool *is_truncated) = 0;
    /** Abort multipart uploads in a bucket */
    virtual int abort_multiparts(const DoutPrefixProvider* dpp,
				 CephContext* cct) = 0;

    /* dang - This is temporary, until the API is completed */
    rgw_bucket& get_key() { return info.bucket; }
    RGWBucketInfo& get_info() { return info; }

    friend inline std::ostream& operator<<(std::ostream& out, const Bucket& b) {
      out << b.info.bucket;
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const Bucket* b) {
      if (!b)
	out << "<NULL>";
      else
	out << b->info.bucket;
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<Bucket>& p) {
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

/**
 * @brief A list of buckets
 *
 * This is the result from a bucket listing operation.
 */
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

  /** Get the list of buckets.  The list is a map of <bucket-name, Bucket> pairs. */
  std::map<std::string, std::unique_ptr<Bucket>>& get_buckets() { return buckets; }
  /** True if the list is truncated (that is, there are more buckets to list) */
  bool is_truncated(void) const { return truncated; }
  /** Set the truncated state of the list */
  void set_truncated(bool trunc) { truncated = trunc; }
  /** Add a bucket to the list.  Takes ownership of the bucket */
  void add(std::unique_ptr<Bucket> bucket) {
    buckets.emplace(bucket->info.bucket.name, std::move(bucket));
  }
  /** The number of buckets in this list */
  size_t count() const { return buckets.size(); }
  /** Clear the list */
  void clear(void) {
    buckets.clear();
    truncated = false;
  }
};

/**
 * @brief Object abstraction
 *
 * This represents an Object.  An Object is the basic unit of data storage.  It
 * represents a blob of data, a set of metadata (such as size, owner, ACLs, etc.) and
 * a set of key/value attributes.  Objects may be versioned.  If a versioned object
 * is written to, a new object with the same name but a different version is created,
 * and the old version of the object is still accessible.  If an unversioned object
 * is written to, it is replaced, and the old data is not accessible.
 */
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

    /**
     * @brief Read operation on an Object
     *
     * This represents a Read operation on an Object.  Read operations are optionally
     * asynchronous, using the iterate() API.
     */
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

      virtual ~ReadOp() = default;

      /** Prepare the Read op.  Must be called first */
      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) = 0;
      /** Synchronous read. Read from @a ofs to @a end into @a bl */
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) = 0;
      /** Asynchronous read.  Read from @a ofs to @a end calling @a cb on each read
       * chunk. */
      virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) = 0;
      /** Get an attribute by name */
      virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) = 0;
    };

    /**
     * @brief Delete operation on an Object
     *
     * This deletes an Object from the backing store.
     */
    struct DeleteOp {
      struct Params {
        ACLOwner bucket_owner;
        ACLOwner obj_owner;
        int versioning_status{0};
        uint64_t olh_epoch{0};
	std::string marker_version_id;
        uint32_t bilog_flags{0};
        std::list<rgw_obj_index_key>* remove_objs{nullptr};
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

      /** Delete the object */
      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) = 0;
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

    /** Shortcut synchronous delete call for common deletes */
    virtual int delete_object(const DoutPrefixProvider* dpp,
			      RGWObjectCtx* obj_ctx,
			      optional_yield y,
			      bool prevent_versioning = false) = 0;
    /** Asynchronous delete call */
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
			       bool keep_index_consistent, optional_yield y) = 0;
    /** Copy an this object to another object. */
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
    /** Get the ACL for this object */
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    /** Set the ACL for this object */
    virtual int set_acl(const RGWAccessControlPolicy& acl) = 0;
    /** Mark further operations on this object as being atomic */
    virtual void set_atomic(RGWObjectCtx* rctx) const = 0;
    /** Pre-fetch data when reading */
    virtual void set_prefetch_data(RGWObjectCtx* rctx) = 0;
    /** Mark data as compressed */
    virtual void set_compressed(RGWObjectCtx* rctx) = 0;

    /** Check to see if this object has an empty key.  This means it's uninitialized */
    bool empty() const { return key.empty(); }
    /** Get the name of this object */
    const std::string &get_name() const { return key.name; }

    /** Get the object state for this object.  Will be removed in the future */
    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh = true) = 0;
    /** Set attributes for this object from the backing store.  Attrs can be set or
     * deleted.  @note the attribute APIs may be revisited in the future. */
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj = NULL) = 0;
    /** Get attributes for this object */
    virtual int get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) = 0;
    /** Modify attributes for this object. */
    virtual int modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) = 0;
    /** Delete attributes for this object */
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y) = 0;
    /** Check to see if this object has expired */
    virtual bool is_expired() = 0;
    /** Create a randomized instance ID for this object */
    virtual void gen_rand_obj_instance_name() = 0;
    /** Get a multipart serializer for this object */
    virtual MPSerializer* get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name) = 0;
    /** Move the data of an object to new placement storage */
    virtual int transition(RGWObjectCtx& rctx,
			   Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) = 0;
    /** Check to see if two placement rules match */
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) = 0;
    /** Dump store-specific object layout info in JSON */
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx) = 0;

    /** Get the cached attributes for this object */
    Attrs& get_attrs(void) { return attrs; }
    /** Get the (const) cached attributes for this object */
    const Attrs& get_attrs(void) const { return attrs; }
    /** Set the cached attributes for this object */
    virtual int set_attrs(Attrs a) { attrs = a; return 0; }
    /** Get the cached modification time for this object */
    ceph::real_time get_mtime(void) const { return mtime; }
    /** Get the cached size for this object */
    uint64_t get_obj_size(void) const { return obj_size; }
    /** Get the bucket containing this object */
    Bucket* get_bucket(void) const { return bucket; }
    /** Set the bucket containing this object */
    void set_bucket(Bucket* b) { bucket = b; }
    /** Get the sharding hash representation of this object */
    std::string get_hash_source(void) { return index_hash_source; }
    /** Set the sharding hash representation of this object */
    void set_hash_source(std::string s) { index_hash_source = s; }
    /** Build an Object Identifier string for this object */
    std::string get_oid(void) const { return key.get_oid(); }
    /** True if this object is a delete marker (newest version is deleted) */
    bool get_delete_marker(void) { return delete_marker; }
    /** True if this object is stored in the extra data pool */
    bool get_in_extra_data(void) { return in_extra_data; }
    /** Set the in_extra_data field */
    void set_in_extra_data(bool i) { in_extra_data = i; }
    /** Helper to sanitize object size, offset, and end values */
    int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
    /** Set the cached size of this object */
    void set_obj_size(uint64_t s) { obj_size = s; }
    /** Set the cached name of this object */
    virtual void set_name(const std::string& n) { key = n; }
    /** Set the cached key of this object */
    virtual void set_key(const rgw_obj_key& k) { key = k; }
    /** Get an rgw_obj representing this object */
    virtual rgw_obj get_obj(void) const {
      rgw_obj obj(bucket->get_key(), key);
      obj.set_in_extra_data(in_extra_data);
      obj.index_hash_source = index_hash_source;
      return obj;
    }

    /** Restore the previous swift version of this object */
    virtual int swift_versioning_restore(RGWObjectCtx* obj_ctx,
					 bool& restored,   /* out */
					 const DoutPrefixProvider* dpp) = 0;
    /** Copy the current version of a swift object to the configured destination bucket*/
    virtual int swift_versioning_copy(RGWObjectCtx* obj_ctx,
				      const DoutPrefixProvider* dpp,
				      optional_yield y) = 0;

    /** Get a new ReadOp for this object */
    virtual std::unique_ptr<ReadOp> get_read_op(RGWObjectCtx*) = 0;
    /** Get a new DeleteOp for this object */
    virtual std::unique_ptr<DeleteOp> get_delete_op(RGWObjectCtx*) = 0;

    /** Get @a count OMAP values via listing, starting at @a marker for this object */
    virtual int omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
			      std::map<std::string, bufferlist>* m,
			      bool* pmore, optional_yield y) = 0;
    /** Get all OMAP key/value pairs for this object */
    virtual int omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>* m,
			     optional_yield y) = 0;
    /** Get the OMAP values matching the given set of keys */
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
			      const std::set<std::string>& keys,
			      Attrs* vals) = 0;
    /** Get a single OMAP value matching the given key */
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) = 0;

    /** Check to see if the give object pointer is uninitialized */
    static bool empty(Object* o) { return (!o || o->empty()); }
    /** Get a unique copy of this object */
    virtual std::unique_ptr<Object> clone() = 0;

    /* dang - This is temporary, until the API is completed */
    /** Get the key for this object */
    rgw_obj_key& get_key() { return key; }
    /** Set the instance for this object */
    void set_instance(const std::string &i) { key.set_instance(i); }
    /** Get the instance for this object */
    const std::string &get_instance() const { return key.instance; }
    /** Check to see if this object has an instance set */
    bool have_instance(void) { return key.have_instance(); }

    friend inline std::ostream& operator<<(std::ostream& out, const Object& o) {
      if (o.bucket)
	out << o.bucket << ":";
      out << o.key;
      return out;
    }
    friend inline std::ostream& operator<<(std::ostream& out, const Object* o) {
      if (!o)
	out << "<NULL>";
      else
	out << *o;
      return out;
    }
    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<Object>& p) {
      out << p.get();
      return out;
    }
};

/**
 * @brief Abstraction of a single part of a multipart upload
 */
class MultipartPart {
protected:
  std::string oid;

public:
  MultipartPart() = default;
  virtual ~MultipartPart() = default;

  /** Get the part number of this part */
  virtual uint32_t get_num() = 0;
  /** Get the size of this part */
  virtual uint64_t get_size() = 0;
  /** Get the etag of this part */
  virtual const std::string& get_etag() = 0;
  /** Get the modification time of this part */
  virtual ceph::real_time& get_mtime() = 0;
};

/**
 * @brief Abstraction of a multipart upload
 *
 * This represents a multipart upload.  For large objects, it's inefficient to do a
 * single, long-lived upload of the object.  Instead, protocols such as S3 allow the
 * client to start a multipart upload, and then upload object in smaller parts in
 * parallel.  A MultipartUpload consists of a target bucket, a unique identifier, and a
 * set of upload parts.
 */
class MultipartUpload {
protected:
  Bucket* bucket;
  std::map<uint32_t, std::unique_ptr<MultipartPart>> parts;
  jspan_context trace_ctx{false, false};
public:
  MultipartUpload(Bucket* _bucket) : bucket(_bucket) {}
  virtual ~MultipartUpload() = default;

  /** Get the name of the object representing this upload in the backing store */
  virtual const std::string& get_meta() const = 0;
  /** Get the name of the target object for this upload */
  virtual const std::string& get_key() const = 0;
  /** Get the unique ID of this upload */
  virtual const std::string& get_upload_id() const = 0;
  /** Get the owner of this upload */
  virtual const ACLOwner& get_owner() const = 0;
  /** Get the modification time of this upload */
  virtual ceph::real_time& get_mtime() = 0;

  /** Get all the cached parts that make up this upload */
  std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() { return parts; }

  /** Get the trace context of this upload */
  const jspan_context& get_trace() { return trace_ctx; }

  /** Get the Object that represents this upload */
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() = 0;

  /** Initialize this upload */
  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, RGWObjectCtx* obj_ctx, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) = 0;
  /** List all the parts of this upload, filling the parts cache */
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) = 0;
  /** Abort this upload */
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct,
		    RGWObjectCtx* obj_ctx) = 0;
  /** Complete this upload, making it available as a normal object */
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj,
		       RGWObjectCtx* obj_ctx) = 0;

  /** Get placement and/or attribute info for this upload */
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, RGWObjectCtx* obj_ctx, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) = 0;

  /** Get a Writer to write to a part of this upload */
  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  std::unique_ptr<rgw::sal::Object> _head_obj,
			  const rgw_user& owner, RGWObjectCtx& obj_ctx,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) = 0;

  friend inline std::ostream& operator<<(std::ostream& out, const MultipartUpload& u) {
    out << u.get_meta();
    if (!u.get_upload_id().empty())
      out << ":" << u.get_upload_id();
    return out;
  }
  friend inline std::ostream& operator<<(std::ostream& out, const MultipartUpload* u) {
    if (!u)
      out << "<NULL>";
    else
      out << *u;
    return out;
  }
  friend inline std::ostream& operator<<(std::ostream& out, const
				    std::unique_ptr<MultipartUpload>& p) {
    out << p.get();
    return out;
  }
};

/**
 * @brief Interface of a lock/serialization
 */
struct Serializer {
  Serializer() = default;
  virtual ~Serializer() = default;

  /** Try to take the lock for the given amount of time. */
  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) = 0;
  /** Unlock the lock */
  virtual int unlock()  = 0;
};

/** @brief Abstraction of a serializer for multipart uploads
 */
struct MPSerializer : Serializer {
  bool locked;
  std::string oid;
  MPSerializer() : locked(false) {}
  virtual ~MPSerializer() = default;

  void clear_locked() {
    locked = false;
  }
};

/** @brief Abstraction of a serializer for Lifecycle
 */
struct LCSerializer : Serializer {
  LCSerializer() {}
  virtual ~LCSerializer() = default;
};

/**
 * @brief Abstraction for lifecycle processing
 *
 * Lifecycle processing loops over the objects in a bucket, applying per-bucket policy
 * to each object.  Examples of policy can be deleting after a certain amount of time,
 * deleting extra versions, changing the storage class, and so on.
 */
class Lifecycle {
public:
  /** Head of a lifecycle run.  Used for tracking parallel lifecycle runs. */
  struct LCHead {
    time_t start_date{0};
    std::string marker;

    LCHead() = default;
    LCHead(time_t _date, std::string& _marker) : start_date(_date), marker(_marker) {}
  };

  /** Single entry in a lifecycle run.  Multiple entries can exist processing different
   * buckets. */
  struct LCEntry {
    std::string bucket;
    uint64_t start_time{0};
    uint32_t status{0};

    LCEntry() = default;
    LCEntry(std::string& _bucket, uint64_t _time, uint32_t _status) : bucket(_bucket), start_time(_time), status(_status) {}
  };

  Lifecycle() = default;
  virtual ~Lifecycle() = default;

  /** Get an entry matching the given marker */
  virtual int get_entry(const std::string& oid, const std::string& marker, LCEntry& entry) = 0;
  /** Get the entry following the given marker */
  virtual int get_next_entry(const std::string& oid, std::string& marker, LCEntry& entry) = 0;
  /** Store a modified entry in then backing store */
  virtual int set_entry(const std::string& oid, const LCEntry& entry) = 0;
  /** List all known entries */
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries, std::vector<LCEntry>& entries) = 0;
  /** Remove an entry from the backing store */
  virtual int rm_entry(const std::string& oid, const LCEntry& entry) = 0;
  /** Get a head */
  virtual int get_head(const std::string& oid, LCHead& head) = 0;
  /** Store a modified head to the backing store */
  virtual int put_head(const std::string& oid, const LCHead& head) = 0;

  /** Get a serializer for lifecycle */
  virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) = 0;
};

/**
 * @brief Abstraction for a Notification event
 *
 * RGW can generate notifications for various events, such as object creation or
 * deletion.
 */
class Notification {
protected:
  Object* obj;
  Object* src_obj;
  rgw::notify::EventType event_type;

  public:
    Notification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
      : obj(_obj), src_obj(_src_obj), event_type(_type)
    {}

    virtual ~Notification() = default;

    /** Indicate the start of the event associated with this notification */
    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) = 0;
    /** Indicate the successful completion of the event associated with this notification */
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) = 0;
};

/**
 * @brief Abstraction for an asynchronous writer
 *
 * Writing is done through a set of filters.  This allows chaining filters to do things
 * like compression and encryption on async writes.  This is the base abstraction for
 * those filters.
 */
class Writer : public ObjectProcessor {
protected:
  const DoutPrefixProvider* dpp;

public:
  Writer(const DoutPrefixProvider *_dpp, optional_yield y) : dpp(_dpp) {}
  virtual ~Writer() = default;

  /** prepare to start processing object data */
  virtual int prepare(optional_yield y) = 0;

  /**
   * Process a buffer. Called multiple times to write different buffers.
   * data.length() == 0 indicates the last call and may be used to flush
   * the data buffers.
   */
  virtual int process(bufferlist&& data, uint64_t offset) = 0;

  /** complete the operation and make its result visible to clients */
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y) = 0;
};

/**
 * @brief Abstraction of a Zone
 *
 * This abstraction allows access to information about zones.  This can be the zone
 * containing the RGW, or another zone.
 */
class Zone {
  public:
    virtual ~Zone() = default;

    /** Get info about the zonegroup containing this zone */
    virtual const RGWZoneGroup& get_zonegroup() = 0;
    /** Get info about a zonegroup by ID */
    virtual int get_zonegroup(const std::string& id, RGWZoneGroup& zonegroup) = 0;
    /** Get the parameters of this zone */
    virtual const RGWZoneParams& get_params() = 0;
    /** Get the ID of this zone */
    virtual const rgw_zone_id& get_id() = 0;
    /** Get info about the realm containing this zone */
    virtual const RGWRealm& get_realm() = 0;
    /** Get the name of this zone */
    virtual const std::string& get_name() const = 0;
    /** True if this zone is writable */
    virtual bool is_writeable() = 0;
    /** Get the URL for the endpoint for redirecting to this zone */
    virtual bool get_redirect_endpoint(std::string* endpoint) = 0;
    /** Check to see if the given API is supported in this zone */
    virtual bool has_zonegroup_api(const std::string& api) const = 0;
    /** Get the current period ID for this zone */
    virtual const std::string& get_current_period_id() = 0;
};

/**
 * @brief Abstraction of a manager for Lua scripts
 *
 * RGW can load and process Lua scripts.  This will handle loading/storing scripts.
 */
class LuaScriptManager {
public:
  virtual ~LuaScriptManager() = default;

  /** Get a script named with the given key from the backing store */
  virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) = 0;
  /** Put a script named with the given key to the backing store */
  virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) = 0;
  /** Delete a script named with the given key from the backing store */
  virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) = 0;
};

/** @} namespace rgw::sal in group RGWSAL */
} } // namespace rgw::sal

/**
 * @brief A manager for Stores
 *
 * This will manage the singleton instances of the various stores.  Stores come in two
 * varieties: Full and Raw.  A full store is suitable for use in a radosgw daemon.  It
 * has full access to the cluster, if any.  A raw store is a stripped down store, used
 * for admin commands.
 */
class StoreManager {
public:
  StoreManager() {}
  /** Get a full store by service name */
  static rgw::sal::Store* get_storage(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
                               bool run_sync_thread, bool run_reshard_thread, bool use_cache = true, bool use_gc = true) {
    rgw::sal::Store* store = init_storage_provider(dpp, cct, svc, use_gc_thread, use_lc_thread,
        quota_threads, run_sync_thread, run_reshard_thread, use_cache, use_gc);
    return store;
  }
  /** Get a stripped down store by service name */
  static rgw::sal::Store* get_raw_storage(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc) {
    rgw::sal::Store* store = init_raw_storage_provider(dpp, cct, svc);
    return store;
  }
  /** Initialize a new full Store */
  static rgw::sal::Store* init_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache, bool use_gc);
  /** Initialize a new raw Store */
  static rgw::sal::Store* init_raw_storage_provider(const DoutPrefixProvider* dpp, CephContext* cct, const std::string svc);
  /** Close a Store when it's no longer needed */
  static void close_storage(rgw::sal::Store* store);

};

/** @} */
