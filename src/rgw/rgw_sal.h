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

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/tracer.h"
#include "rgw_sal_fwd.h"
#include "rgw_lua.h"
#include "rgw_notify_event_type.h"
#include "rgw_req_context.h"
#include "include/random.h"

// FIXME: following subclass dependencies
#include "driver/rados/rgw_user.h"
#include "driver/rados/rgw_datalog_notify.h"

struct RGWBucketEnt;
class RGWRESTMgr;
class RGWLC;
struct rgw_user_bucket;
class RGWUsageBatch;
class RGWCoroutinesManagerRegistry;
class RGWBucketSyncPolicyHandler;
using RGWBucketSyncPolicyHandlerRef = std::shared_ptr<RGWBucketSyncPolicyHandler>;
class RGWDataSyncStatusManager;
class RGWSyncModuleInstance;
typedef std::shared_ptr<RGWSyncModuleInstance> RGWSyncModuleInstanceRef;
class RGWCompressionInfo;
struct rgw_pubsub_topics;
struct rgw_pubsub_bucket_topics;
class RGWZonePlacementInfo;
struct rgw_pubsub_topic;

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

struct RGWObjState {
  rgw_obj obj;
  bool is_atomic{false};
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0}; //< size of raw object
  uint64_t accounted_size{0}; //< size before compression, encryption
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bufferlist tail_tag;
  std::string write_tag;
  bool fake_tag{false};
  std::string shadow_obj;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  bool keep_tail{false};
  bool is_olh{false};
  bufferlist olh_tag;
  uint64_t pg_ver{false};
  uint32_t zone_short_id{0};
  bool compressed{false};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  std::map<std::string, ceph::buffer::list> attrset;

  RGWObjState();
  RGWObjState(const RGWObjState& rhs);
  ~RGWObjState();

  bool get_attr(std::string name, bufferlist& dest) {
    auto iter = attrset.find(name);
    if (iter != attrset.end()) {
      dest = iter->second;
      return true;
    }
    return false;
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

struct MPSerializer;
class GCChain;
class RGWOIDCProvider;
class RGWRole;

enum AttrsMod {
  ATTRSMOD_NONE    = 0,
  ATTRSMOD_REPLACE = 1,
  ATTRSMOD_MERGE   = 2
};

static constexpr uint32_t FLAG_LOG_OP = 0x0001;
static constexpr uint32_t FLAG_PREVENT_VERSIONING = 0x0002;

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
                       const req_context& rctx,
                       uint32_t flags) = 0;
};

/** A list of key-value attributes */
  using Attrs = std::map<std::string, ceph::buffer::list>;

/**
 * @brief Base singleton representing a Store or Filter
 *
 * The Driver is the base abstraction of the SAL layer.  It represents a base storage
 * mechanism, or a intermediate stacking layer.  There is a single instance of a given
 * Driver per RGW, and this Driver mediates all access to it's backing.
 *
 * A Driver contains, loosely, @a User, @a Bucket, and @a Object entities.  The @a Object
 * contains data, and it's associated metadata.  The @a Bucket contains Objects, and
 * metadata about the bucket.  Both Buckets and Objects are owned by a @a User, which is
 * the basic unit of access control.
 *
 * A Driver also has metadata and some global responsibilities.  For example, a driver is
 * responsible for managing the LifeCycle activities for it's data.
 */
class Driver {
  public:
    Driver() {}
    virtual ~Driver() = default;

    /** Post-creation initialization of driver */
    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) = 0;
    /** Name of this driver provider (e.g., "rados") */
    virtual const std::string get_name() const = 0;
    /** Get cluster unique identifier */
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) = 0;
    /** Get a User from a rgw_user.  Does not query driver for user info, so quick */
    virtual std::unique_ptr<User> get_user(const rgw_user& u) = 0;
    /** Lookup a User by access key.  Queries driver for user info. */
    virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Lookup a User by email address.  Queries driver for user info. */
    virtual int get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Lookup a User by swift username.  Queries driver for user info. */
    virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) = 0;
    /** Get a basic Object.  This Object is not looked up, and is incomplete, since is
     * does not have a bucket.  This should only be used when an Object is needed before
     * there is a Bucket, otherwise use the get_object() in the Bucket class. */
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) = 0;
    /** Get a Bucket by info.  Does not query the driver, just uses the give bucket info. */
    virtual std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) = 0;
    /** Load a Bucket by key.  Queries driver for bucket info.  On -ENOENT, the
     * bucket must still be allocated to support bucket->create(). */
    virtual int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                            std::unique_ptr<Bucket>* bucket, optional_yield y) = 0;
    /** For multisite, this driver is the zone's master */
    virtual bool is_meta_master() = 0;
    /** Get zone info for this driver */
    virtual Zone* get_zone() = 0;
    /** Get a unique ID specific to this zone. */
    virtual std::string zone_unique_id(uint64_t unique_num) = 0;
    /** Get a unique Swift transaction ID specific to this zone */
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) = 0;
    /** Lookup a zonegroup by ID */
    virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) = 0;
    /** List all zones in all zone groups by ID */
    virtual int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) = 0;
    /** Get statistics about the cluster represented by this driver */
    virtual int cluster_stat(RGWClusterStat& stats) = 0;
    /** Get a @a Lifecycle object. Used to manage/run lifecycle transitions */
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) = 0;

     /** Get a @a Notification object.  Used to communicate with non-RGW daemons, such as
      * management/tracking software */
    /** RGWOp variant */
    virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
        rgw::notify::EventType event_type, optional_yield y, const std::string* object_name=nullptr) = 0;
    /** No-req_state variant (e.g., rgwlc) */
    virtual std::unique_ptr<Notification> get_notification(
        const DoutPrefixProvider* dpp,
        rgw::sal::Object* obj,
        rgw::sal::Object* src_obj,
        const rgw::notify::EventTypeList& event_types,
        rgw::sal::Bucket* _bucket,
        std::string& _user_id,
        std::string& _user_tenant,
        std::string& _req_id,
        optional_yield y) = 0;
    /** Read the topic config entry into @a data and (optionally) @a objv_tracker */
    virtual int read_topics(const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) = 0;
    /** check if the v1 topics object exists */
    virtual int stat_topics_v1(const std::string& tenant, optional_yield y, const DoutPrefixProvider *dpp) = 0;
    /** Write @a info and (optionally) @a objv_tracker into the config */
    virtual int write_topics(const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) = 0;
    /** Remove the topic config, optionally a specific version */
    virtual int remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker,
        optional_yield y,const DoutPrefixProvider *dpp) = 0;
    /** Read the topic config entry into data and (optionally) objv_tracker */
    virtual int read_topic_v2(const std::string& topic_name,
                              const std::string& tenant,
                              rgw_pubsub_topic& topic,
                              RGWObjVersionTracker* objv_tracker,
                              optional_yield y,
                              const DoutPrefixProvider* dpp) = 0;
    /** Write topic info and @a objv_tracker into the config */
    virtual int write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
                               RGWObjVersionTracker& objv_tracker,
                               optional_yield y,
                               const DoutPrefixProvider* dpp) = 0;
    /** Remove the topic config, optionally a specific version */
    virtual int remove_topic_v2(const std::string& topic_name,
                                const std::string& tenant,
                                RGWObjVersionTracker& objv_tracker,
                                optional_yield y,
                                const DoutPrefixProvider* dpp) = 0;
    /** Update the bucket-topic mapping in the store, if |add_mapping|=true then
     * adding the |bucket_key| |topic| mapping to store, else delete the
     * |bucket_key| |topic| mapping from the store.  The |bucket_key| is
     * in the format |tenant_name + "/" + bucket_name| if tenant is not empty
     * else |bucket_name|*/
    virtual int update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                            const std::string& bucket_key,
                                            bool add_mapping,
                                            optional_yield y,
                                            const DoutPrefixProvider* dpp) = 0;
    /** Remove the |bucket_key| from bucket-topic mapping in the store, for all
    the topics under |bucket_topics|*/
    virtual int remove_bucket_mapping_from_topics(
        const rgw_pubsub_bucket_topics& bucket_topics,
        const std::string& bucket_key,
        optional_yield y,
        const DoutPrefixProvider* dpp) = 0;
    /** Get the bucket-topic mapping from the backend store. The |bucket_keys|
     * are in the format |tenant_name + "/" + bucket_name| if tenant is not
     * empty else |bucket_name|*/
    virtual int get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                         std::set<std::string>& bucket_keys,
                                         optional_yield y,
                                         const DoutPrefixProvider* dpp) = 0;
    /** Get access to the lifecycle management thread */
    virtual RGWLC* get_rgwlc(void) = 0;
    /** Get access to the coroutine registry.  Used to create new coroutine managers */
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() = 0;

    /** Log usage data to the driver.  Usage data is things like bytes sent/received and
     * op count */
    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y) = 0;
    /** Log OP data to the driver.  Data is opaque to SAL */
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) = 0;
    /** Register this driver to the service map.  Somewhat Rados specific; may be removed*/
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
					const std::map<std::string, std::string>& meta) = 0;
    /** Get default quota info.  Used as fallback if a user or bucket has no quota set*/
    virtual void get_quota(RGWQuota& quota) = 0;
    /** Get global rate limit configuration*/
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) = 0;
    /** Enable or disable a set of bucket.  e.g. if a User is suspended */
    virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled, optional_yield y) = 0;
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
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) = 0;
    /** Clear all usage statistics globally */
    virtual int clear_usage(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    /** Get usage statistics for all users and buckets */
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim usage log for all users and buckets */
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) = 0;
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
    /** Get an instance of the Sync module for bucket sync */
    virtual const RGWSyncModuleInstanceRef& get_sync_module() = 0;
    /** Get the ID of the current host */
    virtual std::string get_host_id() = 0;
    /** Get a Lua script manager for running lua scripts and reloading packages */
    virtual std::unique_ptr<LuaManager> get_lua_manager(const std::string& luarocks_path) = 0;
    /** Get an IAM Role by name etc. */
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
					      std::string tenant,
					      std::string path="",
					      std::string trust_policy="",
					      std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) = 0;
    /** Get an IAM Role by ID */
    virtual std::unique_ptr<RGWRole> get_role(std::string id) = 0;
    virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) = 0;
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
				   std::vector<std::unique_ptr<RGWOIDCProvider>>& providers, optional_yield y) = 0;
    /** Get a Writer that appends to an object */
    virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) = 0;
    /** Get a Writer that atomically writes an entire object */
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) = 0;

    /** Get the compression type of a placement rule */
    virtual const std::string& get_compression_type(const rgw_placement_rule& rule) = 0;
    /** Check to see if this placement rule is valid */
    virtual bool valid_placement(const rgw_placement_rule& rule) = 0;

    /** Clean up a driver for termination */
    virtual void finalize(void) = 0;

    /** Get the Ceph context associated with this driver.  May be removed. */
    virtual CephContext* ctx(void) = 0;

    /** Register admin APIs unique to this driver */
    virtual void register_admin_apis(RGWRESTMgr* mgr) = 0;
};


/// \brief Ref-counted callback object for User/Bucket read_stats_async().
class ReadStatsCB : public boost::intrusive_ref_counter<ReadStatsCB> {
 public:
  virtual ~ReadStatsCB() {}
  virtual void handle_response(int r, const RGWStorageStats& stats) = 0;
};

/**
 * @brief A list of buckets
 *
 * This is the result from a bucket listing operation.
 */
struct BucketList {
  /// The list of results, sorted by bucket name
  std::vector<RGWBucketEnt> buckets;
  /// The next marker to resume listing, or empty
  std::string next_marker;
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
  public:
    User() {}
    virtual ~User() = default;

    /** Clone a copy of this user.  Used when modification is necessary of the copy */
    virtual std::unique_ptr<User> clone() = 0;
    /** List the buckets owned by a user */
    virtual int list_buckets(const DoutPrefixProvider* dpp,
			     const std::string& marker, const std::string& end_marker,
			     uint64_t max, bool need_stats, BucketList& buckets,
			     optional_yield y) = 0;

    /** Get the display name for this User */
    virtual std::string& get_display_name() = 0;
    /** Get the tenant name for this User */
    virtual const std::string& get_tenant() = 0;
    /** Set the tenant name for this User */
    virtual void set_tenant(std::string& _t) = 0;
    /** Get the namespace for this User */
    virtual const std::string& get_ns() = 0;
    /** Set the namespace for this User */
    virtual void set_ns(std::string& _ns) = 0;
    /** Clear the namespace for this User */
    virtual void clear_ns() = 0;
    /** Get the full ID for this User */
    virtual const rgw_user& get_id() const = 0;
    /** Get the type of this User */
    virtual uint32_t get_type() const = 0;
    /** Get the maximum number of buckets allowed for this User */
    virtual int32_t get_max_buckets() const = 0;
    /** Set the maximum number of buckets allowed for this User */
    virtual void set_max_buckets(int32_t _max_buckets) = 0;
    /** Set quota info */
    virtual void set_info(RGWQuotaInfo& _quota) = 0;
    /** Get the capabilities for this User */
    virtual const RGWUserCaps& get_caps() const = 0;
    /** Get the version tracker for this User */
    virtual RGWObjVersionTracker& get_version_tracker() = 0;
    /** Get the cached attributes for this User */
    virtual Attrs& get_attrs() = 0;
    /** Set the cached attributes fro this User */
    virtual void set_attrs(Attrs& _attrs) = 0;
    /** Check if a User is empty */
    virtual bool empty() const = 0;
    /** Check if a User pointer is empty */
    static bool empty(const User* u) { return (!u || u->empty()); }
    /** Check if a User unique_pointer is empty */
    static bool empty(const std::unique_ptr<User>& u) { return (!u || u->empty()); }
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
    virtual int read_stats_async(const DoutPrefixProvider *dpp,
                                 boost::intrusive_ptr<ReadStatsCB> cb) = 0;
    /** Flush accumulated stat changes for this User to the backing store */
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) = 0;
    /** Read detailed usage stats for this User from the backing store */
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim User usage stats to the given epoch range */
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) = 0;

    /** Load this User from the backing store.  requires ID to be set, fills all other fields. */
    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Store this User to the backing store */
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) = 0;
    /** Remove this User from the backing store */
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Verify multi-factor authentication for this user */
    virtual int verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp, optional_yield y) = 0;

    /* dang temporary; will be removed when User is complete */
    virtual RGWUserInfo& get_info() = 0;

    /** Print the User to @a out */
    virtual void print(std::ostream& out) const = 0;

    friend inline std::ostream& operator<<(std::ostream& out, const User& u) {
      u.print(out);
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const User* u) {
      if (!u)
	out << "<NULL>";
      else
	u->print(out);
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<User>& p) {
      out << p.get();
      return out;
    }
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
      rgw::AccessListFilter access_list_filter{};
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
    virtual ~Bucket() = default;

    /** Get an @a Object belonging to this bucket */
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) = 0;
    /** List the contents of this bucket */
    virtual int list(const DoutPrefixProvider* dpp, ListParams&, int, ListResults&, optional_yield y) = 0;
    /** Get the cached attributes associated with this bucket */
    virtual Attrs& get_attrs(void) = 0;
    /** Set the cached attributes on this bucket */
    virtual int set_attrs(Attrs a) = 0;
    /** Remove this bucket from the backing store */
    virtual int remove(const DoutPrefixProvider* dpp, bool delete_children, optional_yield y) = 0;
    /** Remove this bucket, bypassing garbage collection.  May be removed */
    virtual int remove_bypass_gc(int concurrent_max, bool
				 keep_index_consistent,
				 optional_yield y, const
				 DoutPrefixProvider *dpp) = 0;
    /** Get then ACL for this bucket */
    virtual RGWAccessControlPolicy& get_acl(void) = 0;
    /** Set the ACL for this bucket */
    virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl, optional_yield y) = 0;

    /// Input parameters for create().
    struct CreateParams {
      rgw_user owner;
      std::string zonegroup_id;
      rgw_placement_rule placement_rule;
      // zone placement is optional on buckets created for another zonegroup
      const RGWZonePlacementInfo* zone_placement;
      RGWAccessControlPolicy policy;
      Attrs attrs;
      bool obj_lock_enabled = false;
      std::string marker;
      std::string bucket_id;
      std::optional<std::string> swift_ver_location;
      std::optional<RGWQuotaInfo> quota;
      std::optional<ceph::real_time> creation_time;
    };

    /// Create this bucket in the backing store.
    virtual int create(const DoutPrefixProvider* dpp,
                       const CreateParams& params,
                       optional_yield y) = 0;

    /** Load this bucket from the backing store.  Requires the key to be set, fills other fields. */
    virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Read the bucket stats from the backing Store, synchronous */
    virtual int read_stats(const DoutPrefixProvider *dpp,
			   const bucket_index_layout_generation& idx_layout,
			   int shard_id, std::string* bucket_ver, std::string* master_ver,
			   std::map<RGWObjCategory, RGWStorageStats>& stats,
			   std::string* max_marker = nullptr,
			   bool* syncstopped = nullptr) = 0;
    /** Read the bucket stats from the backing Store, asynchronous */
    virtual int read_stats_async(const DoutPrefixProvider *dpp,
				 const bucket_index_layout_generation& idx_layout,
				 int shard_id, boost::intrusive_ptr<ReadStatsCB> cb) = 0;
    /** Sync this bucket's stats to the owning user's stats in the backing store */
    virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                RGWBucketEnt* optional_ent) = 0;
    /** Check if this bucket needs resharding, and schedule it if it does */
    virtual int check_bucket_shards(const DoutPrefixProvider* dpp,
                                    uint64_t num_objs, optional_yield y) = 0;
    /** Change the owner of this bucket in the backing store.  Current owner must be set.  Does not
     * change ownership of the objects in the bucket. */
    virtual int chown(const DoutPrefixProvider* dpp, const rgw_user& new_owner, optional_yield y) = 0;
    /** Store the cached bucket info into the backing store */
    virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time mtime, optional_yield y) = 0;
    /** Get the owner of this bucket */
    virtual const rgw_user& get_owner() const = 0;
    /** Check in the backing store if this bucket is empty */
    virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Check if the given size fits within the quota */
    virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) = 0;
    /** Set the attributes in attrs, leaving any other existing attrs set, and
     * write them to the backing store; a merge operation */
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) = 0;
    /** Try to refresh the cached bucket info from the backing store.  Used in
     * read-modify-update loop. */
    virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y) = 0;
    /** Read usage information about this bucket from the backing store */
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) = 0;
    /** Trim the usage information to the given epoch range */
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) = 0;
    /** Remove objects from the bucket index of this bucket.  May be removed from API */
    virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) = 0;
    /** Check the state of the bucket index, and get stats from it.  May be removed from API */
    virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) = 0;
    /** Rebuild the bucket index.  May be removed from API */
    virtual int rebuild_index(const DoutPrefixProvider *dpp) = 0;
    /** Set a timeout on the check_index() call.  May be removed from API */
    virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) = 0;
    /** Remove this specific bucket instance from the backing store.  May be removed from API */
    virtual int purge_instance(const DoutPrefixProvider* dpp, optional_yield y) = 0;

    /** Check if this instantiation is empty */
    virtual bool empty() const = 0;
    /** Get the cached name of this bucket */
    virtual const std::string& get_name() const = 0;
    /** Get the cached tenant of this bucket */
    virtual const std::string& get_tenant() const = 0;
    /** Get the cached marker of this bucket */
    virtual const std::string& get_marker() const = 0;
    /** Get the cached ID of this bucket */
    virtual const std::string& get_bucket_id() const = 0;
    /** Get the cached placement rule of this bucket */
    virtual rgw_placement_rule& get_placement_rule() = 0;
    /** Get the cached creation time of this bucket */
    virtual ceph::real_time& get_creation_time() = 0;
    /** Get the cached modification time of this bucket */
    virtual ceph::real_time& get_modification_time() = 0;
    /** Get the cached version of this bucket */
    virtual obj_version& get_version() = 0;
    /** Set the cached version of this bucket */
    virtual void set_version(obj_version &ver) = 0;
    /** Check if this bucket is versioned */
    virtual bool versioned() = 0;
    /** Check if this bucket has versioning enabled */
    virtual bool versioning_enabled() = 0;

    /** Check if a Bucket pointer is empty */
    static bool empty(const Bucket* b) { return (!b || b->empty()); }
    /** Check if a Bucket unique pointer is empty */
    static bool empty(const std::unique_ptr<Bucket>& b) { return (!b || b->empty()); }
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
				bool *is_truncated, optional_yield y) = 0;
    /** Abort multipart uploads in a bucket */
    virtual int abort_multiparts(const DoutPrefixProvider* dpp,
				 CephContext* cct, optional_yield y) = 0;

    /** Read the bucket notification config into @a notifications with and (optionally) @a objv_tracker */
    virtual int read_topics(rgw_pubsub_bucket_topics& notifications, 
        RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) = 0;
    /** Write @a notifications with (optionally) @a objv_tracker into the bucket notification config */
    virtual int write_topics(const rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) = 0;
    /** Remove the bucket notification config with (optionally) @a objv_tracker */
    virtual int remove_topics(RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) = 0;

    /* dang - This is temporary, until the API is completed */
    virtual rgw_bucket& get_key() = 0;
    virtual RGWBucketInfo& get_info() = 0;

    /** Print the User to @a out */
    virtual void print(std::ostream& out) const = 0;

    friend inline std::ostream& operator<<(std::ostream& out, const Bucket& b) {
      b.print(out);
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const Bucket* b) {
      if (!b)
	out << "<NULL>";
      else
	b->print(out);
      return out;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<Bucket>& p) {
      out << p.get();
      return out;
    }

    virtual bool operator==(const Bucket& b) const = 0;
    virtual bool operator!=(const Bucket& b) const = 0;
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

        /// If non-null, read data/attributes from the given multipart part.
        int* part_num{nullptr};
        /// If part_num is specified and the object is multipart, the total
        /// number of multipart parts is assigned to this output parameter.
        std::optional<int> parts_count;
      } params;

      virtual ~ReadOp() = default;

      /** Prepare the Read op.  Must be called first */
      virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) = 0;

      /** Synchronous read. Read from @a ofs to @a end (inclusive)
       * into @a bl. Length is `end - ofs + 1`. */
      virtual int read(int64_t ofs, int64_t end, bufferlist& bl,
		       optional_yield y, const DoutPrefixProvider* dpp) = 0;

      /** Asynchronous read.  Read from @a ofs to @a end (inclusive)
       * calling @a cb on each read chunk. Length is `end - ofs +
       * 1`. */
      virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs,
			  int64_t end, RGWGetDataCB* cb, optional_yield y) = 0;

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
      virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) = 0;
    };

    Object() {}
    virtual ~Object() = default;

    /** Shortcut synchronous delete call for common deletes */
    virtual int delete_object(const DoutPrefixProvider* dpp,
			      optional_yield y,
			      uint32_t flags) = 0;
    /** Copy an this object to another object. */
    virtual int copy_object(User* user,
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
    virtual void set_atomic() = 0;
    /** Check if this object is atomic */
    virtual bool is_atomic() = 0;
    /** Pre-fetch data when reading */
    virtual void set_prefetch_data() = 0;
    /** Check if this object should prefetch */
    virtual bool is_prefetch_data() = 0;
    /** Mark data as compressed */
    virtual void set_compressed() = 0;
    /** Check if this object is compressed */
    virtual bool is_compressed() = 0;
    /** Invalidate cached info about this object, except atomic, prefetch, and
     * compressed */
    virtual void invalidate() = 0;

    /** Check to see if this object has an empty key.  This means it's uninitialized */
    virtual bool empty() const = 0;
    /** Get the name of this object */
    virtual const std::string &get_name() const = 0;

    /** Get the object state for this object.  Will be removed in the future */
    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh = true) = 0;
    /** Set the object state for this object */
    virtual void set_obj_state(RGWObjState& _state) = 0;
    /** Set attributes for this object from the backing store.  Attrs can be set or
     * deleted.  @note the attribute APIs may be revisited in the future. */
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) = 0;
    /** Get attributes for this object */
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) = 0;
    /** Modify attributes for this object. */
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) = 0;
    /** Delete attributes for this object */
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y) = 0;
    /** Check to see if this object has expired */
    virtual bool is_expired() = 0;
    /** Create a randomized instance ID for this object */
    virtual void gen_rand_obj_instance_name() = 0;
    /** Get a multipart serializer for this object */
    virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp,
							 const std::string& lock_name) = 0;
    /** Move the data of an object to new placement storage */
    virtual int transition(Bucket* bucket,
			   const rgw_placement_rule& placement_rule,
			   const real_time& mtime,
			   uint64_t olh_epoch,
			   const DoutPrefixProvider* dpp,
			   optional_yield y,
                           uint32_t flags) = 0;
    /** Move an object to the cloud */
    virtual int transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) = 0;
    /** Check to see if two placement rules match */
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) = 0;
    /** Dump driver-specific object layout info in JSON */
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) = 0;

    /** Get the cached attributes for this object */
    virtual Attrs& get_attrs(void) = 0;
    /** Get the (const) cached attributes for this object */
    virtual const Attrs& get_attrs(void) const = 0;
    /** Set the cached attributes for this object */
    virtual int set_attrs(Attrs a) = 0;
    /** Check to see if attributes are cached on this object */
    virtual bool has_attrs(void) = 0;
    /** Get the cached modification time for this object */
    virtual ceph::real_time get_mtime(void) const = 0;
    /** Get the cached size for this object */
    virtual uint64_t get_obj_size(void) const = 0;
    /** Get the bucket containing this object */
    virtual Bucket* get_bucket(void) const = 0;
    /** Set the bucket containing this object */
    virtual void set_bucket(Bucket* b) = 0;
    /** Get the sharding hash representation of this object */
    virtual std::string get_hash_source(void) = 0;
    /** Set the sharding hash representation of this object */
    virtual void set_hash_source(std::string s) = 0;
    /** Build an Object Identifier string for this object */
    virtual std::string get_oid(void) const = 0;
    /** True if this object is a delete marker (newest version is deleted) */
    virtual bool get_delete_marker(void) = 0;
    /** True if this object is stored in the extra data pool */
    virtual bool get_in_extra_data(void) = 0;
    /** Set the in_extra_data field */
    virtual void set_in_extra_data(bool i) = 0;
    /** Helper to sanitize object size, offset, and end values */
    int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
    /** Set the cached size of this object */
    virtual void set_obj_size(uint64_t s) = 0;
    /** Set the cached name of this object */
    virtual void set_name(const std::string& n) = 0;
    /** Set the cached key of this object */
    virtual void set_key(const rgw_obj_key& k) = 0;
    /** Get an rgw_obj representing this object */
    virtual rgw_obj get_obj(void) const = 0;

    /** Restore the previous swift version of this object */
    virtual int swift_versioning_restore(bool& restored,   /* out */
					 const DoutPrefixProvider* dpp, optional_yield y) = 0;
    /** Copy the current version of a swift object to the configured destination bucket*/
    virtual int swift_versioning_copy(const DoutPrefixProvider* dpp,
				      optional_yield y) = 0;

    /** Get a new ReadOp for this object */
    virtual std::unique_ptr<ReadOp> get_read_op() = 0;
    /** Get a new DeleteOp for this object */
    virtual std::unique_ptr<DeleteOp> get_delete_op() = 0;

    /// Return stored torrent info or -ENOENT if there isn't any.
    virtual int get_torrent_info(const DoutPrefixProvider* dpp,
                                 optional_yield y, bufferlist& bl) = 0;

    /** Get the OMAP values matching the given set of keys */
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
			      const std::set<std::string>& keys,
			      Attrs* vals) = 0;
    /** Get a single OMAP value matching the given key */
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
				    bool must_exist, optional_yield y) = 0;
    /** Change the ownership of this object */
    virtual int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) = 0;

    /** Check to see if the given object pointer is uninitialized */
    static bool empty(const Object* o) { return (!o || o->empty()); }
    /** Check to see if the given object unique pointer is uninitialized */
    static bool empty(const std::unique_ptr<Object>& o) { return (!o || o->empty()); }
    /** Get a unique copy of this object */
    virtual std::unique_ptr<Object> clone() = 0;

    /* dang - This is temporary, until the API is completed */
    /** Get the key for this object */
    virtual rgw_obj_key& get_key() = 0;
    /** Set the instance for this object */
    virtual void set_instance(const std::string &i) = 0;
    /** Get the instance for this object */
    virtual const std::string &get_instance() const = 0;
    /** Check to see if this object has an instance set */
    virtual bool have_instance(void) = 0;
    /** Clear the instance on this object */
    virtual void clear_instance() = 0;

    /** Print the User to @a out */
    virtual void print(std::ostream& out) const = 0;

    friend inline std::ostream& operator<<(std::ostream& out, const Object& o) {
      o.print(out);
      return out;
    }
    friend inline std::ostream& operator<<(std::ostream& out, const Object* o) {
      if (!o)
	out << "<NULL>";
      else
	o->print(out);
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
public:
  //object lock
  std::optional<RGWObjectRetention> obj_retention = std::nullopt;
  std::optional<RGWObjectLegalHold> obj_legal_hold = std::nullopt;

  MultipartUpload() = default;
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
  virtual std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() = 0;

  /** Get the trace context of this upload */
  virtual const jspan_context& get_trace() = 0;

  /** Get the Object that represents this upload */
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() = 0;

  /** Initialize this upload */
  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) = 0;
  /** List all the parts of this upload, filling the parts cache */
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated, optional_yield y,
			 bool assume_unsorted = false) = 0;
  /** Abort this upload */
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y) = 0;
  /** Complete this upload, making it available as a normal object */
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj) = 0;

  /** Get placement and/or attribute info for this upload */
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) = 0;

  /** Get a Writer to write to a part of this upload */
  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) = 0;

  /** Print the Upload to @a out */
  virtual void print(std::ostream& out) const = 0;

  friend inline std::ostream& operator<<(std::ostream& out, const MultipartUpload& u) {
    u.print(out);
    return out;
  }
  friend inline std::ostream& operator<<(std::ostream& out, const MultipartUpload* u) {
    if (!u)
      out << "<NULL>";
    else
      u->print(out);
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
class Serializer {
public:
  Serializer() = default;
  virtual ~Serializer() = default;

  /** Try to take the lock for the given amount of time. */
  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) = 0;
  /** Unlock the lock */
  virtual int unlock()  = 0;

  /** Print the Serializer to @a out */
  virtual void print(std::ostream& out) const = 0;

  friend inline std::ostream& operator<<(std::ostream& out, const Serializer& s) {
    s.print(out);
    return out;
  }
  friend inline std::ostream& operator<<(std::ostream& out, const Serializer* s) {
    if (!s)
      out << "<NULL>";
    else
      s->print(out);
    return out;
  }
};

/** @brief Abstraction of a serializer for multipart uploads
 */
class MPSerializer : public Serializer {
public:
  MPSerializer() = default;
  virtual ~MPSerializer() = default;

  virtual void clear_locked() = 0;
  /** Check to see if locked */
  virtual bool is_locked() = 0;
};

/** @brief Abstraction of a serializer for Lifecycle
 */
class LCSerializer : public Serializer {
public:
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
    LCHead() = default;
    virtual ~LCHead() = default;

    virtual time_t& get_start_date() = 0;
    virtual void set_start_date(time_t) = 0;
    virtual std::string& get_marker() = 0;
    virtual void set_marker(const std::string&) = 0;
    virtual time_t& get_shard_rollover_date() = 0;
    virtual void set_shard_rollover_date(time_t) = 0;
  };

  /** Single entry in a lifecycle run.  Multiple entries can exist processing different
   * buckets. */
  struct LCEntry {
    LCEntry() = default;
    virtual ~LCEntry() = default;

    virtual std::string& get_bucket() = 0;
    virtual void set_bucket(const std::string&) = 0;
    virtual std::string& get_oid() = 0;
    virtual void set_oid(const std::string&) = 0;
    virtual uint64_t get_start_time() = 0;
    virtual void set_start_time(uint64_t) = 0;
    virtual uint32_t get_status() = 0;
    virtual void set_status(uint32_t) = 0;

    /** Print the entry to @a out */
    virtual void print(std::ostream& out) const = 0;

    friend inline std::ostream& operator<<(std::ostream& out, const LCEntry& e) {
      e.print(out);
      return out;
    }
    friend inline std::ostream& operator<<(std::ostream& out, const LCEntry* e) {
      if (!e)
	out << "<NULL>";
      else
	e->print(out);
      return out;
    }
    friend inline std::ostream& operator<<(std::ostream& out, const std::unique_ptr<LCEntry>& p) {
      out << p.get();
      return out;
      }
  };

  Lifecycle() = default;
  virtual ~Lifecycle() = default;

  /** Get an empty entry */
  virtual std::unique_ptr<LCEntry> get_entry() = 0;
  /** Get an entry matching the given marker */
  virtual int get_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) = 0;
  /** Get the entry following the given marker */
  virtual int get_next_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) = 0;
  /** Store a modified entry in then backing store */
  virtual int set_entry(const std::string& oid, LCEntry& entry) = 0;
  /** List all known entries */
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries,
			   std::vector<std::unique_ptr<LCEntry>>& entries) = 0;
  /** Remove an entry from the backing store */
  virtual int rm_entry(const std::string& oid, LCEntry& entry) = 0;
  /** Get a head */
  virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head) = 0;
  /** Store a modified head to the backing store */
  virtual int put_head(const std::string& oid, LCHead& head) = 0;

  /** Get a serializer for lifecycle */
  virtual std::unique_ptr<LCSerializer> get_serializer(const std::string& lock_name,
						       const std::string& oid,
						       const std::string& cookie) = 0;
};

/**
 * @brief Abstraction for a Notification event
 *
 * RGW can generate notifications for various events, such as object creation or
 * deletion.
 */
class Notification {
protected:
  public:
    Notification() {}

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
public:
  Writer() {}
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
                       const req_context& rctx,
                       uint32_t flags) = 0;
};


/**
 * @brief Abstraction of a placement tier
 *
 * This abstraction allows access to information about placement tiers,
 * including storage class.
 */
class PlacementTier {
public:
  virtual ~PlacementTier() = default;

  /** Get the type of this tier */
  virtual const std::string& get_tier_type() = 0;
  /** Get the storage class of this tier */
  virtual const std::string& get_storage_class() = 0;
  /** Should we retain the head object when transitioning */
  virtual bool retain_head_object() = 0;
  /** Get the placement rule associated with this tier */
};

/**
 * @brief Abstraction of a zone group
 *
 * This class allows access to information about a zonegroup.  It may be the
 * group containing the current zone, or another group.
 */
class ZoneGroup {
public:
  virtual ~ZoneGroup() = default;
  /** Get the ID of this zonegroup */
  virtual const std::string& get_id() const = 0;
  /** Get the name of this zonegroup */
  virtual const std::string& get_name() const = 0;
  /** Determine if two zonegroups are the same */
  virtual int equals(const std::string& other_zonegroup) const = 0;
  /** Check if a placement target (by name) exists in this zonegroup */
  virtual bool placement_target_exists(std::string& target) const = 0;
  /** Check if this is the master zonegroup */
  virtual bool is_master_zonegroup() const = 0;
  /** Get the API name of this zonegroup */
  virtual const std::string& get_api_name() const = 0;
  /** Get the list of placement target names for this zone */
  virtual void get_placement_target_names(std::set<std::string>& names) const = 0;
  /** Get the name of the default placement target for this zone */
  virtual const std::string& get_default_placement_name() const = 0;
  /** Get the list of hostnames from this zone */
  virtual int get_hostnames(std::list<std::string>& names) const = 0;
  /** Get the list of hostnames that host s3 websites from this zone */
  virtual int get_s3website_hostnames(std::list<std::string>& names) const = 0;
  /** Get the number of zones in this zonegroup */
  virtual int get_zone_count() const = 0;
  /** Get the placement tier associated with the rule */
  virtual int get_placement_tier(const rgw_placement_rule& rule, std::unique_ptr<PlacementTier>* tier) = 0;
  /** Get a zone by ID */
  virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone) = 0;
  /** Get a zone by Name */
  virtual int get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone) = 0;
  /** List zones in zone group by ID */
  virtual int list_zones(std::list<std::string>& zone_ids) = 0;
  /** Clone a copy of this zonegroup. */
  virtual std::unique_ptr<ZoneGroup> clone() = 0;
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

    /** Clone a copy of this zone. */
    virtual std::unique_ptr<Zone> clone() = 0;
    /** Get info about the zonegroup containing this zone */
    virtual ZoneGroup& get_zonegroup() = 0;
    /** Get the ID of this zone */
    virtual const std::string& get_id() = 0;
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
    /** Get thes system access key for this zone */
    virtual const RGWAccessKey& get_system_key() = 0;
    /** Get the name of the realm containing this zone */
    virtual const std::string& get_realm_name() = 0;
    /** Get the ID of the realm containing this zone */
    virtual const std::string& get_realm_id() = 0;
    /** Get the tier type for the zone */
    virtual const std::string_view get_tier_type() = 0;
    /** Get a handler for zone sync policy. */
    virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() = 0;
};

/**
 * @brief Abstraction of a manager for Lua scripts and packages
 *
 * RGW can load and process Lua scripts.  This will handle loading/storing scripts; adding, deleting, and listing packages
 */
class LuaManager {
public:
  virtual ~LuaManager() = default;

  /** Get a script named with the given key from the backing store */
  virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) = 0;
  /** Put a script named with the given key to the backing store */
  virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) = 0;
  /** Delete a script named with the given key from the backing store */
  virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) = 0;
  /** Add a lua package */
  virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) = 0;
  /** Remove a lua package */
  virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) = 0;
  /** List lua packages */
  virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages) = 0;
  /** Reload lua packages */
  virtual int reload_packages(const DoutPrefixProvider* dpp, optional_yield y) = 0;
  /** Get the path to the loarocks install location **/
  virtual const std::string& luarocks_path() const = 0;
  /** Set the path to the loarocks install location **/
  virtual void set_luarocks_path(const std::string& path) = 0;
};

/** @} namespace rgw::sal in group RGWSAL */
} } // namespace rgw::sal

/**
 * @brief A manager for Drivers
 *
 * This will manage the singleton instances of the various drivers.  Drivers come in two
 * varieties: Full and Raw.  A full driver is suitable for use in a radosgw daemon.  It
 * has full access to the cluster, if any.  A raw driver is a stripped down driver, used
 * for admin commands.
 */
class DriverManager {
public:
  struct Config {
    /** Name of store to create */
    std::string store_name;
    /** Name of filter to create or "none" */
    std::string filter_name;
  };

  DriverManager() {}
  /** Get a full driver by service name */
  static rgw::sal::Driver* get_storage(const DoutPrefixProvider* dpp,
				      CephContext* cct,
				      const Config& cfg,
				      boost::asio::io_context& io_context,
				      const rgw::SiteConfig& site_config,
				      bool use_gc_thread,
				      bool use_lc_thread,
				      bool quota_threads,
				      bool run_sync_thread,
				      bool run_reshard_thread,
				      bool run_notification_thread, optional_yield y,
				      bool use_cache = true,
				      bool use_gc = true) {
    rgw::sal::Driver* driver = init_storage_provider(dpp, cct, cfg, io_context,
						   site_config,
						   use_gc_thread,
						   use_lc_thread,
						   quota_threads,
						   run_sync_thread,
						   run_reshard_thread,
                                                   run_notification_thread,
						   use_cache, use_gc, y);
    return driver;
  }
  /** Get a stripped down driver by service name */
  static rgw::sal::Driver* get_raw_storage(const DoutPrefixProvider* dpp,
					  CephContext* cct, const Config& cfg,
					  boost::asio::io_context& io_context,
					  const rgw::SiteConfig& site_config) {
    rgw::sal::Driver* driver = init_raw_storage_provider(dpp, cct, cfg,
							 io_context,
							 site_config);
    return driver;
  }
  /** Initialize a new full Driver */
  static rgw::sal::Driver* init_storage_provider(const DoutPrefixProvider* dpp,
						CephContext* cct,
						const Config& cfg,
						boost::asio::io_context& io_context,
						const rgw::SiteConfig& site_config,
						bool use_gc_thread,
						bool use_lc_thread,
						bool quota_threads,
						bool run_sync_thread,
						bool run_reshard_thread,
                                                bool run_notification_thread,
						bool use_metadata_cache,
						bool use_gc, optional_yield y);
  /** Initialize a new raw Driver */
  static rgw::sal::Driver* init_raw_storage_provider(const DoutPrefixProvider* dpp,
						    CephContext* cct,
						    const Config& cfg,
						    boost::asio::io_context& io_context,
						    const rgw::SiteConfig& site_config);
  /** Close a Driver when it's no longer needed */
  static void close_storage(rgw::sal::Driver* driver);

  /** Get the config for Drivers */
  static Config get_config(bool admin, CephContext* cct);

  /** Create a ConfigStore */
  static auto create_config_store(const DoutPrefixProvider* dpp,
                                  std::string_view type)
      -> std::unique_ptr<rgw::sal::ConfigStore>;

};

/** @} */
