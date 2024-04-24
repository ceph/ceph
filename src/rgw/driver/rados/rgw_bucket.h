// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <memory>
#include <variant>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include "include/rados/librados_fwd.hpp"
#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_metadata.h"
#include "rgw/rgw_bucket.h"

#include "rgw_string.h"
#include "rgw_sal.h"

#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/ceph_time.h"

#include "rgw_formats.h"

#include "services/svc_bucket_types.h"
#include "services/svc_bucket_sync.h"

// define as static when RGWBucket implementation completes
extern void rgw_get_buckets_obj(const rgw_user& user_id, std::string& buckets_obj_id);

class RGWSI_Meta;
class RGWBucketMetadataHandler;
class RGWBucketInstanceMetadataHandler;
class RGWUserCtl;
class RGWBucketCtl;
class RGWZone;
struct RGWZoneParams;

// this is used as a filter to RGWRados::cls_bucket_list_ordered; it
// conforms to the type RGWBucketListNameFilter
extern bool rgw_bucket_object_check_filter(const std::string& oid);

void init_default_bucket_layout(CephContext *cct, rgw::BucketLayout& layout,
				const RGWZone& zone,
				std::optional<rgw::BucketIndexType> type);

struct RGWBucketCompleteInfo {
  RGWBucketInfo info;
  std::map<std::string, bufferlist> attrs;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWBucketEntryMetadataObject : public RGWMetadataObject {
  RGWBucketEntryPoint ep;
  std::map<std::string, bufferlist> attrs;
public:
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, const obj_version& v, real_time m) : ep(_ep) {
    objv = v;
    mtime = m;
    set_pattrs (&attrs);
  }
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, const obj_version& v, real_time m, std::map<std::string, bufferlist>&& _attrs) :
    ep(_ep), attrs(std::move(_attrs)) {
    objv = v;
    mtime = m;
    set_pattrs (&attrs);
  }

  void dump(Formatter *f) const override {
    ep.dump(f);
  }

  RGWBucketEntryPoint& get_ep() {
    return ep;
  }

  std::map<std::string, bufferlist>& get_attrs() {
    return attrs;
  }
};

class RGWBucketInstanceMetadataObject : public RGWMetadataObject {
  RGWBucketCompleteInfo info;
public:
  RGWBucketInstanceMetadataObject() {}
  RGWBucketInstanceMetadataObject(RGWBucketCompleteInfo& i, const obj_version& v, real_time m) : info(i) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  void decode_json(JSONObj *obj) {
    info.decode_json(obj);
  }

  RGWBucketCompleteInfo& get_bci() {
    return info;
  }
  RGWBucketInfo& get_bucket_info() {
    return info.info;
  }
};

/**
 * store a list of the user's buckets, with associated functions.
 */
class RGWUserBuckets {
  std::map<std::string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() = default;
  RGWUserBuckets(RGWUserBuckets&&) = default;

  RGWUserBuckets& operator=(const RGWUserBuckets&) = default;

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(buckets, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(buckets, bl);
  }
  /**
   * Check if the user owns a bucket by the given name.
   */
  bool owns(std::string& name) {
    std::map<std::string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  /**
   * Add a (created) bucket to the user's bucket list.
   */
  void add(const RGWBucketEnt& bucket) {
    buckets[bucket.bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(const std::string& name) {
    std::map<std::string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  /**
   * Get the user's buckets as a map.
   */
  std::map<std::string, RGWBucketEnt>& get_buckets() { return buckets; }

  /**
   * Cleanup data structure
   */
  void clear() { buckets.clear(); }

  size_t count() { return buckets.size(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

class RGWBucketMetadataHandlerBase : public RGWMetadataHandler_GenericMetaBE {
public:
  virtual ~RGWBucketMetadataHandlerBase() {}
  virtual void init(RGWSI_Bucket *bucket_svc,
                    RGWBucketCtl *bucket_ctl) = 0;

};

class RGWBucketInstanceMetadataHandlerBase : public RGWMetadataHandler_GenericMetaBE {
public:
  virtual ~RGWBucketInstanceMetadataHandlerBase() {}
  virtual void init(RGWSI_Zone *zone_svc,
                    RGWSI_Bucket *bucket_svc,
                    RGWSI_BucketIndex *bi_svc) = 0;
};

class RGWBucketMetaHandlerAllocator {
public:
  static RGWBucketMetadataHandlerBase *alloc(librados::Rados& rados);
};

class RGWBucketInstanceMetaHandlerAllocator {
public:
  static RGWBucketInstanceMetadataHandlerBase *alloc(rgw::sal::Driver* driver);
};

class RGWArchiveBucketMetaHandlerAllocator {
public:
  static RGWBucketMetadataHandlerBase *alloc(librados::Rados& rados);
};

class RGWArchiveBucketInstanceMetaHandlerAllocator {
public:
  static RGWBucketInstanceMetadataHandlerBase *alloc(rgw::sal::Driver* driver);
};

extern int rgw_remove_object(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, rgw::sal::Bucket* bucket, rgw_obj_key& key, optional_yield y);

extern int rgw_object_get_attr(rgw::sal::Driver* driver, rgw::sal::Object* obj,
			       const char* attr_name, bufferlist& out_bl,
			       optional_yield y);

void check_bad_owner_bucket_mapping(rgw::sal::Driver* driver,
                                    const rgw_owner& owner,
                                    const std::string& tenant,
                                    bool fix, optional_yield y,
                                    const DoutPrefixProvider *dpp);

struct RGWBucketAdminOpState {
  rgw_user uid;
  rgw_account_id account_id;
  std::string display_name;
  std::string bucket_name;
  std::string bucket_id;
  std::string object_name;
  std::string new_bucket_name;
  std::string marker;

  bool list_buckets;
  bool stat_buckets;
  bool check_objects;
  bool fix_index;
  bool delete_child_objects;
  bool bucket_stored;
  bool sync_bucket;
  bool dump_keys;
  bool hide_progress;
  int max_aio = 0;
  ceph::timespan min_age = std::chrono::hours::zero();

  std::unique_ptr<rgw::sal::Bucket>  bucket;

  RGWQuotaInfo quota;
  RGWRateLimitInfo ratelimit_info;

  void set_fetch_stats(bool value) { stat_buckets = value; }
  void set_check_objects(bool value) { check_objects = value; }
  void set_fix_index(bool value) { fix_index = value; }
  void set_delete_children(bool value) { delete_child_objects = value; }
  void set_hide_progress(bool value) { hide_progress = value; }
  void set_dump_keys(bool value) { dump_keys = value; }

  void set_max_aio(int value) { max_aio = value; }
  void set_min_age(ceph::timespan value) { min_age = value; }

  void set_user_id(const rgw_user& user_id) {
    if (!user_id.empty())
      uid = user_id;
  }
  void set_tenant(const std::string& tenant_str) {
    uid.tenant = tenant_str;
  }
  void set_bucket_name(const std::string& bucket_str) {
    bucket_name = bucket_str; 
  }
  void set_object(std::string& object_str) {
    object_name = object_str;
  }
  void set_new_bucket_name(std::string& new_bucket_str) {
    new_bucket_name = new_bucket_str;
  }
  void set_quota(RGWQuotaInfo& value) {
    quota = value;
  }
  void set_bucket_ratelimit(RGWRateLimitInfo& value) {
    ratelimit_info = value;
  }


  void set_sync_bucket(bool value) { sync_bucket = value; }

  rgw_user& get_user_id() { return uid; }
  rgw_account_id& get_account_id() { return account_id; }
  std::string& get_user_display_name() { return display_name; }
  std::string& get_bucket_name() { return bucket_name; }
  std::string& get_object_name() { return object_name; }
  std::string& get_tenant() { return uid.tenant; }

  rgw::sal::Bucket* get_bucket() { return bucket.get(); }
  void set_bucket(std::unique_ptr<rgw::sal::Bucket> _bucket) {
    bucket = std::move(_bucket);
    bucket_stored = true;
  }

  void set_bucket_id(const std::string& bi) {
    bucket_id = bi;
  }
  const std::string& get_bucket_id() { return bucket_id; }

  bool will_fetch_stats() { return stat_buckets; }
  bool will_fix_index() { return fix_index; }
  bool will_delete_children() { return delete_child_objects; }
  bool will_check_objects() { return check_objects; }
  bool is_user_op() { return !uid.empty(); }
  bool is_account_op() { return !account_id.empty(); }
  bool is_system_op() { return uid.empty(); }
  bool has_bucket_stored() { return bucket_stored; }
  int get_max_aio() { return max_aio; }
  bool will_sync_bucket() { return sync_bucket; }

  RGWBucketAdminOpState() : list_buckets(false), stat_buckets(false), check_objects(false), 
                            fix_index(false), delete_child_objects(false),
                            bucket_stored(false), sync_bucket(true),
                            dump_keys(false), hide_progress(false) {}
};


/*
 * A simple wrapper class for administrative bucket operations
 */
class RGWBucket {
  RGWUserBuckets buckets;
  rgw::sal::Driver* driver;
  RGWAccessHandle handle;

  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::User> user;

  bool failure;

  RGWObjVersionTracker ep_objv; // entrypoint object version

public:
  RGWBucket() : driver(NULL), handle(NULL), failure(false) {}
  int init(rgw::sal::Driver* storage, RGWBucketAdminOpState& op_state, optional_yield y,
             const DoutPrefixProvider *dpp, std::string *err_msg = NULL);

  int check_bad_index_multipart(rgw::sal::RadosStore* const rados_store,
				RGWBucketAdminOpState& op_state,
				RGWFormatterFlusher& flusher,
				const DoutPrefixProvider *dpp,
				optional_yield y,
				std::string *err_msg = nullptr);

  int check_object_index(const DoutPrefixProvider *dpp, 
                         RGWBucketAdminOpState& op_state,
                         RGWFormatterFlusher& flusher,
                         optional_yield y,
                         std::string *err_msg = NULL);
  int check_index_olh(rgw::sal::RadosStore* rados_store, const DoutPrefixProvider *dpp, RGWBucketAdminOpState& op_state,
                      RGWFormatterFlusher& flusher);
  int check_index_unlinked(rgw::sal::RadosStore* rados_store, const DoutPrefixProvider *dpp, RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher);

  int check_index(const DoutPrefixProvider *dpp,
          RGWBucketAdminOpState& op_state,
          std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
          std::map<RGWObjCategory, RGWStorageStats>& calculated_stats,
          std::string *err_msg = NULL);

  int chown(RGWBucketAdminOpState& op_state, const std::string& marker,
            optional_yield y, const DoutPrefixProvider *dpp, std::string *err_msg = NULL);
  int set_quota(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = NULL);

  int remove_object(const DoutPrefixProvider *dpp, RGWBucketAdminOpState& op_state, optional_yield y, std::string *err_msg = NULL);
  int get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy, optional_yield y, const DoutPrefixProvider *dpp);
  int sync(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = NULL);

  void clear_failure() { failure = false; }

  const RGWBucketInfo& get_bucket_info() const { return bucket->get_info(); }
};

class RGWBucketAdminOp {
public:
  static int get_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y);
  static int get_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy, const DoutPrefixProvider *dpp, optional_yield y);
  static int dump_s3_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  std::ostream& os, const DoutPrefixProvider *dpp, optional_yield y);

  static int unlink(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = nullptr);
  static int link(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = NULL);
  static int chown(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const std::string& marker, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = NULL);

  static int check_index(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp);
  static int check_index_olh(rgw::sal::RadosStore* driver, RGWBucketAdminOpState& op_state,
                             RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);
  static int check_index_unlinked(rgw::sal::RadosStore* driver, RGWBucketAdminOpState& op_state,
                                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);

  static int remove_bucket(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, optional_yield y,
			   const DoutPrefixProvider *dpp, bool bypass_gc = false, bool keep_index_consistent = true);
  static int remove_object(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y);
  static int info(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp);
  static int limit_check(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
			 const std::list<std::string>& user_ids,
			 RGWFormatterFlusher& flusher, optional_yield y,
                         const DoutPrefixProvider *dpp,
			 bool warnings_only = false);
  static int set_quota(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y);

  static int list_stale_instances(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
				  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y);

  static int clear_stale_instances(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
				   RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y);
  static int fix_lc_shards(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y);
  static int fix_obj_expiry(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
			    RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y, bool dry_run = false);

  static int sync_bucket(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg = NULL);
};

struct rgw_ep_info {
  RGWBucketEntryPoint &ep;
  std::map<std::string, buffer::list>& attrs;
  RGWObjVersionTracker ep_objv;
  rgw_ep_info(RGWBucketEntryPoint &ep, std::map<std::string, bufferlist>& attrs)
    : ep(ep), attrs(attrs) {}
};

class RGWBucketCtl {
  CephContext *cct;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_Bucket_Sync *bucket_sync{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
    RGWSI_User* user = nullptr;
  } svc;

  struct Ctl {
    RGWUserCtl *user{nullptr};
  } ctl;

  RGWBucketMetadataHandler *bm_handler;
  RGWBucketInstanceMetadataHandler *bmi_handler;

  RGWSI_Bucket_BE_Handler bucket_be_handler; /* bucket backend handler */
  RGWSI_BucketInstance_BE_Handler bi_be_handler; /* bucket instance backend handler */

  int call(std::function<int(RGWSI_Bucket_X_Ctx& ctx)> f);

public:
  RGWBucketCtl(RGWSI_Zone *zone_svc,
               RGWSI_Bucket *bucket_svc,
               RGWSI_Bucket_Sync *bucket_sync_svc,
               RGWSI_BucketIndex *bi_svc,
               RGWSI_User* user_svc);

  void init(RGWUserCtl *user_ctl,
            RGWBucketMetadataHandler *_bm_handler,
            RGWBucketInstanceMetadataHandler *_bmi_handler,
            RGWDataChangesLog *datalog,
            const DoutPrefixProvider *dpp);

  struct Bucket {
    struct GetParams {
      RGWObjVersionTracker *objv_tracker{nullptr};
      real_time *mtime{nullptr};
      std::map<std::string, bufferlist> *attrs{nullptr};
      rgw_cache_entry_info *cache_info{nullptr};
      boost::optional<obj_version> refresh_version;
      std::optional<RGWSI_MetaBackend_CtxParams> bectx_params;

      GetParams() {}

      GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      GetParams& set_mtime(ceph::real_time *_mtime) {
        mtime = _mtime;
        return *this;
      }

      GetParams& set_attrs(std::map<std::string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
        cache_info = _cache_info;
        return *this;
      }

      GetParams& set_refresh_version(const obj_version& _refresh_version) {
        refresh_version = _refresh_version;
        return *this;
      }

      GetParams& set_bectx_params(std::optional<RGWSI_MetaBackend_CtxParams> _bectx_params) {
        bectx_params = _bectx_params;
        return *this;
      }
    };

    struct PutParams {
      RGWObjVersionTracker *objv_tracker{nullptr};
      ceph::real_time mtime;
      bool exclusive{false};
      const std::map<std::string, bufferlist> *attrs{nullptr};

      PutParams() {}

      PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      PutParams& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      PutParams& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      PutParams& set_attrs(const std::map<std::string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }
    };

    struct RemoveParams {
      RGWObjVersionTracker *objv_tracker{nullptr};

      RemoveParams() {}

      RemoveParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }
    };
  };

  struct BucketInstance {
    struct GetParams {
      real_time *mtime{nullptr};
      std::map<std::string, bufferlist> *attrs{nullptr};
      rgw_cache_entry_info *cache_info{nullptr};
      boost::optional<obj_version> refresh_version;
      RGWObjVersionTracker *objv_tracker{nullptr};
      std::optional<RGWSI_MetaBackend_CtxParams> bectx_params;

      GetParams() {}

      GetParams& set_mtime(ceph::real_time *_mtime) {
        mtime = _mtime;
        return *this;
      }

      GetParams& set_attrs(std::map<std::string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
        cache_info = _cache_info;
        return *this;
      }

      GetParams& set_refresh_version(const obj_version& _refresh_version) {
        refresh_version = _refresh_version;
        return *this;
      }

      GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      GetParams& set_bectx_params(std::optional<RGWSI_MetaBackend_CtxParams> _bectx_params) {
        bectx_params = _bectx_params;
        return *this;
      }
    };

    struct PutParams {
      std::optional<RGWBucketInfo *> orig_info; /* nullopt: orig_info was not fetched,
                                                   nullptr: orig_info was not found (new bucket instance */
      ceph::real_time mtime;
      bool exclusive{false};
      const std::map<std::string, bufferlist> *attrs{nullptr};
      RGWObjVersionTracker *objv_tracker{nullptr};

      PutParams() {}

      PutParams& set_orig_info(RGWBucketInfo *pinfo) {
        orig_info = pinfo;
        return *this;
      }

      PutParams& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      PutParams& set_exclusive(bool _exclusive) {
        exclusive = _exclusive;
        return *this;
      }

      PutParams& set_attrs(const std::map<std::string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }
    };

    struct RemoveParams {
      RGWObjVersionTracker *objv_tracker{nullptr};

      RemoveParams() {}

      RemoveParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }
    };
  };

  /* bucket entrypoint */
  int read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                  RGWBucketEntryPoint *info,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp,
                                  const Bucket::GetParams& params = {});
  int store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                   RGWBucketEntryPoint& info,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp,
                                   const Bucket::PutParams& params = {});
  int remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp,
                                    const Bucket::RemoveParams& params = {});

  /* bucket instance */
  int read_bucket_instance_info(const rgw_bucket& bucket,
                                  RGWBucketInfo *info,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp,
                                  const BucketInstance::GetParams& params = {});
  int store_bucket_instance_info(const rgw_bucket& bucket,
                                 RGWBucketInfo& info,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp,
                                 const BucketInstance::PutParams& params = {});
  int remove_bucket_instance_info(const rgw_bucket& bucket,
                                  RGWBucketInfo& info,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp,
                                  const BucketInstance::RemoveParams& params = {});

  /*
   * bucket_id may or may not be provided
   *
   * ep_objv_tracker might not be populated even if provided. Will only be set if entrypoint is read
   * (that is: if bucket_id is empty).
   */
  int read_bucket_info(const rgw_bucket& bucket,
                       RGWBucketInfo *info,
                       optional_yield y,
                       const DoutPrefixProvider *dpp,
                       const BucketInstance::GetParams& params = {},
		       RGWObjVersionTracker *ep_objv_tracker = nullptr);


  int set_bucket_instance_attrs(RGWBucketInfo& bucket_info,
                                std::map<std::string, bufferlist>& attrs,
                                RGWObjVersionTracker *objv_tracker,
                                optional_yield y,
                                const DoutPrefixProvider *dpp);

  /* user/bucket */
  int link_bucket(librados::Rados& rados,
                  const rgw_owner& owner,
                  const rgw_bucket& bucket,
                  ceph::real_time creation_time,
		  optional_yield y,
                  const DoutPrefixProvider *dpp,
                  bool update_entrypoint = true,
                  rgw_ep_info *pinfo = nullptr);

  int unlink_bucket(librados::Rados& rados,
                    const rgw_owner& owner,
                    const rgw_bucket& bucket,
		    optional_yield y,
                    const DoutPrefixProvider *dpp,
                    bool update_entrypoint = true);

  int read_buckets_stats(std::vector<RGWBucketEnt>& buckets,
                         optional_yield y,
                         const DoutPrefixProvider *dpp);

  int read_bucket_stats(const rgw_bucket& bucket,
                        RGWBucketEnt *result,
                        optional_yield y,
                        const DoutPrefixProvider *dpp);

  /* quota related */
  int sync_owner_stats(const DoutPrefixProvider *dpp,
                       librados::Rados& rados,
                       const rgw_owner& owner,
                       const RGWBucketInfo& bucket_info,
                       optional_yield y,
                       RGWBucketEnt* pent);

  /* bucket sync */
  int get_sync_policy_handler(std::optional<rgw_zone_id> zone,
                              std::optional<rgw_bucket> bucket,
			      RGWBucketSyncPolicyHandlerRef *phandler,
			      optional_yield y,
                              const DoutPrefixProvider *dpp);
  int bucket_exports_data(const rgw_bucket& bucket,
                          optional_yield y,
                          const DoutPrefixProvider *dpp);
  int bucket_imports_data(const rgw_bucket& bucket,
                          optional_yield y,
                          const DoutPrefixProvider *dpp);

private:
  int convert_old_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                              const rgw_bucket& bucket,
                              optional_yield y,
                              const DoutPrefixProvider *dpp);

  int do_store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                    const rgw_bucket& bucket,
                                    RGWBucketInfo& info,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp,
                                    const BucketInstance::PutParams& params);

  int do_store_linked_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                  RGWBucketInfo& info,
                                  RGWBucketInfo *orig_info,
                                  bool exclusive, real_time mtime,
                                  obj_version *pep_objv,
                                  std::map<std::string, bufferlist> *pattrs,
                                  bool create_entry_point,
				  optional_yield,
                                  const DoutPrefixProvider *dpp);

  int do_link_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                     librados::Rados& rados,
                     const rgw_owner& owner,
                     const rgw_bucket& bucket,
                     ceph::real_time creation_time,
                     bool update_entrypoint,
                     rgw_ep_info *pinfo,
		     optional_yield y,
                     const DoutPrefixProvider *dpp);

  int do_unlink_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                       librados::Rados& rados,
                       const rgw_owner& owner,
                       const rgw_bucket& bucket,
                       bool update_entrypoint,
		       optional_yield y,
                       const DoutPrefixProvider *dpp);

};

bool rgw_find_bucket_by_id(const DoutPrefixProvider *dpp, CephContext *cct, rgw::sal::Driver* driver, const std::string& marker,
                           const std::string& bucket_id, rgw_bucket* bucket_out);
