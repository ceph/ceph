// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <iostream>
#include <functional>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "include/random.h"
#include "common/RefCountedObj.h"
#include "common/ceph_time.h"
#include "common/Timer.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/timeindex/cls_timeindex_types.h"
#include "cls/otp/cls_otp_types.h"
#include "rgw_quota.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_period_puller.h"
#include "rgw_obj_manifest.h"
#include "rgw_sync_module.h"
#include "rgw_trim_bilog.h"
#include "rgw_service.h"
#include "rgw_sal.h"
#include "rgw_aio.h"
#include "rgw_d3n_cacherequest.h"

#include "services/svc_bi_rados.h"
#include "common/Throttle.h"
#include "common/ceph_mutex.h"
#include "rgw_cache.h"
#include "rgw_sal_fwd.h"
#include "rgw_pubsub.h"
#include "rgw_tools.h"

struct D3nDataCache;

class RGWWatcher;
class ACLOwner;
class RGWGC;
class RGWMetaNotifier;
class RGWDataNotifier;
class RGWLC;
class RGWObjectExpirer;
class RGWMetaSyncProcessorThread;
class RGWDataSyncProcessorThread;
class RGWSyncLogTrimThread;
class RGWSyncTraceManager;
struct RGWZoneGroup;
struct RGWZoneParams;
class RGWReshard;
class RGWReshardWait;
namespace rgw { class SiteConfig; }

struct get_obj_data;

/* flags for put_obj_meta() */
#define PUT_OBJ_CREATE      0x01
#define PUT_OBJ_EXCL        0x02
#define PUT_OBJ_CREATE_EXCL (PUT_OBJ_CREATE | PUT_OBJ_EXCL)

static inline void prepend_bucket_marker(const rgw_bucket& bucket, const std::string& orig_oid, std::string& oid)
{
  if (bucket.marker.empty() || orig_oid.empty()) {
    oid = orig_oid;
  } else {
    oid = bucket.marker;
    oid.append("_");
    oid.append(orig_oid);
  }
}

static inline void get_obj_bucket_and_oid_loc(const rgw_obj& obj, std::string& oid, std::string& locator)
{
  const rgw_bucket& bucket = obj.bucket;
  prepend_bucket_marker(bucket, obj.get_oid(), oid);
  const std::string& loc = obj.key.get_loc();
  if (!loc.empty()) {
    prepend_bucket_marker(bucket, loc, locator);
  } else {
    locator.clear();
  }
}

struct RGWOLHInfo {
  rgw_obj target;
  bool removed;

  RGWOLHInfo() : removed(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(target, bl);
    encode(removed, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(target, bl);
     decode(removed, bl);
     DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<RGWOLHInfo*>& o);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(RGWOLHInfo)

struct RGWOLHPendingInfo {
  ceph::real_time time;

  RGWOLHPendingInfo() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(time, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWOLHPendingInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWOLHPendingInfo)

struct RGWUsageBatch {
  std::map<ceph::real_time, rgw_usage_log_entry> m;

  void insert(ceph::real_time& t, rgw_usage_log_entry& entry, bool *account) {
    bool exists = m.find(t) != m.end();
    *account = !exists;
    m[t].aggregate(entry);
  }
};

struct RGWCloneRangeInfo {
  rgw_obj src;
  off_t src_ofs;
  off_t dst_ofs;
  uint64_t len;
};

class RGWFetchObjFilter {
public:
  virtual ~RGWFetchObjFilter() {}

  virtual int filter(CephContext *cct,
                     const rgw_obj_key& source_key,
                     const RGWBucketInfo& dest_bucket_info,
                     std::optional<rgw_placement_rule> dest_placement_rule,
                     const std::map<std::string, bufferlist>& obj_attrs,
                     std::optional<rgw_user> *poverride_owner,
                     const rgw_placement_rule **prule) = 0;
};

class RGWFetchObjFilter_Default : public RGWFetchObjFilter {
protected:
  rgw_placement_rule dest_rule;
public:
  RGWFetchObjFilter_Default() {}

  int filter(CephContext *cct,
             const rgw_obj_key& source_key,
             const RGWBucketInfo& dest_bucket_info,
             std::optional<rgw_placement_rule> dest_placement_rule,
             const std::map<std::string, bufferlist>& obj_attrs,
             std::optional<rgw_user> *poverride_owner,
             const rgw_placement_rule **prule) override;
};

struct RGWObjStateManifest {
  RGWObjState state;
  std::optional<RGWObjManifest> manifest;
};

class RGWObjectCtx {
  rgw::sal::Driver* driver;
  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWObjectCtx");

  std::map<rgw_obj, RGWObjStateManifest> objs_state;
public:
  explicit RGWObjectCtx(rgw::sal::Driver* _driver) : driver(_driver) {}
  RGWObjectCtx(RGWObjectCtx& _o) {
    std::unique_lock wl{lock};
    this->driver = _o.driver;
    this->objs_state = _o.objs_state;
  }

  rgw::sal::Driver* get_driver() {
    return driver;
  }

  RGWObjStateManifest *get_state(const rgw_obj& obj);

  void set_compressed(const rgw_obj& obj);
  void set_atomic(const rgw_obj& obj);
  void set_prefetch_data(const rgw_obj& obj);
  void invalidate(const rgw_obj& obj);
};


struct RGWRawObjState {
  rgw_raw_obj obj;
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0};
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  uint64_t pg_ver{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  std::map<std::string, bufferlist> attrset;
  RGWRawObjState() {}
  RGWRawObjState(const RGWRawObjState& rhs) : obj (rhs.obj) {
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    pg_ver = rhs.pg_ver;
    objv_tracker = rhs.objv_tracker;
  }
};

struct RGWPoolIterCtx {
  librados::IoCtx io_ctx;
  librados::NObjectIterator iter;
};

struct RGWListRawObjsCtx {
  bool initialized;
  RGWPoolIterCtx iter_ctx;

  RGWListRawObjsCtx() : initialized(false) {}
};

struct objexp_hint_entry {
  std::string tenant;
  std::string bucket_name;
  std::string bucket_id;
  rgw_obj_key obj_key;
  ceph::real_time exp_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket_name, bl);
    encode(bucket_id, bl);
    encode(obj_key, bl);
    encode(exp_time, bl);
    encode(tenant, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    // XXX Do we want DECODE_START_LEGACY_COMPAT_LEN(2, 1, 1, bl); ?
    DECODE_START(2, bl);
    decode(bucket_name, bl);
    decode(bucket_id, bl);
    decode(obj_key, bl);
    decode(exp_time, bl);
    if (struct_v >= 2) {
      decode(tenant, bl);
    } else {
      tenant.clear();
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<objexp_hint_entry*>& o);
};
WRITE_CLASS_ENCODER(objexp_hint_entry)

class RGWMetaSyncStatusManager;
class RGWDataSyncStatusManager;
class RGWCoroutinesManagerRegistry;

class RGWGetDirHeader_CB;
class RGWGetUserHeader_CB;
namespace rgw { namespace sal {
  class RadosStore;
  class MPRadosSerializer;
  class LCRadosSerializer;
} }

class RGWAsyncRadosProcessor;

template <class T>
class RGWChainedCacheImpl;

struct bucket_info_entry {
  RGWBucketInfo info;
  real_time mtime;
  std::map<std::string, bufferlist> attrs;
};

struct pubsub_bucket_topics_entry {
  rgw_pubsub_bucket_topics info;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;
};

struct tombstone_entry;

template <class K, class V>
class lru_map;
using tombstone_cache_t = lru_map<rgw_obj, tombstone_entry>;

class RGWIndexCompletionManager;

class RGWRados
{
  friend class RGWGC;
  friend class RGWMetaNotifier;
  friend class RGWDataNotifier;
  friend class RGWObjectExpirer;
  friend class RGWMetaSyncProcessorThread;
  friend class RGWDataSyncProcessorThread;
  friend class RGWReshard;
  friend class RGWBucketReshard;
  friend class RGWBucketReshardLock;
  friend class BucketIndexLockGuard;
  friend class rgw::sal::MPRadosSerializer;
  friend class rgw::sal::LCRadosSerializer;
  friend class rgw::sal::RadosStore;

  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx(const DoutPrefixProvider *dpp);
  int open_gc_pool_ctx(const DoutPrefixProvider *dpp);
  int open_lc_pool_ctx(const DoutPrefixProvider *dpp);
  int open_objexp_pool_ctx(const DoutPrefixProvider *dpp);
  int open_reshard_pool_ctx(const DoutPrefixProvider *dpp);
  int open_notif_pool_ctx(const DoutPrefixProvider *dpp);

  int open_pool_ctx(const DoutPrefixProvider *dpp, const rgw_pool& pool, librados::IoCtx&  io_ctx,
		    bool mostly_omap, bool bulk);


  ceph::mutex lock{ceph::make_mutex("rados_timer_lock")};
  SafeTimer* timer{nullptr};

  rgw::sal::RadosStore* driver{nullptr};
  RGWGC* gc{nullptr};
  RGWLC* lc{nullptr};
  RGWObjectExpirer* obj_expirer{nullptr};
  bool use_gc_thread{false};
  bool use_lc_thread{false};
  bool quota_threads{false};
  bool run_sync_thread{false};
  bool run_reshard_thread{false};
  bool run_notification_thread{false};

  RGWMetaNotifier* meta_notifier{nullptr};
  RGWDataNotifier* data_notifier{nullptr};
  RGWMetaSyncProcessorThread* meta_sync_processor_thread{nullptr};
  RGWSyncTraceManager* sync_tracer{nullptr};
  std::map<rgw_zone_id, RGWDataSyncProcessorThread *> data_sync_processor_threads;

  boost::optional<rgw::BucketTrimManager> bucket_trim;
  RGWSyncLogTrimThread* sync_log_trimmer{nullptr};

  ceph::mutex meta_sync_thread_lock{ceph::make_mutex("meta_sync_thread_lock")};
  ceph::mutex data_sync_thread_lock{ceph::make_mutex("data_sync_thread_lock")};

  librados::IoCtx root_pool_ctx;      // .rgw

  ceph::mutex bucket_id_lock{ceph::make_mutex("rados_bucket_id")};

  // This field represents the number of bucket index object shards
  uint32_t bucket_index_max_shards{0};

  std::string get_cluster_fsid(const DoutPrefixProvider *dpp, optional_yield y);

  int get_obj_head_ref(const DoutPrefixProvider *dpp, const rgw_placement_rule& target_placement_rule, const rgw_obj& obj, rgw_rados_ref *ref);
  int get_obj_head_ref(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_rados_ref *ref);
  int get_system_obj_ref(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, rgw_rados_ref *ref);
  uint64_t max_bucket_id{0};

  int clear_olh(const DoutPrefixProvider *dpp,
                RGWObjectCtx& obj_ctx,
                const rgw_obj& obj,
                RGWBucketInfo& bucket_info,
                rgw_rados_ref& ref,
                const std::string& tag,
                const uint64_t ver,
                optional_yield y);

  int get_olh_target_state(const DoutPrefixProvider *dpp, RGWObjectCtx& rctx,
			   RGWBucketInfo& bucket_info, const rgw_obj& obj,
			   RGWObjState *olh_state, RGWObjStateManifest **psm,
			   optional_yield y);
  int get_obj_state_impl(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx,
                         RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         RGWObjStateManifest** psm, bool follow_olh,
                         optional_yield y, bool assume_noent = false);
  int append_atomic_test(const DoutPrefixProvider *dpp, RGWObjectCtx* rctx, RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         librados::ObjectOperation& op, RGWObjState **state,
			 RGWObjManifest** pmanifest, optional_yield y);

  int update_placement_map();

  void remove_rgw_head_obj(librados::ObjectWriteOperation& op);
  void cls_obj_check_prefix_exist(librados::ObjectOperation& op, const std::string& prefix, bool fail_if_exist);
  void cls_obj_check_mtime(librados::ObjectOperation& op, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type);
protected:
  CephContext* cct{nullptr};

  librados::Rados rados;

  using RGWChainedCacheImpl_bucket_info_entry = RGWChainedCacheImpl<bucket_info_entry>;
  RGWChainedCacheImpl_bucket_info_entry* binfo_cache{nullptr};

  tombstone_cache_t* obj_tombstone_cache{nullptr};

  using RGWChainedCacheImpl_bucket_topics_entry = RGWChainedCacheImpl<pubsub_bucket_topics_entry>;
  RGWChainedCacheImpl_bucket_topics_entry* topic_cache{nullptr};

  librados::IoCtx gc_pool_ctx;        // .rgw.gc
  librados::IoCtx lc_pool_ctx;        // .rgw.lc
  librados::IoCtx objexp_pool_ctx;
  librados::IoCtx reshard_pool_ctx;
  librados::IoCtx notif_pool_ctx;     // .rgw.notif

  bool pools_initialized{false};

  RGWQuotaHandler* quota_handler{nullptr};

  RGWCoroutinesManagerRegistry* cr_registry{nullptr};

  RGWSyncModuleInstanceRef sync_module;
  bool writeable_zone{false};

  RGWIndexCompletionManager *index_completion_manager{nullptr};

  bool use_cache{false};
  bool use_gc{true};
  bool use_datacache{false};

  int get_obj_head_ioctx(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx);

public:
  RGWRados() = default;

  RGWRados& set_use_cache(bool status) {
    use_cache = status;
    return *this;
  }

  RGWRados& set_use_gc(bool status) {
    use_gc = status;
    return *this;
  }

  RGWRados& set_use_datacache(bool status) {
    use_datacache = status;
    return *this;
  }

  bool get_use_datacache() {
    return use_datacache;
  }

  RGWLC *get_lc() {
    return lc;
  }

  RGWGC *get_gc() {
    return gc;
  }

  RGWRados& set_run_gc_thread(bool _use_gc_thread) {
    use_gc_thread = _use_gc_thread;
    return *this;
  }

  RGWRados& set_run_lc_thread(bool _use_lc_thread) {
    use_lc_thread = _use_lc_thread;
    return *this;
  }

  RGWRados& set_run_quota_threads(bool _run_quota_threads) {
    quota_threads = _run_quota_threads;
    return *this;
  }

  RGWRados& set_run_sync_thread(bool _run_sync_thread) {
    run_sync_thread = _run_sync_thread;
    return *this;
  }

  RGWRados& set_run_reshard_thread(bool _run_reshard_thread) {
    run_reshard_thread = _run_reshard_thread;
    return *this;
  }
  
  RGWRados& set_run_notification_thread(bool _run_notification_thread) {
    run_notification_thread = _run_notification_thread;
    return *this;
  }

  librados::IoCtx* get_lc_pool_ctx() {
    return &lc_pool_ctx;
  }

  librados::IoCtx& get_notif_pool_ctx() {
    return notif_pool_ctx;
  }
  
  void set_context(CephContext *_cct) {
    cct = _cct;
  }
  void set_store(rgw::sal::RadosStore* _driver) {
    driver = _driver;
  }

  RGWServices svc;
  RGWCtl ctl;

  RGWCtl* const pctl{&ctl};

  /**
   * AmazonS3 errors contain a HostId string, but is an opaque base64 blob; we
   * try to be more transparent. This has a wrapper so we can update it when zonegroup/zone are changed.
   */
  std::string host_id;

  RGWReshard* reshard{nullptr};
  std::shared_ptr<RGWReshardWait> reshard_wait;

  virtual ~RGWRados() = default;

  tombstone_cache_t *get_tombstone_cache() {
    return obj_tombstone_cache;
  }
  const RGWSyncModuleInstanceRef& get_sync_module() {
    return sync_module;
  }
  RGWSyncTraceManager *get_sync_tracer() {
    return sync_tracer;
  }

  int get_required_alignment(const DoutPrefixProvider *dpp, const rgw_pool& pool, uint64_t *alignment);
  void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t *max_size);
  int get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size, const DoutPrefixProvider *dpp, uint64_t *palignment = nullptr);
  int get_max_chunk_size(const rgw_placement_rule& placement_rule, const rgw_obj& obj, uint64_t *max_chunk_size, const DoutPrefixProvider *dpp, uint64_t *palignment = nullptr);

  uint32_t get_max_bucket_shards() {
    return RGWSI_BucketIndex_RADOS::shards_max();
  }


  int get_raw_obj_ref(const DoutPrefixProvider *dpp, rgw_raw_obj obj, rgw_rados_ref *ref);

  int list_raw_objects_init(const DoutPrefixProvider *dpp, const rgw_pool& pool, const std::string& marker, RGWListRawObjsCtx *ctx);
  int list_raw_objects_next(const DoutPrefixProvider *dpp, const std::string& prefix_filter, int max,
                            RGWListRawObjsCtx& ctx, std::list<std::string>& oids,
                            bool *is_truncated);
  int list_raw_objects(const DoutPrefixProvider *dpp, const rgw_pool& pool, const std::string& prefix_filter, int max,
                       RGWListRawObjsCtx& ctx, std::list<std::string>& oids,
                       bool *is_truncated);
  std::string list_raw_objs_get_cursor(RGWListRawObjsCtx& ctx);

  CephContext *ctx() { return cct; }
  /** do all necessary setup of the storage device */
  int init_begin(CephContext *_cct, const DoutPrefixProvider *dpp,
                         const rgw::SiteConfig& site);
  /** Initialize the RADOS instance and prepare to do other ops */
  int init_svc(bool raw, const DoutPrefixProvider *dpp, const rgw::SiteConfig& site);
  virtual int init_rados();
  int init_complete(const DoutPrefixProvider *dpp, optional_yield y);
  void finalize();

  int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type, const std::map<std::string, std::string>& meta);
  int update_service_map(const DoutPrefixProvider *dpp, std::map<std::string, std::string>&& status);

  /// list logs
  int log_list_init(const DoutPrefixProvider *dpp, const std::string& prefix, RGWAccessHandle *handle);
  int log_list_next(RGWAccessHandle handle, std::string *name);

  /// remove log
  int log_remove(const DoutPrefixProvider *dpp, const std::string& name);

  /// show log
  int log_show_init(const DoutPrefixProvider *dpp, const std::string& name, RGWAccessHandle *handle);
  int log_show_next(const DoutPrefixProvider *dpp, RGWAccessHandle handle, rgw_log_entry *entry);

  // log bandwidth info
  int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y);
  int read_usage(const DoutPrefixProvider *dpp, const rgw_user& user, const std::string& bucket_name, uint64_t start_epoch, uint64_t end_epoch,
                 uint32_t max_entries, bool *is_truncated, RGWUsageIter& read_iter, std::map<rgw_user_bucket,
		 rgw_usage_log_entry>& usage);
  int trim_usage(const DoutPrefixProvider *dpp, const rgw_user& user, const std::string& bucket_name, uint64_t start_epoch, uint64_t end_epoch, optional_yield y);
  int clear_usage(const DoutPrefixProvider *dpp, optional_yield y);

  int create_pool(const DoutPrefixProvider *dpp, const rgw_pool& pool);

  void create_bucket_id(std::string *bucket_id);

  bool get_obj_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool);
  bool obj_to_raw(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj);

  int create_bucket(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const rgw_bucket& bucket,
                    const rgw_user& owner,
                    const std::string& zonegroup_id,
                    const rgw_placement_rule& placement_rule,
                    const RGWZonePlacementInfo* zone_placement,
                    const std::map<std::string, bufferlist>& attrs,
                    bool obj_lock_enabled,
                    const std::optional<std::string>& swift_ver_location,
                    const std::optional<RGWQuotaInfo>& quota,
                    std::optional<ceph::real_time> creation_time,
                    obj_version* pep_objv,
                    RGWBucketInfo& info);

  RGWCoroutinesManagerRegistry *get_cr_registry() { return cr_registry; }

  struct BucketShard {
    RGWRados *store;
    rgw_bucket bucket;
    int shard_id;
    rgw_rados_ref bucket_obj;

    explicit BucketShard(RGWRados *_store) : store(_store), shard_id(-1) {}
    int init(const rgw_bucket& _bucket, const rgw_obj& obj,
             RGWBucketInfo* out, const DoutPrefixProvider *dpp, optional_yield y);
    int init(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y);
    int init(const DoutPrefixProvider *dpp,
	     const RGWBucketInfo& bucket_info,
	     const rgw::bucket_index_layout_generation& index, int sid, optional_yield y);

    friend std::ostream& operator<<(std::ostream& out, const BucketShard& bs) {
      out << "BucketShard:{ bucket=" << bs.bucket <<
	", shard_id=" << bs.shard_id <<
	", bucket_obj=" << bs.bucket_obj << "}";
      return out;
    }
  };

  class Object {
    RGWRados *store;
    RGWBucketInfo bucket_info;
    RGWObjectCtx& ctx;
    rgw_obj obj;

    BucketShard bs;

    RGWObjState *state;
    RGWObjManifest *manifest;

    bool versioning_disabled;

    bool bs_initialized;

    const rgw_placement_rule *pmeta_placement_rule;

  protected:
    int get_state(const DoutPrefixProvider *dpp, RGWObjState **pstate, RGWObjManifest **pmanifest, bool follow_olh, optional_yield y, bool assume_noent = false);
    void invalidate_state();

    int prepare_atomic_modification(const DoutPrefixProvider *dpp, librados::ObjectWriteOperation& op, bool reset_obj, const std::string *ptag,
                                    const char *ifmatch, const char *ifnomatch, bool removal_op, bool modify_tail, optional_yield y);
    int complete_atomic_modification(const DoutPrefixProvider *dpp, optional_yield y);

  public:
    Object(RGWRados *_store, const RGWBucketInfo& _bucket_info, RGWObjectCtx& _ctx, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info),
                                                                                               ctx(_ctx), obj(_obj), bs(store),
                                                                                               state(NULL), manifest(nullptr), versioning_disabled(false),
                                                                                               bs_initialized(false),
                                                                                               pmeta_placement_rule(nullptr) {}

    RGWRados *get_store() { return store; }
    rgw_obj& get_obj() { return obj; }
    RGWObjectCtx& get_ctx() { return ctx; }
    RGWBucketInfo& get_bucket_info() { return bucket_info; }
    //const std::string& get_instance() { return obj->get_instance(); }
    //rgw::sal::Object* get_target() { return obj; }
    int get_manifest(const DoutPrefixProvider *dpp, RGWObjManifest **pmanifest, optional_yield y);

    int get_bucket_shard(BucketShard **pbs, const DoutPrefixProvider *dpp, optional_yield y) {
      if (!bs_initialized) {
        int r =
	  bs.init(bucket_info.bucket, obj, nullptr /* no RGWBucketInfo */, dpp, y);
        if (r < 0) {
          return r;
        }
        bs_initialized = true;
      }
      *pbs = &bs;
      return 0;
    }

    void set_versioning_disabled(bool status) {
      versioning_disabled = status;
    }

    bool versioning_enabled() {
      return (!versioning_disabled && bucket_info.versioning_enabled());
    }

    void set_meta_placement_rule(const rgw_placement_rule *p) {
        pmeta_placement_rule = p;
    }

    const rgw_placement_rule& get_meta_placement_rule() {
        return pmeta_placement_rule ? *pmeta_placement_rule : bucket_info.placement_rule;
    }

    struct Read {
      RGWRados::Object *source;

      struct GetObjState {
        std::map<rgw_pool, librados::IoCtx> io_ctxs;
        rgw_pool cur_pool;
        librados::IoCtx *cur_ioctx{nullptr};
        rgw_obj obj;
        rgw_raw_obj head_obj;
      } state;

      struct ConditionParams {
        const ceph::real_time *mod_ptr;
        const ceph::real_time *unmod_ptr;
        bool high_precision_time;
        uint32_t mod_zone_id;
        uint64_t mod_pg_ver;
        const char *if_match;
        const char *if_nomatch;

        ConditionParams() :
                 mod_ptr(NULL), unmod_ptr(NULL), high_precision_time(false), mod_zone_id(0), mod_pg_ver(0),
                 if_match(NULL), if_nomatch(NULL) {}
      } conds;

      struct Params {
        ceph::real_time *lastmod;
        uint64_t *obj_size;
        std::map<std::string, bufferlist> *attrs;
        rgw_obj *target_obj;
	uint64_t *epoch;
        int* part_num = nullptr;
        std::optional<int> parts_count;

        Params() : lastmod(nullptr), obj_size(nullptr), attrs(nullptr),
		   target_obj(nullptr), epoch(nullptr)
	{}
      } params;

      explicit Read(RGWRados::Object *_source) : source(_source) {}

      int prepare(optional_yield y, const DoutPrefixProvider *dpp);
      static int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
      int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider *dpp);
      int iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb, optional_yield y);
      int get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest, optional_yield y);
    }; // struct RGWRados::Object::Read

    struct Write {
      RGWRados::Object *target;

      struct MetaParams {
        ceph::real_time *mtime;
        std::map<std::string, bufferlist>* rmattrs;
        const bufferlist *data;
        RGWObjManifest *manifest;
        const std::string *ptag;
        std::list<rgw_obj_index_key> *remove_objs;
        ceph::real_time set_mtime;
        rgw_user owner;
        RGWObjCategory category;
        int flags;
        const char *if_match;
        const char *if_nomatch;
        std::optional<uint64_t> olh_epoch;
        ceph::real_time delete_at;
        bool canceled;
        const std::string *user_data;
        rgw_zone_set *zones_trace;
        bool modify_tail;
        bool completeMultipart;
        bool appendable;

        MetaParams() : mtime(NULL), rmattrs(NULL), data(NULL), manifest(NULL), ptag(NULL),
                 remove_objs(NULL), category(RGWObjCategory::Main), flags(0),
                 if_match(NULL), if_nomatch(NULL), canceled(false), user_data(nullptr), zones_trace(nullptr),
                 modify_tail(false),  completeMultipart(false), appendable(false) {}
      } meta;

      explicit Write(RGWRados::Object *_target) : target(_target) {}

      int _do_write_meta(uint64_t size, uint64_t accounted_size,
                     std::map<std::string, bufferlist>& attrs,
                     bool modify_tail, bool assume_noent,
                     void *index_op, const req_context& rctx,
                     jspan_context& trace,
                     bool log_op = true);
      int write_meta(uint64_t size, uint64_t accounted_size,
                     std::map<std::string, bufferlist>& attrs,
                     const req_context& rctx, jspan_context& trace, bool log_op = true);
      int write_data(const char *data, uint64_t ofs, uint64_t len, bool exclusive);
      const req_state* get_req_state() {
        return nullptr;  /* XXX dang Only used by LTTng, and it handles null anyway */
      }
    }; // struct RGWRados::Object::Write

    struct Delete {
      RGWRados::Object *target;

      struct DeleteParams {
        rgw_user bucket_owner;
        int versioning_status; // versioning flags defined in enum RGWBucketFlags
        ACLOwner obj_owner;    // needed for creation of deletion marker
        uint64_t olh_epoch;
        std::string marker_version_id;
        uint32_t bilog_flags;
        std::list<rgw_obj_index_key> *remove_objs;
        ceph::real_time expiration_time;
        ceph::real_time unmod_since;
        ceph::real_time mtime; /* for setting delete marker mtime */
        bool high_precision_time;
        rgw_zone_set *zones_trace;
        bool null_verid;
	bool abortmp;
	uint64_t parts_accounted_size;

        DeleteParams() : versioning_status(0), olh_epoch(0), bilog_flags(0), remove_objs(NULL), high_precision_time(false), zones_trace(nullptr), null_verid(false), abortmp(false), parts_accounted_size(0) {}
      } params;

      struct DeleteResult {
        bool delete_marker;
        std::string version_id;

        DeleteResult() : delete_marker(false) {}
      } result;

      explicit Delete(RGWRados::Object *_target) : target(_target) {}

      int delete_obj(optional_yield y, const DoutPrefixProvider *dpp, bool log_op = true);
    }; // struct RGWRados::Object::Delete

    struct Stat {
      RGWRados::Object *source;

      struct Result {
        rgw_obj obj;
	std::optional<RGWObjManifest> manifest;
        uint64_t size{0};
	struct timespec mtime {};
        std::map<std::string, bufferlist> attrs;
      } result;

      struct State {
        librados::IoCtx io_ctx;
        librados::AioCompletion *completion;
        int ret;

        State() : completion(NULL), ret(0) {}
      } state;

      explicit Stat(RGWRados::Object *_source) : source(_source) {}

      int stat_async(const DoutPrefixProvider *dpp);
      int wait(const DoutPrefixProvider *dpp);

    private:
      int finish(const DoutPrefixProvider *dpp);
    }; // struct RGWRados::Object::Stat
  }; // class RGWRados::Object

  class Bucket {
    RGWRados *store;
    RGWBucketInfo bucket_info;
    rgw_bucket& bucket;
    int shard_id;

  public:
    Bucket(RGWRados *_store, const RGWBucketInfo& _bucket_info) : store(_store), bucket_info(_bucket_info), bucket(bucket_info.bucket),
                                                            shard_id(RGW_NO_SHARD) {}
    RGWRados *get_store() { return store; }
    rgw_bucket& get_bucket() { return bucket; }
    RGWBucketInfo& get_bucket_info() { return bucket_info; }

    int update_bucket_id(const std::string& new_bucket_id, const DoutPrefixProvider *dpp, optional_yield y);

    int get_shard_id() { return shard_id; }
    void set_shard_id(int id) {
      shard_id = id;
    }

    class UpdateIndex {
      RGWRados::Bucket *target;
      std::string optag;
      rgw_obj obj;
      uint16_t bilog_flags{0};
      BucketShard bs;
      bool bs_initialized{false};
      bool blind;
      bool prepared{false};
      bool null_verid{false};
      rgw_zone_set *zones_trace{nullptr};

      int init_bs(const DoutPrefixProvider *dpp, optional_yield y) {
        int r =
	  bs.init(target->get_bucket(), obj, &target->bucket_info, dpp, y);
        if (r < 0) {
          return r;
        }
        bs_initialized = true;
        return 0;
      }

      void invalidate_bs() {
        bs_initialized = false;
      }

      int guard_reshard(const DoutPrefixProvider *dpp, const rgw_obj& obj_instance, BucketShard **pbs, std::function<int(BucketShard *)> call, optional_yield y);

    public:

      UpdateIndex(RGWRados::Bucket *_target, const rgw_obj& _obj) : target(_target),
								    obj(_obj),
								    bs(target->get_store()) {
	blind = target->get_bucket_info().layout.current_index.layout.type == rgw::BucketIndexType::Indexless;
      }

      int get_bucket_shard(BucketShard **pbs, const DoutPrefixProvider *dpp, optional_yield y) {
        if (!bs_initialized) {
          int r = init_bs(dpp, y);
          if (r < 0) {
            return r;
          }
        }
        *pbs = &bs;
        return 0;
      }

      void set_bilog_flags(uint16_t flags) {
        bilog_flags = flags;
      }

      void set_zones_trace(rgw_zone_set *_zones_trace) {
        zones_trace = _zones_trace;
      }

      void set_null_verid(bool null_version_id) {
        null_verid = null_version_id;
      }

      int prepare(const DoutPrefixProvider *dpp, RGWModifyOp, const std::string *write_tag, optional_yield y, bool log_op = true);
      int complete(const DoutPrefixProvider *dpp, int64_t poolid, uint64_t epoch, uint64_t size,
                   uint64_t accounted_size, const ceph::real_time& ut,
                   const std::string& etag, const std::string& content_type,
                   const std::string& storage_class,
                   bufferlist *acl_bl, RGWObjCategory category,
		   std::list<rgw_obj_index_key> *remove_objs,
		   optional_yield y,
		   const std::string *user_data = nullptr,
		   bool appendable = false,
                   bool log_op = true);
      int complete_del(const DoutPrefixProvider *dpp,
                       int64_t poolid, uint64_t epoch,
                       ceph::real_time& removed_mtime, /* mtime of removed object */
                       std::list<rgw_obj_index_key> *remove_objs,
                       optional_yield y, bool null_verid,
                       bool log_op = true);
      int cancel(const DoutPrefixProvider *dpp,
                 std::list<rgw_obj_index_key> *remove_objs,
                 optional_yield y,
                 bool log_op = true);

      const std::string *get_optag() { return &optag; }

      bool is_prepared() { return prepared; }
    }; // class RGWRados::Bucket::UpdateIndex

    class List {
    protected:
      // absolute maximum number of objects that
      // list_objects_(un)ordered can return
      static constexpr int64_t bucket_list_objects_absolute_max = 25000;

      RGWRados::Bucket *target;
      rgw_obj_key next_marker;

      int list_objects_ordered(const DoutPrefixProvider *dpp,
                               int64_t max,
			       std::vector<rgw_bucket_dir_entry> *result,
			       std::map<std::string, bool> *common_prefixes,
			       bool *is_truncated,
                               optional_yield y);
      int list_objects_unordered(const DoutPrefixProvider *dpp,
                                 int64_t max,
				 std::vector<rgw_bucket_dir_entry> *result,
				 std::map<std::string, bool> *common_prefixes,
				 bool *is_truncated,
                                 optional_yield y);

    public:

      struct Params {
        std::string prefix;
        std::string delim;
        rgw_obj_key marker;
        rgw_obj_key end_marker;
        std::string ns;
        bool enforce_ns;
	rgw::AccessListFilter access_list_filter;
	RGWBucketListNameFilter force_check_filter;
        bool list_versions;
	bool allow_unordered;

        Params() :
	  enforce_ns(true),
	  list_versions(false),
	  allow_unordered(false)
	{}
      } params;

      explicit List(RGWRados::Bucket *_target) : target(_target) {}

      int list_objects(const DoutPrefixProvider *dpp, int64_t max,
		       std::vector<rgw_bucket_dir_entry> *result,
		       std::map<std::string, bool> *common_prefixes,
		       bool *is_truncated,
                       optional_yield y) {
	if (params.allow_unordered) {
	  return list_objects_unordered(dpp, max, result, common_prefixes,
					is_truncated, y);
	} else {
	  return list_objects_ordered(dpp, max, result, common_prefixes,
				      is_truncated, y);
	}
      }
      rgw_obj_key& get_next_marker() {
        return next_marker;
      }
    }; // class RGWRados::Bucket::List
  }; // class RGWRados::Bucket

  int on_last_entry_in_listing(const DoutPrefixProvider *dpp,
                               RGWBucketInfo& bucket_info,
                               const std::string& obj_prefix,
                               const std::string& obj_delim,
                               std::function<int(const rgw_bucket_dir_entry&)> handler, optional_yield y);

  bool swift_versioning_enabled(const RGWBucketInfo& bucket_info) const;

  int swift_versioning_copy(RGWObjectCtx& obj_ctx,              /* in/out */
                            const rgw_user& user,               /* in */
                            RGWBucketInfo& bucket_info,         /* in */
                            const rgw_obj& obj,                 /* in */
                            const DoutPrefixProvider *dpp,      /* in */
                            optional_yield y);                  /* in */
  int swift_versioning_restore(RGWObjectCtx& obj_ctx,           /* in/out */
                               const rgw_user& user,            /* in */
                               RGWBucketInfo& bucket_info,      /* in */
                               rgw_obj& obj,                    /* in/out */
                               bool& restored,                  /* out */
                               const DoutPrefixProvider *dpp, optional_yield y);  /* in */
  int copy_obj_to_remote_dest(const DoutPrefixProvider *dpp,
                              RGWObjState *astate,
                              std::map<std::string, bufferlist>& src_attrs,
                              RGWRados::Object::Read& read_op,
                              const rgw_user& user_id,
                              const rgw_obj& dest_obj,
                              ceph::real_time *mtime, optional_yield y);

  enum AttrsMod {
    ATTRSMOD_NONE    = 0,
    ATTRSMOD_REPLACE = 1,
    ATTRSMOD_MERGE   = 2
  };

  D3nDataCache* d3n_data_cache{nullptr};

  int rewrite_obj(RGWBucketInfo& dest_bucket_info, const rgw_obj& obj, const DoutPrefixProvider *dpp, optional_yield y);
  int reindex_obj(rgw::sal::Driver* driver,
		  RGWBucketInfo& dest_bucket_info,
		  const rgw_obj& obj,
		  const DoutPrefixProvider* dpp,
		  optional_yield y);

  int stat_remote_obj(const DoutPrefixProvider *dpp,
               RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const rgw_zone_id& source_zone,
               const rgw_obj& src_obj,
               const RGWBucketInfo *src_bucket_info,
               real_time *src_mtime,
               uint64_t *psize,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               std::map<std::string, bufferlist> *pattrs,
               std::map<std::string, std::string> *pheaders,
               std::string *version_id,
               std::string *ptag,
               std::string *petag, optional_yield y);

  int fetch_remote_obj(RGWObjectCtx& obj_ctx,
                       const rgw_user& user_id,
                       req_info *info,
                       const rgw_zone_id& source_zone,
                       const rgw_obj& dest_obj,
                       const rgw_obj& src_obj,
                       RGWBucketInfo& dest_bucket_info,
                       RGWBucketInfo *src_bucket_info,
		       std::optional<rgw_placement_rule> dest_placement,
                       ceph::real_time *src_mtime,
                       ceph::real_time *mtime,
                       const ceph::real_time *mod_ptr,
                       const ceph::real_time *unmod_ptr,
                       bool high_precision_time,
                       const char *if_match,
                       const char *if_nomatch,
                       AttrsMod attrs_mod,
                       bool copy_if_newer,
                       rgw::sal::Attrs& attrs,
                       RGWObjCategory category,
                       std::optional<uint64_t> olh_epoch,
		       ceph::real_time delete_at,
                       std::string *ptag,
                       std::string *petag,
                       void (*progress_cb)(off_t, void *),
                       void *progress_data,
                       const req_context& rctx,
                       RGWFetchObjFilter *filter,
                       bool stat_follow_olh,
                       const rgw_obj& stat_dest_obj,
                       const rgw_zone_set_entry& source_trace_entry,
                       rgw_zone_set *zones_trace = nullptr,
                       std::optional<uint64_t>* bytes_transferred = 0);
  /**
   * Copy an object.
   * dest_obj: the object to copy into
   * src_obj: the object to copy from
   * attrs: usage depends on attrs_mod parameter
   * attrs_mod: the modification mode of the attrs, may have the following values:
   *            ATTRSMOD_NONE - the attributes of the source object will be
   *                            copied without modifications, attrs parameter is ignored;
   *            ATTRSMOD_REPLACE - new object will have the attributes provided by attrs
   *                               parameter, source object attributes are not copied;
   *            ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes
   *                             are overwritten by values contained in attrs parameter.
   * Returns: 0 on success, -ERR# otherwise.
   */
  int copy_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const rgw_zone_id& source_zone,
               const rgw_obj& dest_obj,
               const rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               const rgw_placement_rule& dest_placement,
               ceph::real_time *src_mtime,
               ceph::real_time *mtime,
               const ceph::real_time *mod_ptr,
               const ceph::real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               std::map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               std::string *version_id,
               std::string *ptag,
               std::string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data,
               const DoutPrefixProvider *dpp,
               optional_yield y,
               jspan_context& trace);

  int copy_obj_data(RGWObjectCtx& obj_ctx,
               RGWBucketInfo& dest_bucket_info,
               const rgw_placement_rule& dest_placement,
	       RGWRados::Object::Read& read_op, off_t end,
               const rgw_obj& dest_obj,
	       ceph::real_time *mtime,
	       ceph::real_time set_mtime,
               std::map<std::string, bufferlist>& attrs,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               std::string *petag,
               const DoutPrefixProvider *dpp,
               optional_yield y,
               bool log_op = true);

  int transition_obj(RGWObjectCtx& obj_ctx,
                     RGWBucketInfo& bucket_info,
                     const rgw_obj& obj,
                     const rgw_placement_rule& placement_rule,
                     const real_time& mtime,
                     uint64_t olh_epoch,
                     const DoutPrefixProvider *dpp,
                     optional_yield y,
                     bool log_op = true);

  int check_bucket_empty(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, optional_yield y);

  /**
   * Delete a bucket.
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  int delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp, bool check_empty = true);

  void wakeup_meta_sync_shards(std::set<int>& shard_ids);

  void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& entries);

  RGWMetaSyncStatusManager* get_meta_sync_manager();
  RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone);

  int set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner, const DoutPrefixProvider *dpp, optional_yield y);
  int set_buckets_enabled(std::vector<rgw_bucket>& buckets, bool enabled, const DoutPrefixProvider *dpp, optional_yield y);
  int bucket_suspended(const DoutPrefixProvider *dpp, rgw_bucket& bucket, bool *suspended, optional_yield y);

  /** Delete an object.*/
  int delete_obj(const DoutPrefixProvider *dpp,
		 RGWObjectCtx& obj_ctx,
		 const RGWBucketInfo& bucket_info,
		 const rgw_obj& obj,
		 int versioning_status, optional_yield y,  // versioning flags defined in enum RGWBucketFlags
		 bool null_verid,
                 uint16_t bilog_flags = 0,
		 const ceph::real_time& expiration_time = ceph::real_time(),
		 rgw_zone_set *zones_trace = nullptr,
                 bool log_op = true);

  int delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, optional_yield y);

  /** Remove an object from the bucket index */
  int delete_obj_index(const rgw_obj& obj, ceph::real_time mtime,
		       const DoutPrefixProvider *dpp, optional_yield y);

  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  int set_attr(const DoutPrefixProvider *dpp, RGWObjectCtx* ctx, RGWBucketInfo& bucket_info, const rgw_obj& obj, const char *name, bufferlist& bl, optional_yield y);

  int set_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx* ctx, RGWBucketInfo& bucket_info, const rgw_obj& obj,
                        std::map<std::string, bufferlist>& attrs,
                        std::map<std::string, bufferlist>* rmattrs,
                        optional_yield y,
                        ceph::real_time set_mtime = ceph::real_clock::zero());

  int get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx,
                    RGWBucketInfo& bucket_info, const rgw_obj& obj,
                    RGWObjStateManifest** psm, bool follow_olh,
                    optional_yield y, bool assume_noent = false);

  int get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx,
                    RGWBucketInfo& bucket_info, const rgw_obj& obj,
                    RGWObjState** pstate, RGWObjManifest** pmanifest,
                    bool follow_olh, optional_yield y,
                    bool assume_noent = false);

  using iterate_obj_cb = int (*)(const DoutPrefixProvider*, const rgw_raw_obj&, off_t, off_t,
                                 off_t, bool, RGWObjState*, void*);

  int iterate_obj(const DoutPrefixProvider *dpp, RGWObjectCtx& ctx, RGWBucketInfo& bucket_info,
                  const rgw_obj& obj, off_t ofs, off_t end,
                  uint64_t max_chunk_size, iterate_obj_cb cb, void *arg,
                  optional_yield y);

  int append_atomic_test(const DoutPrefixProvider *dpp, const RGWObjState* astate, librados::ObjectOperation& op);

  virtual int get_obj_iterate_cb(const DoutPrefixProvider *dpp,
                         const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg);

  /**
   * a simple object read without keeping state
   */

  int raw_obj_stat(const DoutPrefixProvider *dpp,
                   rgw_raw_obj& obj, uint64_t *psize, ceph::real_time *pmtime, uint64_t *epoch,
                   std::map<std::string, bufferlist> *attrs, bufferlist *first_chunk,
                   RGWObjVersionTracker *objv_tracker, optional_yield y);

  int obj_operate(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectWriteOperation *op, optional_yield y);
  int obj_operate(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectReadOperation *op, optional_yield y);

  int guard_reshard(const DoutPrefixProvider *dpp,
                    BucketShard *bs,
		    const rgw_obj& obj_instance,
		    RGWBucketInfo& bucket_info,
		    std::function<int(BucketShard *)> call, optional_yield y);
  int block_while_resharding(RGWRados::BucketShard *bs,
                             const rgw_obj& obj_instance,
			     RGWBucketInfo& bucket_info,
                             optional_yield y,
                             const DoutPrefixProvider *dpp);

  void bucket_index_guard_olh_op(const DoutPrefixProvider *dpp, RGWObjState& olh_state, librados::ObjectOperation& op);
  void olh_cancel_modification(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, const std::string& op_tag, optional_yield y);
  int olh_init_modification(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, std::string *op_tag, optional_yield y);
  int olh_init_modification_impl(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, std::string *op_tag, optional_yield y);
  int bucket_index_link_olh(const DoutPrefixProvider *dpp,
                            RGWBucketInfo& bucket_info, RGWObjState& olh_state,
                            const rgw_obj& obj_instance, bool delete_marker,
                            const std::string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch,
                            ceph::real_time unmod_since, bool high_precision_time,
			    optional_yield y,
                            rgw_zone_set *zones_trace = nullptr,
                            bool log_data_change = false);
  int bucket_index_unlink_instance(const DoutPrefixProvider *dpp,
                                   RGWBucketInfo& bucket_info,
                                   const rgw_obj& obj_instance,
                                   const std::string& op_tag, const std::string& olh_tag,
                                   uint64_t olh_epoch, optional_yield y,
                                   rgw_zone_set *zones_trace = nullptr,
                                   bool log_op = true);
  int bucket_index_read_olh_log(const DoutPrefixProvider *dpp,
                                RGWBucketInfo& bucket_info, RGWObjState& state,
                                const rgw_obj& obj_instance, uint64_t ver_marker,
                                std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry> > *log, bool *is_truncated, optional_yield y);
  int bucket_index_trim_olh_log(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, RGWObjState& obj_state, const rgw_obj& obj_instance, uint64_t ver, optional_yield y);
  int bucket_index_clear_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const std::string& olh_tag, const rgw_obj& obj_instance, optional_yield y);
  int apply_olh_log(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx, RGWObjState& obj_state, RGWBucketInfo& bucket_info, const rgw_obj& obj,
                    bufferlist& obj_tag, std::map<uint64_t, std::vector<rgw_bucket_olh_log_entry> >& log,
                    uint64_t *plast_ver, optional_yield y, bool null_verid, rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int update_olh(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx, RGWObjState *state, RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y, bool null_verid,
		 rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int clear_olh(const DoutPrefixProvider *dpp,
                RGWObjectCtx& obj_ctx,
                const rgw_obj& obj,
                RGWBucketInfo& bucket_info,
                const std::string& tag,
                const uint64_t ver,
                optional_yield y);
  int set_olh(const DoutPrefixProvider *dpp,
	      RGWObjectCtx& obj_ctx,
	      RGWBucketInfo& bucket_info,
	      const rgw_obj& target_obj,
	      bool delete_marker,
	      rgw_bucket_dir_entry_meta *meta,
              uint64_t olh_epoch,
	      ceph::real_time unmod_since,
	      bool high_precision_time,
              optional_yield y,
              bool null_verid,
	      rgw_zone_set *zones_trace = nullptr,
	      bool log_data_change = false,
	      bool skip_olh_obj_update = false); // can skip the OLH object update if, for example, repairing index
  int repair_olh(const DoutPrefixProvider *dpp, RGWObjState* state, const RGWBucketInfo& bucket_info,
                 const rgw_obj& obj, optional_yield y);
  int unlink_obj_instance(const DoutPrefixProvider *dpp, RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                          uint64_t olh_epoch, optional_yield y, bool null_verid, rgw_zone_set *zones_trace = nullptr, bool log_op = true);

  void check_pending_olh_entries(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>& pending_entries, std::map<std::string, bufferlist> *rm_pending_entries);
  int remove_olh_pending_entries(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, std::map<std::string, bufferlist>& pending_attrs, optional_yield y);
  int follow_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, RGWObjectCtx& ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target, optional_yield y, bool null_verid);
  int get_olh(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh, optional_yield y);

  void gen_rand_obj_instance_name(rgw_obj_key *target_key);
  void gen_rand_obj_instance_name(rgw_obj *target);

  int append_async(const DoutPrefixProvider *dpp, rgw_raw_obj& obj, size_t size, bufferlist& bl);

public:
  void set_atomic(void *ctx, const rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_atomic(obj);
  }
  void set_prefetch_data(void *ctx, const rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_prefetch_data(obj);
  }
  void set_compressed(void *ctx, const rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_compressed(obj);
  }
  int decode_policy(const DoutPrefixProvider *dpp, bufferlist& bl, ACLOwner *owner);
  int get_bucket_stats(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout, int shard_id, std::string *bucket_ver, std::string *master_ver,
      std::map<RGWObjCategory, RGWStorageStats>& stats, std::string *max_marker, bool* syncstopped = NULL);
  int get_bucket_stats_async(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout, int shard_id, boost::intrusive_ptr<rgw::sal::ReadStatsCB> cb);

  int put_bucket_instance_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, const std::map<std::string, bufferlist> *pattrs, const DoutPrefixProvider *dpp, optional_yield y);
  /* xxx dang obj_ctx -> svc */
  int get_bucket_instance_info(const std::string& meta_key, RGWBucketInfo& info, ceph::real_time *pmtime, std::map<std::string, bufferlist> *pattrs, optional_yield y, const DoutPrefixProvider *dpp);
  int get_bucket_instance_info(const rgw_bucket& bucket, RGWBucketInfo& info, ceph::real_time *pmtime, std::map<std::string, bufferlist> *pattrs, optional_yield y, const DoutPrefixProvider *dpp);

  static void make_bucket_entry_name(const std::string& tenant_name, const std::string& bucket_name, std::string& bucket_entry);

  int get_bucket_info(RGWServices *svc,
		      const std::string& tenant_name, const std::string& bucket_name,
		      RGWBucketInfo& info,
		      ceph::real_time *pmtime, optional_yield y,
                      const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *pattrs = NULL);

  RGWChainedCacheImpl_bucket_topics_entry *get_topic_cache() { return topic_cache; }

  // Returns 0 on successful refresh. Returns error code if there was
  // an error or the version stored on the OSD is the same as that
  // presented in the BucketInfo structure.
  //
  int try_refresh_bucket_info(RGWBucketInfo& info,
			      ceph::real_time *pmtime,
                              const DoutPrefixProvider *dpp, optional_yield y,
			      std::map<std::string, bufferlist> *pattrs = nullptr);

  int put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, obj_version *pep_objv,
			     const std::map<std::string, bufferlist> *pattrs, bool create_entry_point,
                             const DoutPrefixProvider *dpp, optional_yield y);

  int cls_obj_prepare_op(const DoutPrefixProvider *dpp, BucketShard& bs, RGWModifyOp op, std::string& tag, rgw_obj& obj,
                         uint16_t bilog_flags, optional_yield y, rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, std::string& tag, int64_t pool, uint64_t epoch,
                          rgw_bucket_dir_entry& ent, RGWObjCategory category, std::list<rgw_obj_index_key> *remove_objs,
                          uint16_t bilog_flags, bool null_verid, rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, std::string& tag, int64_t pool, uint64_t epoch, rgw_bucket_dir_entry& ent,
                           RGWObjCategory category, std::list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags,
                           rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int cls_obj_complete_del(BucketShard& bs, std::string& tag, int64_t pool, uint64_t epoch, rgw_obj& obj,
                           ceph::real_time& removed_mtime, std::list<rgw_obj_index_key> *remove_objs,
                           uint16_t bilog_flags, bool null_verid, rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int cls_obj_complete_cancel(BucketShard& bs, std::string& tag, rgw_obj& obj,
                              std::list<rgw_obj_index_key> *remove_objs,
                              uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr, bool log_op = true);
  int cls_obj_set_bucket_tag_timeout(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, uint64_t timeout);

  using ent_map_t =
    boost::container::flat_map<std::string, rgw_bucket_dir_entry>;

  int cls_bucket_list_ordered(const DoutPrefixProvider *dpp,
                              RGWBucketInfo& bucket_info,
                              const rgw::bucket_index_layout_generation& idx_layout,
                              const int shard_id,
			      const rgw_obj_index_key& start_after,
			      const std::string& prefix,
			      const std::string& delimiter,
			      const uint32_t num_entries,
			      const bool list_versions,
			      const uint16_t exp_factor, // 0 means ignore
			      ent_map_t& m,
			      bool* is_truncated,
			      bool* cls_filtered,
			      rgw_obj_index_key *last_entry,
                              optional_yield y,
			      RGWBucketListNameFilter force_check_filter = {});
  int cls_bucket_list_unordered(const DoutPrefixProvider *dpp,
                                RGWBucketInfo& bucket_info,
                                const rgw::bucket_index_layout_generation& idx_layout,
                                int shard_id,
				const rgw_obj_index_key& start_after,
				const std::string& prefix,
				uint32_t num_entries,
				bool list_versions,
				std::vector<rgw_bucket_dir_entry>& ent_list,
				bool *is_truncated,
				rgw_obj_index_key *last_entry,
                                optional_yield y,
				RGWBucketListNameFilter force_check_filter = {});
  int cls_bucket_head(const DoutPrefixProvider *dpp,
		      const RGWBucketInfo& bucket_info,
		      const rgw::bucket_index_layout_generation& idx_layout,
		      int shard_id, std::vector<rgw_bucket_dir_header>& headers,
		      std::map<int, std::string> *bucket_instance_ids = NULL);
  int cls_bucket_head_async(const DoutPrefixProvider *dpp,
			    const RGWBucketInfo& bucket_info,
			    const rgw::bucket_index_layout_generation& idx_layout,
			    int shard_id, boost::intrusive_ptr<RGWGetDirHeader_CB> cb, int *num_aio);
  int bi_get_instance(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_dir_entry *dirent, optional_yield y);
  int bi_get_olh(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_olh_entry *olh, optional_yield y);
  int bi_get(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, BIIndexType index_type, rgw_cls_bi_entry *entry, optional_yield y);
  void bi_put(librados::ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry, optional_yield y);
  int bi_put(BucketShard& bs, rgw_cls_bi_entry& entry, optional_yield y);
  int bi_put(const DoutPrefixProvider *dpp, rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry, optional_yield y);
  int bi_list(const DoutPrefixProvider *dpp,
	      const RGWBucketInfo& bucket_info,
	      int shard_id,
	      const std::string& filter_obj,
	      const std::string& marker,
	      uint32_t max,
	      std::list<rgw_cls_bi_entry> *entries,
	      bool *is_truncated, optional_yield y);
  int bi_list(BucketShard& bs, const std::string& filter_obj, const std::string& marker, uint32_t max, std::list<rgw_cls_bi_entry> *entries, bool *is_truncated, optional_yield y);
  int bi_list(const DoutPrefixProvider *dpp, rgw_bucket& bucket, const std::string& obj_name, const std::string& marker, uint32_t max,
              std::list<rgw_cls_bi_entry> *entries, bool *is_truncated, optional_yield y);
  int bi_remove(const DoutPrefixProvider *dpp, BucketShard& bs);

  int cls_obj_usage_log_add(const DoutPrefixProvider *dpp, const std::string& oid, rgw_usage_log_info& info, optional_yield y);
  int cls_obj_usage_log_read(const DoutPrefixProvider *dpp, const std::string& oid, const std::string& user, const std::string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch, uint32_t max_entries, std::string& read_iter,
			     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated);
  int cls_obj_usage_log_trim(const DoutPrefixProvider *dpp, const std::string& oid, const std::string& user, const std::string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch, optional_yield y);
  int cls_obj_usage_log_clear(const DoutPrefixProvider *dpp, std::string& oid, optional_yield y);

  int get_target_shard_id(const rgw::bucket_index_normal_layout& layout, const std::string& obj_key, int *shard_id);

  int lock_exclusive(const rgw_pool& pool, const std::string& oid, ceph::timespan& duration, rgw_zone_id& zone_id, std::string& owner_id);
  int unlock(const rgw_pool& pool, const std::string& oid, rgw_zone_id& zone_id, std::string& owner_id);

  void update_gc_chain(const DoutPrefixProvider *dpp, rgw_obj head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain);
  std::tuple<int, std::optional<cls_rgw_obj_chain>> send_chain_to_gc(cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y);
  void delete_objs_inline(const DoutPrefixProvider *dpp, cls_rgw_obj_chain& chain, const std::string& tag);
  int gc_operate(const DoutPrefixProvider *dpp, std::string& oid, librados::ObjectWriteOperation *op, optional_yield y);
  int gc_aio_operate(const std::string& oid, librados::AioCompletion *c,
                     librados::ObjectWriteOperation *op);
  int gc_operate(const DoutPrefixProvider *dpp, std::string& oid, librados::ObjectReadOperation *op, bufferlist *pbl, optional_yield y);

  int list_gc_objs(int *index, std::string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated, bool& processing_queue);
  int process_gc(bool expired_only, optional_yield y);
  bool process_expire_objects(const DoutPrefixProvider *dpp, optional_yield y);
  int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx* ctx, RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y);

  int process_lc(const std::unique_ptr<rgw::sal::Bucket>& optional_bucket);
  int list_lc_progress(std::string& marker, uint32_t max_entries,
		       std::vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>>& progress_map,
		       int& index);

  int bucket_check_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info,
                         std::map<RGWObjCategory, RGWStorageStats> *existing_stats,
                         std::map<RGWObjCategory, RGWStorageStats> *calculated_stats);
  int bucket_rebuild_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info);

  // Search the bucket for encrypted multipart uploads, and increase their mtime
  // slightly to generate a bilog entry to trigger a resync to repair any
  // corrupted replicas. See https://tracker.ceph.com/issues/46062
  int bucket_resync_encrypted_multipart(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        rgw::sal::RadosStore* driver,
                                        RGWBucketInfo& bucket_info,
                                        const std::string& marker,
                                        RGWFormatterFlusher& flusher);

  int bucket_set_reshard(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry);
  int remove_objs_from_index(const DoutPrefixProvider *dpp,
			     RGWBucketInfo& bucket_info,
			     const std::list<rgw_obj_index_key>& oid_list);
  int move_rados_obj(const DoutPrefixProvider *dpp,
                     librados::IoCtx& src_ioctx,
		     const std::string& src_oid, const std::string& src_locator,
	             librados::IoCtx& dst_ioctx,
		     const std::string& dst_oid, const std::string& dst_locator, optional_yield y);
  int fix_head_obj_locator(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key, optional_yield y);
  int fix_tail_obj_locator(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info,
                           rgw_obj_key& key, bool fix, bool *need_fix, optional_yield y);

  int check_quota(const DoutPrefixProvider *dpp, const rgw_user& bucket_owner, rgw_bucket& bucket,
                  RGWQuota& quota, uint64_t obj_size,
		  optional_yield y, bool check_size_only = false);

  int check_bucket_shards(const RGWBucketInfo& bucket_info, uint64_t num_objs,
                          const DoutPrefixProvider *dpp, optional_yield y);

  int add_bucket_to_reshard(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, uint32_t new_num_shards, optional_yield y);

  uint64_t instance_id();

  librados::Rados* get_rados_handle();

  int delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, std::list<librados::AioCompletion *>& handles);
  int delete_obj_aio(const DoutPrefixProvider *dpp, const rgw_obj& obj, RGWBucketInfo& info, RGWObjState *astate,
                     std::list<librados::AioCompletion *>& handles, bool keep_index_consistent,
                     optional_yield y);

 private:
  /**
   * Check the actual on-disk state of the object specified
   * by list_state, and fill in the time and size of object.
   * Then append any changes to suggested_updates for
   * the rgw class' dir_suggest_changes function.
   *
   * Note that this can maul list_state; don't use it afterwards. Also
   * it expects object to already be filled in from list_state; it only
   * sets the size and mtime.
   *
   * Returns 0 on success, -ENOENT if the object doesn't exist on disk,
   * and -errno on other failures. (-ENOENT is not a failure, and it
   * will encode that info as a suggested update.)
   */
  int check_disk_state(const DoutPrefixProvider *dpp,
                       librados::IoCtx io_ctx,
                       RGWBucketInfo& bucket_info,
                       rgw_bucket_dir_entry& list_state,
                       rgw_bucket_dir_entry& object,
                       bufferlist& suggested_updates,
                       optional_yield y);

  /**
   * Init pool iteration
   * pool: pool to use for the ctx initialization
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(const DoutPrefixProvider *dpp, const rgw_pool& pool, RGWPoolIterCtx& ctx);

  /**
   * Init pool iteration
   * pool: pool to use
   * cursor: position to start iteration
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(const DoutPrefixProvider *dpp, const rgw_pool& pool, const std::string& cursor, RGWPoolIterCtx& ctx);

  /**
   * Get pool iteration position
   * ctx: context object to use for the iteration
   * Returns: std::string representation of position
   */
  std::string pool_iterate_get_cursor(RGWPoolIterCtx& ctx);

  /**
   * Iterate over pool return object names, use optional filter
   * ctx: iteration context, initialized with pool_iterate_begin()
   * num: max number of objects to return
   * objs: a vector that the results will append into
   * is_truncated: if not NULL, will hold true iff iteration is complete
   * filter: if not NULL, will be used to filter returned objects
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate(const DoutPrefixProvider *dpp, RGWPoolIterCtx& ctx, uint32_t num,
		   std::vector<rgw_bucket_dir_entry>& objs,
                   bool *is_truncated, const rgw::AccessListFilter& filter);

  uint64_t next_bucket_id();

  /**
   * This is broken out to facilitate unit testing.
   */
  static uint32_t calc_ordered_bucket_list_per_shard(uint32_t num_entries,
						     uint32_t num_shards);
};


struct get_obj_data {
  RGWRados* rgwrados;
  RGWGetDataCB* client_cb = nullptr;
  rgw::Aio* aio;
  uint64_t offset; // next offset to write to client
  rgw::AioResultList completed; // completed read results, sorted by offset
  optional_yield yield;

  get_obj_data(RGWRados* rgwrados, RGWGetDataCB* cb, rgw::Aio* aio,
               uint64_t offset, optional_yield yield)
               : rgwrados(rgwrados), client_cb(cb), aio(aio), offset(offset), yield(yield) {}
  ~get_obj_data() {
    if (rgwrados->get_use_datacache()) {
      const std::lock_guard l(d3n_get_data.d3n_lock);
    }
  }

  D3nGetObjData d3n_get_data;
  std::atomic_bool d3n_bypass_cache_write{false};

  int flush(rgw::AioResultList&& results);

  void cancel() {
    // wait for all completions to drain and ignore the results
    aio->drain();
  }

  int drain() {
    auto c = aio->wait();
    while (!c.empty()) {
      int r = flush(std::move(c));
      if (r < 0) {
        cancel();
        return r;
      }
      c = aio->wait();
    }
    return flush(std::move(c));
  }
};
