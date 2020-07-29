// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGWRADOS_H
#define CEPH_RGWRADOS_H

#include <functional>
#include <boost/container/flat_map.hpp>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include "common/ceph_time.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/timeindex/cls_timeindex_types.h"
#include "cls/otp/cls_otp_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_period_puller.h"
#include "rgw_obj_manifest.h"
#include "rgw_sync_module.h"
#include "rgw_trim_bilog.h"
#include "rgw_service.h"

#include "services/svc_rados.h"
#include "services/svc_bi_rados.h"

class RGWWatcher;
class SafeTimer;
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

class RGWSysObjectCtx;

/* flags for put_obj_meta() */
#define PUT_OBJ_CREATE      0x01
#define PUT_OBJ_EXCL        0x02
#define PUT_OBJ_CREATE_EXCL (PUT_OBJ_CREATE | PUT_OBJ_EXCL)

#define RGW_OBJ_NS_MULTIPART "multipart"
#define RGW_OBJ_NS_SHADOW    "shadow"

static inline void prepend_bucket_marker(const rgw_bucket& bucket, const string& orig_oid, string& oid)
{
  if (bucket.marker.empty() || orig_oid.empty()) {
    oid = orig_oid;
  } else {
    oid = bucket.marker;
    oid.append("_");
    oid.append(orig_oid);
  }
}

static inline void get_obj_bucket_and_oid_loc(const rgw_obj& obj, string& oid, string& locator)
{
  const rgw_bucket& bucket = obj.bucket;
  prepend_bucket_marker(bucket, obj.get_oid(), oid);
  const string& loc = obj.key.get_loc();
  if (!loc.empty()) {
    prepend_bucket_marker(bucket, loc, locator);
  } else {
    locator.clear();
  }
}

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy);

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
  static void generate_test_instances(list<RGWOLHInfo*>& o);
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
};
WRITE_CLASS_ENCODER(RGWOLHPendingInfo)

struct RGWUsageBatch {
  map<ceph::real_time, rgw_usage_log_entry> m;

  void insert(ceph::real_time& t, rgw_usage_log_entry& entry, bool *account) {
    bool exists = m.find(t) != m.end();
    *account = !exists;
    m[t].aggregate(entry);
  }
};

struct RGWUsageIter {
  string read_iter;
  uint32_t index;

  RGWUsageIter() : index(0) {}
};

class RGWGetDataCB {
public:
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) = 0;
  RGWGetDataCB() {}
  virtual ~RGWGetDataCB() {}
};

struct RGWCloneRangeInfo {
  rgw_obj src;
  off_t src_ofs;
  off_t dst_ofs;
  uint64_t len;
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
  string write_tag;
  bool fake_tag{false};
  std::optional<RGWObjManifest> manifest;
  string shadow_obj;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  bool keep_tail{false};
  bool is_olh{false};
  bufferlist olh_tag;
  uint64_t pg_ver{false};
  uint32_t zone_short_id{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;

  RGWObjState();
  RGWObjState(const RGWObjState& rhs);
  ~RGWObjState();

  bool get_attr(string name, bufferlist& dest) {
    map<string, bufferlist>::iterator iter = attrset.find(name);
    if (iter != attrset.end()) {
      dest = iter->second;
      return true;
    }
    return false;
  }
};

class RGWFetchObjFilter {
public:
  virtual ~RGWFetchObjFilter() {}

  virtual int filter(CephContext *cct,
                     const rgw_obj_key& source_key,
                     const RGWBucketInfo& dest_bucket_info,
                     std::optional<rgw_placement_rule> dest_placement_rule,
                     const map<string, bufferlist>& obj_attrs,
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
             const map<string, bufferlist>& obj_attrs,
             std::optional<rgw_user> *poverride_owner,
             const rgw_placement_rule **prule) override;
};

class RGWObjectCtx {
  rgw::sal::RGWRadosStore *store;
  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWObjectCtx");
  void *s{nullptr};

  std::map<rgw_obj, RGWObjState> objs_state;
public:
  explicit RGWObjectCtx(rgw::sal::RGWRadosStore *_store) : store(_store) {}
  explicit RGWObjectCtx(rgw::sal::RGWRadosStore *_store, void *_s) : store(_store), s(_s) {}

  void *get_private() {
    return s;
  }

  rgw::sal::RGWRadosStore *get_store() {
    return store;
  }

  RGWObjState *get_state(const rgw_obj& obj);

  void set_atomic(rgw_obj& obj);
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

  map<string, bufferlist> attrset;
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
  string tenant;
  string bucket_name;
  string bucket_id;
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
  static void generate_test_instances(list<objexp_hint_entry*>& o);
};
WRITE_CLASS_ENCODER(objexp_hint_entry)

class RGWDataChangesLog;
class RGWMetaSyncStatusManager;
class RGWDataSyncStatusManager;
class RGWCoroutinesManagerRegistry;

class RGWGetBucketStats_CB : public RefCountedObject {
protected:
  rgw_bucket bucket;
  map<RGWObjCategory, RGWStorageStats> *stats;
public:
  explicit RGWGetBucketStats_CB(const rgw_bucket& _bucket) : bucket(_bucket), stats(NULL) {}
  ~RGWGetBucketStats_CB() override {}
  virtual void handle_response(int r) = 0;
  virtual void set_response(map<RGWObjCategory, RGWStorageStats> *_stats) {
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

class RGWGetDirHeader_CB;
class RGWGetUserHeader_CB;
namespace rgw { namespace sal { class RGWRadosStore; } }

class RGWAsyncRadosProcessor;

template <class T>
class RGWChainedCacheImpl;

struct bucket_info_entry {
  RGWBucketInfo info;
  real_time mtime;
  map<string, bufferlist> attrs;
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
  friend class RGWLC;
  friend class RGWObjectExpirer;
  friend class RGWMetaSyncProcessorThread;
  friend class RGWDataSyncProcessorThread;
  friend class RGWReshard;
  friend class RGWBucketReshard;
  friend class RGWBucketReshardLock;
  friend class BucketIndexLockGuard;
  friend class RGWCompleteMultipart;

  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx();
  int open_gc_pool_ctx();
  int open_lc_pool_ctx();
  int open_objexp_pool_ctx();
  int open_reshard_pool_ctx();

  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx&  io_ctx,
		    bool mostly_omap);

  std::atomic<int64_t> max_req_id = { 0 };
  ceph::mutex lock = ceph::make_mutex("rados_timer_lock");
  SafeTimer *timer;

  rgw::sal::RGWRadosStore *store;
  RGWGC *gc;
  RGWLC *lc;
  RGWObjectExpirer *obj_expirer;
  bool use_gc_thread;
  bool use_lc_thread;
  bool quota_threads;
  bool run_sync_thread;
  bool run_reshard_thread;

  RGWMetaNotifier *meta_notifier;
  RGWDataNotifier *data_notifier;
  RGWMetaSyncProcessorThread *meta_sync_processor_thread;
  RGWSyncTraceManager *sync_tracer = nullptr;
  map<rgw_zone_id, RGWDataSyncProcessorThread *> data_sync_processor_threads;

  boost::optional<rgw::BucketTrimManager> bucket_trim;
  RGWSyncLogTrimThread *sync_log_trimmer{nullptr};

  ceph::mutex meta_sync_thread_lock = ceph::make_mutex("meta_sync_thread_lock");
  ceph::mutex data_sync_thread_lock = ceph::make_mutex("data_sync_thread_lock");

  librados::IoCtx root_pool_ctx;      // .rgw

  double inject_notify_timeout_probability = 0;
  unsigned max_notify_retries = 0;

  friend class RGWWatcher;

  ceph::mutex bucket_id_lock = ceph::make_mutex("rados_bucket_id");

  // This field represents the number of bucket index object shards
  uint32_t bucket_index_max_shards;

  int get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx);
  int get_obj_head_ref(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_rados_ref *ref);
  int get_system_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref);
  uint64_t max_bucket_id;

  int get_olh_target_state(RGWObjectCtx& rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                           RGWObjState *olh_state, RGWObjState **target_state, optional_yield y);
  int get_obj_state_impl(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                         bool follow_olh, optional_yield y, bool assume_noent = false);
  int append_atomic_test(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                         librados::ObjectOperation& op, RGWObjState **state, optional_yield y);
  int append_atomic_test(const RGWObjState* astate, librados::ObjectOperation& op);

  int update_placement_map();
  int store_bucket_info(RGWBucketInfo& info, map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker, bool exclusive);

  void remove_rgw_head_obj(librados::ObjectWriteOperation& op);
  void cls_obj_check_prefix_exist(librados::ObjectOperation& op, const string& prefix, bool fail_if_exist);
  void cls_obj_check_mtime(librados::ObjectOperation& op, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type);
protected:
  CephContext *cct;

  librados::Rados rados;

  using RGWChainedCacheImpl_bucket_info_entry = RGWChainedCacheImpl<bucket_info_entry>;
  RGWChainedCacheImpl_bucket_info_entry *binfo_cache;

  tombstone_cache_t *obj_tombstone_cache;

  librados::IoCtx gc_pool_ctx;        // .rgw.gc
  librados::IoCtx lc_pool_ctx;        // .rgw.lc
  librados::IoCtx objexp_pool_ctx;
  librados::IoCtx reshard_pool_ctx;

  bool pools_initialized;

  RGWQuotaHandler *quota_handler;

  RGWCoroutinesManagerRegistry *cr_registry;

  RGWSyncModuleInstanceRef sync_module;
  bool writeable_zone{false};

  RGWIndexCompletionManager *index_completion_manager{nullptr};

  bool use_cache{false};
public:
  RGWRados(): timer(NULL),
               gc(NULL), lc(NULL), obj_expirer(NULL), use_gc_thread(false), use_lc_thread(false), quota_threads(false),
               run_sync_thread(false), run_reshard_thread(false), meta_notifier(NULL),
               data_notifier(NULL), meta_sync_processor_thread(NULL),
               bucket_index_max_shards(0),
               max_bucket_id(0), cct(NULL),
               binfo_cache(NULL), obj_tombstone_cache(nullptr),
               pools_initialized(false),
               quota_handler(NULL),
               cr_registry(NULL),
               pctl(&ctl),
               reshard(NULL) {}

  RGWRados& set_use_cache(bool status) {
    use_cache = status;
    return *this;
  }

  RGWLC *get_lc() {
    return lc;
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

  uint64_t get_new_req_id() {
    return ++max_req_id;
  }

  librados::IoCtx* get_lc_pool_ctx() {
    return &lc_pool_ctx;
  }
  void set_context(CephContext *_cct) {
    cct = _cct;
  }
  void set_store(rgw::sal::RGWRadosStore *_store) {
    store = _store;
  }

  RGWServices svc;
  RGWCtl ctl;

  RGWCtl *pctl{nullptr};

  /**
   * AmazonS3 errors contain a HostId string, but is an opaque base64 blob; we
   * try to be more transparent. This has a wrapper so we can update it when zonegroup/zone are changed.
   */
  string host_id;

  RGWReshard *reshard;
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

  int get_required_alignment(const rgw_pool& pool, uint64_t *alignment);
  void get_max_aligned_size(uint64_t size, uint64_t alignment, uint64_t *max_size);
  int get_max_chunk_size(const rgw_pool& pool, uint64_t *max_chunk_size, uint64_t *palignment = nullptr);
  int get_max_chunk_size(const rgw_placement_rule& placement_rule, const rgw_obj& obj, uint64_t *max_chunk_size, uint64_t *palignment = nullptr);

  uint32_t get_max_bucket_shards() {
    return RGWSI_BucketIndex_RADOS::shards_max();
  }


  int get_raw_obj_ref(const rgw_raw_obj& obj, rgw_rados_ref *ref);

  int list_raw_objects_init(const rgw_pool& pool, const string& marker, RGWListRawObjsCtx *ctx);
  int list_raw_objects_next(const string& prefix_filter, int max,
                            RGWListRawObjsCtx& ctx, list<string>& oids,
                            bool *is_truncated);
  int list_raw_objects(const rgw_pool& pool, const string& prefix_filter, int max,
                       RGWListRawObjsCtx& ctx, list<string>& oids,
                       bool *is_truncated);
  string list_raw_objs_get_cursor(RGWListRawObjsCtx& ctx);

  CephContext *ctx() { return cct; }
  /** do all necessary setup of the storage device */
  int initialize(CephContext *_cct) {
    set_context(_cct);
    return initialize();
  }
  /** Initialize the RADOS instance and prepare to do other ops */
  int init_svc(bool raw);
  int init_ctl();
  int init_rados();
  int init_complete();
  int initialize();
  void finalize();

  int register_to_service_map(const string& daemon_type, const map<string, string>& meta);
  int update_service_map(std::map<std::string, std::string>&& status);

  /// list logs
  int log_list_init(const string& prefix, RGWAccessHandle *handle);
  int log_list_next(RGWAccessHandle handle, string *name);

  /// remove log
  int log_remove(const string& name);

  /// show log
  int log_show_init(const string& name, RGWAccessHandle *handle);
  int log_show_next(RGWAccessHandle handle, rgw_log_entry *entry);

  // log bandwidth info
  int log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info);
  int read_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch,
                 uint32_t max_entries, bool *is_truncated, RGWUsageIter& read_iter, map<rgw_user_bucket,
		 rgw_usage_log_entry>& usage);
  int trim_usage(const rgw_user& user, const string& bucket_name, uint64_t start_epoch, uint64_t end_epoch);
  int clear_usage();

  int create_pool(const rgw_pool& pool);

  void create_bucket_id(string *bucket_id);

  bool get_obj_data_pool(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_pool *pool);
  bool obj_to_raw(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj *raw_obj);

  int create_bucket(const RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& zonegroup_id,
                            const rgw_placement_rule& placement_rule,
                            const string& swift_ver_location,
                            const RGWQuotaInfo * pquota_info,
                            map<std::string,bufferlist>& attrs,
                            RGWBucketInfo& bucket_info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            ceph::real_time creation_time,
                            rgw_bucket *master_bucket,
                            uint32_t *master_num_shards,
                            bool exclusive = true);

  RGWCoroutinesManagerRegistry *get_cr_registry() { return cr_registry; }

  struct BucketShard {
    RGWRados *store;
    rgw_bucket bucket;
    int shard_id;
    RGWSI_RADOS::Obj bucket_obj;

    explicit BucketShard(RGWRados *_store) : store(_store), shard_id(-1) {}
    int init(const rgw_bucket& _bucket, const rgw_obj& obj, RGWBucketInfo* out);
    int init(const rgw_bucket& _bucket, int sid, const rgw::bucket_index_layout_generation& idx_layout, RGWBucketInfo* out);
    int init(const RGWBucketInfo& bucket_info, const rgw_obj& obj);
    int init(const RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout, int sid);
  };

  class Object {
    RGWRados *store;
    RGWBucketInfo bucket_info;
    RGWObjectCtx& ctx;
    rgw_obj obj;

    BucketShard bs;

    RGWObjState *state;

    bool versioning_disabled;

    bool bs_initialized;

  protected:
    int get_state(RGWObjState **pstate, bool follow_olh, optional_yield y, bool assume_noent = false);
    void invalidate_state();

    int prepare_atomic_modification(librados::ObjectWriteOperation& op, bool reset_obj, const string *ptag,
                                    const char *ifmatch, const char *ifnomatch, bool removal_op, bool modify_tail, optional_yield y);
    int complete_atomic_modification();

  public:
    Object(RGWRados *_store, const RGWBucketInfo& _bucket_info, RGWObjectCtx& _ctx, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info),
                                                                                               ctx(_ctx), obj(_obj), bs(store),
                                                                                               state(NULL), versioning_disabled(false),
                                                                                               bs_initialized(false) {}

    RGWRados *get_store() { return store; }
    rgw_obj& get_obj() { return obj; }
    RGWObjectCtx& get_ctx() { return ctx; }
    RGWBucketInfo& get_bucket_info() { return bucket_info; }
    int get_manifest(RGWObjManifest **pmanifest, optional_yield y);

    int get_bucket_shard(BucketShard **pbs) {
      if (!bs_initialized) {
        int r =
	  bs.init(bucket_info.bucket, obj, nullptr /* no RGWBucketInfo */);
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

    struct Read {
      RGWRados::Object *source;

      struct GetObjState {
        map<rgw_pool, librados::IoCtx> io_ctxs;
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
        map<string, bufferlist> *attrs;
        rgw_obj *target_obj;

        Params() : lastmod(nullptr), obj_size(nullptr), attrs(nullptr),
		 target_obj(nullptr) {}
      } params;

      explicit Read(RGWRados::Object *_source) : source(_source) {}

      int prepare(optional_yield y);
      static int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
      int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y);
      int iterate(int64_t ofs, int64_t end, RGWGetDataCB *cb, optional_yield y);
      int get_attr(const char *name, bufferlist& dest, optional_yield y);
    };

    struct Write {
      RGWRados::Object *target;
      
      struct MetaParams {
        ceph::real_time *mtime;
        map<std::string, bufferlist>* rmattrs;
        const bufferlist *data;
        RGWObjManifest *manifest;
        const string *ptag;
        list<rgw_obj_index_key> *remove_objs;
        ceph::real_time set_mtime;
        rgw_user owner;
        RGWObjCategory category;
        int flags;
        const char *if_match;
        const char *if_nomatch;
        std::optional<uint64_t> olh_epoch;
        ceph::real_time delete_at;
        bool canceled;
        const string *user_data;
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
                     map<std::string, bufferlist>& attrs,
                     bool modify_tail, bool assume_noent,
                     void *index_op, optional_yield y);
      int write_meta(uint64_t size, uint64_t accounted_size,
                     map<std::string, bufferlist>& attrs, optional_yield y);
      int write_data(const char *data, uint64_t ofs, uint64_t len, bool exclusive);
      const req_state* get_req_state() {
        return (req_state *)target->get_ctx().get_private();
      }
    };

    struct Delete {
      RGWRados::Object *target;

      struct DeleteParams {
        rgw_user bucket_owner;
        int versioning_status;
        ACLOwner obj_owner; /* needed for creation of deletion marker */
        uint64_t olh_epoch;
        string marker_version_id;
        uint32_t bilog_flags;
        list<rgw_obj_index_key> *remove_objs;
        ceph::real_time expiration_time;
        ceph::real_time unmod_since;
        ceph::real_time mtime; /* for setting delete marker mtime */
        bool high_precision_time;
        rgw_zone_set *zones_trace;
	bool abortmp;
	uint64_t parts_accounted_size;

        DeleteParams() : versioning_status(0), olh_epoch(0), bilog_flags(0), remove_objs(NULL), high_precision_time(false), zones_trace(nullptr), abortmp(false), parts_accounted_size(0) {}
      } params;

      struct DeleteResult {
        bool delete_marker;
        string version_id;

        DeleteResult() : delete_marker(false) {}
      } result;
      
      explicit Delete(RGWRados::Object *_target) : target(_target) {}

      int delete_obj(optional_yield y);
    };

    struct Stat {
      RGWRados::Object *source;

      struct Result {
        rgw_obj obj;
        std::optional<RGWObjManifest> manifest;
        uint64_t size{0};
	struct timespec mtime {};
        map<string, bufferlist> attrs;
      } result;

      struct State {
        librados::IoCtx io_ctx;
        librados::AioCompletion *completion;
        int ret;

        State() : completion(NULL), ret(0) {}
      } state;


      explicit Stat(RGWRados::Object *_source) : source(_source) {}

      int stat_async();
      int wait();
      int stat();
    private:
      int finish();
    };
  };

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

    int update_bucket_id(const string& new_bucket_id);

    int get_shard_id() { return shard_id; }
    void set_shard_id(int id) {
      shard_id = id;
    }

    class UpdateIndex {
      RGWRados::Bucket *target;
      string optag;
      rgw_obj obj;
      uint16_t bilog_flags{0};
      BucketShard bs;
      bool bs_initialized{false};
      bool blind;
      bool prepared{false};
      rgw_zone_set *zones_trace{nullptr};

      int init_bs() {
        int r =
	  bs.init(target->get_bucket(), obj, nullptr /* no RGWBucketInfo */);
        if (r < 0) {
          return r;
        }
        bs_initialized = true;
        return 0;
      }

      void invalidate_bs() {
        bs_initialized = false;
      }

      int guard_reshard(BucketShard **pbs, std::function<int(BucketShard *)> call);
    public:

      UpdateIndex(RGWRados::Bucket *_target, const rgw_obj& _obj) : target(_target), obj(_obj),
                                                              bs(target->get_store()) {
                                                                blind = (target->get_bucket_info().layout.current_index.layout.type == rgw::BucketIndexType::Indexless);
                                                              }

      int get_bucket_shard(BucketShard **pbs) {
        if (!bs_initialized) {
          int r = init_bs();
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

      int prepare(RGWModifyOp, const string *write_tag, optional_yield y);
      int complete(int64_t poolid, uint64_t epoch, uint64_t size,
                   uint64_t accounted_size, ceph::real_time& ut,
                   const string& etag, const string& content_type,
                   const string& storage_class,
                   bufferlist *acl_bl, RGWObjCategory category,
		   list<rgw_obj_index_key> *remove_objs, const string *user_data = nullptr, bool appendable = false);
      int complete_del(int64_t poolid, uint64_t epoch,
                       ceph::real_time& removed_mtime, /* mtime of removed object */
                       list<rgw_obj_index_key> *remove_objs);
      int cancel();

      const string *get_optag() { return &optag; }

      bool is_prepared() { return prepared; }
    }; // class UpdateIndex

    class List {
    protected:
      // absolute maximum number of objects that
      // list_objects_(un)ordered can return
      static constexpr int64_t bucket_list_objects_absolute_max = 25000;

      RGWRados::Bucket *target;
      rgw_obj_key next_marker;

      int list_objects_ordered(int64_t max,
			       vector<rgw_bucket_dir_entry> *result,
			       map<string, bool> *common_prefixes,
			       bool *is_truncated,
                               optional_yield y);
      int list_objects_unordered(int64_t max,
				 vector<rgw_bucket_dir_entry> *result,
				 map<string, bool> *common_prefixes,
				 bool *is_truncated,
                                 optional_yield y);

    public:

      struct Params {
        string prefix;
        string delim;
        rgw_obj_key marker;
        rgw_obj_key end_marker;
        string ns;
        bool enforce_ns;
        RGWAccessListFilter *filter;
        bool list_versions;
	bool allow_unordered;

        Params() :
	  enforce_ns(true),
	  filter(NULL),
	  list_versions(false),
	  allow_unordered(false)
	{}
      } params;

      explicit List(RGWRados::Bucket *_target) : target(_target) {}

      int list_objects(int64_t max,
		       vector<rgw_bucket_dir_entry> *result,
		       map<string, bool> *common_prefixes,
		       bool *is_truncated,
                       optional_yield y) {
	if (params.allow_unordered) {
	  return list_objects_unordered(max, result, common_prefixes,
					is_truncated, y);
	} else {
	  return list_objects_ordered(max, result, common_prefixes,
				      is_truncated, y);
	}
      }
      rgw_obj_key& get_next_marker() {
        return next_marker;
      }
    }; // class List
  }; // class Bucket

  int on_last_entry_in_listing(RGWBucketInfo& bucket_info,
                               const std::string& obj_prefix,
                               const std::string& obj_delim,
                               std::function<int(const rgw_bucket_dir_entry&)> handler);

  bool swift_versioning_enabled(rgw::sal::RGWBucket* bucket) const;

  int swift_versioning_copy(RGWObjectCtx& obj_ctx,              /* in/out */
                            const rgw_user& user,               /* in */
                            rgw::sal::RGWBucket* bucket,        /* in */
                            rgw::sal::RGWObject* obj,           /* in */
                            const DoutPrefixProvider *dpp,      /* in/out */ 
                            optional_yield y);                  /* in */                
  int swift_versioning_restore(RGWObjectCtx& obj_ctx,           /* in/out */
                               const rgw_user& user,            /* in */
                               rgw::sal::RGWBucket* bucket,     /* in */
                               rgw::sal::RGWObject* obj,        /* in */
                               bool& restored,                 /* out */
                               const DoutPrefixProvider *dpp);     /* in/out */                
  int copy_obj_to_remote_dest(RGWObjState *astate,
                              map<string, bufferlist>& src_attrs,
                              RGWRados::Object::Read& read_op,
                              const rgw_user& user_id,
                              rgw::sal::RGWObject* dest_obj,
                              ceph::real_time *mtime);

  enum AttrsMod {
    ATTRSMOD_NONE    = 0,
    ATTRSMOD_REPLACE = 1,
    ATTRSMOD_MERGE   = 2
  };

  int rewrite_obj(RGWBucketInfo& dest_bucket_info, rgw::sal::RGWObject* obj, const DoutPrefixProvider *dpp, optional_yield y);

  int stat_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               req_info *info,
               const rgw_zone_id& source_zone,
               rgw::sal::RGWObject* src_obj,
               const RGWBucketInfo *src_bucket_info,
               real_time *src_mtime,
               uint64_t *psize,
               const real_time *mod_ptr,
               const real_time *unmod_ptr,
               bool high_precision_time,
               const char *if_match,
               const char *if_nomatch,
               map<string, bufferlist> *pattrs,
               map<string, string> *pheaders,
               string *version_id,
               string *ptag,
               string *petag);

  int fetch_remote_obj(RGWObjectCtx& obj_ctx,
                       const rgw_user& user_id,
                       req_info *info,
                       const rgw_zone_id& source_zone,
                       rgw::sal::RGWObject* dest_obj,
                       rgw::sal::RGWObject* src_obj,
		       rgw::sal::RGWBucket* dest_bucket,
		       rgw::sal::RGWBucket* src_bucket,
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
                       map<string, bufferlist>& attrs,
                       RGWObjCategory category,
                       std::optional<uint64_t> olh_epoch,
		       ceph::real_time delete_at,
                       string *ptag,
                       string *petag,
                       void (*progress_cb)(off_t, void *),
                       void *progress_data,
                       const DoutPrefixProvider *dpp,
                       RGWFetchObjFilter *filter,
                       rgw_zone_set *zones_trace= nullptr,
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
               rgw::sal::RGWObject* dest_obj,
               rgw::sal::RGWObject* src_obj,
               rgw::sal::RGWBucket* dest_bucket,
               rgw::sal::RGWBucket* src_bucket,
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
               map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               void (*progress_cb)(off_t, void *),
               void *progress_data,
               const DoutPrefixProvider *dpp,
               optional_yield y);

  int copy_obj_data(RGWObjectCtx& obj_ctx,
               rgw::sal::RGWBucket* bucket,
               const rgw_placement_rule& dest_placement,
	       RGWRados::Object::Read& read_op, off_t end,
               rgw::sal::RGWObject* dest_obj,
	       ceph::real_time *mtime,
	       ceph::real_time set_mtime,
               map<string, bufferlist>& attrs,
               uint64_t olh_epoch,
	       ceph::real_time delete_at,
               string *petag,
               const DoutPrefixProvider *dpp,
               optional_yield y);
  
  int transition_obj(RGWObjectCtx& obj_ctx,
                     rgw::sal::RGWBucket* bucket,
                     rgw::sal::RGWObject& obj,
                     const rgw_placement_rule& placement_rule,
                     const real_time& mtime,
                     uint64_t olh_epoch,
                     const DoutPrefixProvider *dpp,
                     optional_yield y);

  int check_bucket_empty(RGWBucketInfo& bucket_info, optional_yield y);

  /**
   * Delete a bucket.
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  int delete_bucket(RGWBucketInfo& bucket_info, RGWObjVersionTracker& objv_tracker, optional_yield y, bool check_empty = true);

  void wakeup_meta_sync_shards(set<int>& shard_ids);
  void wakeup_data_sync_shards(const rgw_zone_id& source_zone, map<int, set<string> >& shard_ids);

  RGWMetaSyncStatusManager* get_meta_sync_manager();
  RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone);

  int set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner);
  int set_buckets_enabled(std::vector<rgw_bucket>& buckets, bool enabled);
  int bucket_suspended(rgw_bucket& bucket, bool *suspended);

  /** Delete an object.*/
  int delete_obj(RGWObjectCtx& obj_ctx,
		 const RGWBucketInfo& bucket_owner,
		 const rgw_obj& src_obj,
		 int versioning_status,
		 uint16_t bilog_flags = 0,
		 const ceph::real_time& expiration_time = ceph::real_time(),
		 rgw_zone_set *zones_trace = nullptr);

  int delete_raw_obj(const rgw_raw_obj& obj);

  /** Remove an object from the bucket index */
  int delete_obj_index(const rgw_obj& obj, ceph::real_time mtime);

  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  int set_attr(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj, const char *name, bufferlist& bl);

  int set_attrs(void *ctx, const RGWBucketInfo& bucket_info, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs,
                        optional_yield y);

  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state,
                    bool follow_olh, optional_yield y, bool assume_noent = false);
  int get_obj_state(RGWObjectCtx *rctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWObjState **state, optional_yield y) {
    return get_obj_state(rctx, bucket_info, obj, state, true, y);
  }

  using iterate_obj_cb = int (*)(const rgw_raw_obj&, off_t, off_t,
                                 off_t, bool, RGWObjState*, void*);

  int iterate_obj(RGWObjectCtx& ctx, const RGWBucketInfo& bucket_info,
                  const rgw_obj& obj, off_t ofs, off_t end,
                  uint64_t max_chunk_size, iterate_obj_cb cb, void *arg,
                  optional_yield y);

  int get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg);

  void get_obj_aio_completion_cb(librados::completion_t cb, void *arg);

  /**
   * a simple object read without keeping state
   */

  int raw_obj_stat(rgw_raw_obj& obj, uint64_t *psize, ceph::real_time *pmtime, uint64_t *epoch,
                   map<string, bufferlist> *attrs, bufferlist *first_chunk,
                   RGWObjVersionTracker *objv_tracker, optional_yield y);

  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectWriteOperation *op);
  int obj_operate(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::ObjectReadOperation *op);

  int guard_reshard(BucketShard *bs,
		    const rgw_obj& obj_instance,
		    const RGWBucketInfo& bucket_info,
		    std::function<int(BucketShard *)> call);
  int block_while_resharding(RGWRados::BucketShard *bs,
			     string *new_bucket_id,
			     const RGWBucketInfo& bucket_info,
                             optional_yield y);

  void bucket_index_guard_olh_op(RGWObjState& olh_state, librados::ObjectOperation& op);
  int olh_init_modification(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int olh_init_modification_impl(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, string *op_tag);
  int bucket_index_link_olh(const RGWBucketInfo& bucket_info, RGWObjState& olh_state,
                            const rgw_obj& obj_instance, bool delete_marker,
                            const string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch,
                            ceph::real_time unmod_since, bool high_precision_time,
                            rgw_zone_set *zones_trace = nullptr,
                            bool log_data_change = false);
  int bucket_index_unlink_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj_instance, const string& op_tag, const string& olh_tag, uint64_t olh_epoch, rgw_zone_set *zones_trace = nullptr);
  int bucket_index_read_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance, uint64_t ver_marker,
                                map<uint64_t, vector<rgw_bucket_olh_log_entry> > *log, bool *is_truncated);
  int bucket_index_trim_olh_log(const RGWBucketInfo& bucket_info, RGWObjState& obj_state, const rgw_obj& obj_instance, uint64_t ver);
  int bucket_index_clear_olh(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& obj_instance);
  int apply_olh_log(RGWObjectCtx& ctx, RGWObjState& obj_state, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                    bufferlist& obj_tag, map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log,
                    uint64_t *plast_ver, rgw_zone_set *zones_trace = nullptr);
  int update_olh(RGWObjectCtx& obj_ctx, RGWObjState *state, const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_zone_set *zones_trace = nullptr);
  int set_olh(RGWObjectCtx& obj_ctx, const RGWBucketInfo& bucket_info, const rgw_obj& target_obj, bool delete_marker, rgw_bucket_dir_entry_meta *meta,
              uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time,
              optional_yield y, rgw_zone_set *zones_trace = nullptr, bool log_data_change = false);
  int repair_olh(RGWObjState* state, const RGWBucketInfo& bucket_info,
                 const rgw_obj& obj);
  int unlink_obj_instance(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, const rgw_obj& target_obj,
                          uint64_t olh_epoch, optional_yield y, rgw_zone_set *zones_trace = nullptr);

  void check_pending_olh_entries(map<string, bufferlist>& pending_entries, map<string, bufferlist> *rm_pending_entries);
  int remove_olh_pending_entries(const RGWBucketInfo& bucket_info, RGWObjState& state, const rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs);
  int follow_olh(const RGWBucketInfo& bucket_info, RGWObjectCtx& ctx, RGWObjState *state, const rgw_obj& olh_obj, rgw_obj *target);
  int get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, RGWOLHInfo *olh);

  void gen_rand_obj_instance_name(rgw_obj_key *target_key);
  void gen_rand_obj_instance_name(rgw_obj *target);

  int update_containers_stats(map<string, RGWBucketEnt>& m);
  int append_async(rgw_raw_obj& obj, size_t size, bufferlist& bl);

public:
  void set_atomic(void *ctx, rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_atomic(obj);
  }
  void set_prefetch_data(void *ctx, const rgw_obj& obj) {
    RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
    rctx->set_prefetch_data(obj);
  }
  int decode_policy(bufferlist& bl, ACLOwner *owner);
  int get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id, string *bucket_ver, string *master_ver,
      map<RGWObjCategory, RGWStorageStats>& stats, string *max_marker, bool* syncstopped = NULL);
  int get_bucket_stats_async(RGWBucketInfo& bucket_info, int shard_id, RGWGetBucketStats_CB *cb);

  int put_bucket_instance_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, map<string, bufferlist> *pattrs);
  /* xxx dang obj_ctx -> svc */
  int get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const string& meta_key, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs, optional_yield y);
  int get_bucket_instance_info(RGWSysObjectCtx& obj_ctx, const rgw_bucket& bucket, RGWBucketInfo& info, ceph::real_time *pmtime, map<string, bufferlist> *pattrs, optional_yield y);

  static void make_bucket_entry_name(const string& tenant_name, const string& bucket_name, string& bucket_entry);

  int get_bucket_info(RGWServices *svc,
		      const string& tenant_name, const string& bucket_name,
		      RGWBucketInfo& info,
		      ceph::real_time *pmtime, optional_yield y, map<string, bufferlist> *pattrs = NULL);

  // Returns 0 on successful refresh. Returns error code if there was
  // an error or the version stored on the OSD is the same as that
  // presented in the BucketInfo structure.
  //
  int try_refresh_bucket_info(RGWBucketInfo& info,
			      ceph::real_time *pmtime,
			      map<string, bufferlist> *pattrs = nullptr);

  int put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, ceph::real_time mtime, obj_version *pep_objv,
			     map<string, bufferlist> *pattrs, bool create_entry_point);

  int cls_obj_prepare_op(BucketShard& bs, RGWModifyOp op, string& tag, rgw_obj& obj, uint16_t bilog_flags, optional_yield y, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_op(BucketShard& bs, const rgw_obj& obj, RGWModifyOp op, string& tag, int64_t pool, uint64_t epoch,
                          rgw_bucket_dir_entry& ent, RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_add(BucketShard& bs, const rgw_obj& obj, string& tag, int64_t pool, uint64_t epoch, rgw_bucket_dir_entry& ent,
                           RGWObjCategory category, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_del(BucketShard& bs, string& tag, int64_t pool, uint64_t epoch, rgw_obj& obj,
                           ceph::real_time& removed_mtime, list<rgw_obj_index_key> *remove_objs, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj, uint16_t bilog_flags, rgw_zone_set *zones_trace = nullptr);
  int cls_obj_set_bucket_tag_timeout(RGWBucketInfo& bucket_info, uint64_t timeout);

  using ent_map_t =
    boost::container::flat_map<std::string, rgw_bucket_dir_entry>;

  using check_filter_t = bool (*)(const std::string&);

  int cls_bucket_list_ordered(RGWBucketInfo& bucket_info,
			      const int shard_id,
			      const rgw_obj_index_key& start_after,
			      const string& prefix,
			      const string& delimiter,
			      const uint32_t num_entries,
			      const bool list_versions,
			      const uint16_t exp_factor, // 0 means ignore
			      ent_map_t& m,
			      bool* is_truncated,
			      bool* cls_filtered,
			      rgw_obj_index_key *last_entry,
                              optional_yield y,
			      check_filter_t force_check_filter = nullptr);
  int cls_bucket_list_unordered(RGWBucketInfo& bucket_info,
				int shard_id,
				const rgw_obj_index_key& start_after,
				const string& prefix,
				uint32_t num_entries,
				bool list_versions,
				vector<rgw_bucket_dir_entry>& ent_list,
				bool *is_truncated,
				rgw_obj_index_key *last_entry,
                                optional_yield y,
				check_filter_t = nullptr);
  int cls_bucket_head(const RGWBucketInfo& bucket_info, int shard_id, vector<rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids = NULL);
  int cls_bucket_head_async(const RGWBucketInfo& bucket_info, int shard_id, RGWGetDirHeader_CB *ctx, int *num_aio);

  int bi_get_instance(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_dir_entry *dirent);
  int bi_get_olh(const RGWBucketInfo& bucket_info, const rgw_obj& obj, rgw_bucket_olh_entry *olh);
  int bi_get(const RGWBucketInfo& bucket_info, const rgw_obj& obj, BIIndexType index_type, rgw_cls_bi_entry *entry);
  void bi_put(librados::ObjectWriteOperation& op, BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(BucketShard& bs, rgw_cls_bi_entry& entry);
  int bi_put(rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry);
  int bi_list(const RGWBucketInfo& bucket_info, int shard_id, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(BucketShard& bs, const string& filter_obj, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_list(rgw_bucket& bucket, const string& obj_name, const string& marker, uint32_t max,
              list<rgw_cls_bi_entry> *entries, bool *is_truncated);
  int bi_remove(BucketShard& bs);

  int cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info);
  int cls_obj_usage_log_read(const string& oid, const string& user, const string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch, uint32_t max_entries, string& read_iter, map<rgw_user_bucket,
			     rgw_usage_log_entry>& usage, bool *is_truncated);
  int cls_obj_usage_log_trim(const string& oid, const string& user, const string& bucket, uint64_t start_epoch,
                             uint64_t end_epoch);
  int cls_obj_usage_log_clear(string& oid);

  int get_target_shard_id(const rgw::bucket_index_normal_layout& layout, const string& obj_key, int *shard_id);

  int lock_exclusive(const rgw_pool& pool, const string& oid, ceph::timespan& duration, rgw_zone_id& zone_id, string& owner_id);
  int unlock(const rgw_pool& pool, const string& oid, rgw_zone_id& zone_id, string& owner_id);

  void update_gc_chain(rgw_obj& head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain);
  int send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag);
  void delete_objs_inline(cls_rgw_obj_chain& chain, const string& tag);
  int gc_operate(string& oid, librados::ObjectWriteOperation *op);
  int gc_aio_operate(const std::string& oid, librados::AioCompletion *c,
                     librados::ObjectWriteOperation *op);
  int gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl);

  int list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated, bool& processing_queue);
  int process_gc(bool expired_only);
  bool process_expire_objects();
  int defer_gc(void *ctx, const RGWBucketInfo& bucket_info, const rgw_obj& obj, optional_yield y);

  int process_lc();
  int list_lc_progress(string& marker, uint32_t max_entries,
		       vector<cls_rgw_lc_entry>& progress_map, int& index);

  int bucket_check_index(RGWBucketInfo& bucket_info,
                         map<RGWObjCategory, RGWStorageStats> *existing_stats,
                         map<RGWObjCategory, RGWStorageStats> *calculated_stats);
  int bucket_rebuild_index(RGWBucketInfo& bucket_info);
  int bucket_set_reshard(const RGWBucketInfo& bucket_info, const cls_rgw_bucket_instance_entry& entry);
  int remove_objs_from_index(RGWBucketInfo& bucket_info, list<rgw_obj_index_key>& oid_list);
  int move_rados_obj(librados::IoCtx& src_ioctx,
		     const string& src_oid, const string& src_locator,
	             librados::IoCtx& dst_ioctx,
		     const string& dst_oid, const string& dst_locator);
  int fix_head_obj_locator(const RGWBucketInfo& bucket_info, bool copy_obj, bool remove_bad, rgw_obj_key& key);
  int fix_tail_obj_locator(const RGWBucketInfo& bucket_info, rgw_obj_key& key, bool fix, bool *need_fix, optional_yield y);

  int check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                  RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, bool check_size_only = false);

  int check_bucket_shards(const RGWBucketInfo& bucket_info, const rgw_bucket& bucket,
			  uint64_t num_objs);

  int add_bucket_to_reshard(const RGWBucketInfo& bucket_info, uint32_t new_num_shards);

  uint64_t instance_id();

  librados::Rados* get_rados_handle();

  int delete_raw_obj_aio(const rgw_raw_obj& obj, list<librados::AioCompletion *>& handles);
  int delete_obj_aio(const rgw_obj& obj, RGWBucketInfo& info, RGWObjState *astate,
                     list<librados::AioCompletion *>& handles, bool keep_index_consistent,
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
  int check_disk_state(librados::IoCtx io_ctx,
                       const RGWBucketInfo& bucket_info,
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
  int pool_iterate_begin(const rgw_pool& pool, RGWPoolIterCtx& ctx);

  /**
   * Init pool iteration
   * pool: pool to use
   * cursor: position to start iteration
   * ctx: context object to use for the iteration
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate_begin(const rgw_pool& pool, const string& cursor, RGWPoolIterCtx& ctx);

  /**
   * Get pool iteration position
   * ctx: context object to use for the iteration
   * Returns: string representation of position
   */
  string pool_iterate_get_cursor(RGWPoolIterCtx& ctx);

  /**
   * Iterate over pool return object names, use optional filter
   * ctx: iteration context, initialized with pool_iterate_begin()
   * num: max number of objects to return
   * objs: a vector that the results will append into
   * is_truncated: if not NULL, will hold true iff iteration is complete
   * filter: if not NULL, will be used to filter returned objects
   * Returns: 0 on success, -ERR# otherwise.
   */
  int pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                   bool *is_truncated, RGWAccessListFilter *filter);

  uint64_t next_bucket_id();

  /**
   * This is broken out to facilitate unit testing.
   */
  static uint32_t calc_ordered_bucket_list_per_shard(uint32_t num_entries,
						     uint32_t num_shards);
};

#endif
