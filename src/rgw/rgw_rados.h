#ifndef CEPH_RGWRADOS_H
#define CEPH_RGWRADOS_H

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "rgw_access.h"
#include "rgw_common.h"
#include "rgw_cls_api.h"

class RGWWatcher;
class SafeTimer;
class ACLOwner;

#define DEFAULT_BUCKET_STORE_POOL ".rgw.buckets"

struct RGWObjState {
  bool is_atomic;
  bool has_attrs;
  bool exists;
  uint64_t size;
  time_t mtime;
  bufferlist obj_tag;
  string shadow_obj;

  map<string, bufferlist> attrset;
  RGWObjState() : is_atomic(false), has_attrs(0), exists(false) {}

  bool get_attr(string name, bufferlist& dest) {
    map<string, bufferlist>::iterator iter = attrset.find(name);
    if (iter != attrset.end()) {
      dest = iter->second;
      return true;
    }
    return false;
  }

  void clear() {
    has_attrs = false;
    exists = false;
    size = 0;
    mtime = 0;
    obj_tag.clear();
    shadow_obj.clear();
    attrset.clear();
  }
};

struct RGWRadosCtx {
  map<rgw_obj, RGWObjState> objs_state;
  int (*intent_cb)(void *user_ctx, rgw_obj& obj, RGWIntentEvent intent);
  void *user_ctx;
  RGWObjState *get_state(rgw_obj& obj) {
    return &objs_state[obj];
  }
  void set_atomic(rgw_obj& obj) {
    objs_state[obj].is_atomic = true;
  }
  void set_intent_cb(int (*cb)(void *user_ctx, rgw_obj& obj, RGWIntentEvent intent)) {
    intent_cb = cb;
  }

  int notify_intent(rgw_obj& obj, RGWIntentEvent intent) {
    if (intent_cb) {
      return intent_cb(user_ctx, obj, intent);
    }
    return 0;
  }
};
  
class RGWRados  : public RGWAccess
{
  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx();

  int open_bucket_ctx(rgw_bucket& bucket, librados::IoCtx&  io_ctx);

  struct GetObjState {
    librados::IoCtx io_ctx;
    bool sent_data;

    GetObjState() : sent_data(false) {}
  };

  int set_buckets_auid(vector<rgw_bucket>& buckets, uint64_t auid);

  Mutex lock;
  SafeTimer *timer;

  class C_Tick : public Context {
    RGWRados *rados;
  public:
    C_Tick(RGWRados *_r) : rados(_r) {}
    void finish(int r) {
      rados->tick();
    }
  };


  RGWWatcher *watcher;
  uint64_t watch_handle;
  librados::IoCtx root_pool_ctx;      // .rgw
  librados::IoCtx control_pool_ctx;   // .rgw.control

  int get_obj_state(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx, string& actual_obj, RGWObjState **state);
  int append_atomic_test(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                         string& actual_obj, librados::ObjectOperation& op, RGWObjState **state);
  int prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                         string& actual_obj, librados::ObjectWriteOperation& op, RGWObjState **pstate);
  int prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                         string& actual_obj, librados::ObjectWriteOperation& op, RGWObjState **pstate);

  void atomic_write_finish(RGWObjState *state, int r) {
    if (state && r == -ECANCELED) {
      state->clear();
    }
  }

  int clone_objs_impl(void *ctx, rgw_obj& dst_obj, 
                 vector<RGWCloneRangeInfo>& ranges,
                 map<string, bufferlist> attrs,
                 RGWObjCategory category,
                 time_t *pmtime,
                 bool truncate_dest,
                 bool exclusive,
                 pair<string, bufferlist> *cmp_xattr);
  int delete_obj_impl(void *ctx, std::string& id, rgw_obj& src_obj, bool sync);
public:
  RGWRados() : lock("rados_timer_lock"), timer(NULL), watcher(NULL), watch_handle(0) {}

  void tick();

  /** Initialize the RADOS instance and prepare to do other ops */
  virtual int initialize(CephContext *cct);
  /** set up a bucket listing. id is ignored, handle is filled in. */
  virtual int list_buckets_init(std::string& id, RGWAccessHandle *handle);
  /** 
   * get the next bucket in the listing. id is ignored, obj is filled in,
   * handle is updated.
   */
  virtual int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle);

  /* raw object list interface */
  virtual int list_objects_raw_init(rgw_bucket& bucket, RGWAccessHandle *handle);
  virtual int list_objects_raw_next(RGWObjEnt& obj, RGWAccessHandle *handle);

  /** get listing of the objects in a bucket */
  virtual int list_objects(std::string& id, rgw_bucket& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
		   bool get_content_type, string& ns, bool *is_truncated, RGWAccessListFilter *filter);

  /**
   * create a bucket with name bucket and the given list of attrs
   * returns 0 on success, -ERR# otherwise.
   */
  virtual int create_bucket(std::string& id, rgw_bucket& bucket,
                            map<std::string,bufferlist>& attrs,
                            bool system_bucket, bool exclusive = true,
                            uint64_t auid = 0);
  virtual int select_bucket_placement(std::string& bucket_name, rgw_bucket& bucket);
  virtual int add_bucket_placement(std::string& new_pool);
  virtual int create_pools(std::string& id, vector<string>& names, vector<int>& retcodes, int auid = 0);

  /** Write/overwrite an object to the bucket storage. */
  virtual int put_obj_meta(void *ctx, std::string& id, rgw_obj& obj, uint64_t size, time_t *mtime,
              map<std::string, bufferlist>& attrs, RGWObjCategory category, bool exclusive);
  virtual int put_obj_data(void *ctx, std::string& id, rgw_obj& obj, const char *data,
              off_t ofs, size_t len);
  virtual int aio_put_obj_data(void *ctx, std::string& id, rgw_obj& obj, const char *data,
                               off_t ofs, size_t len, void **handle);
  virtual int aio_wait(void *handle);
  virtual bool aio_completed(void *handle);
  virtual int clone_objs(void *ctx, rgw_obj& dst_obj, 
                         vector<RGWCloneRangeInfo>& ranges,
                         map<string, bufferlist> attrs,
                         RGWObjCategory category,
                         time_t *pmtime, bool truncate_dest, bool exclusive) {
    return clone_objs(ctx, dst_obj, ranges, attrs, category, pmtime, truncate_dest, exclusive, NULL);
  }

  int clone_objs(void *ctx, rgw_obj& dst_obj, 
                 vector<RGWCloneRangeInfo>& ranges,
                 map<string, bufferlist> attrs,
                 RGWObjCategory category,
                 time_t *pmtime,
                 bool truncate_dest,
                 bool exclusive,
                 pair<string, bufferlist> *cmp_xattr);

  int clone_obj_cond(void *ctx, rgw_obj& dst_obj, off_t dst_ofs,
                rgw_obj& src_obj, off_t src_ofs,
                uint64_t size, map<string, bufferlist> attrs,
                RGWObjCategory category,
                time_t *pmtime,
                bool truncate_dest,
                bool exclusive,
                pair<string, bufferlist> *xattr_cond) {
    RGWCloneRangeInfo info;
    vector<RGWCloneRangeInfo> v;
    info.src = src_obj;
    info.src_ofs = src_ofs;
    info.dst_ofs = dst_ofs;
    info.len = size;
    v.push_back(info);
    return clone_objs(ctx, dst_obj, v, attrs, category, pmtime, truncate_dest, exclusive, xattr_cond);
  }

  /** Copy an object, with many extra options */
  virtual int copy_obj(void *ctx, std::string& id, rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               struct rgw_err *err);
  /** delete a bucket*/
  virtual int delete_bucket(std::string& id, rgw_bucket& bucket, bool remove_pool);
  virtual int purge_buckets(std::string& id, vector<rgw_bucket>& buckets);

  virtual int disable_buckets(std::vector<rgw_bucket>& buckets);
  virtual int enable_buckets(std::vector<rgw_bucket>& buckets, uint64_t auid);
  virtual int bucket_suspended(rgw_bucket& bucket, bool *suspended);

  /** Delete an object.*/
  virtual int delete_obj(void *ctx, std::string& id, rgw_obj& src_obj, bool sync);

  /** Get the attributes for an object.*/
  virtual int get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest);

  /** Set an attr on an object. */
  virtual int set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl);

  /** Get data about an object out of RADOS and into memory. */
  virtual int prepare_get_obj(void *ctx, rgw_obj& obj,
            off_t ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *total_size,
            uint64_t *obj_size,
            void **handle,
            struct rgw_err *err);

  virtual int get_obj(void *ctx, void **handle, rgw_obj& obj,
            char **data, off_t ofs, off_t end);

  virtual void finish_get_obj(void **handle);

  virtual int read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl);

  virtual int obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime);

  virtual bool supports_tmap() { return true; }
  virtual int tmap_get(rgw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m);
  virtual int tmap_set(rgw_obj& obj, std::string& key, bufferlist& bl);
  virtual int tmap_set(rgw_obj& obj, map<std::string, bufferlist>& m);
  virtual int tmap_create(rgw_obj& obj, std::string& key, bufferlist& bl);
  virtual int tmap_del(rgw_obj& obj, std::string& key);
  virtual int update_containers_stats(map<string, RGWBucketEnt>& m);
  virtual int append_async(rgw_obj& obj, size_t size, bufferlist& bl);

  virtual int init_watch();
  virtual void finalize_watch();
  virtual int distribute(bufferlist& bl);
  virtual int watch_cb(int opcode, uint64_t ver, bufferlist& bl) { return 0; }

  void *create_context(void *user_ctx) {
    RGWRadosCtx *rctx = new RGWRadosCtx();
    rctx->user_ctx = user_ctx;
    return rctx;
  }
  void destroy_context(void *ctx) {
    delete (RGWRadosCtx *)ctx;
  }
  void set_atomic(void *ctx, rgw_obj& obj) {
    RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
    rctx->set_atomic(obj);
  }
  void set_intent_cb(void *ctx, int (*cb)(void *user_ctx, rgw_obj& obj, RGWIntentEvent intent)) {
    RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
    rctx->set_intent_cb(cb);
  }

  int decode_policy(bufferlist& bl, ACLOwner *owner);
  int get_bucket_stats(rgw_bucket& bucket, map<RGWObjCategory, RGWBucketStats>& stats);

  int cls_rgw_init_index(rgw_bucket& bucket, string& oid);
  int cls_obj_prepare_op(rgw_bucket& bucket, uint8_t op, string& tag, string& name);
  int cls_obj_complete_op(rgw_bucket& bucket, uint8_t op, string& tag, uint64_t epoch,
                          RGWObjEnt& ent, RGWObjCategory category);
  int cls_obj_complete_add(rgw_bucket& bucket, string& tag, uint64_t epoch, RGWObjEnt& ent, RGWObjCategory category);
  int cls_obj_complete_del(rgw_bucket& bucket, string& tag, uint64_t epoch, string& name);
  int cls_bucket_list(rgw_bucket& bucket, string start, uint32_t num, map<string, RGWObjEnt>& m, bool *is_truncated);
  int cls_bucket_head(rgw_bucket& bucket, struct rgw_bucket_dir_header& header);
  int prepare_update_index(RGWObjState *state, rgw_bucket& bucket, string& oid, string& tag);
  int complete_update_index(rgw_bucket& bucket, string& oid, string& tag, uint64_t epoch, uint64_t size,
                            utime_t& ut, string& etag, bufferlist *acl_bl, RGWObjCategory category);
  int complete_update_index_del(rgw_bucket& bucket, string& oid, string& tag, uint64_t epoch) {
    return cls_obj_complete_del(bucket, tag, epoch, oid);
  }
};

#endif
