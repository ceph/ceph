#ifndef CEPH_RGW_ACCESS_H
#define CEPH_RGW_ACCESS_H

#include <time.h>
#include <errno.h>
#include <string>
#include <vector>
#include <include/types.h>

#include "rgw_common.h"

struct md_config_t;

class RGWAccessListFilter {
public:
  virtual ~RGWAccessListFilter() {}
  virtual bool filter(string& name, string& key) = 0;
};

struct RGWCloneRangeInfo {
  rgw_obj src;
  off_t src_ofs;
  off_t dst_ofs;
  uint64_t len;
};

/**
 * Abstract class defining the interface for storage devices used by RGW.
 */
class RGWAccess {
public:
  virtual ~RGWAccess();
  /** do all necessary setup of the storage device */
  virtual int initialize(CephContext *cct) = 0;
  virtual void finalize() {}
  /** prepare a listing of all buckets. */
  virtual int list_buckets_init(std::string& id, RGWAccessHandle *handle) = 0;
  /** get the next bucket in the provided listing context. */
  virtual int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle) = 0;

  virtual int list_objects_raw_init(rgw_bucket& bucket, RGWAccessHandle *handle) = 0;
  virtual int list_objects_raw_next(RGWObjEnt& obj, RGWAccessHandle *handle) = 0;

  /** 
   * get listing of the objects in a bucket.
   * id: ignored in current implementations
   * bucket: bucket to list contents of
   * max: maximum number of results to return
   * prefix: only return results that match this prefix
   * delim: do not include results that match this string.
   *     Any skipped results will have the matching portion of their name
   *     inserted in common_prefixes with a "true" mark.
   * marker: if filled in, begin the listing with this object.
   * result: the objects are put in here.
   * common_prefixes: if delim is filled in, any matching prefixes are placed
   *     here.
   */
  virtual int list_objects(std::string& id, rgw_bucket& bucket, int max, std::string& prefix, std::string& delim,
                           std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
                           bool get_content_type, std::string& ns, bool *is_truncated, RGWAccessListFilter *filter) = 0;

  /** Create a new bucket*/
  virtual int create_bucket(std::string& id, rgw_bucket& bucket,
                            map<std::string, bufferlist>& attrs,
                            bool system_bucket, bool exclusive = true,
                            uint64_t auid = 0) = 0;
  virtual int select_bucket_placement(string& bucket_name, rgw_bucket& bucket) { return 0; }
  virtual int add_bucket_placement(std::string& new_placement) { return 0; }
  virtual int create_pools(std::string& id, vector<string>& names, vector<int>& retcodes, int auid = 0) { return -ENOTSUP; }
  /** write an object to the storage device in the appropriate pool
    with the given stats */
  virtual int put_obj_meta(void *ctx, std::string& id, rgw_obj& obj, uint64_t size, time_t *mtime,
                      map<std::string, bufferlist>& attrs, RGWObjCategory category, bool exclusive) = 0;
  virtual int put_obj_data(void *ctx, std::string& id, rgw_obj& obj, const char *data,
                      off_t ofs, size_t len) = 0;
  virtual int aio_put_obj_data(void *ctx, std::string& id, rgw_obj& obj, const char *data,
                      off_t ofs, size_t len, void **handle) { return -ENOTSUP; }

  /* note that put_obj doesn't set category on an object, only use it for none user objects */
  int put_obj(void *ctx, std::string& id, rgw_obj& obj, const char *data, size_t len,
              time_t *mtime, map<std::string, bufferlist>& attrs) {
    int ret = put_obj_data(ctx, id, obj, data, -1, len);
    if (ret >= 0) {
      ret = put_obj_meta(ctx, id, obj, len, mtime, attrs, RGW_OBJ_CATEGORY_NONE, false);
    }
    return ret;
  }

  virtual int init_watch() { return -ENOTSUP; }
  virtual void finalize_watch() {}
  virtual int distribute(bufferlist& bl) { return -ENOTSUP; }
  virtual int aio_wait(void *handle) { return -ENOTSUP; }

  virtual bool aio_completed(void *handle) { return false; }

  /**
   * Copy an object.
   * id: unused (well, it's passed to put_obj)
   * dest_bucket: the bucket to copy into
   * dest_obj: the object to copy into
   * src_bucket: the bucket to copy from
   * src_obj: the object to copy from
   * mod_ptr, unmod_ptr, if_match, if_nomatch: as used in get_obj
   * attrs: these are placed on the new object IN ADDITION to
   *    (or overwriting) any attrs copied from the original object
   * err: stores any errors resulting from the get of the original object
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int copy_obj(void *ctx, std::string& id, rgw_obj& dest_obj,
                      rgw_obj& src_obj,
                      time_t *mtime,
                      const time_t *mod_ptr,
                      const time_t *unmod_ptr,
                      const char *if_match,
                      const char *if_nomatch,
		      map<std::string, bufferlist>& attrs,
                      RGWObjCategory category,
                      struct rgw_err *err) = 0;
  /**
   * Delete a bucket.
   * id: unused in implementations
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  virtual int delete_bucket(std::string& id, rgw_bucket& bucket, bool remove_pool) = 0;
  virtual int purge_buckets(std::string& id, vector<rgw_bucket>& buckets) { return -ENOTSUP; }

  virtual int disable_buckets(std::vector<rgw_bucket>& buckets) { return -ENOTSUP; }
  virtual int enable_buckets(std::vector<rgw_bucket>& buckets, uint64_t auid) { return -ENOTSUP; }
  virtual int bucket_suspended(rgw_bucket& bucket, bool *suspended) {
    *suspended = false;
    return 0;
  }

  /**
   * Delete an object.
   * id: unused in current implementations
   * bucket: name of the bucket storing the object
   * obj: name of the object to delete
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int delete_obj(void *ctx, std::string& id, rgw_obj& obj, bool sync = true) = 0;

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * obj: name/key of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *          (if get_data==true) length of read data,
 *          (if get_data==false) length of the object
 */
  virtual int prepare_get_obj(void *ctx,
            rgw_obj& obj,
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
            struct rgw_err *err) = 0;

  virtual int get_obj(void *ctx, void **handle, rgw_obj& obj,
            char **data, off_t ofs, off_t end) = 0;

  virtual void finish_get_obj(void **handle) = 0;

  virtual int clone_obj(void *ctx, rgw_obj& dst_obj, off_t dst_ofs,
                          rgw_obj& src_obj, off_t src_ofs,
                          uint64_t size, time_t *pmtime,
                          map<string, bufferlist> attrs,
                          RGWObjCategory category) {
    RGWCloneRangeInfo info;
    vector<RGWCloneRangeInfo> v;
    info.src = src_obj;
    info.src_ofs = src_ofs;
    info.dst_ofs = dst_ofs;
    info.len = size;
    v.push_back(info);
    return clone_objs(ctx, dst_obj, v, attrs, category, pmtime, true, false);
  }

  virtual int clone_objs(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        RGWObjCategory category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive) { return -ENOTSUP; }
 /**
   * a simple object read without keeping state
   */
  virtual int read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl) = 0;

  /**
   * Get the attributes for an object.
   * bucket: name of the bucket holding the object.
   * obj: name of the object
   * name: name of the attr to retrieve
   * dest: bufferlist to store the result in
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest) = 0;

  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl) = 0;

 /**
  * stat an object
  */
  virtual int obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime) = 0;

  virtual bool supports_tmap() { return false; }

  virtual int tmap_get(rgw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m) { return -ENOTSUP; }
  virtual int tmap_set(rgw_obj& obj, std::string& key, bufferlist& bl) { return -ENOTSUP; }
  virtual int tmap_set(rgw_obj& obj, map<std::string, bufferlist>& m) { return -ENOTSUP; }
  virtual int tmap_create(rgw_obj& obj, std::string& key, bufferlist& bl) { return -ENOTSUP; }
  virtual int tmap_del(rgw_obj& obj, std::string& key) { return -ENOTSUP; }

  virtual int update_containers_stats(map<string, RGWBucketEnt>& m) { return -ENOTSUP; }

  virtual int append_async(rgw_obj& obj, size_t size, bufferlist& bl) { return -ENOTSUP; }


 /** 
   * Given the name of the storage provider, initialize it
   * with the given arguments.
   */
  static RGWAccess *init_storage_provider(const char *type, CephContext *cct);
  static void close_storage();
  static RGWAccess *store;

  virtual void *create_context(void *user_ctx) { return NULL; }
  virtual void destroy_context(void *ctx) {}
  virtual void set_atomic(void *ctx, rgw_obj& obj) {}

  // to notify upper layer that we need to do some operation on an object, and it's up to
  // the upper layer to schedule this operation.. e.g., log intent in intent log
  virtual void set_intent_cb(void *ctx, int (*cb)(void *user_ctx, rgw_obj& obj, RGWIntentEvent intent)) {}

  virtual int get_bucket_stats(rgw_bucket& bucket, map<RGWObjCategory, RGWBucketStats>& stats) { return -ENOTSUP; }
};

class RGWStoreManager {
  RGWAccess *store;
public:
  RGWStoreManager() : store(NULL) {}
  ~RGWStoreManager() {
    if (store)
      RGWAccess::close_storage();
  }
  RGWAccess *init(const char *type, CephContext *cct) {
    store = RGWAccess::init_storage_provider(type, cct);
    return store;
  }

};

#define rgwstore RGWAccess::store



#endif
