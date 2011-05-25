#ifndef CEPH_RGW_ACCESS_H
#define CEPH_RGW_ACCESS_H

#include <time.h>
#include <errno.h>
#include <string>
#include <vector>
#include <include/types.h>

#include "rgw_common.h"

struct md_config_t;

/**
 * Abstract class defining the interface for storage devices used by RGW.
 */
class RGWAccess {
public:
  virtual ~RGWAccess();
  /** do all necessary setup of the storage device */
  virtual int initialize(CephContext *cct) = 0;
  /** prepare a listing of all buckets. */
  virtual int list_buckets_init(std::string& id, RGWAccessHandle *handle) = 0;
  /** get the next bucket in the provided listing context. */
  virtual int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle) = 0;

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
  virtual int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& delim,
                           std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
                           bool get_content_type) = 0;

  /** Create a new bucket*/
  virtual int create_bucket(std::string& id, std::string& bucket, map<std::string, bufferlist>& attrs, uint64_t auid=0) = 0;
  /** write an object to the storage device in the appropriate pool
    with the given stats */
  virtual int put_obj_meta(std::string& id, std::string& bucket, std::string& obj, time_t *mtime,
                      map<std::string, bufferlist>& attrs) = 0;
  virtual int put_obj_data(std::string& id, std::string& bucket, std::string& obj, const char *data,
                      off_t ofs, size_t len, time_t *mtime) = 0;

  int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t len,
              time_t *mtime, map<std::string, bufferlist>& attrs) {
    int ret = put_obj_meta(id, bucket, obj, NULL, attrs);
    if (ret >= 0) {
      ret = put_obj_data(id, bucket, obj, data, -1, len, mtime);
    }
    return ret;
  }

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
  virtual int copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
                      std::string& src_bucket, std::string& src_obj,
                      time_t *mtime,
                      const time_t *mod_ptr,
                      const time_t *unmod_ptr,
                      const char *if_match,
                      const char *if_nomatch,
		       map<std::string, bufferlist>& attrs,
                      struct rgw_err *err) = 0;
  /**
   * Delete a bucket.
   * id: unused in implementations
   * bucket: the name of the bucket to delete
   * Returns 0 on success, -ERR# otherwise.
   */
  virtual int delete_bucket(std::string& id, std::string& bucket) = 0;

  /**
   * Delete an object.
   * id: unused in current implementations
   * bucket: name of the bucket storing the object
   * obj: name of the object to delete
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int delete_obj(std::string& id, std::string& bucket, std::string& obj) = 0;

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
  virtual int prepare_get_obj(std::string& bucket, std::string& obj, 
            off_t ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            size_t *total_size,
            void **handle,
            struct rgw_err *err) = 0;

  virtual int get_obj(void **handle, std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end) = 0;

  virtual void finish_get_obj(void **handle) = 0;

  /**
   * a simple object read without keeping state
   */
  virtual int read(std::string& bucket, std::string& oid, off_t ofs, size_t size, bufferlist& bl) = 0;

  /**
   * Get the attributes for an object.
   * bucket: name of the bucket holding the object.
   * obj: name of the object
   * name: name of the attr to retrieve
   * dest: bufferlist to store the result in
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int get_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& dest) = 0;
  /**
   * Set an attr on an object.
   * bucket: name of the bucket holding the object
   * obj: name of the object to set the attr on
   * name: the attr to set
   * bl: the contents of the attr
   * Returns: 0 on success, -ERR# otherwise.
   */
  virtual int set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl) = 0;

 /**
  * stat an object
  */
  virtual int obj_stat(std::string& bucket, std::string& obj, uint64_t *psize, time_t *pmtime) = 0;

  virtual bool supports_tmap() { return false; }

  virtual int tmap_set(std::string& bucket, std::string& obj, std::string& key, bufferlist& bl) { return -ENOTSUP; }
  virtual int tmap_create(std::string& bucket, std::string& obj, std::string& key, bufferlist& bl) { return -ENOTSUP; }
  virtual int tmap_del(std::string& bucket, std::string& obj, std::string& key) { return -ENOTSUP; }

  virtual int update_containers_stats(map<string, RGWBucketEnt>& m) { return -ENOTSUP; }

  virtual int append_async(std::string& bucket, std::string& oid, size_t size, bufferlist& bl) { return -ENOTSUP; }


 /** 
   * Given the name of the storage provider, initialize it
   * with the given arguments.
   */
  static RGWAccess *init_storage_provider(const char *type, CephContext *cct);
  static RGWAccess *store;
};

#define rgwstore RGWAccess::store



#endif
