#ifndef CEPH_RGWRADOS_H
#define CEPH_RGWRADOS_H

#include "include/rados/librados.hpp"
#include "rgw_access.h"
#include "rgw_common.h"

class RGWRados  : public RGWAccess
{
  /** Open the pool used as root for this gateway */
  int open_root_pool_ctx();

  int open_bucket_ctx(std::string& bucket, librados::IoCtx&  io_ctx);

  struct GetObjState {
    librados::IoCtx io_ctx;
    bool sent_data;

    GetObjState() : sent_data(false) {}
  };

public:
  /** Initialize the RADOS instance and prepare to do other ops */
  virtual int initialize(CephContext *cct);
  /** set up a bucket listing. id is ignored, handle is filled in. */
  virtual int list_buckets_init(std::string& id, RGWAccessHandle *handle);
  /** 
   * get the next bucket in the listing. id is ignored, obj is filled in,
   * handle is updated.
   */
  virtual int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle);

  /** get listing of the objects in a bucket */
  virtual int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
		   bool get_content_type);

  /**
   * create a bucket with name bucket and the given list of attrs
   * returns 0 on success, -ERR# otherwise.
   */
  virtual int create_bucket(std::string& id, std::string& bucket, map<std::string,bufferlist>& attrs, uint64_t auid=0);

  /** Write/overwrite an object to the bucket storage. */
  virtual int put_obj_meta(std::string& id, std::string& bucket, std::string& obj, std::string& loc, time_t *mtime,
              map<std::string, bufferlist>& attrs, bool exclusive);
  virtual int put_obj_data(std::string& id, std::string& bucket, std::string& obj, std::string& loc, const char *data,
              off_t ofs, size_t len, time_t *mtime);
  /** Copy an object, with many extra options */
  virtual int copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
               std::string& src_bucket, std::string& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<std::string, bufferlist>& attrs,
               struct rgw_err *err);
  /** delete a bucket*/
  virtual int delete_bucket(std::string& id, std::string& bucket);

  /** Delete an object.*/
  virtual int delete_obj(std::string& id, std::string& bucket, std::string& obj);

  /** Get the attributes for an object.*/
  virtual int get_attr(std::string& bucket, std::string& obj, std::string& loc,
               const char *name, bufferlist& dest);

  /** Set an attr on an object. */
  virtual int set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl);

  /** Get data about an object out of RADOS and into memory. */
  virtual int prepare_get_obj(std::string& bucket, std::string& obj, std::string& loc,  
            off_t ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            size_t *total_size,
            void **handle,
            struct rgw_err *err);

  virtual int get_obj(void **handle, std::string& bucket, std::string& oid, std::string& loc,
            char **data, off_t ofs, off_t end);

  virtual void finish_get_obj(void **handle);

  virtual int read(std::string& bucket, std::string& oid, off_t ofs, size_t size, bufferlist& bl);

  virtual int obj_stat(std::string& bucket, std::string& obj, uint64_t *psize, time_t *pmtime);

  virtual bool supports_tmap() { return true; }
  virtual int tmap_set(std::string& bucket, std::string& obj, std::string& key, bufferlist& bl);
  virtual int tmap_create(std::string& bucket, std::string& obj, std::string& key, bufferlist& bl);
  virtual int tmap_del(std::string& bucket, std::string& obj, std::string& key);
  virtual int update_containers_stats(map<string, RGWBucketEnt>& m);
  virtual int append_async(std::string& bucket, std::string& oid, size_t size, bufferlist& bl);
};

#endif
