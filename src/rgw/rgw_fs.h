#ifndef CEPH_RGW_FS_H
#define CEPH_RGW_FS_H

#include "rgw_access.h"


class RGWFS  : public RGWAccess
{
  struct GetObjState {
    int fd;
  };
public:
  virtual int initialize(CephContext *cct);
  int list_buckets_init(std::string& id, RGWAccessHandle *handle);
  int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle);

  // not implemented here
  int list_objects_raw_init(rgw_bucket& bucket, RGWAccessHandle *handle) { assert(0); }
  int list_objects_raw_next(RGWObjEnt& obj, RGWAccessHandle *handle) { assert(0); }

  int list_objects(std::string& id, rgw_bucket& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
                   bool get_content_type, string& ns, bool *is_truncated, RGWAccessListFilter *filter);

  int create_bucket(std::string& id, rgw_bucket& bucket, map<std::string, bufferlist>& attrs, bool system_bucket, bool exclusive, uint64_t auid=0);
  int put_obj_meta(void *ctx, std::string& id, rgw_obj& obj, uint64_t size, time_t *mtime,
	      map<std::string, bufferlist>& attrs, RGWObjCategory category, bool exclusive);
  int put_obj_data(void *ctx, std::string& id, rgw_obj& obj, const char *data,
              off_t ofs, size_t size);
  int copy_obj(void *ctx, std::string& id, rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<std::string, bufferlist>& attrs,
               RGWObjCategory category,
               struct rgw_err *err);
  int delete_bucket(std::string& id, rgw_bucket& bucket, bool remove_pool);
  int delete_obj(void *ctx, std::string& id, rgw_obj& obj, bool sync);

  int get_attr(const char *name, int fd, char **attr);
  int get_attr(const char *name, const char *path, char **attr);
  int get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest);
  int set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl);

  int prepare_get_obj(void *ctx,
            rgw_obj& obj,
            off_t ofs, off_t *end,
	    map<std::string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *size,
            uint64_t *obj_size,
            void **handle,
            struct rgw_err *err);

  int get_obj(void *ctx, void **handle, rgw_obj& obj, char **data, off_t ofs, off_t end);

  void finish_get_obj(void **handle);
  int read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl);
  int obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime);

  virtual int get_bucket_info(string& bucket_name, RGWBucketInfo& info);
};

#endif
