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

  int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
                   bool get_content_type);

  int create_bucket(std::string& id, std::string& bucket, map<std::string, bufferlist>& attrs, uint64_t auid=0);
  int put_obj_meta(std::string& id, rgw_obj& obj, time_t *mtime,
	      map<std::string, bufferlist>& attrs, bool exclusive);
  int put_obj_data(std::string& id, rgw_obj& obj, const char *data,
              off_t ofs, size_t size, time_t *mtime);
  int clone_range(rgw_obj& dst_obj, off_t dst_ofs,
                  rgw_obj& src_obj, off_t src_ofs, size_t size) { return -ENOTSUP; }
  int copy_obj(std::string& id, rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<std::string, bufferlist>& attrs,
               struct rgw_err *err);
  int delete_bucket(std::string& id, std::string& bucket);
  int delete_obj(std::string& id, rgw_obj& obj);

  int get_attr(const char *name, int fd, char **attr);
  int get_attr(const char *name, const char *path, char **attr);
  int get_attr(rgw_obj& obj, const char *name, bufferlist& dest);
  int set_attr(rgw_obj& obj, const char *name, bufferlist& bl);

  int prepare_get_obj(rgw_obj& obj,
            off_t ofs, off_t *end,
	    map<std::string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            size_t *size,
            void **handle,
            struct rgw_err *err);

  int get_obj(void **handle, rgw_obj& obj, char **data, off_t ofs, off_t end);

  void finish_get_obj(void **handle);
  int read(rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl);
  int obj_stat(rgw_obj& obj, uint64_t *psize, time_t *pmtime);
};

#endif
