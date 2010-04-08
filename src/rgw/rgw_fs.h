#ifndef __RGW_FS_H
#define __RGW_FS_H

#include "rgw_access.h"


class RGWFS  : public RGWAccess
{
public:
  int list_buckets_init(std::string& id, RGWAccessHandle *handle);
  int list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle);

  int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& delim,
                   std::string& marker, std::vector<RGWObjEnt>& result, map<string, bool>& common_prefixes);

  int create_bucket(std::string& id, std::string& bucket, map<std::string, bufferlist>& attrs, __u64 auid=0);
  int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size,
              time_t *mtime,
	      map<std::string, bufferlist>& attrs);
  int copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
               std::string& src_bucket, std::string& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<std::string, bufferlist>& attrs,
               struct rgw_err *err);
  int delete_bucket(std::string& id, std::string& bucket);
  int delete_obj(std::string& id, std::string& bucket, std::string& obj);

  int get_attr(const char *name, int fd, char **attr);
  int get_attr(const char *name, const char *path, char **attr);
  int get_attr(std::string& bucket, std::string& obj,
               const char *name, bufferlist& dest);
  int set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl);

 int get_obj(std::string& bucket, std::string& obj, 
            char **data, off_t ofs, off_t end,
	     map<std::string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            const char *if_match,
            const char *if_nomatch,
            bool get_data,
            struct rgw_err *err);
};

#endif
