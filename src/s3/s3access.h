#ifndef __S3ACCESS_H
#define __S3ACCESS_H

#include <time.h>
#include <string>
#include <vector>
#include <include/types.h>

#define SERVER_NAME "S3FS"

typedef void *S3AccessHandle;

struct s3_err {
  const char *num;
  const char *code;
  const char *message;

  s3_err() : num(NULL), code(NULL), message(NULL) {}
};


struct S3ObjEnt {
  std::string name;
  size_t size;
  time_t mtime;
};

class S3Access {
public:
#if 0
  S3Access();
  virtual ~S3Access();
#endif
  virtual int list_buckets_init(std::string& id, S3AccessHandle *handle) = 0;
  virtual int list_buckets_next(std::string& id, S3ObjEnt& obj, S3AccessHandle *handle) = 0;

  virtual int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& marker, std::vector<S3ObjEnt>& result) = 0;

  virtual int create_bucket(std::string& id, std::string& bucket) = 0;
  virtual int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size,
                      std::vector<std::pair<std::string, bufferlist> >& attrs) = 0;

  virtual int delete_bucket(std::string& id, std::string& bucket) = 0;
  virtual int delete_obj(std::string& id, std::string& bucket, std::string& obj) = 0;

  virtual int get_obj(std::string& bucket, std::string& obj, 
            char **data, off_t ofs, off_t end,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            const char *if_match,
            const char *if_nomatch,
            bool get_data,
            struct s3_err *err) = 0;

  static S3Access *init_storage_provider(const char *type);
  static S3Access *store;
};

#define s3store S3Access::store



#endif
