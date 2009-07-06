#ifndef __S3ACCESS_H
#define __S3ACCESS_H

#include <time.h>
#include <string>
#include <vector>
#include <include/types.h>

#include <openssl/md5.h>

#define SERVER_NAME "S3FS"

#define S3_ATTR_ACL	"user.s3.acl"
#define S3_ATTR_ETAG    "user.s3.etag"
#define S3_ATTR_BUCKETS	"user.s3.buckets"

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
  char etag[MD5_DIGEST_LENGTH * 2 + 1];

  void encode(bufferlist& bl) const {
     ::encode(name, bl);
     ::encode(size, bl);
     ::encode(mtime, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(name, bl);
    ::decode(size, bl);
    ::decode(mtime, bl);
  }
};
WRITE_CLASS_ENCODER(S3ObjEnt)

class S3Access {
public:
  virtual int list_buckets_init(std::string& id, S3AccessHandle *handle) = 0;
  virtual int list_buckets_next(std::string& id, S3ObjEnt& obj, S3AccessHandle *handle) = 0;

  virtual int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& marker, std::vector<S3ObjEnt>& result) = 0;

  virtual int create_bucket(std::string& id, std::string& bucket, std::vector<std::pair<std::string, bufferlist> >& attrs) = 0;
  virtual int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size,
                      time_t *mtime,
                      std::vector<std::pair<std::string, bufferlist> >& attrs) = 0;

  virtual int copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
                      std::string& src_bucket, std::string& src_obj,
                      char **petag,
                      time_t *mtime,
                      const time_t *mod_ptr,
                      const time_t *unmod_ptr,
                      const char *if_match,
                      const char *if_nomatch,
                      std::vector<std::pair<std::string, bufferlist> >& attrs,
                     struct s3_err *err) = 0;
  virtual int delete_bucket(std::string& id, std::string& bucket) = 0;
  virtual int delete_obj(std::string& id, std::string& bucket, std::string& obj) = 0;

  virtual int get_obj(std::string& bucket, std::string& obj, 
            char **data, off_t ofs, off_t end,
            char **petag,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            const char *if_match,
            const char *if_nomatch,
            bool get_data,
            struct s3_err *err) = 0;

  virtual int get_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& dest) = 0;
  virtual int set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl) = 0;

  static S3Access *init_storage_provider(const char *type);
  static S3Access *store;
};

#define s3store S3Access::store



#endif
