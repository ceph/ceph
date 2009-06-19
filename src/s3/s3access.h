#ifndef __S3ACCESS_H
#define __S3ACCESS_H

#include <time.h>
#include <string>
#include <vector>

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

int list_buckets_init(std::string& id, S3AccessHandle *handle);
int list_buckets_next(std::string& id, S3ObjEnt& obj, S3AccessHandle *handle);

int list_objects(std::string& id, std::string& bucket, int max, std::string& prefix, std::string& marker, std::vector<S3ObjEnt>& result);

int create_bucket(std::string& id, std::string& bucket);
int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size, std::string& md5);

int delete_bucket(std::string& id, std::string& bucket);
int delete_obj(std::string& id, std::string& bucket, std::string& obj);

int get_obj(std::string& bucket, std::string& obj, 
            char **data, off_t ofs, off_t end,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            const char *if_match,
            const char *if_nomatch,
            struct s3_err *err);

#endif
