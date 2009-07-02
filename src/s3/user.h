#ifndef __USER_H
#define __USER_H

#include <string>

#include "include/types.h"
#include "s3/s3access.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"
#define USER_INFO_VER 1

#define S3_USER_ANON_ID "anonymous"


struct S3UserInfo
{
  string user_id;
  string secret_key;
  string display_name;

  void encode(bufferlist& bl) const {
     __u32 ver = USER_INFO_VER;
     ::encode(ver, bl);
     ::encode(user_id, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
     __u32 ver;
    ::decode(ver, bl);
    ::decode(user_id, bl);
    ::decode(secret_key, bl);
    ::decode(display_name, bl);
  }

  void clear() {
    user_id.clear();
    secret_key.clear();
    display_name.clear();
  }
};
WRITE_CLASS_ENCODER(S3UserInfo)

extern int s3_get_user_info(string user_id, S3UserInfo& info);
extern void s3_get_anon_user(S3UserInfo& info);
extern int s3_store_user_info(S3UserInfo& info);

class S3UserBuckets
{
  map<string, S3ObjEnt> buckets;

public:
  S3UserBuckets() {}
  void encode(bufferlist& bl) const {
     ::encode(buckets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(buckets, bl);
  }

  bool owns(string& name) {
    map<string, S3ObjEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  void add(S3ObjEnt& bucket) {
    buckets[bucket.name] = bucket;
  }

  void remove(string& name) {
    map<string, S3ObjEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  map<string, S3ObjEnt>& get_buckets() { return buckets; }
};
WRITE_CLASS_ENCODER(S3UserBuckets)

extern int s3_get_user_buckets(string user_id, S3UserBuckets& buckets);
extern int s3_put_user_buckets(string user_id, S3UserBuckets& buckets);

#endif
