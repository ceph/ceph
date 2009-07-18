#ifndef __USER_H
#define __USER_H

#include <string>

#include "include/types.h"
#include "s3common.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"
#define USER_INFO_EMAIL_BUCKET_NAME ".users.email"

#define S3_USER_ANON_ID "anonymous"

struct S3UID
{
  string user_id;
  void encode(bufferlist& bl) const {
     ::encode(user_id, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(user_id, bl);
  }
};
WRITE_CLASS_ENCODER(S3UID)

extern int s3_get_user_info(string user_id, S3UserInfo& info);
extern void s3_get_anon_user(S3UserInfo& info);
extern int s3_store_user_info(S3UserInfo& info);
extern int s3_get_uid_by_email(string& email, string& user_id);

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
