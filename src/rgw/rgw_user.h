#ifndef __USER_H
#define __USER_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"
#define USER_INFO_EMAIL_BUCKET_NAME ".users.email"

#define RGW_USER_ANON_ID "anonymous"

struct RGWUID
{
  string user_id;
  void encode(bufferlist& bl) const {
     ::encode(user_id, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(user_id, bl);
  }
};
WRITE_CLASS_ENCODER(RGWUID)

extern int rgw_get_user_info(string user_id, RGWUserInfo& info);
extern void rgw_get_anon_user(RGWUserInfo& info);
extern int rgw_store_user_info(RGWUserInfo& info);
extern int rgw_get_uid_by_email(string& email, string& user_id);

class RGWUserBuckets
{
  map<string, RGWObjEnt> buckets;

public:
  RGWUserBuckets() {}
  void encode(bufferlist& bl) const {
     ::encode(buckets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(buckets, bl);
  }

  bool owns(string& name) {
    map<string, RGWObjEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  void add(RGWObjEnt& bucket) {
    buckets[bucket.name] = bucket;
  }

  void remove(string& name) {
    map<string, RGWObjEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  map<string, RGWObjEnt>& get_buckets() { return buckets; }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

extern int rgw_get_user_buckets(string user_id, RGWUserBuckets& buckets);
extern int rgw_put_user_buckets(string user_id, RGWUserBuckets& buckets);

#endif
