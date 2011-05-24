#ifndef CEPH_RGW_USER_H
#define CEPH_RGW_USER_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"
#define USER_INFO_EMAIL_BUCKET_NAME ".users.email"
#define USER_INFO_OPENSTACK_BUCKET_NAME ".users.openstack"
#define USER_INFO_UID_BUCKET_NAME ".users.uid"
#define RGW_USER_ANON_ID "anonymous"

/**
 * A string wrapper that includes encode/decode functions
 * for easily accessing a UID in all forms
 */
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

/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
extern void rgw_get_anon_user(RGWUserInfo& info);
/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_store_user_info(RGWUserInfo& info);
/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_uid(string& user_id, RGWUserInfo& info);
/**
 * Given an openstack username, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_email(string& email, RGWUserInfo& info);
/**
 * Given an openstack username, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_openstack(string& openstack_name, RGWUserInfo& info);
/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(string& access_key, RGWUserInfo& info);
/**
 * Given an RGWUserInfo, deletes the user and its bucket ACLs.
 */
extern int rgw_delete_user(RGWUserInfo& user);
/**
 * Store a list of the user's buckets, with associated functinos.
 */
class RGWUserBuckets
{
  map<string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() {}
  void encode(bufferlist& bl) const {
     ::encode(buckets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(buckets, bl);
  }
  /**
   * Check if the user owns a bucket by the given name.
   */
  bool owns(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  /**
   * Add a (created) bucket to the user's bucket list.
   */
  void add(RGWBucketEnt& bucket) {
    buckets[bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  /**
   * Get the user's buckets as a map.
   */
  map<string, RGWBucketEnt>& get_buckets() { return buckets; }

  /**
   * Cleanup data structure
   */
  void clear() { buckets.clear(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(string user_id, RGWUserBuckets& buckets, bool need_stats);

/**
 * Store the set of buckets associated with a user.
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
extern int rgw_write_buckets_attr(string user_id, RGWUserBuckets& buckets);

extern int rgw_add_bucket(string user_id, string bucket_name);
extern int rgw_remove_bucket(string user_id, string bucket_name);

extern int rgw_remove_key_storage(RGWAccessKey& access_key);

#endif
