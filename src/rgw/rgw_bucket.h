#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "rgw_formats.h"


using namespace std;

// define as static when RGWBucket implementation compete
extern void rgw_get_buckets_obj(string& user_id, string& buckets_obj_id);


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
    buckets[bucket.bucket.name] = bucket;
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

  size_t count() { return buckets.size(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets,
                                 const string& marker, uint64_t max, bool need_stats);

/**
 * Store the set of buckets associated with a user.
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
extern int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets);

extern int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket);
extern int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket);

extern int rgw_remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object);
extern int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);

extern void check_bad_user_bucket_mapping(RGWRados *store, const string& user_id, bool fix);

struct RGWBucketAdminOpState {
  std::string uid;
  std::string display_name;
  std::string bucket_name;
  std::string bucket_id;
  std::string object_name;

  bool list_buckets;
  bool stat_buckets;
  bool check_objects;
  bool fix_index;
  bool delete_child_objects;
  bool bucket_stored;

  rgw_bucket bucket;

  void set_fetch_stats(bool value) { stat_buckets = value; }
  void set_check_objects(bool value) { check_objects = value; }
  void set_fix_index(bool value) { fix_index = value; }
  void set_delete_children(bool value) { delete_child_objects = value; }

  void set_user_id(std::string& user_id) {
    if (!user_id.empty())
      uid = user_id;
  }
  void set_bucket_name(std::string& bucket_str) {
    bucket_name = bucket_str; 
  }
  void set_object(std::string& object_str) {
    object_name = object_str;
  }

  std::string& get_user_id() { return uid; };
  std::string& get_user_display_name() { return display_name; };
  std::string& get_bucket_name() { return bucket_name; };
  std::string& get_object_name() { return object_name; };

  rgw_bucket& get_bucket() { return bucket; };
  void set_bucket(rgw_bucket& _bucket) {
    bucket = _bucket; 
    bucket_stored = true;
  }

  bool will_fetch_stats() { return stat_buckets; };
  bool will_fix_index() { return fix_index; };
  bool will_delete_children() { return delete_child_objects; };
  bool will_check_objects() { return check_objects; };
  bool is_user_op() { return !uid.empty(); };
  bool is_system_op() { return uid.empty(); }; 
  bool has_bucket_stored() { return bucket_stored; };

  RGWBucketAdminOpState() : list_buckets(false), stat_buckets(false), check_objects(false), 
                            fix_index(false), delete_child_objects(false),
                            bucket_stored(false)  {}
};

/*
 * A simple wrapper class for administrative bucket operations
 */

class RGWBucket
{
  RGWUserBuckets buckets;
  RGWRados *store;
  RGWAccessHandle handle;

  std::string user_id;
  std::string bucket_name;

  bool failure;

private:

public:
  RGWBucket() : store(NULL), failure(false) {}
  int init(RGWRados *storage, RGWBucketAdminOpState& op_state);

  int create_bucket(string bucket_str, string& user_id, string& display_name);
  
  int check_bad_index_multipart(RGWBucketAdminOpState& op_state,
          list<std::string>& objs_to_unlink, std::string *err_msg = NULL);

  int check_object_index(RGWBucketAdminOpState& op_state,
          map<string, RGWObjEnt> result, std::string *err_msg = NULL);

  int check_index(RGWBucketAdminOpState& op_state,
          map<RGWObjCategory, RGWBucketStats>& existing_stats,
          map<RGWObjCategory, RGWBucketStats>& calculated_stats,
          std::string *err_msg = NULL);

  int remove(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int link(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int unlink(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);

  int remove_object(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int get_policy(RGWBucketAdminOpState& op_state, ostream& o);

  void clear_failure() { failure = false; };
};

class RGWBucketAdminOp
{
public:
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os);


  static int unlink(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int link(RGWRados *store, RGWBucketAdminOpState& op_state);

  static int check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int remove_object(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int info(RGWRados *store, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

#endif
