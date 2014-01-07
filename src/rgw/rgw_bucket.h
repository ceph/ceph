#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>
#include <memory>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "common/lru_map.h"
#include "rgw_formats.h"


using namespace std;

// define as static when RGWBucket implementation compete
extern void rgw_get_buckets_obj(string& user_id, string& buckets_obj_id);

extern int rgw_bucket_store_info(RGWRados *store, const string& bucket_name, bufferlist& bl, bool exclusive,
                                 map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                                 time_t mtime);
extern int rgw_bucket_instance_store_info(RGWRados *store, string& oid, bufferlist& bl, bool exclusive,
                                 map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                                 time_t mtime);

extern int rgw_bucket_instance_remove_entry(RGWRados *store, string& entry, RGWObjVersionTracker *objv_tracker);

extern int rgw_bucket_delete_bucket_obj(RGWRados *store, string& bucket_name, RGWObjVersionTracker& objv_tracker);

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

class RGWMetadataManager;

extern void rgw_bucket_init(RGWMetadataManager *mm);
/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets,
                                 const string& marker, uint64_t max, bool need_stats);

extern int rgw_link_bucket(RGWRados *store, string user_id, rgw_bucket& bucket, time_t creation_time, bool update_entrypoint = true);
extern int rgw_unlink_bucket(RGWRados *store, string user_id, const string& bucket_name, bool update_entrypoint = true);

extern int rgw_remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object);
extern int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);

extern int rgw_bucket_set_attrs(RGWRados *store, RGWBucketInfo& bucket_info,
                                map<string, bufferlist>& attrs,
                                map<string, bufferlist>* rmattrs,
                                RGWObjVersionTracker *objv_tracker);

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

  RGWUserInfo user_info;
  std::string bucket_name;

  bool failure;

private:

public:
  RGWBucket() : store(NULL), handle(NULL), failure(false) {}
  int init(RGWRados *storage, RGWBucketAdminOpState& op_state);

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


enum DataLogEntityType {
  ENTITY_TYPE_BUCKET = 1,
};

struct rgw_data_change {
  DataLogEntityType entity_type;
  string key;
  utime_t timestamp;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    uint8_t t = (uint8_t)entity_type;
    ::encode(t, bl);
    ::encode(key, bl);
    ::encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     uint8_t t;
     ::decode(t, bl);
     entity_type = (DataLogEntityType)t;
     ::decode(key, bl);
     ::decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_data_change)

struct RGWDataChangesLogInfo {
  string marker;
  utime_t last_update;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWDataChangesLog {
  CephContext *cct;
  RGWRados *store;

  int num_shards;
  string *oids;

  Mutex lock;

  atomic_t down_flag;

  struct ChangeStatus {
    utime_t cur_expiration;
    utime_t cur_sent;
    bool pending;
    RefCountedCond *cond;
    Mutex *lock;

    ChangeStatus() : pending(false), cond(NULL) {
      lock = new Mutex("RGWDataChangesLog::ChangeStatus");
    }

    ~ChangeStatus() {
      delete lock;
    }
  };

  typedef std::tr1::shared_ptr<ChangeStatus> ChangeStatusPtr;

  lru_map<string, ChangeStatusPtr> changes;

  map<string, rgw_bucket> cur_cycle;

  void _get_change(string& bucket_name, ChangeStatusPtr& status);
  void register_renew(rgw_bucket& bucket);
  void update_renewed(string& bucket_name, utime_t& expiration);

  class ChangesRenewThread : public Thread {
    CephContext *cct;
    RGWDataChangesLog *log;
    Mutex lock;
    Cond cond;

  public:
    ChangesRenewThread(CephContext *_cct, RGWDataChangesLog *_log) : cct(_cct), log(_log), lock("ChangesRenewThread") {}
    void *entry();
    void stop();
  };

  ChangesRenewThread *renew_thread;

public:

  RGWDataChangesLog(CephContext *_cct, RGWRados *_store) : cct(_cct), store(_store), lock("RGWDataChangesLog"),
                                                           changes(cct->_conf->rgw_data_log_changes_size) {
    num_shards = cct->_conf->rgw_data_log_num_shards;

    oids = new string[num_shards];

    string prefix = cct->_conf->rgw_data_log_obj_prefix;

    if (prefix.empty()) {
      prefix = "data_log";
    }

    for (int i = 0; i < num_shards; i++) {
      char buf[16];
      snprintf(buf, sizeof(buf), "%s.%d", prefix.c_str(), i);
      oids[i] = buf;
    }

    renew_thread = new ChangesRenewThread(cct, this);
    renew_thread->create();
  }

  ~RGWDataChangesLog();

  int choose_oid(rgw_bucket& bucket);
  int add_entry(rgw_bucket& bucket);
  int renew_entries();
  int list_entries(int shard, utime_t& start_time, utime_t& end_time, int max_entries,
		   list<rgw_data_change>& entries,
		   const string& marker,
		   string *out_marker,
		   bool *truncated);
  int trim_entries(int shard_id, const utime_t& start_time, const utime_t& end_time,
                   const string& start_marker, const string& end_marker);
  int trim_entries(const utime_t& start_time, const utime_t& end_time,
                   const string& start_marker, const string& end_marker);
  int get_info(int shard_id, RGWDataChangesLogInfo *info);
  int lock_exclusive(int shard_id, utime_t& duration, string& zone_id, string& owner_id) {
    return store->lock_exclusive(store->zone.log_pool, oids[shard_id], duration, zone_id, owner_id);
  }
  int unlock(int shard_id, string& zone_id, string& owner_id) {
    return store->unlock(store->zone.log_pool, oids[shard_id], zone_id, owner_id);
  }
  struct LogMarker {
    int shard;
    string marker;

    LogMarker() : shard(0) {}
  };
  int list_entries(utime_t& start_time, utime_t& end_time, int max_entries,
               list<rgw_data_change>& entries, LogMarker& marker, bool *ptruncated);

  bool going_down();
};


#endif
