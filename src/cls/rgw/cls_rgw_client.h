// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/str_list.h"
#include "include/rados/librados.hpp"
#include "cls_rgw_ops.h"
#include "cls_rgw_const.h"
#include "common/RefCountedObj.h"
#include "include/compat.h"
#include "common/ceph_time.h"
#include "common/Mutex.h"
#include "common/Cond.h"

// Forward declaration
class BucketIndexAioManager;
/*
 * Bucket index AIO request argument, this is used to pass a argument
 * to callback.
 */
struct BucketIndexAioArg : public RefCountedObject {
  BucketIndexAioArg(int _id, BucketIndexAioManager* _manager) :
    id(_id), manager(_manager) {}
  int id;
  BucketIndexAioManager* manager;
};

/*
 * This class manages AIO completions. This class is not completely thread-safe,
 * methods like *get_next* is not thread-safe and is expected to be called from
 * within one thread.
 */
class BucketIndexAioManager {
private:
  map<int, librados::AioCompletion*> pendings;
  map<int, librados::AioCompletion*> completions;
  map<int, string> pending_objs;
  map<int, string> completion_objs;
  int next;
  Mutex lock;
  Cond cond;
  /*
   * Callback implementation for AIO request.
   */
  static void bucket_index_op_completion_cb(void* cb, void* arg) {
    BucketIndexAioArg* cb_arg = (BucketIndexAioArg*) arg;
    cb_arg->manager->do_completion(cb_arg->id);
    cb_arg->put();
  }

  /*
   * Get next request ID. This method is not thread-safe.
   *
   * Return next request ID.
   */
  int get_next() { return next++; }
    
  /*
   * Add a new pending AIO completion instance.
   *
   * @param id         - the request ID.
   * @param completion - the AIO completion instance.
   * @param oid        - the object id associated with the object, if it is NULL, we don't
   *                     track the object id per callback.
   */
  void add_pending(int id, librados::AioCompletion* completion, const string& oid) {
    pendings[id] = completion;
    pending_objs[id] = oid;
  }
public:
  /*
   * Create a new instance.
   */
  BucketIndexAioManager() : next(0), lock("BucketIndexAioManager::lock") {}


  /*
   * Do completion for the given AIO request.
   */
  void do_completion(int id);

  /*
   * Wait for AIO completions.
   *
   * valid_ret_code  - valid AIO return code.
   * num_completions - number of completions.
   * ret_code        - return code of failed AIO.
   * objs            - a list of objects that has been finished the AIO.
   *
   * Return false if there is no pending AIO, true otherwise.
   */
  bool wait_for_completions(int valid_ret_code, int *num_completions, int *ret_code,
      map<int, string> *objs);

  /**
   * Do aio read operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const string& oid, librados::ObjectReadOperation *op) {
    Mutex::Locker l(lock);
    BucketIndexAioArg *arg = new BucketIndexAioArg(get_next(), this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, NULL, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectReadOperation*)op, NULL);
    if (r >= 0) {
      add_pending(arg->id, c, oid);
    } else {
      c->release();
    }
    return r;
  }

  /**
   * Do aio write operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const string& oid, librados::ObjectWriteOperation *op) {
    Mutex::Locker l(lock);
    BucketIndexAioArg *arg = new BucketIndexAioArg(get_next(), this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, NULL, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectWriteOperation*)op);
    if (r >= 0) {
      add_pending(arg->id, c, oid);
    } else {
      c->release();
    }
    return r;
  }
};

class RGWGetDirHeader_CB : public RefCountedObject {
public:
  ~RGWGetDirHeader_CB() override {}
  virtual void handle_response(int r, rgw_bucket_dir_header& header) = 0;
};

class BucketIndexShardsManager {
private:
  // Per shard setting manager, for example, marker.
  map<int, string> value_by_shards;
public:
  const static string KEY_VALUE_SEPARATOR;
  const static string SHARDS_SEPARATOR;

  void add(int shard, const string& value) {
    value_by_shards[shard] = value;
  }

  const string& get(int shard, const string& default_value) {
    map<int, string>::iterator iter = value_by_shards.find(shard);
    return (iter == value_by_shards.end() ? default_value : iter->second);
  }

  map<int, string>& get() {
    return value_by_shards;
  }

  bool empty() {
    return value_by_shards.empty();
  }

  void to_string(string *out) const {
    if (!out) {
      return;
    }
    out->clear();
    map<int, string>::const_iterator iter = value_by_shards.begin();
    for (; iter != value_by_shards.end(); ++iter) {
      if (out->length()) {
        // Not the first item, append a separator first
        out->append(SHARDS_SEPARATOR);
      }
      char buf[16];
      snprintf(buf, sizeof(buf), "%d", iter->first);
      out->append(buf);
      out->append(KEY_VALUE_SEPARATOR);
      out->append(iter->second);
    }
  }

  static bool is_shards_marker(const string& marker) {
    return marker.find(KEY_VALUE_SEPARATOR) != string::npos;
  }

  /*
   * convert from string. There are two options of how the string looks like:
   *
   * 1. Single shard, no shard id specified, e.g. 000001.23.1
   *
   * for this case, if passed shard_id >= 0, use this shard id, otherwise assume that it's a
   * bucket with no shards.
   *
   * 2. One or more shards, shard id specified for each shard, e.g., 0#00002.12,1#00003.23.2
   *
   */
  int from_string(const string& composed_marker, int shard_id) {
    value_by_shards.clear();
    vector<string> shards;
    get_str_vec(composed_marker, SHARDS_SEPARATOR.c_str(), shards);
    if (shards.size() > 1 && shard_id >= 0) {
      return -EINVAL;
    }
    vector<string>::const_iterator iter = shards.begin();
    for (; iter != shards.end(); ++iter) {
      size_t pos = iter->find(KEY_VALUE_SEPARATOR);
      if (pos == string::npos) {
        if (!value_by_shards.empty()) {
          return -EINVAL;
        }
        if (shard_id < 0) {
          add(0, *iter);
        } else {
          add(shard_id, *iter);
        }
        return 0;
      }
      string shard_str = iter->substr(0, pos);
      string err;
      int shard = (int)strict_strtol(shard_str.c_str(), 10, &err);
      if (!err.empty()) {
        return -EINVAL;
      }
      add(shard, iter->substr(pos + 1));
    }
    return 0;
  }

  // trim the '<shard-id>#' prefix from a single shard marker if present
  static std::string get_shard_marker(const std::string& marker) {
    auto p = marker.find(KEY_VALUE_SEPARATOR);
    if (p == marker.npos) {
      return marker;
    }
    return marker.substr(p + 1);
  }
};

/* bucket index */
void cls_rgw_bucket_init_index(librados::ObjectWriteOperation& o);

class CLSRGWConcurrentIO {
protected:
  librados::IoCtx& io_ctx;
  map<int, string>& objs_container;
  map<int, string>::iterator iter;
  uint32_t max_aio;
  BucketIndexAioManager manager;

  virtual int issue_op(int shard_id, const string& oid) = 0;

  virtual void cleanup() {}
  virtual int valid_ret_code() { return 0; }
  // Return true if multiple rounds of OPs might be needed, this happens when
  // OP needs to be re-send until a certain code is returned.
  virtual bool need_multiple_rounds() { return false; }
  // Add a new object to the end of the container.
  virtual void add_object(int shard, const string& oid) {}
  virtual void reset_container(map<int, string>& objs) {}

public:

  CLSRGWConcurrentIO(librados::IoCtx& ioc,
		     map<int, string>& _objs_container,
		     uint32_t _max_aio) :
  io_ctx(ioc), objs_container(_objs_container), max_aio(_max_aio)
  {}

  virtual ~CLSRGWConcurrentIO()
  {}

  int operator()() {
    int ret = 0;
    iter = objs_container.begin();
    for (; iter != objs_container.end() && max_aio-- > 0; ++iter) {
      ret = issue_op(iter->first, iter->second);
      if (ret < 0)
        break;
    }

    int num_completions, r = 0;
    map<int, string> objs;
    map<int, string> *pobjs = (need_multiple_rounds() ? &objs : NULL);
    while (manager.wait_for_completions(valid_ret_code(), &num_completions, &r, pobjs)) {
      if (r >= 0 && ret >= 0) {
        for(int i = 0; i < num_completions && iter != objs_container.end(); ++i, ++iter) {
          int issue_ret = issue_op(iter->first, iter->second);
          if(issue_ret < 0) {
            ret = issue_ret;
            break;
          }
        }
      } else if (ret >= 0) {
        ret = r;
      }
      if (need_multiple_rounds() && iter == objs_container.end() && !objs.empty()) {
        // For those objects which need another round, use them to reset
        // the container
        reset_container(objs);
      }
    }

    if (ret < 0) {
      cleanup();
    }
    return ret;
  }
};

class CLSRGWIssueBucketIndexInit : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const string& oid) override;
  int valid_ret_code() override { return -EEXIST; }
  void cleanup() override;
public:
  CLSRGWIssueBucketIndexInit(librados::IoCtx& ioc, map<int, string>& _bucket_objs,
                     uint32_t _max_aio) :
    CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio) {}
};


class CLSRGWIssueBucketIndexClean : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const string& oid) override;
  int valid_ret_code() override {
    return -ENOENT;
  }

public:
  CLSRGWIssueBucketIndexClean(librados::IoCtx& ioc,
			      map<int, string>& _bucket_objs,
			      uint32_t _max_aio) :
  CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio)
  {}
};


class CLSRGWIssueSetTagTimeout : public CLSRGWConcurrentIO {
  uint64_t tag_timeout;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueSetTagTimeout(librados::IoCtx& ioc, map<int, string>& _bucket_objs,
                     uint32_t _max_aio, uint64_t _tag_timeout) :
    CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio), tag_timeout(_tag_timeout) {}
};

void cls_rgw_bucket_update_stats(librados::ObjectWriteOperation& o,
                                 bool absolute,
                                 const map<RGWObjCategory, rgw_bucket_category_stats>& stats);

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               const cls_rgw_obj_key& key, const string& locator, bool log_op,
                               uint16_t bilog_op, rgw_zone_set& zones_trace);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver,
                                const cls_rgw_obj_key& key,
                                rgw_bucket_dir_entry_meta& dir_meta,
				list<cls_rgw_obj_key> *remove_objs, bool log_op,
                                uint16_t bilog_op, rgw_zone_set *zones_trace);

void cls_rgw_remove_obj(librados::ObjectWriteOperation& o, list<string>& keep_attr_prefixes);
void cls_rgw_obj_store_pg_ver(librados::ObjectWriteOperation& o, const string& attr);
void cls_rgw_obj_check_attrs_prefix(librados::ObjectOperation& o, const string& prefix, bool fail_if_exist);
void cls_rgw_obj_check_mtime(librados::ObjectOperation& o, const ceph::real_time& mtime, bool high_precision_time, RGWCheckMTimeType type);

int cls_rgw_bi_get(librados::IoCtx& io_ctx, const string oid,
                   BIIndexType index_type, cls_rgw_obj_key& key,
                   rgw_cls_bi_entry *entry);
int cls_rgw_bi_put(librados::IoCtx& io_ctx, const string oid, rgw_cls_bi_entry& entry);
void cls_rgw_bi_put(librados::ObjectWriteOperation& op, const string oid, rgw_cls_bi_entry& entry);
int cls_rgw_bi_list(librados::IoCtx& io_ctx, const string oid,
                   const string& name, const string& marker, uint32_t max,
                   list<rgw_cls_bi_entry> *entries, bool *is_truncated);


int cls_rgw_bucket_link_olh(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op,
                            const string& oid, const cls_rgw_obj_key& key, bufferlist& olh_tag,
                            bool delete_marker, const string& op_tag, rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op, rgw_zone_set& zones_trace);
int cls_rgw_bucket_unlink_instance(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op,
                                   const string& oid, const cls_rgw_obj_key& key, const string& op_tag,
                                   const string& olh_tag, uint64_t olh_epoch, bool log_op, rgw_zone_set& zones_trace);
int cls_rgw_get_olh_log(librados::IoCtx& io_ctx, string& oid, librados::ObjectReadOperation& op, const cls_rgw_obj_key& olh, uint64_t ver_marker,
                        const string& olh_tag,
                        map<uint64_t, vector<rgw_bucket_olh_log_entry> > *log, bool *is_truncated);
void cls_rgw_trim_olh_log(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, uint64_t ver, const string& olh_tag);
int cls_rgw_clear_olh(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op, string& oid, const cls_rgw_obj_key& olh, const string& olh_tag);

/**
 * List the bucket with the starting object and filter prefix.
 * NOTE: this method do listing requests for each bucket index shards identified by
 *       the keys of the *list_results* map, which means the map should be popludated
 *       by the caller to fill with each bucket index object id.
 *
 * io_ctx        - IO context for rados.
 * start_obj     - marker for the listing.
 * filter_prefix - filter prefix.
 * num_entries   - number of entries to request for each object (note the total
 *                 amount of entries returned depends on the number of shardings).
 * list_results  - the list results keyed by bucket index object id.
 * max_aio       - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
*/

class CLSRGWIssueBucketList : public CLSRGWConcurrentIO {
  cls_rgw_obj_key start_obj;
  string filter_prefix;
  uint32_t num_entries;
  bool list_versions;
  map<int, rgw_cls_list_ret>& result;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueBucketList(librados::IoCtx& io_ctx, const cls_rgw_obj_key& _start_obj,
                        const string& _filter_prefix, uint32_t _num_entries,
                        bool _list_versions,
                        map<int, string>& oids,
                        map<int, rgw_cls_list_ret>& list_results,
                        uint32_t max_aio) :
  CLSRGWConcurrentIO(io_ctx, oids, max_aio),
  start_obj(_start_obj), filter_prefix(_filter_prefix), num_entries(_num_entries), list_versions(_list_versions), result(list_results) {}
};

class CLSRGWIssueBILogList : public CLSRGWConcurrentIO {
  map<int, cls_rgw_bi_log_list_ret>& result;
  BucketIndexShardsManager& marker_mgr;
  uint32_t max;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueBILogList(librados::IoCtx& io_ctx, BucketIndexShardsManager& _marker_mgr, uint32_t _max,
                       map<int, string>& oids,
                       map<int, cls_rgw_bi_log_list_ret>& bi_log_lists, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, oids, max_aio), result(bi_log_lists),
    marker_mgr(_marker_mgr), max(_max) {}
};

class CLSRGWIssueBILogTrim : public CLSRGWConcurrentIO {
  BucketIndexShardsManager& start_marker_mgr;
  BucketIndexShardsManager& end_marker_mgr;
protected:
  int issue_op(int shard_id, const string& oid) override;
  // Trim until -ENODATA is returned.
  int valid_ret_code() override { return -ENODATA; }
  bool need_multiple_rounds() override { return true; }
  void add_object(int shard, const string& oid) override { objs_container[shard] = oid; }
  void reset_container(map<int, string>& objs) override {
    objs_container.swap(objs);
    iter = objs_container.begin();
    objs.clear();
  }
public:
  CLSRGWIssueBILogTrim(librados::IoCtx& io_ctx, BucketIndexShardsManager& _start_marker_mgr,
      BucketIndexShardsManager& _end_marker_mgr, map<int, string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio),
    start_marker_mgr(_start_marker_mgr), end_marker_mgr(_end_marker_mgr) {}
};

/**
 * Check the bucket index.
 *
 * io_ctx          - IO context for rados.
 * bucket_objs_ret - check result for all shards.
 * max_aio         - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
 */
class CLSRGWIssueBucketCheck : public CLSRGWConcurrentIO /*<map<string, rgw_cls_check_index_ret> >*/ {
  map<int, rgw_cls_check_index_ret>& result;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueBucketCheck(librados::IoCtx& ioc, map<int, string>& oids,
			 map<int, rgw_cls_check_index_ret>& bucket_objs_ret,
			 uint32_t _max_aio) :
    CLSRGWConcurrentIO(ioc, oids, _max_aio), result(bucket_objs_ret) {}
};

class CLSRGWIssueBucketRebuild : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueBucketRebuild(librados::IoCtx& io_ctx, map<int, string>& bucket_objs,
                           uint32_t max_aio) : CLSRGWConcurrentIO(io_ctx, bucket_objs, max_aio) {}
};

class CLSRGWIssueGetDirHeader : public CLSRGWConcurrentIO {
  map<int, rgw_cls_list_ret>& result;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueGetDirHeader(librados::IoCtx& io_ctx, map<int, string>& oids, map<int, rgw_cls_list_ret>& dir_headers,
                          uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, oids, max_aio), result(dir_headers) {}
};

class CLSRGWIssueSetBucketResharding : public CLSRGWConcurrentIO {
  cls_rgw_bucket_instance_entry entry;
protected:
  int issue_op(int shard_id, const string& oid) override;
public:
  CLSRGWIssueSetBucketResharding(librados::IoCtx& ioc, map<int, string>& _bucket_objs,
                                 const cls_rgw_bucket_instance_entry& _entry,
                                 uint32_t _max_aio) : CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio), entry(_entry) {}
};

class CLSRGWIssueResyncBucketBILog : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const string& oid);
public:
  CLSRGWIssueResyncBucketBILog(librados::IoCtx& io_ctx, map<int, string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio) {}
};

class CLSRGWIssueBucketBILogStop : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const string& oid);
public:
  CLSRGWIssueBucketBILogStop(librados::IoCtx& io_ctx, map<int, string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio) {}
};

int cls_rgw_get_dir_header_async(librados::IoCtx& io_ctx, string& oid, RGWGetDirHeader_CB *ctx);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, bufferlist& updates);

/* usage logging */
int cls_rgw_usage_log_read(librados::IoCtx& io_ctx, const string& oid, const string& user, const string& bucket,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, string& read_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated);

int cls_rgw_usage_log_trim(librados::IoCtx& io_ctx, const string& oid, const string& user, const string& bucket,
                           uint64_t start_epoch, uint64_t end_epoch);

void cls_rgw_usage_log_clear(librados::ObjectWriteOperation& op);
void cls_rgw_usage_log_add(librados::ObjectWriteOperation& op, rgw_usage_log_info& info);

/* garbage collection */
void cls_rgw_gc_set_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);
void cls_rgw_gc_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag);

int cls_rgw_gc_list(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated, string& next_marker);

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const vector<string>& tags);

/* lifecycle */
int cls_rgw_lc_get_head(librados::IoCtx& io_ctx, const string& oid, cls_rgw_lc_obj_head& head);
int cls_rgw_lc_put_head(librados::IoCtx& io_ctx, const string& oid, cls_rgw_lc_obj_head& head);
int cls_rgw_lc_get_next_entry(librados::IoCtx& io_ctx, const string& oid, string& marker, pair<string, int>& entry);
int cls_rgw_lc_rm_entry(librados::IoCtx& io_ctx, const string& oid, const pair<string, int>& entry);
int cls_rgw_lc_set_entry(librados::IoCtx& io_ctx, const string& oid, const pair<string, int>& entry);
int cls_rgw_lc_get_entry(librados::IoCtx& io_ctx, const string& oid, const std::string& marker, rgw_lc_entry_t& entry);
int cls_rgw_lc_list(librados::IoCtx& io_ctx, const string& oid,
                    const string& marker,
                    uint32_t max_entries,
                    map<string, int>& entries);

/* resharding */
void cls_rgw_reshard_add(librados::ObjectWriteOperation& op, const cls_rgw_reshard_entry& entry);
int cls_rgw_reshard_list(librados::IoCtx& io_ctx, const string& oid, string& marker, uint32_t max,
                         list<cls_rgw_reshard_entry>& entries, bool* is_truncated);
int cls_rgw_reshard_get(librados::IoCtx& io_ctx, const string& oid, cls_rgw_reshard_entry& entry);
int cls_rgw_reshard_get_head(librados::IoCtx& io_ctx, const string& oid, cls_rgw_reshard_entry& entry);
void cls_rgw_reshard_remove(librados::ObjectWriteOperation& op, const cls_rgw_reshard_entry& entry);

/* resharding attribute on bucket index shard headers */
int cls_rgw_set_bucket_resharding(librados::IoCtx& io_ctx, const string& oid,
                                  const cls_rgw_bucket_instance_entry& entry);
int cls_rgw_clear_bucket_resharding(librados::IoCtx& io_ctx, const string& oid);
void cls_rgw_guard_bucket_resharding(librados::ObjectOperation& op, int ret_err);
int cls_rgw_get_bucket_resharding(librados::IoCtx& io_ctx, const string& oid,
                                  cls_rgw_bucket_instance_entry *entry);

#endif
