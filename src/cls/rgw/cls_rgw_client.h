#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/types.h"
#include "include/str_list.h"
#include "include/rados/librados.hpp"
#include "cls_rgw_types.h"
#include "cls_rgw_ops.h"
#include "common/RefCountedObj.h"

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
  list<librados::AioCompletion*> completions;
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
   */
  void add_pending(int id, librados::AioCompletion* completion) {
    Mutex::Locker l(lock);
    pendings[id] = completion;
  }
public:
  /*
   * Create a new instance.
   */
  BucketIndexAioManager() : pendings(), completions(), next(0),
      lock("BucketIndexAioManager::lock"), cond() {}

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
   *
   * Return false if there is no pending AIO, true otherwise.
   */
  bool wait_for_completions(int valid_ret_code, int *num_completions, int *ret_code);

  /**
   * Do aio read operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const string& oid, librados::ObjectReadOperation *op) {
    BucketIndexAioArg *arg = new BucketIndexAioArg(get_next(), this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, NULL, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectReadOperation*)op, NULL);
    if (r >= 0) {
      add_pending(arg->id, c);
    }
    return r;
  }

  /**
   * Do aio write operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const string& oid, librados::ObjectWriteOperation *op) {
    BucketIndexAioArg *arg = new BucketIndexAioArg(get_next(), this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, NULL, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectWriteOperation*)op);
    if (r >= 0) {
      add_pending(arg->id, c);
    }
    return r;
  }
};

class RGWGetDirHeader_CB : public RefCountedObject {
public:
  virtual ~RGWGetDirHeader_CB() {}
  virtual void handle_response(int r, rgw_bucket_dir_header& header) = 0;
};

class BucketIndexShardsManager {
private:
  // Per shard setting manager, for example, marker.
  map<string, string> value_by_shards;
public:
  const static string KEY_VALUE_SEPARATOR;
  const static string SHARDS_SEPARATOR;

  void add(const string& shard, const string& value) {
    value_by_shards[shard] = value;
  }

  const string& get(const string& shard, const string& default_value) {
    map<string, string>::iterator iter = value_by_shards.find(shard);
    return (iter == value_by_shards.end() ? default_value : iter->second);
  }

  bool empty() {
    return value_by_shards.empty();
  }

  void to_string(string *out) const {
    if (out) {
      map<string, string>::const_iterator iter = value_by_shards.begin();
      // No shards
      if (value_by_shards.size() == 1) {
        *out = iter->second;
      } else {
        for (; iter != value_by_shards.end(); ++iter) {
          if (out->length()) {
            // Not the first item, append a separator first
            out->append(SHARDS_SEPARATOR);
          }
          out->append(iter->first);
          out->append(KEY_VALUE_SEPARATOR);
          out->append(iter->second);
        }
      }
    }
  }

  int from_string(const string& composed_marker, bool has_shards, const string& oid) {
    value_by_shards.clear();
    if (!has_shards) {
      add(oid, composed_marker);
    } else {
      list<string> shards;
      get_str_list(composed_marker, SHARDS_SEPARATOR.c_str(), shards);
      list<string>::const_iterator iter = shards.begin();
      for (; iter != shards.end(); ++iter) {
        size_t pos = iter->find(KEY_VALUE_SEPARATOR);
        if (pos == string::npos)
          return -EINVAL;
        string name = iter->substr(0, pos);
        value_by_shards[name] = iter->substr(pos + 1, iter->length() - pos - 1);
      }
    }
    return 0;
  }
};

/* bucket index */
void cls_rgw_bucket_init(librados::ObjectWriteOperation& o);

template<class T>
class CLSRGWConcurrentIO {
protected:
  librados::IoCtx& io_ctx;
  T& objs_container;
  typename T::iterator iter;
  uint32_t max_aio;
  BucketIndexAioManager manager;

  virtual int issue_op() = 0;

  virtual void cleanup() {}
  virtual int valid_ret_code() { return 0; }

public:
  CLSRGWConcurrentIO(librados::IoCtx& ioc, T& _objs_container,
                     uint32_t _max_aio) : io_ctx(ioc), objs_container(_objs_container), max_aio(_max_aio) {}
  virtual ~CLSRGWConcurrentIO() {}

  int operator()() {
    int ret = 0;
    iter = objs_container.begin();
    for (; iter != objs_container.end() && max_aio-- > 0; ++iter) {
      ret = issue_op();
      if (ret < 0)
        break;
    }

    int num_completions, r = 0;
    while (manager.wait_for_completions(0, &num_completions, &r)) {
      if (r >= 0 && ret >= 0) {
        for(int i = 0; i < num_completions && iter != objs_container.end(); ++i, ++iter) {
          int issue_ret = issue_op();
          if(issue_ret < 0) {
            ret = issue_ret;
            break;
          }
        }
      } else if (ret >= 0) {
        ret = r;
      }
    }

    if (ret < 0) {
      cleanup();
    }
    return ret;
  }
};

class CLSRGWIssueBucketIndexInit : public CLSRGWConcurrentIO<vector<string> > {
protected:
  int issue_op();
  int valid_ret_code() { return -EEXIST; }
  void cleanup();
public:
  CLSRGWIssueBucketIndexInit(librados::IoCtx& ioc, vector<string>& _bucket_objs,
                     uint32_t _max_aio) :
    CLSRGWConcurrentIO<vector<string> >(ioc, _bucket_objs, _max_aio) {}
};

class CLSRGWIssueSetTagTimeout : public CLSRGWConcurrentIO<vector<string> > {
  uint64_t tag_timeout;
protected:
  int issue_op();
public:
  CLSRGWIssueSetTagTimeout(librados::IoCtx& ioc, vector<string>& _bucket_objs,
                     uint32_t _max_aio, uint64_t _tag_timeout) :
    CLSRGWConcurrentIO<vector<string> >(ioc, _bucket_objs, _max_aio),
    tag_timeout(_tag_timeout) {}
};

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               string& name, string& locator, bool log_op);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver, string& name, rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs, bool log_op);

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

class CLSRGWIssueBucketList : public CLSRGWConcurrentIO<map<string, rgw_cls_list_ret> > {
  string start_obj;
  string filter_prefix;
  uint32_t num_entries;
protected:
  int issue_op();
public:
  CLSRGWIssueBucketList(librados::IoCtx& io_ctx, const string& _start_obj,
                        const string& _filter_prefix, uint32_t _num_entries,
                        map<string, struct rgw_cls_list_ret>& list_results,
                        uint32_t max_aio) :
  CLSRGWConcurrentIO<map<string, rgw_cls_list_ret> > (io_ctx, list_results, max_aio),
  start_obj(_start_obj), filter_prefix(_filter_prefix), num_entries(_num_entries) {}
};

class CLSRGWIssueBILogList : public CLSRGWConcurrentIO<map<string, cls_rgw_bi_log_list_ret> > {
  BucketIndexShardsManager& marker_mgr;
  uint32_t max;
protected:
  int issue_op();
public:
  CLSRGWIssueBILogList(librados::IoCtx& io_ctx, BucketIndexShardsManager& _marker_mgr, uint32_t _max,
      map<string, struct cls_rgw_bi_log_list_ret>& bi_log_lists, uint32_t max_aio) :
    CLSRGWConcurrentIO<map<string, cls_rgw_bi_log_list_ret> >(io_ctx, bi_log_lists, max_aio),
    marker_mgr(_marker_mgr), max(_max) {}
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
class CLSRGWIssueBucketCheck : public CLSRGWConcurrentIO<map<string, struct rgw_cls_check_index_ret> > {
protected:
  int issue_op();
public:
  CLSRGWIssueBucketCheck(librados::IoCtx& ioc, map<string, struct rgw_cls_check_index_ret>& bucket_objs_ret,
                     uint32_t _max_aio) :
    CLSRGWConcurrentIO<map<string, struct rgw_cls_check_index_ret> >(ioc, bucket_objs_ret, _max_aio) {}
};

class CLSRGWIssueBucketRebuild : public CLSRGWConcurrentIO<vector<string> > {
protected:
  int issue_op();
public:
  CLSRGWIssueBucketRebuild(librados::IoCtx& io_ctx, vector<string>& bucket_objs,
                           uint32_t max_aio) : CLSRGWConcurrentIO<vector<string> >(io_ctx, bucket_objs, max_aio) {}
};

class CLSRGWIssueGetDirHeader : public CLSRGWConcurrentIO<map<string, rgw_cls_list_ret> > {
protected:
  int issue_op();
public:
  CLSRGWIssueGetDirHeader(librados::IoCtx& io_ctx, map<string, rgw_cls_list_ret>& dir_headers,
                          uint32_t max_aio) :
    CLSRGWConcurrentIO<map<string, rgw_cls_list_ret> >(io_ctx, dir_headers, max_aio) {}
};

int cls_rgw_get_dir_header(librados::IoCtx& io_ctx, map<string, rgw_cls_list_ret>& dir_headers,
    uint32_t max_aio);
int cls_rgw_get_dir_header_async(librados::IoCtx& io_ctx, string& oid, RGWGetDirHeader_CB *ctx);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, bufferlist& updates);

int cls_rgw_bi_log_trim(librados::IoCtx& io_ctx, string& oid, string& start_marker, string& end_marker);

/* usage logging */
int cls_rgw_usage_log_read(librados::IoCtx& io_ctx, string& oid, string& user,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated);

void cls_rgw_usage_log_trim(librados::ObjectWriteOperation& op, string& user,
                           uint64_t start_epoch, uint64_t end_epoch);

void cls_rgw_usage_log_add(librados::ObjectWriteOperation& op, rgw_usage_log_info& info);

/* garbage collection */
void cls_rgw_gc_set_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);
void cls_rgw_gc_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag);

int cls_rgw_gc_list(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated);

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const list<string>& tags);

#endif
