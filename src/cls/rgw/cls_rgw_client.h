// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "include/str_list.h"
#include "include/rados/librados.hpp"
#include "cls_rgw_ops.h"
#include "cls_rgw_const.h"
#include "common/RefCountedObj.h"
#include "common/strtol.h"
#include "include/compat.h"
#include "common/ceph_time.h"
#include "common/ceph_mutex.h"


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
 * This class manages AIO completions. This class is not completely
 * thread-safe, methods like *get_next_request_id* is not thread-safe
 * and is expected to be called from within one thread.
 */
class BucketIndexAioManager {
public:

  // allows us to reaccess the shard id and shard's oid during and
  // after the asynchronous call is made
  struct RequestObj {
    int shard_id;
    std::string oid;

    RequestObj(int _shard_id, const std::string& _oid) :
      shard_id(_shard_id), oid(_oid)
    {/* empty */}
  };


private:
  // NB: the following 4 maps use the request_id as the key; this
  // is not the same as the shard_id!
  std::map<int, librados::AioCompletion*> pendings;
  std::map<int, librados::AioCompletion*> completions;
  std::map<int, const RequestObj> pending_objs;
  std::map<int, const RequestObj> completion_objs;

  int next = 0;
  ceph::mutex lock = ceph::make_mutex("BucketIndexAioManager::lock");
  ceph::condition_variable cond;
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
  int get_next_request_id() { return next++; }

  /*
   * Add a new pending AIO completion instance.
   *
   * @param id         - the request ID.
   * @param completion - the AIO completion instance.
   * @param oid        - the object id associated with the object, if it is NULL, we don't
   *                     track the object id per callback.
   */
  void add_pending(int request_id, librados::AioCompletion* completion, const int shard_id, const std::string& oid) {
    pendings[request_id] = completion;
    pending_objs.emplace(request_id, RequestObj(shard_id, oid));
  }

public:
  /*
   * Create a new instance.
   */
  BucketIndexAioManager() = default;

  /*
   * Do completion for the given AIO request.
   */
  void do_completion(int request_id);

  /*
   * Wait for AIO completions.
   *
   * valid_ret_code  - valid AIO return code.
   * num_completions - number of completions.
   * ret_code        - return code of failed AIO.
   * objs            - a std::list of objects that has been finished the AIO.
   *
   * Return false if there is no pending AIO, true otherwise.
   */
  bool wait_for_completions(int valid_ret_code,
			    int *num_completions = nullptr,
			    int *ret_code = nullptr,
			    std::map<int, std::string> *completed_objs = nullptr,
			    std::map<int, std::string> *retry_objs = nullptr);

  /**
   * Do aio read operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const int shard_id, const std::string& oid, librados::ObjectReadOperation *op) {
    std::lock_guard l{lock};
    const int request_id = get_next_request_id();
    BucketIndexAioArg *arg = new BucketIndexAioArg(request_id, this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectReadOperation*)op, NULL);
    if (r >= 0) {
      add_pending(arg->id, c, shard_id, oid);
    } else {
      arg->put();
      c->release();
    }
    return r;
  }

  /**
   * Do aio write operation.
   */
  bool aio_operate(librados::IoCtx& io_ctx, const int shard_id, const std::string& oid, librados::ObjectWriteOperation *op) {
    std::lock_guard l{lock};
    const int request_id = get_next_request_id();
    BucketIndexAioArg *arg = new BucketIndexAioArg(request_id, this);
    librados::AioCompletion *c = librados::Rados::aio_create_completion((void*)arg, bucket_index_op_completion_cb);
    int r = io_ctx.aio_operate(oid, c, (librados::ObjectWriteOperation*)op);
    if (r >= 0) {
      add_pending(arg->id, c, shard_id, oid);
    } else {
      arg->put();
      c->release();
    }
    return r;
  }
};

class RGWGetDirHeader_CB : public boost::intrusive_ref_counter<RGWGetDirHeader_CB> {
public:
  virtual ~RGWGetDirHeader_CB() {}
  virtual void handle_response(int r, const rgw_bucket_dir_header& header) = 0;
};

class BucketIndexShardsManager {
private:
  // Per shard setting manager, for example, marker.
  std::map<int, std::string> value_by_shards;
public:
  const static std::string KEY_VALUE_SEPARATOR;
  const static std::string SHARDS_SEPARATOR;

  void add(int shard, const std::string& value) {
    value_by_shards[shard] = value;
  }

  const std::string& get(int shard, const std::string& default_value) const {
    auto iter = value_by_shards.find(shard);
    return (iter == value_by_shards.end() ? default_value : iter->second);
  }

  const std::map<int, std::string>& get() const {
    return value_by_shards;
  }
  std::map<int, std::string>& get() {
    return value_by_shards;
  }

  bool empty() const {
    return value_by_shards.empty();
  }

  void to_string(std::string *out) const {
    if (!out) {
      return;
    }
    out->clear();
    for (auto iter = value_by_shards.begin();
	 iter != value_by_shards.end(); ++iter) {
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

  static bool is_shards_marker(const std::string& marker) {
    return marker.find(KEY_VALUE_SEPARATOR) != std::string::npos;
  }

  /*
   * convert from std::string. There are two options of how the std::string looks like:
   *
   * 1. Single shard, no shard id specified, e.g. 000001.23.1
   *
   * for this case, if passed shard_id >= 0, use this shard id, otherwise assume that it's a
   * bucket with no shards.
   *
   * 2. One or more shards, shard id specified for each shard, e.g., 0#00002.12,1#00003.23.2
   *
   */
  int from_string(std::string_view composed_marker, int shard_id) {
    value_by_shards.clear();
    std::vector<std::string> shards;
    get_str_vec(composed_marker, SHARDS_SEPARATOR.c_str(), shards);
    if (shards.size() > 1 && shard_id >= 0) {
      return -EINVAL;
    }
    for (auto iter = shards.begin(); iter != shards.end(); ++iter) {
      size_t pos = iter->find(KEY_VALUE_SEPARATOR);
      if (pos == std::string::npos) {
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
      std::string shard_str = iter->substr(0, pos);
      std::string err;
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

  // map of shard # to oid; the shards that are remaining to be processed
  std::map<int, std::string>& objs_container;
  // iterator to work through objs_container
  std::map<int, std::string>::iterator iter;

  uint32_t max_aio;
  BucketIndexAioManager manager;

  virtual int issue_op(int shard_id, const std::string& oid) = 0;

  virtual void cleanup() {}
  virtual int valid_ret_code() { return 0; }
  // Return true if multiple rounds of OPs might be needed, this happens when
  // OP needs to be re-send until a certain code is returned.
  virtual bool need_multiple_rounds() { return false; }
  // Add a new object to the end of the container.
  virtual void add_object(int shard, const std::string& oid) {}
  virtual void reset_container(std::map<int, std::string>& objs) {}

public:

  CLSRGWConcurrentIO(librados::IoCtx& ioc,
		     std::map<int, std::string>& _objs_container,
		     uint32_t _max_aio) :
  io_ctx(ioc), objs_container(_objs_container), max_aio(_max_aio)
  {}

  virtual ~CLSRGWConcurrentIO() {}

  int operator()();
}; // class CLSRGWConcurrentIO


class CLSRGWIssueBucketIndexInit : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const std::string& oid) override;
  int valid_ret_code() override { return -EEXIST; }
  void cleanup() override;
public:
  CLSRGWIssueBucketIndexInit(librados::IoCtx& ioc,
			     std::map<int, std::string>& _bucket_objs,
			     uint32_t _max_aio) :
    CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio) {}
  virtual ~CLSRGWIssueBucketIndexInit() override {}
};


class CLSRGWIssueBucketIndexClean : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const std::string& oid) override;
  int valid_ret_code() override {
    return -ENOENT;
  }

public:
  CLSRGWIssueBucketIndexClean(librados::IoCtx& ioc,
			      std::map<int, std::string>& _bucket_objs,
			      uint32_t _max_aio) :
  CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio)
  {}
  virtual ~CLSRGWIssueBucketIndexClean() override {}
};


class CLSRGWIssueSetTagTimeout : public CLSRGWConcurrentIO {
  uint64_t tag_timeout;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueSetTagTimeout(librados::IoCtx& ioc, std::map<int, std::string>& _bucket_objs,
                     uint32_t _max_aio, uint64_t _tag_timeout) :
    CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio), tag_timeout(_tag_timeout) {}
  virtual ~CLSRGWIssueSetTagTimeout() override {}
};

void cls_rgw_bucket_update_stats(librados::ObjectWriteOperation& o,
                                 bool absolute,
                                 const std::map<RGWObjCategory, rgw_bucket_category_stats>& stats);

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, RGWModifyOp op, const std::string& tag,
                               const cls_rgw_obj_key& key, const std::string& locator, bool log_op,
                               uint16_t bilog_op, const rgw_zone_set& zones_trace);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, RGWModifyOp op, const std::string& tag,
                                const rgw_bucket_entry_ver& ver,
                                const cls_rgw_obj_key& key,
                                const rgw_bucket_dir_entry_meta& dir_meta,
				const std::list<cls_rgw_obj_key> *remove_objs, bool log_op,
                                uint16_t bilog_op, const rgw_zone_set *zones_trace,
				const std::string& obj_locator = ""); // ignored if it's the empty string

void cls_rgw_remove_obj(librados::ObjectWriteOperation& o, std::list<std::string>& keep_attr_prefixes);
void cls_rgw_obj_store_pg_ver(librados::ObjectWriteOperation& o, const std::string& attr);
void cls_rgw_obj_check_attrs_prefix(librados::ObjectOperation& o, const std::string& prefix, bool fail_if_exist);
void cls_rgw_obj_check_mtime(librados::ObjectOperation& o, const ceph::real_time& mtime, bool high_precision_time, RGWCheckMTimeType type);

int cls_rgw_bi_get(librados::IoCtx& io_ctx, const std::string oid,
                   BIIndexType index_type, const cls_rgw_obj_key& key,
                   rgw_cls_bi_entry *entry);
int cls_rgw_bi_put(librados::IoCtx& io_ctx, const std::string oid, const rgw_cls_bi_entry& entry);
void cls_rgw_bi_put(librados::ObjectWriteOperation& op, const std::string oid, const rgw_cls_bi_entry& entry);
int cls_rgw_bi_list(librados::IoCtx& io_ctx, const std::string& oid,
                   const std::string& name, const std::string& marker, uint32_t max,
                   std::list<rgw_cls_bi_entry> *entries, bool *is_truncated);


void cls_rgw_bucket_link_olh(librados::ObjectWriteOperation& op,
                            const cls_rgw_obj_key& key, const ceph::buffer::list& olh_tag,
                            bool delete_marker, const std::string& op_tag, const rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op, const rgw_zone_set& zones_trace);
void cls_rgw_bucket_unlink_instance(librados::ObjectWriteOperation& op,
                                   const cls_rgw_obj_key& key, const std::string& op_tag,
                                   const std::string& olh_tag, uint64_t olh_epoch, bool log_op, const rgw_zone_set& zones_trace);
void cls_rgw_get_olh_log(librados::ObjectReadOperation& op, const cls_rgw_obj_key& olh, uint64_t ver_marker, const std::string& olh_tag, rgw_cls_read_olh_log_ret& log_ret, int& op_ret);
void cls_rgw_trim_olh_log(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, uint64_t ver, const std::string& olh_tag);
void cls_rgw_clear_olh(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, const std::string& olh_tag);

// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_bucket_link_olh(librados::IoCtx& io_ctx, const std::string& oid,
                            const cls_rgw_obj_key& key, const ceph::buffer::list& olh_tag,
                            bool delete_marker, const std::string& op_tag, const rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op, const rgw_zone_set& zones_trace);
int cls_rgw_bucket_unlink_instance(librados::IoCtx& io_ctx, const std::string& oid,
                                   const cls_rgw_obj_key& key, const std::string& op_tag,
                                   const std::string& olh_tag, uint64_t olh_epoch, bool log_op, const rgw_zone_set& zones_trace);
int cls_rgw_get_olh_log(librados::IoCtx& io_ctx, std::string& oid, const cls_rgw_obj_key& olh, uint64_t ver_marker,
                        const std::string& olh_tag, rgw_cls_read_olh_log_ret& log_ret);
int cls_rgw_clear_olh(librados::IoCtx& io_ctx, std::string& oid, const cls_rgw_obj_key& olh, const std::string& olh_tag);
int cls_rgw_usage_log_trim(librados::IoCtx& io_ctx, const std::string& oid, const std::string& user, const std::string& bucket,
                           uint64_t start_epoch, uint64_t end_epoch);
#endif


/**
 * Std::list the bucket with the starting object and filter prefix.
 * NOTE: this method do listing requests for each bucket index shards identified by
 *       the keys of the *list_results* std::map, which means the std::map should be populated
 *       by the caller to fill with each bucket index object id.
 *
 * io_ctx        - IO context for rados.
 * start_obj     - marker for the listing.
 * filter_prefix - filter prefix.
 * num_entries   - number of entries to request for each object (note the total
 *                 amount of entries returned depends on the number of shardings).
 * list_results  - the std::list results keyed by bucket index object id.
 * max_aio       - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
*/

class CLSRGWIssueBucketList : public CLSRGWConcurrentIO {
  cls_rgw_obj_key start_obj;
  std::string filter_prefix;
  std::string delimiter;
  uint32_t num_entries;
  bool list_versions;
  std::map<int, rgw_cls_list_ret>& result; // request_id -> return value

protected:
  int issue_op(int shard_id, const std::string& oid) override;
  void reset_container(std::map<int, std::string>& objs) override;

public:
  CLSRGWIssueBucketList(librados::IoCtx& io_ctx,
			const cls_rgw_obj_key& _start_obj,
                        const std::string& _filter_prefix,
			const std::string& _delimiter,
			uint32_t _num_entries,
                        bool _list_versions,
                        std::map<int, std::string>& oids, // shard_id -> shard_oid
			// shard_id -> return value
                        std::map<int, rgw_cls_list_ret>& list_results,
                        uint32_t max_aio) :
  CLSRGWConcurrentIO(io_ctx, oids, max_aio),
    start_obj(_start_obj), filter_prefix(_filter_prefix), delimiter(_delimiter),
    num_entries(_num_entries), list_versions(_list_versions),
    result(list_results)
  {}
};

void cls_rgw_bucket_list_op(librados::ObjectReadOperation& op,
                            const cls_rgw_obj_key& start_obj,
                            const std::string& filter_prefix,
			    const std::string& delimiter,
                            uint32_t num_entries,
                            bool list_versions,
                            rgw_cls_list_ret* result);

void cls_rgw_bilog_list(librados::ObjectReadOperation& op,
                        const std::string& marker, uint32_t max,
                        cls_rgw_bi_log_list_ret *pdata, int *ret = nullptr);

class CLSRGWIssueBILogList : public CLSRGWConcurrentIO {
  std::map<int, cls_rgw_bi_log_list_ret>& result;
  BucketIndexShardsManager& marker_mgr;
  uint32_t max;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueBILogList(librados::IoCtx& io_ctx, BucketIndexShardsManager& _marker_mgr, uint32_t _max,
                       std::map<int, std::string>& oids,
                       std::map<int, cls_rgw_bi_log_list_ret>& bi_log_lists, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, oids, max_aio), result(bi_log_lists),
    marker_mgr(_marker_mgr), max(_max) {}
  virtual ~CLSRGWIssueBILogList() override {}
};

void cls_rgw_bilog_trim(librados::ObjectWriteOperation& op,
                        const std::string& start_marker,
                        const std::string& end_marker);

class CLSRGWIssueBILogTrim : public CLSRGWConcurrentIO {
  BucketIndexShardsManager& start_marker_mgr;
  BucketIndexShardsManager& end_marker_mgr;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
  // Trim until -ENODATA is returned.
  int valid_ret_code() override { return -ENODATA; }
  bool need_multiple_rounds() override { return true; }
  void add_object(int shard, const std::string& oid) override { objs_container[shard] = oid; }
  void reset_container(std::map<int, std::string>& objs) override {
    objs_container.swap(objs);
    iter = objs_container.begin();
    objs.clear();
  }
public:
  CLSRGWIssueBILogTrim(librados::IoCtx& io_ctx, BucketIndexShardsManager& _start_marker_mgr,
      BucketIndexShardsManager& _end_marker_mgr, std::map<int, std::string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio),
    start_marker_mgr(_start_marker_mgr), end_marker_mgr(_end_marker_mgr) {}
  virtual ~CLSRGWIssueBILogTrim() override {}
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
class CLSRGWIssueBucketCheck : public CLSRGWConcurrentIO /*<std::map<std::string, rgw_cls_check_index_ret> >*/ {
  std::map<int, rgw_cls_check_index_ret>& result;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueBucketCheck(librados::IoCtx& ioc, std::map<int, std::string>& oids,
			 std::map<int, rgw_cls_check_index_ret>& bucket_objs_ret,
			 uint32_t _max_aio) :
    CLSRGWConcurrentIO(ioc, oids, _max_aio), result(bucket_objs_ret) {}
  virtual ~CLSRGWIssueBucketCheck() override {}
};

class CLSRGWIssueBucketRebuild : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueBucketRebuild(librados::IoCtx& io_ctx, std::map<int, std::string>& bucket_objs,
                           uint32_t max_aio) : CLSRGWConcurrentIO(io_ctx, bucket_objs, max_aio) {}
  virtual ~CLSRGWIssueBucketRebuild() override {}
};

class CLSRGWIssueGetDirHeader : public CLSRGWConcurrentIO {
  std::map<int, rgw_cls_list_ret>& result;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueGetDirHeader(librados::IoCtx& io_ctx, std::map<int, std::string>& oids, std::map<int, rgw_cls_list_ret>& dir_headers,
                          uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, oids, max_aio), result(dir_headers) {}
  virtual ~CLSRGWIssueGetDirHeader() override {}
};

class CLSRGWIssueSetBucketResharding : public CLSRGWConcurrentIO {
  cls_rgw_bucket_instance_entry entry;
protected:
  int issue_op(int shard_id, const std::string& oid) override;
public:
  CLSRGWIssueSetBucketResharding(librados::IoCtx& ioc, std::map<int, std::string>& _bucket_objs,
                                 const cls_rgw_bucket_instance_entry& _entry,
                                 uint32_t _max_aio) : CLSRGWConcurrentIO(ioc, _bucket_objs, _max_aio), entry(_entry) {}
  virtual ~CLSRGWIssueSetBucketResharding() override {}
};

class CLSRGWIssueResyncBucketBILog : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const std::string& oid);
public:
  CLSRGWIssueResyncBucketBILog(librados::IoCtx& io_ctx, std::map<int, std::string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio) {}
  virtual ~CLSRGWIssueResyncBucketBILog() override {}
};

class CLSRGWIssueBucketBILogStop : public CLSRGWConcurrentIO {
protected:
  int issue_op(int shard_id, const std::string& oid);
public:
  CLSRGWIssueBucketBILogStop(librados::IoCtx& io_ctx, std::map<int, std::string>& _bucket_objs, uint32_t max_aio) :
    CLSRGWConcurrentIO(io_ctx, _bucket_objs, max_aio) {}
  virtual ~CLSRGWIssueBucketBILogStop() override {}
};

int cls_rgw_get_dir_header_async(librados::IoCtx& io_ctx, const std::string& oid,
                                 boost::intrusive_ptr<RGWGetDirHeader_CB> cb);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, ceph::buffer::list& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, ceph::buffer::list& updates);

/* usage logging */
// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_usage_log_read(librados::IoCtx& io_ctx, const std::string& oid, const std::string& user, const std::string& bucket,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, std::string& read_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated);
#endif

void cls_rgw_usage_log_trim(librados::ObjectWriteOperation& op, const std::string& user, const std::string& bucket, uint64_t start_epoch, uint64_t end_epoch);

void cls_rgw_usage_log_clear(librados::ObjectWriteOperation& op);
void cls_rgw_usage_log_add(librados::ObjectWriteOperation& op, rgw_usage_log_info& info);

/* garbage collection */
void cls_rgw_gc_set_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);
void cls_rgw_gc_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const std::string& tag);
void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const std::vector<std::string>& tags);

// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_gc_list(librados::IoCtx& io_ctx, std::string& oid, std::string& marker, uint32_t max, bool expired_only,
                    std::list<cls_rgw_gc_obj_info>& entries, bool *truncated, std::string& next_marker);
#endif

/* lifecycle */
// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_lc_get_head(librados::IoCtx& io_ctx, const std::string& oid, cls_rgw_lc_obj_head& head);
int cls_rgw_lc_put_head(librados::IoCtx& io_ctx, const std::string& oid, cls_rgw_lc_obj_head& head);
int cls_rgw_lc_get_next_entry(librados::IoCtx& io_ctx, const std::string& oid, const std::string& marker, cls_rgw_lc_entry& entry);
int cls_rgw_lc_rm_entry(librados::IoCtx& io_ctx, const std::string& oid, const cls_rgw_lc_entry& entry);
int cls_rgw_lc_set_entry(librados::IoCtx& io_ctx, const std::string& oid, const cls_rgw_lc_entry& entry);
int cls_rgw_lc_get_entry(librados::IoCtx& io_ctx, const std::string& oid, const std::string& marker, cls_rgw_lc_entry& entry);
int cls_rgw_lc_list(librados::IoCtx& io_ctx, const std::string& oid,
		    const std::string& marker, uint32_t max_entries,
                    std::vector<cls_rgw_lc_entry>& entries);
#endif

/* multipart */
void cls_rgw_mp_upload_part_info_update(librados::ObjectWriteOperation& op, const std::string& part_key, const RGWUploadPartInfo& info);

/* resharding */
void cls_rgw_reshard_add(librados::ObjectWriteOperation& op,
			 const cls_rgw_reshard_entry& entry,
			 const bool create_only);
void cls_rgw_reshard_remove(librados::ObjectWriteOperation& op, const cls_rgw_reshard_entry& entry);
// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_reshard_list(librados::IoCtx& io_ctx, const std::string& oid, std::string& marker, uint32_t max,
                         std::list<cls_rgw_reshard_entry>& entries, bool* is_truncated);
int cls_rgw_reshard_get(librados::IoCtx& io_ctx, const std::string& oid, cls_rgw_reshard_entry& entry);
#endif

/* resharding attribute on bucket index shard headers */
void cls_rgw_guard_bucket_resharding(librados::ObjectOperation& op, int ret_err);
// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_rgw_set_bucket_resharding(librados::IoCtx& io_ctx, const std::string& oid,
                                  const cls_rgw_bucket_instance_entry& entry);
int cls_rgw_clear_bucket_resharding(librados::IoCtx& io_ctx, const std::string& oid);
int cls_rgw_get_bucket_resharding(librados::IoCtx& io_ctx, const std::string& oid,
                                  cls_rgw_bucket_instance_entry *entry);
#endif
