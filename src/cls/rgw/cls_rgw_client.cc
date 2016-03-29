// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "include/types.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "include/rados/librados.hpp"

#include "common/debug.h"

using namespace librados;

const string BucketIndexShardsManager::KEY_VALUE_SEPARATOR = "#";
const string BucketIndexShardsManager::SHARDS_SEPARATOR = ",";

/**
 * This class represents the bucket index object operation callback context.
 */
template <typename T>
class ClsBucketIndexOpCtx : public ObjectOperationCompletion {
private:
  T *data;
  int *ret_code;
public:
  ClsBucketIndexOpCtx(T* _data, int *_ret_code) : data(_data), ret_code(_ret_code) { assert(data); }
  ~ClsBucketIndexOpCtx() {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode((*data), iter);
      } catch (buffer::error& err) {
        r = -EIO;
      }
    }
    if (ret_code) {
      *ret_code = r;
    }
  }
};

void BucketIndexAioManager::do_completion(int id) {
  Mutex::Locker l(lock);

  map<int, librados::AioCompletion*>::iterator iter = pendings.find(id);
  assert(iter != pendings.end());
  completions[id] = iter->second;
  pendings.erase(iter);

  // If the caller needs a list of finished objects, store them
  // for further processing
  map<int, string>::iterator miter = pending_objs.find(id);
  if (miter != pending_objs.end()) {
    completion_objs[id] = miter->second;
    pending_objs.erase(miter);
  }

  cond.Signal();
}

bool BucketIndexAioManager::wait_for_completions(int valid_ret_code,
    int *num_completions, int *ret_code, map<int, string> *objs) {
  lock.Lock();
  if (pendings.empty() && completions.empty()) {
    lock.Unlock();
    return false;
  }

  if (completions.empty()) {
    // Wait for AIO completion
    cond.Wait(lock);
  }

  // Clear the completed AIOs
  map<int, librados::AioCompletion*>::iterator iter = completions.begin();
  for (; iter != completions.end(); ++iter) {
    int r = iter->second->get_return_value();
    if (objs && r == 0) { /* update list of successfully completed objs */
      map<int, string>::iterator liter = completion_objs.find(iter->first);
      if (liter != completion_objs.end()) {
        (*objs)[liter->first] = liter->second;
      }
    }
    if (ret_code && (r < 0 && r != valid_ret_code))
      (*ret_code) = r;
    iter->second->release();
  }
  if (num_completions)
    (*num_completions) = completions.size();
  completions.clear();
  lock.Unlock();

  return true;
}

void cls_rgw_bucket_init(ObjectWriteOperation& o)
{
  bufferlist in;
  o.exec("rgw", "bucket_init_index", in);
}

static bool issue_bucket_index_init_op(librados::IoCtx& io_ctx,
    const string& oid, BucketIndexAioManager *manager) {
  bufferlist in;
  librados::ObjectWriteOperation op;
  op.create(true);
  op.exec("rgw", "bucket_init_index", in);
  return manager->aio_operate(io_ctx, oid, &op);
}

static bool issue_bucket_set_tag_timeout_op(librados::IoCtx& io_ctx,
    const string& oid, uint64_t timeout, BucketIndexAioManager *manager) {
  bufferlist in;
  struct rgw_cls_tag_timeout_op call;
  call.tag_timeout = timeout;
  ::encode(call, in);
  ObjectWriteOperation op;
  op.exec("rgw", "bucket_set_tag_timeout", in);
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBucketIndexInit::issue_op(int shard_id, const string& oid)
{
  return issue_bucket_index_init_op(io_ctx, oid, &manager);
}

void CLSRGWIssueBucketIndexInit::cleanup()
{
  // Do best effort removal
  for (map<int, string>::iterator citer = objs_container.begin(); citer != iter; ++citer) {
    io_ctx.remove(citer->second);
  }
}

int CLSRGWIssueSetTagTimeout::issue_op(int shard_id, const string& oid)
{
  return issue_bucket_set_tag_timeout_op(io_ctx, oid, tag_timeout, &manager);
}

void cls_rgw_bucket_prepare_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               const cls_rgw_obj_key& key, const string& locator, bool log_op,
                               uint16_t bilog_flags)
{
  struct rgw_cls_obj_prepare_op call;
  call.op = op;
  call.tag = tag;
  call.key = key;
  call.locator = locator;
  call.log_op = log_op;
  call.bilog_flags = bilog_flags;
  bufferlist in;
  ::encode(call, in);
  o.exec("rgw", "bucket_prepare_op", in);
}

void cls_rgw_bucket_complete_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver,
                                const cls_rgw_obj_key& key,
                                rgw_bucket_dir_entry_meta& dir_meta,
				list<cls_rgw_obj_key> *remove_objs, bool log_op,
                                uint16_t bilog_flags)
{

  bufferlist in;
  struct rgw_cls_obj_complete_op call;
  call.op = op;
  call.tag = tag;
  call.key = key;
  call.ver = ver;
  call.meta = dir_meta;
  call.log_op = log_op;
  call.bilog_flags = bilog_flags;
  if (remove_objs)
    call.remove_objs = *remove_objs;
  ::encode(call, in);
  o.exec("rgw", "bucket_complete_op", in);
}

static bool issue_bucket_list_op(librados::IoCtx& io_ctx,
    const string& oid, const cls_rgw_obj_key& start_obj, const string& filter_prefix,
    uint32_t num_entries, bool list_versions, BucketIndexAioManager *manager,
    struct rgw_cls_list_ret *pdata) {
  bufferlist in;
  struct rgw_cls_list_op call;
  call.start_obj = start_obj;
  call.filter_prefix = filter_prefix;
  call.num_entries = num_entries;
  call.list_versions = list_versions;
  ::encode(call, in);

  librados::ObjectReadOperation op;
  op.exec("rgw", "bucket_list", in, new ClsBucketIndexOpCtx<struct rgw_cls_list_ret>(pdata, NULL));
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBucketList::issue_op(int shard_id, const string& oid)
{
  return issue_bucket_list_op(io_ctx, oid, start_obj, filter_prefix, num_entries, list_versions, &manager, &result[shard_id]);
}

void cls_rgw_remove_obj(librados::ObjectWriteOperation& o, list<string>& keep_attr_prefixes)
{
  bufferlist in;
  struct rgw_cls_obj_remove_op call;
  call.keep_attr_prefixes = keep_attr_prefixes;
  ::encode(call, in);
  o.exec("rgw", "obj_remove", in);
}

void cls_rgw_obj_store_pg_ver(librados::ObjectWriteOperation& o, const string& attr)
{
  bufferlist in;
  struct rgw_cls_obj_store_pg_ver_op call;
  call.attr = attr;
  ::encode(call, in);
  o.exec("rgw", "obj_store_pg_ver", in);
}

void cls_rgw_obj_check_attrs_prefix(librados::ObjectOperation& o, const string& prefix, bool fail_if_exist)
{
  bufferlist in;
  struct rgw_cls_obj_check_attrs_prefix call;
  call.check_prefix = prefix;
  call.fail_if_exist = fail_if_exist;
  ::encode(call, in);
  o.exec("rgw", "obj_check_attrs_prefix", in);
}

void cls_rgw_obj_check_mtime(librados::ObjectOperation& o, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type)
{
  bufferlist in;
  struct rgw_cls_obj_check_mtime call;
  call.mtime = mtime;
  call.high_precision_time = high_precision_time;
  call.type = type;
  ::encode(call, in);
  o.exec("rgw", "obj_check_mtime", in);
}

int cls_rgw_bi_get(librados::IoCtx& io_ctx, const string oid,
                   BIIndexType index_type, cls_rgw_obj_key& key,
                   rgw_cls_bi_entry *entry)
{
  bufferlist in, out;
  struct rgw_cls_bi_get_op call;
  call.key = key;
  call.type = index_type;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "bi_get", in, out);
  if (r < 0)
    return r;

  struct rgw_cls_bi_get_ret op_ret;
  bufferlist::iterator iter = out.begin();
  try {
    ::decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  *entry = op_ret.entry;

  return 0;
}

int cls_rgw_bi_put(librados::IoCtx& io_ctx, const string oid, rgw_cls_bi_entry& entry)
{
  bufferlist in, out;
  struct rgw_cls_bi_put_op call;
  call.entry = entry;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "bi_put", in, out);
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_bi_list(librados::IoCtx& io_ctx, const string oid,
                   const string& name, const string& marker, uint32_t max,
                   list<rgw_cls_bi_entry> *entries, bool *is_truncated)
{
  bufferlist in, out;
  struct rgw_cls_bi_list_op call;
  call.name = name;
  call.marker = marker;
  call.max = max;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "bi_list", in, out);
  if (r < 0)
    return r;

  struct rgw_cls_bi_list_ret op_ret;
  bufferlist::iterator iter = out.begin();
  try {
    ::decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries->swap(op_ret.entries);
  *is_truncated = op_ret.is_truncated;

  return 0;
}

int cls_rgw_bucket_link_olh(librados::IoCtx& io_ctx, const string& oid, const cls_rgw_obj_key& key, bufferlist& olh_tag,
                            bool delete_marker, const string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op)
{
  bufferlist in, out;
  struct rgw_cls_link_olh_op call;
  call.key = key;
  call.olh_tag = string(olh_tag.c_str(), olh_tag.length());
  call.op_tag = op_tag;
  call.delete_marker = delete_marker;
  if (meta) {
    call.meta = *meta;
  }
  call.olh_epoch = olh_epoch;
  call.log_op = log_op;
  call.unmod_since = unmod_since;
  call.high_precision_time = high_precision_time;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "bucket_link_olh", in, out);
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_bucket_unlink_instance(librados::IoCtx& io_ctx, const string& oid,
                                   const cls_rgw_obj_key& key, const string& op_tag,
                                   const string& olh_tag, uint64_t olh_epoch, bool log_op)
{
  bufferlist in, out;
  struct rgw_cls_unlink_instance_op call;
  call.key = key;
  call.op_tag = op_tag;
  call.olh_epoch = olh_epoch;
  call.olh_tag = olh_tag;
  call.log_op = log_op;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "bucket_unlink_instance", in, out);
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_get_olh_log(IoCtx& io_ctx, string& oid, librados::ObjectReadOperation& op, const cls_rgw_obj_key& olh, uint64_t ver_marker,
                        const string& olh_tag,
                        map<uint64_t, vector<struct rgw_bucket_olh_log_entry> > *log, bool *is_truncated)
{
  bufferlist in, out;
  struct rgw_cls_read_olh_log_op call;
  call.olh = olh;
  call.ver_marker = ver_marker;
  call.olh_tag = olh_tag;
  ::encode(call, in);
  int op_ret;
  op.exec("rgw", "bucket_read_olh_log", in, &out, &op_ret);
  int r = io_ctx.operate(oid, &op, NULL);
  if (r < 0) {
    return r;
  }
  if (op_ret < 0) {
    return op_ret;
  }

  struct rgw_cls_read_olh_log_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  if (log) {
    *log = ret.log;
  }
  if (is_truncated) {
    *is_truncated = ret.is_truncated;
  }

 return r;
}

void cls_rgw_trim_olh_log(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, uint64_t ver, const string& olh_tag)
{
  bufferlist in;
  struct rgw_cls_trim_olh_log_op call;
  call.olh = olh;
  call.ver = ver;
  call.olh_tag = olh_tag;
  ::encode(call, in);
  op.exec("rgw", "bucket_trim_olh_log", in);
}

int cls_rgw_clear_olh(IoCtx& io_ctx, string& oid, const cls_rgw_obj_key& olh, const string& olh_tag)
{
  bufferlist in, out;
  struct rgw_cls_bucket_clear_olh_op call;
  call.key = olh;
  call.olh_tag = olh_tag;
  ::encode(call, in);
  librados::ObjectWriteOperation op;
  int op_ret;
  op.exec("rgw", "bucket_clear_olh", in, &out, &op_ret);
  int r = io_ctx.operate(oid, &op);
  if (r < 0) {
    return r;
  }
  return op_ret;
}

static bool issue_bi_log_list_op(librados::IoCtx& io_ctx, const string& oid, int shard_id,
                                 BucketIndexShardsManager& marker_mgr, uint32_t max, BucketIndexAioManager *manager,
    struct cls_rgw_bi_log_list_ret *pdata) {
  bufferlist in;
  cls_rgw_bi_log_list_op call;
  call.marker = marker_mgr.get(shard_id, "");
  call.max = max;
  ::encode(call, in);

  librados::ObjectReadOperation op;
  op.exec("rgw", "bi_log_list", in, new ClsBucketIndexOpCtx<struct cls_rgw_bi_log_list_ret>(pdata, NULL));
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBILogList::issue_op(int shard_id, const string& oid)
{
  return issue_bi_log_list_op(io_ctx, oid, shard_id, marker_mgr, max, &manager, &result[shard_id]);
}

static bool issue_bi_log_trim(librados::IoCtx& io_ctx, const string& oid, int shard_id,
                              BucketIndexShardsManager& start_marker_mgr,
                              BucketIndexShardsManager& end_marker_mgr, BucketIndexAioManager *manager) {
  bufferlist in;
  cls_rgw_bi_log_trim_op call;
  call.start_marker = start_marker_mgr.get(shard_id, "");
  call.end_marker = end_marker_mgr.get(shard_id, "");
  ::encode(call, in);
  ObjectWriteOperation op;
  op.exec("rgw", "bi_log_trim", in);
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBILogTrim::issue_op(int shard_id, const string& oid)
{
  return issue_bi_log_trim(io_ctx, oid, shard_id, start_marker_mgr, end_marker_mgr, &manager);
}

static bool issue_bucket_check_index_op(IoCtx& io_ctx, const string& oid, BucketIndexAioManager *manager,
    struct rgw_cls_check_index_ret *pdata) {
  bufferlist in;
  librados::ObjectReadOperation op;
  op.exec("rgw", "bucket_check_index", in, new ClsBucketIndexOpCtx<struct rgw_cls_check_index_ret>(
        pdata, NULL));
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBucketCheck::issue_op(int shard_id, const string& oid)
{
  return issue_bucket_check_index_op(io_ctx, oid, &manager, &result[shard_id]);
}

static bool issue_bucket_rebuild_index_op(IoCtx& io_ctx, const string& oid,
    BucketIndexAioManager *manager) {
  bufferlist in;
  librados::ObjectWriteOperation op;
  op.exec("rgw", "bucket_rebuild_index", in);
  return manager->aio_operate(io_ctx, oid, &op);
}

int CLSRGWIssueBucketRebuild::issue_op(int shard_id, const string& oid)
{
  return issue_bucket_rebuild_index_op(io_ctx, oid, &manager);
}

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates)
{
  updates.append(op);
  ::encode(dirent, updates);
}

void cls_rgw_suggest_changes(ObjectWriteOperation& o, bufferlist& updates)
{
  o.exec("rgw", "dir_suggest_changes", updates);
}

int CLSRGWIssueGetDirHeader::issue_op(int shard_id, const string& oid)
{
  cls_rgw_obj_key nokey;
  return issue_bucket_list_op(io_ctx, oid, nokey, "", 0, false, &manager, &result[shard_id]);
}

class GetDirHeaderCompletion : public ObjectOperationCompletion {
  RGWGetDirHeader_CB *ret_ctx;
public:
  explicit GetDirHeaderCompletion(RGWGetDirHeader_CB *_ctx) : ret_ctx(_ctx) {}
  ~GetDirHeaderCompletion() {
    ret_ctx->put();
  }
  void handle_completion(int r, bufferlist& outbl) {
    struct rgw_cls_list_ret ret;
    try {
      bufferlist::iterator iter = outbl.begin();
      ::decode(ret, iter);
    } catch (buffer::error& err) {
      r = -EIO;
    }

    ret_ctx->handle_response(r, ret.dir.header);
  }
};

int cls_rgw_get_dir_header_async(IoCtx& io_ctx, string& oid, RGWGetDirHeader_CB *ctx)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.num_entries = 0;
  ::encode(call, in);
  ObjectReadOperation op;
  GetDirHeaderCompletion *cb = new GetDirHeaderCompletion(ctx);
  op.exec("rgw", "bucket_list", in, cb);
  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int r = io_ctx.aio_operate(oid, c, &op, NULL);
  c->release();
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_usage_log_read(IoCtx& io_ctx, string& oid, string& user,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated)
{
  if (is_truncated)
    *is_truncated = false;

  bufferlist in, out;
  rgw_cls_usage_log_read_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.owner = user;
  call.max_entries = max_entries;
  call.iter = read_iter;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "user_usage_log_read", in, out);
  if (r < 0)
    return r;

  try {
    rgw_cls_usage_log_read_ret result;
    bufferlist::iterator iter = out.begin();
    ::decode(result, iter);
    read_iter = result.next_iter;
    if (is_truncated)
      *is_truncated = result.truncated;

    usage = result.usage;
  } catch (buffer::error& e) {
    return -EINVAL;
  }

  return 0;
}

void cls_rgw_usage_log_trim(ObjectWriteOperation& op, string& user,
                           uint64_t start_epoch, uint64_t end_epoch)
{
  bufferlist in;
  rgw_cls_usage_log_trim_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.user = user;
  ::encode(call, in);
  op.exec("rgw", "user_usage_log_trim", in);
}

void cls_rgw_usage_log_add(ObjectWriteOperation& op, rgw_usage_log_info& info)
{
  bufferlist in;
  rgw_cls_usage_log_add_op call;
  call.info = info;
  ::encode(call, in);
  op.exec("rgw", "user_usage_log_add", in);
}

/* garbage collection */

void cls_rgw_gc_set_entry(ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_set_entry_op call;
  call.expiration_secs = expiration_secs;
  call.info = info;
  ::encode(call, in);
  op.exec("rgw", "gc_set_entry", in);
}

void cls_rgw_gc_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag)
{
  bufferlist in;
  cls_rgw_gc_defer_entry_op call;
  call.expiration_secs = expiration_secs;
  call.tag = tag;
  ::encode(call, in);
  op.exec("rgw", "gc_defer_entry", in);
}

int cls_rgw_gc_list(IoCtx& io_ctx, string& oid, string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated)
{
  bufferlist in, out;
  cls_rgw_gc_list_op call;
  call.marker = marker;
  call.max = max;
  call.expired_only = expired_only;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "rgw", "gc_list", in, out);
  if (r < 0)
    return r;

  cls_rgw_gc_list_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries.swap(ret.entries);

  if (truncated)
    *truncated = ret.truncated;

 return r;
}

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const list<string>& tags)
{
  bufferlist in;
  cls_rgw_gc_remove_op call;
  call.tags = tags;
  ::encode(call, in);
  op.exec("rgw", "gc_remove", in);
}
