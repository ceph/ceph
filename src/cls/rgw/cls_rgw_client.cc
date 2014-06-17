#include <errno.h>

#include <boost/algorithm/string.hpp>

#include "include/types.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "include/rados/librados.hpp"

#include "common/debug.h"

using namespace librados;

void cls_rgw_bucket_init(ObjectWriteOperation& o)
{
  bufferlist in;
  o.exec("rgw", "bucket_init_index", in);
}

int cls_rgw_bucket_set_tag_timeout(librados::IoCtx& io_ctx, const vector<string>& bucket_objs,
        uint64_t tag_timeout, uint32_t max_io)
{
  int ret = 0;
  vector<librados::AioCompletion*> completions;
  vector<string>::const_iterator iter = bucket_objs.begin();
  while (iter != bucket_objs.end()) {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (; iter != bucket_objs.end() && max_aio_requests-- > 0; ++iter) {
      bufferlist in;
      struct rgw_cls_tag_timeout_op call;
      call.tag_timeout = tag_timeout;
      ::encode(call, in);
      ObjectWriteOperation op;
      op.exec("rgw", "bucket_set_tag_timeout", in);
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operation
      ret = io_ctx.aio_operate(*iter, c, &op, NULL);
      if (ret < 0)
        return ret;
    }
    ret = cls_rgw_util_wait_for_complete(completions);
    if (ret < 0)
      return ret;
  }
  return 0;
}

void cls_rgw_bucket_prepare_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               string& name, string& locator, bool log_op)
{
  struct rgw_cls_obj_prepare_op call;
  call.op = op;
  call.tag = tag;
  call.name = name;
  call.locator = locator;
  call.log_op = log_op;
  bufferlist in;
  ::encode(call, in);
  o.exec("rgw", "bucket_prepare_op", in);
}

void cls_rgw_bucket_complete_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver, string& name, rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs, bool log_op)
{

  bufferlist in;
  struct rgw_cls_obj_complete_op call;
  call.op = op;
  call.tag = tag;
  call.name = name;
  call.ver = ver;
  call.meta = dir_meta;
  call.log_op = log_op;
  if (remove_objs)
    call.remove_objs = *remove_objs;
  ::encode(call, in);
  o.exec("rgw", "bucket_complete_op", in);
}

/**
 * This class represents the bucket index object callback context.
 */
template<typename T>
class ClsBucketIndexOpCtx : public ObjectOperationCompletion {
private:
  T* data;
  // Return code of the operation
  int* ret_code;
public:
  ClsBucketIndexOpCtx(T* _data, int *_ret_code) : data(_data), ret_code(_ret_code) {}
  ~ClsBucketIndexOpCtx() {}

  // The completion callback, fill the response data
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

int cls_rgw_list_op(IoCtx& io_ctx, const string& start_obj,
        const string& filter_prefix, uint32_t num_entries,
        map<string, struct rgw_cls_list_ret>& list_results, uint32_t max_aio)
{
  int ret;
  vector<librados::AioCompletion*> completions;

  map<string, struct rgw_cls_list_ret>::iterator liter = list_results.begin();
  while (liter != list_results.end()) {
    uint32_t max_aio_requests = max_aio;
    completions.clear();
    for (; liter != list_results.end() && max_aio_requests-- > 0; ++liter) {
      bufferlist in;
      struct rgw_cls_list_op call;
      call.start_obj = start_obj;
      call.filter_prefix = filter_prefix;
      call.num_entries = num_entries;
      ::encode(call, in);

      librados::ObjectReadOperation op;
      op.exec("rgw", "bucket_list", in, new ClsBucketIndexOpCtx<struct rgw_cls_list_ret>(
            &liter->second, NULL));
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operate
      io_ctx.aio_operate(liter->first, c, &op, NULL);
    }
    ret = cls_rgw_util_wait_for_complete_and_cb(completions);
    if (ret < 0)
      return ret;
  }

  return 0;
}

int cls_rgw_bucket_check_index_op(IoCtx& io_ctx,
        map<string, struct rgw_cls_check_index_ret>& results, uint32_t max_io)
{
  int ret;
  vector<librados::AioCompletion*> completions;
  bufferlist in, out;
  map<string, struct rgw_cls_check_index_ret>::iterator iter = results.begin();
  while (iter != results.end()) {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (; iter != results.end() && max_aio_requests-- > 0; ++iter) {
      librados::ObjectReadOperation op;
      op.exec("rgw", "bucket_check_index", in, new ClsBucketIndexOpCtx<struct rgw_cls_check_index_ret>(
                  &iter->second, NULL));
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operation
      io_ctx.aio_operate(iter->first, c, &op, NULL);
    }
    ret = cls_rgw_util_wait_for_complete_and_cb(completions);
    if (ret < 0)
      return ret;
  }
  return 0;
}

int cls_rgw_bucket_remove_objs_op(librados::IoCtx& io_ctx,
        const map<string, vector<rgw_bucket_dir_entry> >& updates, uint32_t max_io)
{
  vector<librados::AioCompletion*> completions;
  map<string, vector<rgw_bucket_dir_entry> >::const_iterator iter = updates.begin();
  while (iter != updates.end()) {
      completions.clear();
      uint32_t max_aio_requests = max_io;
      for (; iter != updates.end() && max_aio_requests-- > 0; ++iter) {
        bufferlist update;
        vector<rgw_bucket_dir_entry>::const_iterator viter;
        for (viter = (iter->second).begin(); viter != (iter->second).end(); ++viter) {
          update.append(CEPH_RGW_REMOVE);
          ::encode(*viter, update);
        }
        librados::ObjectWriteOperation op;
        op.exec("rgw", "dir_suggest_changes", update);
        AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
        completions.push_back(c);

        // Do AIO operation
        io_ctx.aio_operate(iter->first, c, &op, NULL);
      }
      int r = cls_rgw_util_wait_for_complete(completions);
      if (r < 0)
        return r;
  }
  return 0;
}

int cls_rgw_bucket_index_init_op(librados::IoCtx &io_ctx,
        const vector<string>& bucket_objs, uint32_t max_io)
{
  int ret = 0;
  vector<librados::AioCompletion*> completions;
  vector<string> created_objs;
  vector<string>::const_iterator iter = bucket_objs.begin();
  bufferlist in;
  while (iter != bucket_objs.end()) {
    int max_aio_requests = max_io;
    completions.clear();
    for (; iter != bucket_objs.end() && max_aio_requests-- > 0; ++iter) {
      librados::ObjectWriteOperation op;
      op.create(true);
      op.exec("rgw", "bucket_init_index", in);
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operation
      io_ctx.aio_operate(*iter, c, &op);
    }

    for (size_t i = 0; i < completions.size(); ++i) {
      completions[i]->wait_for_safe();
      int aio_ret = completions[i]->get_return_value();
      if (aio_ret < 0 && aio_ret != -EEXIST) {
        ret = aio_ret;
      }
      completions[i]->release();
      created_objs.push_back(bucket_objs[i]);
    }
    if (ret < 0) {
      // Do best effort removal
      for (iter = created_objs.begin(); iter != created_objs.end(); ++iter) {
        io_ctx.remove(*iter);
      }
      break;
    }
  }

  return ret;
}

int cls_rgw_bucket_rebuild_index_op(IoCtx& io_ctx, const vector<string>& bucket_objs,
        uint32_t max_io)
{
  int ret;
  vector<librados::AioCompletion*> completions;
  bufferlist in;

  vector<string>::const_iterator iter = bucket_objs.begin();
  while (iter != bucket_objs.end()) {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (; iter != bucket_objs.end() && max_aio_requests-- > 0; ++iter) {
      librados::ObjectWriteOperation op;
      op.exec("rgw", "bucket_rebuild_index", in);
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operation
      io_ctx.aio_operate(*iter, c, &op, NULL);
  }
  ret = cls_rgw_util_wait_for_complete(completions);
  if (ret < 0)
    return ret;
  }
  return 0;
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

int cls_rgw_get_dir_header(IoCtx& io_ctx, map<string, rgw_cls_list_ret>& list_results,
        uint32_t max_io)
{
  int ret;
  vector<librados::AioCompletion*> completions;
  map<string, rgw_cls_list_ret>::iterator iter = list_results.begin();
  while (iter != list_results.end()) {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (; iter != list_results.end() && max_aio_requests-- > 0; ++iter) {
      bufferlist in;
      struct rgw_cls_list_op call;
      call.num_entries = 0;
      ::encode(call, in);

      librados::ObjectReadOperation op;
      op.exec("rgw", "bucket_list", in, new ClsBucketIndexOpCtx<struct rgw_cls_list_ret>(
                  &iter->second, NULL));
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operate
      io_ctx.aio_operate(iter->first, c, &op, NULL);
    }
    ret = cls_rgw_util_wait_for_complete_and_cb(completions);
    if (ret < 0)
      return ret;
  }
  return 0;
}

class GetDirHeaderCompletion : public ObjectOperationCompletion {
  RGWGetDirHeader_CB *ret_ctx;
public:
  GetDirHeaderCompletion(RGWGetDirHeader_CB *_ctx) : ret_ctx(_ctx) {}
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

int cls_rgw_bi_log_list(IoCtx& io_ctx, map<string, cls_rgw_bi_log_list_ret>& list_results,
        string& marker, uint32_t max, uint32_t max_io)
{
  int ret;
  // If it is a composed marker (for each bucket index shard), let us get marker
  // for each shard first
  map<string, string> markers;
  if (marker.find('#') != string::npos) {
    ret = cls_rgw_util_parse_markers(marker, markers);
    if (ret < 0)
      return ret;
  }

  uint32_t num = max / (list_results.size());
  vector<librados::AioCompletion*> completions;
  map<string, cls_rgw_bi_log_list_ret>::iterator iter = list_results.begin();
  while (iter != list_results.end()) {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (; iter != list_results.end() && max_aio_requests-- > 0; ++iter) {
      bufferlist in, out;
      cls_rgw_bi_log_list_op call;
      if (!markers.empty()) {
        // It requires a marker for each shard
        if (!markers.count(iter->first))
          return -EINVAL;
        call.marker = markers[iter->first];
      } else {
        call.marker = marker;
      }
      if (iter == list_results.begin()) {
        call.max = num + (max % (list_results.size()));
      } else {
        call.max = num;
      }
      ::encode(call, in);

      librados::ObjectReadOperation op;
      op.exec("rgw", "bi_log_list", in, new ClsBucketIndexOpCtx<cls_rgw_bi_log_list_ret>(
                  &iter->second, NULL));
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions.push_back(c);

      // Do AIO operation
      io_ctx.aio_operate(iter->first, c, &op, NULL);
    }
    ret = cls_rgw_util_wait_for_complete_and_cb(completions);
    if (ret < 0)
      return ret;
  }
  return 0;
}

int cls_rgw_bi_log_trim(IoCtx& io_ctx, vector<string>& bucket_objs, string& start_marker, string& end_marker, uint32_t max_io)
{
  int ret = 0;
  map<string, string> start_markers;
  map<string, string> end_markers;
  if (start_marker.find('#') != string::npos) {
    ret = cls_rgw_util_parse_markers(start_marker, start_markers);
    if (ret < 0)
      return ret;
    ret = cls_rgw_util_parse_markers(end_marker, end_markers);
    if (ret < 0)
      return ret;
    // Validation
    vector<string>::iterator viter;
    for (viter = bucket_objs.begin(); viter != bucket_objs.end(); ++viter) {
       if (start_markers.count(*viter) == 0)
         return -EINVAL;
       if (end_markers.count(*viter) == 0)
         return -EINVAL;
    }
  }

  map<librados::AioCompletion*, string> completions;
  map<librados::AioCompletion*, string>::iterator miter;
  vector<string>::iterator iter;
  do {
    completions.clear();
    uint32_t max_aio_requests = max_io;
    for (iter = bucket_objs.begin(); iter != bucket_objs.end() && max_aio_requests-- > 0; ++iter)
    {
      bufferlist in, out;
      cls_rgw_bi_log_trim_op call;
      if (!start_markers.empty()) {
        call.start_marker = start_markers[*iter];
        call.end_marker = end_markers[*iter];
      } else {
        call.start_marker = start_marker;
        call.end_marker = end_marker;
      }
      ::encode(call, in);
      ObjectWriteOperation op;
      op.exec("rgw", "bi_log_trim", in);
      AioCompletion* c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      completions[c] = *iter;

      // Do AIO request
      ret = io_ctx.aio_operate(*iter, c, &op, NULL);
      if (ret < 0)
        return ret;
    }

    // Wait for completions
    for (miter = completions.begin(); miter != completions.end(); ++miter) {
      miter->first->wait_for_complete();
      int r = miter->first->get_return_value();
      if (r == -ENODATA) {
        bucket_objs.erase(std::remove(bucket_objs.begin(), bucket_objs.end(), miter->second), bucket_objs.end());
      } else if (ret < 0) {
        ret = r;
      }
      miter->first->release();
    }
  } while (!bucket_objs.empty());

  return ret;
}

int cls_rgw_usage_log_read(IoCtx& io_ctx, string& oid, string& user,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated)
{
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

  entries = ret.entries;

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

int cls_rgw_util_wait_for_complete_and_cb(const vector<librados::AioCompletion*>& completions)
{
  int ret = 0;
  vector<librados::AioCompletion*>::const_iterator iter;
  for (iter = completions.begin(); iter != completions.end(); ++iter) {
    (*iter)->wait_for_complete_and_cb();
    int r = (*iter)->get_return_value();
    if (r != 0) {
      ret = r;
    }

    (*iter)->release();
  }

  return ret;
}

int cls_rgw_util_wait_for_complete(const vector<librados::AioCompletion*>& completions)
{
  int ret = 0;
  vector<librados::AioCompletion*>::const_iterator iter;
  for (iter = completions.begin(); iter != completions.end(); ++iter) {
    (*iter)->wait_for_complete();
    int r = (*iter)->get_return_value();
    if (r != 0) {
      ret = r;
    }

    (*iter)->release();
  }

  return ret;
}

int cls_rgw_util_parse_markers(const string& marker, map<string, string>& markers)
{
  vector<string> parts;
  boost::split(parts, marker, boost::is_any_of(",#"));
  if (parts.size() % 2 != 0)
    return -EINVAL;
  for (size_t i = 0; i < parts.size(); ) {
    markers[parts[i]] = parts[i + 1];
    i += 2;
  }
  return 0;
}
