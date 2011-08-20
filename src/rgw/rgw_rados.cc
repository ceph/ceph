#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "rgw_access.h"
#include "rgw_rados.h"
#include "rgw_acl.h"

#include "include/rados/librados.hpp"
using namespace librados;

#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>

using namespace std;

Rados *rados = NULL;

static string notify_oid = "notify";
static string shadow_ns = "shadow";

static string shadow_category = "rgw.shadow";
static string main_category = "rgw.main";

class RGWWatcher : public librados::WatchCtx {
  RGWRados *rados;
public:
  RGWWatcher(RGWRados *r) : rados(r) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    cout << "RGWWatcher::notify() opcode=" << (int)opcode << " ver=" << ver << " bl.length()=" << bl.length() << std::endl;
    rados->watch_cb(opcode, ver, bl);
  }
};


/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize(CephContext *cct)
{
  int ret;
  rados = new Rados();
  if (!rados)
    return -ENOMEM;

  ret = rados->init_with_context(cct);
  if (ret < 0)
   return ret;

  ret = rados->connect();
  if (ret < 0)
   return ret;

  ret = open_root_pool_ctx();
  if (ret < 0)
    return ret;

  return ret;
}

void RGWRados::finalize_watch()
{
  control_pool_ctx.unwatch(notify_oid, watch_handle);
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx()
{
  int r = rados->ioctx_create(RGW_ROOT_BUCKET, root_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(RGW_ROOT_BUCKET);
    if (r < 0)
      return r;

    r = rados->ioctx_create(RGW_ROOT_BUCKET, root_pool_ctx);
  }

  return r;
}

int RGWRados::init_watch()
{
  int r = rados->ioctx_create(RGW_CONTROL_BUCKET, control_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(RGW_CONTROL_BUCKET);
    if (r < 0)
      return r;

    r = rados->ioctx_create(RGW_CONTROL_BUCKET, control_pool_ctx);
    if (r < 0)
      return r;
  }

  r = control_pool_ctx.create(notify_oid, false);
  if (r < 0 && r != -EEXIST)
    return r;

  watcher = new RGWWatcher(this);
  r = control_pool_ctx.watch(notify_oid, 0, &watch_handle, watcher);

  return r;
}

int RGWRados::open_bucket_ctx(std::string& bucket, librados::IoCtx&  io_ctx)
{
  int r = rados->ioctx_create(bucket.c_str(), io_ctx);
  if (r != -ENOENT)
    return r;

  /* couldn't find bucket, might be a racing bucket creation,
     where client haven't gotten updated map, try to read
     the bucket object .. which will trigger update of osdmap
     if that is the case */
  time_t mtime;
  r = root_pool_ctx.stat(bucket, NULL, &mtime);
  if (r < 0)
    return -ENOENT;

  r = rados->ioctx_create(bucket.c_str(), io_ctx);

  return r;
}

class RGWRadosListState {
public:
  std::list<string> list;
  std::list<string>::iterator pos;
  RGWRadosListState() : pos(0) {}
};

/**
 * set up a bucket listing.
 * id is ignored
 * handle is filled in.
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_init(std::string& id, RGWAccessHandle *handle)
{
  RGWRadosListState *state = new RGWRadosListState();

  if (!state)
    return -ENOMEM;

  int r = rados->pool_list(state->list);
  if (r < 0)
    return r;
  state->pos = state->list.begin();

  *handle = (RGWAccessHandle)state;

  return 0;
}

/** 
 * get the next bucket in the listing.
 * id is ignored
 * obj is filled in,
 * handle is updated.
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_next(std::string& id, RGWObjEnt& obj, RGWAccessHandle *handle)
{
  RGWRadosListState *state = (RGWRadosListState *)*handle;

  if (state->pos == state->list.end()) {
    delete state;
    return -ENOENT;
  }

  obj.name = *state->pos;
  state->pos++;

  /* FIXME: should read mtime/size vals for bucket */

  return 0;
}

/** 
 * get listing of the objects in a bucket.
 * id: ignored.
 * bucket: bucket to list contents of
 * max: maximum number of results to return
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed
 *     here.
 */
int RGWRados::list_objects(string& id, string& bucket, int max, string& prefix, string& delim,
			   string& marker, vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
			   bool get_content_type, string& ns, bool *is_truncated, RGWAccessListFilter *filter)
{
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  std::map<string, string> dir_map;
  {
    librados::ObjectIterator i_end = io_ctx.objects_end();
    for (librados::ObjectIterator i = io_ctx.objects_begin(); i != i_end; ++i) {
        string obj = *i;
        string key = obj;

        if (!rgw_obj::translate_raw_obj(obj, ns))
          continue;

        if (filter && !filter->filter(obj, key))
          continue;

	if (prefix.empty() ||
	    ((obj).compare(0, prefix.size(), prefix) == 0)) {
	  dir_map[obj] = key;
	}
    }
  }

  std::map<string, string>::iterator p;
  if (!marker.empty())
    p = dir_map.upper_bound(marker);
  else
    p = dir_map.begin();

  if (max < 0) {
    max = dir_map.size();
  }

  result.clear();
  int i;
  for (i=0; i<max && p != dir_map.end(); i++, ++p) {
    RGWObjEnt obj;
    string name = p->first;
    string key = p->second;
    obj.name = name;

    if (!delim.empty()) {
      int delim_pos = name.find(delim, prefix.size());

      if (delim_pos >= 0) {
        common_prefixes[name.substr(0, delim_pos + 1)] = true;
        continue;
      }
    }
    string oid = name;
    if (!ns.empty()) {
      oid = "_";
      oid.append(ns);
      oid.append("_");
      oid.append(name);
    }

    uint64_t s;
    if (key.compare(name) != 0)
      io_ctx.locator_set_key(key);
    if (io_ctx.stat(oid, &s, &obj.mtime) < 0)
      continue;
    obj.size = s;

    obj.etag[0] = '\0';
    map<string, bufferlist> attrset;
    if (io_ctx.getxattrs(oid, attrset) >= 0) {
      map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
      if (iter != attrset.end()) {
        bufferlist& bl = iter->second;
        strncpy(obj.etag, bl.c_str(), sizeof(obj.etag));
        obj.etag[sizeof(obj.etag)-1] = '\0';
      }
      iter = attrset.find(RGW_ATTR_ACL);
      if (iter != attrset.end()) {
        bufferlist& bl = iter->second;
        bufferlist::iterator i = bl.begin();
        RGWAccessControlPolicy policy;
        policy.decode_owner(i);
        ACLOwner& owner = policy.get_owner();
        obj.owner = owner.get_id();
        obj.owner_display_name = owner.get_display_name();
      }
    }
    bufferlist bl;
    if (get_content_type) {
      obj.content_type = "";
      if (io_ctx.getxattr(oid, RGW_ATTR_CONTENT_TYPE, bl) >= 0) {
        obj.content_type = bl.c_str();
      }
    }
    result.push_back(obj);
  }
  if (is_truncated)
    *is_truncated = (p != dir_map.end());

  return 0;
}

/**
 * create a bucket with name bucket and the given list of attrs
 * if auid is set, it sets the auid of the underlying rados io_ctx
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(std::string& id, std::string& bucket, map<std::string, bufferlist>& attrs, bool exclusive, uint64_t auid)
{
  librados::ObjectWriteOperation op;
  op.create(exclusive);

  for (map<string, bufferlist>::iterator iter = attrs.begin(); iter != attrs.end(); ++iter)
    op.setxattr(iter->first.c_str(), iter->second);

  bufferlist outbl;
  int ret = root_pool_ctx.operate(bucket, &op);
  if (ret < 0)
    return ret;

  ret = rados->pool_create(bucket.c_str(), auid);
  if (ret)
    root_pool_ctx.remove(bucket);

  return ret;
}

/**
 * Write/overwrite an object to the bucket storage.
 * id: ignored
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * size: the amount of data to write (data must be this long)
 * mtime: if non-NULL, writes the given mtime to the bucket storage
 * attrs: all the given attrs are written to bucket storage for the given object
 * exclusive: create object exclusively
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_meta(void *ctx, std::string& id, rgw_obj& obj,
                  time_t *mtime, map<string, bufferlist>& attrs, string& category, bool exclusive)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  ObjectWriteOperation op;

  if (category.size())
    op.create(exclusive, category);
  else
    op.create(exclusive);

  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (bl.length()) {
      op.setxattr(name.c_str(), bl);
    }
  }

  if (!op.size())
    return 0;

  r = io_ctx.operate(oid, &op);
  if (r < 0)
    return r;

  if (mtime) {
    r = io_ctx.stat(oid, NULL, mtime);
    if (r < 0)
      return r;
  }

  return 0;
}

/**
 * Write/overwrite an object to the bucket storage.
 * id: ignored
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * offset: the offet to write to in the object
 *         If this is -1, we will overwrite the whole object.
 * size: the amount of data to write (data must be this long)
 * attrs: all the given attrs are written to bucket storage for the given object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_data(void *ctx, std::string& id, rgw_obj& obj,
			   const char *data, off_t ofs, size_t len)
{
  void *handle;
  int r = aio_put_obj_data(ctx, id, obj, data, ofs, len, &handle);
  if (r < 0)
    return r;
  return aio_wait(handle);
}

int RGWRados::aio_put_obj_data(void *ctx, std::string& id, rgw_obj& obj,
			       const char *data, off_t ofs, size_t len,
                               void **handle)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  bufferlist bl;
  bl.append(data, len);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  *handle = c;
  

  if (ofs == -1) {
    // write_full wants to write the complete bufferlist, not part of it
    assert(bl.length() == len);

    r = io_ctx.aio_write_full(oid, c, bl);
  }
  else {
    r = io_ctx.aio_write(oid, c, bl, len, ofs);
  }
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::aio_wait(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  c->wait_for_complete();
  int ret = c->get_return_value();
  c->release();
  return ret;
}

bool RGWRados::aio_completed(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  return c->is_complete();
}
/**
 * Copy an object.
 * id: unused (well, it's passed to put_obj)
 * dest_bucket: the bucket to copy into
 * dest_obj: the object to copy into
 * src_bucket: the bucket to copy from
 * src_obj: the object to copy from
 * mod_ptr, unmod_ptr, if_match, if_nomatch: as used in get_obj
 * attrs: these are placed on the new object IN ADDITION to
 *    (or overwriting) any attrs copied from the original object
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(void *ctx, std::string& id,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<string, bufferlist>& attrs,  /* in/out */
               string& category,
               struct rgw_err *err)
{
  int ret, r;
  char *data;
  off_t end = -1;
  uint64_t total_len, obj_size;
  time_t lastmod;
  map<string, bufferlist>::iterator iter;
  rgw_obj tmp_obj = dest_obj;
  string tmp_oid;

  append_rand_alpha(dest_obj.object, tmp_oid, 32);
  tmp_obj.set_obj(tmp_oid);
  tmp_obj.set_key(dest_obj.object);

  rgw_obj tmp_dest;

  RGW_LOG(5) << "Copy object " << src_obj.bucket << ":" << src_obj.object << " => " << dest_obj.bucket << ":" << dest_obj.object << dendl;

  void *handle = NULL;

  map<string, bufferlist> attrset;
  ret = prepare_get_obj(ctx, src_obj, 0, &end, &attrset,
                mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &obj_size, &handle, err);

  if (ret < 0)
    return ret;

  off_t ofs = 0;
  do {
    ret = get_obj(ctx, &handle, src_obj, &data, ofs, end);
    if (ret < 0)
      return ret;

    // In the first call to put_obj_data, we pass ofs == -1 so that it will do
    // a write_full, wiping out whatever was in the object before this
    r = put_obj_data(ctx, id, tmp_obj, data, ((ofs == 0) ? -1 : ofs), ret);
    free(data);
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    attrset[iter->first] = iter->second;
  }
  attrs = attrset;

  ret = clone_obj(ctx, dest_obj, 0, tmp_obj, 0, end + 1, NULL, attrs, category);
  if (mtime)
    obj_stat(ctx, tmp_obj, NULL, mtime);

  r = rgwstore->delete_obj(ctx, id, tmp_obj, false);
  if (r < 0)
    RGW_LOG(0) << "ERROR: could not remove " << tmp_obj << dendl;

  finish_get_obj(&handle);

  return ret;
done_err:
  rgwstore->delete_obj(ctx, id, tmp_obj, false);
  finish_get_obj(&handle);
  return r;
}

/**
 * Delete a bucket.
 * id: unused
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(std::string& id, std::string& bucket)
{
  librados::IoCtx list_ctx;
  int r = open_bucket_ctx(bucket, list_ctx);
  if (r < 0)
    return r;

  ObjectIterator iter = list_ctx.objects_begin();
  string ns;
  for (; iter != list_ctx.objects_end(); iter++) {
    string obj = *iter;
    if (rgw_obj::translate_raw_obj(obj, ns))
      return -ENOTEMPTY;
  }

  r = rados->pool_delete(bucket.c_str());
  if (r < 0)
    return r;

  rgw_obj obj(rgw_root_bucket, bucket);
  r = delete_obj(NULL, id, obj, true);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Delete buckets, don't care about content
 * id: unused
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::purge_buckets(std::string& id, vector<std::string>& buckets)
{
  librados::IoCtx list_ctx;
  vector<std::string>::iterator iter;
  vector<librados::PoolAsyncCompletion *> completions;
  int ret = 0, r;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    string bucket = *iter;
    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    r = rados->pool_delete_async(bucket.c_str(), c);
    if (r < 0) {
      RGW_LOG(0) << "WARNING: rados->pool_delete_async(bucket=" << bucket << ") returned err=" << r << dendl;
      ret = r;
    } else {
      completions.push_back(c);
    }

    rgw_obj obj(rgw_root_bucket, bucket);
    r = delete_obj(NULL, id, obj, true);
    if (r < 0) {
      RGW_LOG(0) << "WARNING: could not remove bucket object: " << RGW_ROOT_BUCKET << ":" << bucket << dendl;
      ret = r;
      continue;
    }
  }

  vector<librados::PoolAsyncCompletion *>::iterator citer;
  for (citer = completions.begin(); citer != completions.end(); ++citer) {
    PoolAsyncCompletion *c = *citer;
    c->wait();
    r = c->get_return_value();
    if (r < 0) {
      RGW_LOG(0) << "WARNING: async pool_removal returned " << r << dendl;
      ret = r;
    }
    c->release();
  }

  return ret;
}

int RGWRados::set_buckets_auid(vector<std::string>& buckets, uint64_t auid)
{
  librados::IoCtx ctx;
  vector<librados::PoolAsyncCompletion *> completions;

  vector<std::string>::iterator iter;
  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    string& bucket = *iter;
    int r = open_bucket_ctx(bucket, ctx);
    if (r < 0)
      return r;

    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    ctx.set_auid_async(auid, c);
  }

  vector<librados::PoolAsyncCompletion *>::iterator citer;
  for (citer = completions.begin(); citer != completions.end(); ++citer) {
    PoolAsyncCompletion *c = *citer;
    c->wait();
    c->release();
  }

  return 0;
}

int RGWRados::disable_buckets(vector<std::string>& buckets)
{
  return set_buckets_auid(buckets, RGW_SUSPENDED_USER_AUID);
}

int RGWRados::enable_buckets(vector<std::string>& buckets, uint64_t auid)
{
  return set_buckets_auid(buckets, auid);
}

int RGWRados::bucket_suspended(std::string& bucket, bool *suspended)
{
  librados::IoCtx ctx;
  int r = open_bucket_ctx(bucket, ctx);
  if (r < 0)
    return r;

  uint64_t auid;
  int ret = ctx.get_auid(&auid);
  if (ret < 0)
    return ret;

  *suspended = (auid == RGW_SUSPENDED_USER_AUID);
  return 0;
}

/**
 * Delete an object.
 * id: unused
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_obj_impl(void *ctx, std::string& id, rgw_obj& obj, bool sync)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  ObjectWriteOperation op;

  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, obj, io_ctx, oid, op, &state);
  if (r < 0)
    return r;

  op.remove();
  if (sync) {
    r = io_ctx.operate(oid, &op);
  } else {
    librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);
    r = io_ctx.aio_operate(obj.object, completion, &op);
    completion->release();
  }

  atomic_write_finish(state, r);

  if (r < 0)
    return r;

  return 0;
}

int RGWRados::delete_obj(void *ctx, std::string& id, rgw_obj& obj, bool sync)
{
  int r;
  do {
    r = delete_obj_impl(ctx, id, obj, sync);
  } while (r == -ECANCELED);
  return r;
}

int RGWRados::get_obj_state(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx, string& actual_obj, RGWObjState **state)
{
  RGWObjState *s = rctx->get_state(obj);
  *state = s;
  if (s->has_attrs)
    return 0;

  ObjectReadOperation op;
  op.getxattrs();
  op.stat();
  bufferlist outbl;
  int r = io_ctx.operate(actual_obj, &op, &outbl);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;

  bufferlist::iterator oiter = outbl.begin();
  ::decode(s->attrset, oiter);

  map<string, bufferlist>::iterator aiter;
  for (aiter = s->attrset.begin(); aiter != s->attrset.end(); ++aiter) {
    RGW_LOG(0) << "iter->first=" << aiter->first << dendl;
  }
  ::decode(s->size, oiter);
  utime_t ut;
  ::decode(ut, oiter);
  s->mtime = ut.sec();

  s->has_attrs = true;
  map<string, bufferlist>::iterator iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ);
  if (iter != s->attrset.end()) {
    bufferlist bl = iter->second;
    bufferlist::iterator it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];
  return 0;
}

/**
 * Get the attributes for an object.
 * bucket: name of the bucket holding the object.
 * obj: name of the object
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  string actual_bucket = bucket;
  string actual_obj = oid;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  if (actual_obj.size() == 0) {
    actual_obj = bucket;
    actual_bucket = rgw_root_bucket;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  if (rctx) {
    RGWObjState *state;
    r = get_obj_state(rctx, obj, io_ctx, actual_obj, &state);
    if (r < 0)
      return r;
    if (!state->exists)
      return -ENOENT;
    if (state->get_attr(name, dest))
      return 0;
    return -ENODATA;
  }
  
  r = io_ctx.getxattr(actual_obj, name, dest);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::append_atomic_test(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                            string& actual_obj, ObjectOperation& op, RGWObjState **pstate)
{
  if (!rctx)
    return 0;

  int r = get_obj_state(rctx, obj, io_ctx, actual_obj, pstate);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  if (!state->is_atomic)
    return 0;

  if (state->obj_tag.length() > 0) {// check for backward compatibility
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
  }
  return 0;
}

int RGWRados::prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                            string& actual_obj, ObjectWriteOperation& op, RGWObjState **pstate)
{
  int r = get_obj_state(rctx, obj, io_ctx, actual_obj, pstate);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  if (!state->is_atomic)
    return 0;

  if (state->obj_tag.length() == 0 ||
      state->shadow_obj.size() == 0) {
    RGW_LOG(0) << "can't clone object " << obj << " to shadow object, tag/shadow_obj haven't been set" << dendl;
    // FIXME: need to test object does not exist
  } else {
    RGW_LOG(0) << "cloning object " << obj << " to name=" << state->shadow_obj << dendl;
    rgw_obj dest_obj(obj.bucket, state->shadow_obj);
    dest_obj.set_ns(shadow_ns);
    if (obj.key.size())
      dest_obj.set_key(obj.key);
    else
      dest_obj.set_key(obj.object);

    pair<string, bufferlist> cond(RGW_ATTR_ID_TAG, state->obj_tag);
    RGW_LOG(0) << "cloning: dest_obj=" << dest_obj << " size=" << state->size << " tag=" << state->obj_tag.c_str() << dendl;
    r = clone_obj_cond(NULL, dest_obj, 0, obj, 0, state->size, state->attrset, shadow_category, &state->mtime, false, true, &cond);
    if (r == -EEXIST)
      r = 0;
    if (r == -ECANCELED) {
      /* we lost in a race here, original object was replaced, we assume it was cloned
         as required */
      RGW_LOG(0) << "clone_obj_cond was cancelled, lost in a race" << dendl;
      state->clear();
      return r;
    } else {
      int ret = rctx->notify_intent(dest_obj, DEL_OBJ);
      if (ret < 0) {
        RGW_LOG(0) << "WARNING: failed to log intent ret=" << ret << dendl;
      }
    }
    if (r < 0) {
      RGW_LOG(0) << "ERROR: failed to clone object r=" << r << dendl;
      return r;
    }

    /* first verify that the object wasn't replaced under */
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
    // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
  }

  string tag;
  append_rand_alpha(tag, tag, 32);
  bufferlist bl;
  bl.append(tag);


  op.setxattr(RGW_ATTR_ID_TAG, bl);

  string shadow = obj.object;
  shadow.append(".");
  shadow.append(tag);

  bufferlist shadow_bl;
  shadow_bl.append(shadow);
  op.setxattr(RGW_ATTR_SHADOW_OBJ, shadow_bl);

  return 0;
}

int RGWRados::prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& obj, librados::IoCtx& io_ctx,
                            string& actual_obj, ObjectWriteOperation& op, RGWObjState **pstate)
{
  if (!rctx) {
    *pstate = NULL;
    return 0;
  }

  int r;
  do {
    r = prepare_atomic_for_write_impl(rctx, obj, io_ctx, actual_obj, op, pstate);
  } while (r == -ECANCELED);

  return r;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  string actual_bucket = bucket;
  string actual_obj = oid;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  if (actual_obj.size() == 0) {
    actual_obj = bucket;
    actual_bucket = rgw_root_bucket;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, obj, io_ctx, actual_obj, op, &state);
  if (r < 0)
    return r;

  op.setxattr(name, bl);
  r = io_ctx.operate(actual_obj, &op);

  if (state && r >= 0)
    state->attrset[name] = bl;

  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    dout(0) << "RGWRados::set_attr: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, state->shadow_obj, loc, shadow_ns);
    r = set_attr(NULL, shadow, name, bl);
  }

  if (r < 0)
    return r;

  return 0;
}

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * obj: name/key of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * get_data: if true, the object's data/value will be read out, otherwise not
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *          (if get_data==true) length of read data,
 *          (if get_data==false) length of the object
 */
int RGWRados::prepare_get_obj(void *ctx, rgw_obj& obj,
            off_t ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *total_size,
            uint64_t *obj_size,
            void **handle,
            struct rgw_err *err)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  int r = -EINVAL;
  bufferlist etag;
  time_t ctime;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  RGWRadosCtx *new_ctx = NULL;
  RGWObjState *astate = NULL;

  map<string, bufferlist>::iterator iter;

  *handle = NULL;

  GetObjState *state = new GetObjState;
  if (!state)
    return -ENOMEM;

  *handle = state;

  r = open_bucket_ctx(bucket, state->io_ctx);
  if (r < 0)
    goto done_err;

  state->io_ctx.locator_set_key(obj.key);

  if (!rctx) {
    new_ctx = new RGWRadosCtx();
    rctx = new_ctx;
  }

  r = get_obj_state(rctx, obj, state->io_ctx, oid, &astate);
  if (r < 0)
    goto done_err;

  if (attrs) {
    *attrs = astate->attrset;
    if (g_conf->rgw_log >= 20) {
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        RGW_LOG(20) << "Read xattr: " << iter->first << dendl;
      }
    }
    if (r < 0)
      goto done_err;
  }

  /* Convert all times go GMT to make them compatible */
  struct tm mtm;
  ctime = mktime(gmtime_r(&astate->mtime, &mtm));

  r = -ECANCELED;
  if (mod_ptr) {
    RGW_LOG(10) << "If-Modified-Since: " << *mod_ptr << " Last-Modified: " << ctime << dendl;
    if (ctime < *mod_ptr) {
      err->http_ret = 304;
      err->s3_code = "NotModified";

      goto done_err;
    }
  }

  if (unmod_ptr) {
    RGW_LOG(10) << "If-UnModified-Since: " << *unmod_ptr << " Last-Modified: " << ctime << dendl;
    if (ctime > *unmod_ptr) {
      err->http_ret = 412;
      err->s3_code = "PreconditionFailed";
      goto done_err;
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(rctx, obj, RGW_ATTR_ETAG, etag);
    if (r < 0)
      goto done_err;

    r = -ECANCELED;
    if (if_match) {
      RGW_LOG(10) << "ETag: " << etag.c_str() << " " << " If-Match: " << if_match << dendl;
      if (strcmp(if_match, etag.c_str())) {
        err->http_ret = 412;
        err->s3_code = "PreconditionFailed";
        goto done_err;
      }
    }

    if (if_nomatch) {
      RGW_LOG(10) << "ETag: " << etag.c_str() << " " << " If-NoMatch: " << if_nomatch << dendl;
      if (strcmp(if_nomatch, etag.c_str()) == 0) {
        err->http_ret = 304;
        err->s3_code = "NotModified";
        goto done_err;
      }
    }
  }

  if (end && *end < 0)
    *end = astate->size - 1;

  if (total_size)
    *total_size = (ofs <= *end ? *end + 1 - ofs : 0);
  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return 0;

done_err:
  delete new_ctx;
  delete state;
  *handle = NULL;
  return r;
}

int RGWRados::clone_objs_impl(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        string& category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  std::string& bucket = dst_obj.bucket;
  std::string& dst_oid = dst_obj.object;
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(dst_obj.key);
  ObjectWriteOperation op;
  if (truncate_dest) {
    op.remove();
    op.set_op_flags(OP_FAILOK); // don't fail if object didn't exist
  }

  if (category.size())
    op.create(exclusive, category);
  else
    op.create(exclusive);


  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;
    op.setxattr(name.c_str(), bl);
  }
  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, dst_obj, io_ctx, dst_oid, op, &state);
  if (r < 0)
    return r;

  vector<RGWCloneRangeInfo>::iterator range_iter;
  for (range_iter = ranges.begin(); range_iter != ranges.end(); ++range_iter) {
    RGWCloneRangeInfo range = *range_iter;
    vector<RGWCloneRangeInfo>::iterator next_iter = range_iter;

    // merge ranges
    while (++next_iter !=  ranges.end()) {
      RGWCloneRangeInfo& next = *next_iter;
      if (range.src_ofs + (int64_t)range.len != next.src_ofs ||
          range.dst_ofs + (int64_t)range.len != next.dst_ofs)
        break;
      range_iter++;
      range.len += next.len;
    }
    if (range.len) {
      RGW_LOG(20) << "calling op.clone_range(dst_ofs=" << range.dst_ofs << ", src.object=" <<  range.src.object << " range.src_ofs=" << range.src_ofs << " range.len=" << range.len << dendl;
      if (xattr_cond) {
        op.src_cmpxattr(range.src.object, xattr_cond->first.c_str(),
                        LIBRADOS_CMPXATTR_OP_EQ, xattr_cond->second);
      }
      op.clone_range(range.dst_ofs, range.src.object, range.src_ofs, range.len);
    }
  }

  if (pmtime)
    op.mtime(pmtime);

  bufferlist outbl;
  int ret = io_ctx.operate(dst_oid, &op);

  atomic_write_finish(state, ret);

  return ret;
}

int RGWRados::clone_objs(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        string& category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  int r;
  do {
    r = clone_objs_impl(ctx, dst_obj, ranges, attrs, category, pmtime, truncate_dest, exclusive, xattr_cond);
  } while (ctx && r == -ECANCELED);
  return r;
}


int RGWRados::get_obj(void *ctx, void **handle, rgw_obj& obj,
            char **data, off_t ofs, off_t end)
{
  std::string& oid = obj.object;
  uint64_t len;
  bufferlist bl;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  GetObjState *state = *(GetObjState **)handle;
  RGWObjState *astate = NULL;

  if (end <= 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (len > RGW_MAX_CHUNK_SIZE)
    len = RGW_MAX_CHUNK_SIZE;

  state->io_ctx.locator_set_key(obj.key);

  ObjectReadOperation op;

  int r = append_atomic_test(rctx, obj, state->io_ctx, oid, op, &astate);
  if (r < 0)
    return r;

  RGW_LOG(20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len);

  r = state->io_ctx.operate(oid, &op, &bl);
  RGW_LOG(20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    dout(0) << "RGWRados::get_obj: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, astate->shadow_obj, loc, shadow_ns);
    r = get_obj(NULL, handle, shadow, data, ofs, end);
    return r;
  }

  if (bl.length() > 0) {
    r = bl.length();
    *data = (char *)malloc(r);
    memcpy(*data, bl.c_str(), bl.length());
  }

  if (r < 0 || !len || ((off_t)(ofs + len - 1) == end)) {
    delete state;
    *handle = NULL;
  }

  return r;
}

void RGWRados::finish_get_obj(void **handle)
{
  if (*handle) {
    GetObjState *state = *(GetObjState **)handle;
    delete state;
    *handle = NULL;
  }
}

/* a simple object read */
int RGWRados::read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  RGWObjState *astate = NULL;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  ObjectReadOperation op;

  r = append_atomic_test(rctx, obj, io_ctx, oid, op, &astate);
  if (r < 0)
    return r;

  op.read(ofs, size);

  r = io_ctx.operate(oid, &op, &bl);
  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    dout(0) << "RGWRados::get_obj: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, astate->shadow_obj, loc, shadow_ns);
    r = read(NULL, shadow, ofs, size, bl);
  }
  return r;
}

int RGWRados::obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  if (rctx) {
    RGWObjState *astate;
    r = get_obj_state(rctx, obj, io_ctx, oid, &astate);
    if (r < 0)
      return r;

    if (!astate->exists)
      return -ENOENT;

    if (psize)
      *psize = astate->size;
    if (pmtime)
      *pmtime = astate->mtime;

    return 0;
  }

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.stat(oid, psize, pmtime);
  return r;
}

int RGWRados::get_bucket_id(std::string& bucket)
{
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  return io_ctx.get_id();
}

int RGWRados::tmap_set(rgw_obj& obj, std::string& key, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;

  ::encode(c, cmdbl);
  ::encode(key, cmdbl);
  ::encode(bl, cmdbl);

  RGW_LOG(15) << "tmap_set bucket=" << bucket << " oid=" << oid << " key=" << key << dendl;

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.tmap_update(oid, cmdbl);

  return r;
}

int RGWRados::tmap_create(rgw_obj& obj, std::string& key, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_CREATE;

  ::encode(c, cmdbl);
  ::encode(key, cmdbl);
  ::encode(bl, cmdbl);

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.tmap_update(oid, cmdbl);
  return r;
}

int RGWRados::tmap_del(rgw_obj& obj, std::string& key)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;

  ::encode(c, cmdbl);
  ::encode(key, cmdbl);

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.tmap_update(oid, cmdbl);
  return r;
}
int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  int count = 0;

  map<string, RGWBucketEnt>::iterator iter;
  list<string> buckets_list;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    string bucket_name = iter->first;
    buckets_list.push_back(bucket_name);
  }
  map<std::string,librados::stats_map> sm;
  int r = rados->get_pool_stats(buckets_list, rgw_obj_category_main, sm);
  if (r < 0)
    return r;

  map<string, librados::stats_map>::iterator miter;

  for (miter = sm.begin(), iter = m.begin(); miter != sm.end(), iter != m.end(); ++iter, ++miter) {
    stats_map stats = miter->second;
    stats_map::iterator stats_iter = stats.begin();

    string bucket_name = miter->first;
    RGWBucketEnt& ent = iter->second;
    if (bucket_name.compare(ent.name) != 0)
      continue;

    pool_stat_t stat = stats_iter->second;
    ent.count = stat.num_objects;
    ent.size = stat.num_bytes;
    count++;
  }

  return count;
}

int RGWRados::append_async(rgw_obj& obj, size_t size, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.aio_append(oid, completion, bl, size);
  completion->release();
  return r;
}

int RGWRados::distribute(bufferlist& bl)
{
  RGW_LOG(10) << "distributing notification oid=" << notify_oid << " bl.length()=" << bl.length() << dendl;
  int r = control_pool_ctx.notify(notify_oid, 0, bl);
  return r;
}
