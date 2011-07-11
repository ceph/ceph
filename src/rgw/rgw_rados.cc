#include <errno.h>
#include <stdlib.h>

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

  ret = init_watch();

  return ret;
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
  librados::ObjectOperation op;
  op.create(exclusive);

  for (map<string, bufferlist>::iterator iter = attrs.begin(); iter != attrs.end(); ++iter)
    op.setxattr(iter->first.c_str(), iter->second);

  bufferlist outbl;
  int ret = root_pool_ctx.operate(bucket, &op, &outbl);
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
int RGWRados::put_obj_meta(std::string& id, rgw_obj& obj,
                  time_t *mtime, map<string, bufferlist>& attrs, bool exclusive)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  if (exclusive) {
    r = io_ctx.create(oid, true);
    if (r < 0)
      return r;
  }

  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (bl.length()) {
      r = io_ctx.setxattr(oid, name.c_str(), bl);
      if (r < 0)
        return r;
    }
  }

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
int RGWRados::put_obj_data(std::string& id, rgw_obj& obj,
			   const char *data, off_t ofs, size_t len)
{
  void *handle;
  int r = aio_put_obj_data(id, obj, data, ofs, len, &handle);
  if (r < 0)
    return r;
  return aio_wait(handle);
}

int RGWRados::aio_put_obj_data(std::string& id, rgw_obj& obj,
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
int RGWRados::copy_obj(std::string& id, rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<string, bufferlist>& attrs,  /* in/out */
               struct rgw_err *err)
{
  int ret, r;
  char *data;
  off_t end = -1;
  size_t total_len;
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
  ret = prepare_get_obj(src_obj, 0, &end, &attrset,
                mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &handle, err);

  if (ret < 0)
    return ret;

  off_t ofs = 0;
  do {
    ret = get_obj(&handle, src_obj, &data, ofs, end);
    if (ret < 0)
      return ret;

    // In the first call to put_obj_data, we pass ofs == -1 so that it will do
    // a write_full, wiping out whatever was in the object before this
    r = put_obj_data(id, tmp_obj, data, ((ofs == 0) ? -1 : ofs), ret);
    free(data);
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    attrset[iter->first] = iter->second;
  }
  attrs = attrset;

  ret = clone_obj(dest_obj, 0, tmp_obj, 0, end + 1, attrs);
  if (mtime)
    obj_stat(tmp_obj, NULL, mtime);

  r = rgwstore->delete_obj(id, tmp_obj, false);
  if (r < 0)
    RGW_LOG(0) << "ERROR: could not remove " << tmp_obj << dendl;

  finish_get_obj(&handle);

  return ret;
done_err:
  rgwstore->delete_obj(id, tmp_obj, false);
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

  if (list_ctx.objects_begin() != list_ctx.objects_end())
    return -ENOTEMPTY;

  r = rados->pool_delete(bucket.c_str());
  if (r < 0)
    return r;

  rgw_obj obj(rgw_root_bucket, bucket);
  r = delete_obj(id, obj, true);
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
    r = delete_obj(id, obj, true);
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
int RGWRados::delete_obj(std::string& id, rgw_obj& obj, bool sync)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);
  if (sync) {
    r = io_ctx.remove(oid);
  } else {
    ObjectOperation op;
    op.remove();
    librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);
    r = io_ctx.aio_operate(obj.object, completion, &op, NULL);
    completion->release();
  }
  if (r < 0)
    return r;

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
int RGWRados::get_attr(rgw_obj& obj, const char *name, bufferlist& dest)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  string actual_bucket = bucket;
  string actual_obj = oid;

  if (actual_obj.size() == 0) {
    actual_obj = bucket;
    actual_bucket = rgw_root_bucket;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.getxattr(actual_obj, name, dest);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(rgw_obj& obj, const char *name, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  string actual_bucket = bucket;
  string actual_obj = oid;

  if (actual_obj.size() == 0) {
    actual_obj = bucket;
    actual_bucket = rgw_root_bucket;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.setxattr(actual_obj, name, bl);
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
int RGWRados::prepare_get_obj(rgw_obj& obj,
            off_t ofs, off_t *end,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            size_t *total_size,
            void **handle,
            struct rgw_err *err)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  int r = -EINVAL;
  uint64_t size;
  bufferlist etag;
  time_t mtime;
  time_t ctime;

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

  if (total_size || end) {
    r = state->io_ctx.stat(oid, &size, &mtime);
    if (r < 0)
      goto done_err;
  }

  if (attrs) {
    r = state->io_ctx.getxattrs(oid, *attrs);
    if (g_conf->rgw_log >= 20) {
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        RGW_LOG(20) << "Read xattr: " << iter->first << dendl;
      }
    }
    if (r < 0)
      goto done_err;
  }

  /* Convert all times go GMT to make them compatible */
  ctime = mktime(gmtime(&mtime));

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
    r = get_attr(obj, RGW_ATTR_ETAG, etag);
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
    *end = size - 1;

  if (total_size)
    *total_size = (ofs <= *end ? *end + 1 - ofs : 0);
  if (lastmod)
    *lastmod = mtime;

  return 0;

done_err:
  delete state;
  *handle = NULL;
  return r;
}

int RGWRados::clone_range(rgw_obj& dst_obj, off_t dst_ofs,
                          rgw_obj& src_obj, off_t src_ofs, size_t size)
{
  std::string& bucket = dst_obj.bucket;
  std::string& dst_oid = dst_obj.object;
  std::string& src_oid = src_obj.object;
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(dst_obj.key);

  return io_ctx.clone_range(dst_oid, dst_ofs, src_oid, src_ofs, size);
}

int RGWRados::clone_objs(rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        bool truncate_dest)
{
  std::string& bucket = dst_obj.bucket;
  std::string& dst_oid = dst_obj.object;
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(dst_obj.key);
  ObjectOperation op;
  op.create(false);
  if (truncate_dest)
    op.truncate(0);
  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;
    op.setxattr(name.c_str(), bl);
  }
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
    RGW_LOG(20) << "calling op.clone_range(dst_ofs=" << range.dst_ofs << ", src.object=" <<  range.src.object << " range.src_ofs=" << range.src_ofs << " range.len=" << range.len << dendl;
    op.clone_range(range.dst_ofs, range.src.object, range.src_ofs, range.len);
  }

  bufferlist outbl;
  int ret = io_ctx.operate(dst_oid, &op, &outbl);
  return ret;
}

int RGWRados::get_obj(void **handle, rgw_obj& obj,
            char **data, off_t ofs, off_t end)
{
  std::string& oid = obj.object;
  uint64_t len;
  bufferlist bl;

  GetObjState *state = *(GetObjState **)handle;

  if (end <= 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (len > RGW_MAX_CHUNK_SIZE)
    len = RGW_MAX_CHUNK_SIZE;

  state->io_ctx.locator_set_key(obj.key);

  RGW_LOG(20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  int r = state->io_ctx.read(oid, bl, len, ofs);
  RGW_LOG(20) << "rados->read r=" << r << dendl;

  if (r > 0) {
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
int RGWRados::read(rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(obj.key);

  r = io_ctx.read(oid, bl, size, ofs);
  return r;
}

int RGWRados::obj_stat(rgw_obj& obj, uint64_t *psize, time_t *pmtime)
{
  std::string& bucket = obj.bucket;
  std::string& oid = obj.object;
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  if (r < 0)
    return r;

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
  map<std::string,librados::pool_stat_t> stats;
  int r = rados->get_pool_stats(buckets_list, stats);
  if (r < 0)
    return r;

  map<string,pool_stat_t>::iterator stats_iter = stats.begin();

  for (iter = m.begin(); iter != m.end(); ++iter) {
    string bucket_name = iter->first;
    if (stats_iter->first.compare(bucket_name) == 0) {
      RGWBucketEnt& ent = iter->second;
      pool_stat_t stat = stats_iter->second;
      ent.count = stat.num_objects;
      ent.size = stat.num_bytes;
      stats_iter++;
      count++;
    }
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
  RGW_LOG(0) << "JJJ sending notification oid=" << notify_oid << " bl.length()=" << bl.length() << dendl;
  int r = control_pool_ctx.notify(notify_oid, 0, bl);
  return r;
}
