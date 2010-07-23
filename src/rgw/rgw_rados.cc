#include <errno.h>
#include <stdlib.h>

#include "rgw_access.h"
#include "rgw_rados.h"

#include "include/librados.hpp"
using namespace librados;

#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>

using namespace std;

Rados *rados = NULL;

#define ROOT_BUCKET ".rgw" //keep this synced to rgw_user.cc::root_bucket!

static string root_bucket(ROOT_BUCKET);
static rados_pool_t root_pool;

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize(int argc, char *argv[])
{
  rados = new Rados();
  if (!rados)
    return -ENOMEM;

  int ret = rados->initialize(argc, (const char **)argv);
  if (ret < 0)
   return ret;

  ret = open_root_pool(&root_pool);

  return ret;
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool(rados_pool_t *pool)
{
  int r = rados->open_pool(root_bucket.c_str(), pool);
  if (r < 0) {
    r = rados->create_pool(root_bucket.c_str());
    if (r < 0)
      return r;

    r = rados->open_pool(root_bucket.c_str(), pool);
  }

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

  int r = rados->list_pools(state->list);
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

static int open_pool(string& bucket, rados_pool_t *pool)
{
  return rados->open_pool(bucket.c_str(), pool);
}

static int close_pool(rados_pool_t pool)
{
  return rados->close_pool(pool);
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
			   string& marker, vector<RGWObjEnt>& result, map<string, bool>& common_prefixes)
{
  rados_pool_t pool;
  set<string> dir_set;

  int r = rados->open_pool(bucket.c_str(), &pool);
  if (r < 0)
    return r;


#define MAX_ENTRIES 1000
  Rados::ListCtx ctx;
  rados->list_objects_open(pool, &ctx);
  do {
    list<string> entries;
    r = rados->list_objects_more(ctx, MAX_ENTRIES, entries);
    if (r < 0)
      return r;

    for (list<string>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
      if (prefix.empty() ||
          (iter->compare(0, prefix.size(), prefix) == 0)) {
        dir_set.insert(*iter);
      }
    }
  } while (r);
  rados->list_objects_close(ctx);

  set<string>::iterator p;
  if (!marker.empty())
    p = dir_set.lower_bound(marker);
  else
    p = dir_set.begin();

  if (max < 0) {
    max = dir_set.size();
  }

  result.clear();
  int i, count = 0;
  for (i=0; i<max && p != dir_set.end(); i++, ++p) {
    RGWObjEnt obj;
    obj.name = *p;

    if (!delim.empty()) {
      int delim_pos = obj.name.find(delim, prefix.size());

      if (delim_pos >= 0) {
        common_prefixes[obj.name.substr(0, delim_pos + 1)] = true;
        continue;
      }
    }

    uint64_t s;
    if (rados->stat(pool, *p, &s, &obj.mtime) < 0)
      continue;
    obj.size = s;

    bufferlist bl; 
    obj.etag[0] = '\0';
    if (rados->getxattr(pool, *p, RGW_ATTR_ETAG, bl) >= 0) {
      strncpy(obj.etag, bl.c_str(), sizeof(obj.etag));
      obj.etag[sizeof(obj.etag)-1] = '\0';
    }
    result.push_back(obj);
  }
  rados->close_pool(pool);

  return count;
}

/**
 * create a bucket with name bucket and the given list of attrs
 * if auid is set, it sets the auid of the underlying rados pool
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(std::string& id, std::string& bucket, map<std::string, bufferlist>& attrs, uint64_t auid)
{
  int ret = rados->create(root_pool, bucket, true);
  if (ret < 0)
    return ret;

  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    string name = iter->first;
    bufferlist& bl = iter->second;
    
    if (bl.length()) {
      ret = rados->setxattr(root_pool, bucket, name.c_str(), bl);
      if (ret < 0) {
        delete_bucket(id, bucket);
        return ret;
      }
    }
  }

  ret = rados->create_pool(bucket.c_str(), auid);

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
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_meta(std::string& id, std::string& bucket, std::string& oid,
                  time_t *mtime, map<string, bufferlist>& attrs)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (bl.length()) {
      r = rados->setxattr(pool, oid, name.c_str(), bl);
      if (r < 0)
        return r;
    }
  }

  if (mtime) {
    r = rados->stat(pool, oid, NULL, mtime);
    if (r < 0)
      return r;
  }

  close_pool(pool);

  return 0;
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
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_data(std::string& id, std::string& bucket, std::string& oid, const char *data, off_t ofs, size_t len,
                  time_t *mtime)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  bufferlist bl;
  bl.append(data, len);
  r = rados->write(pool, oid, ofs, bl, len);
  if (r < 0)
    return r;

  if (mtime) {
    r = rados->stat(pool, oid, NULL, mtime);
    if (r < 0)
      return r;
  }

  close_pool(pool);

  return 0;
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
int RGWRados::copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
               std::string& src_bucket, std::string& src_obj,
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
  off_t ofs = 0, end = -1;
  size_t total_len;
  time_t lastmod;
  map<string, bufferlist>::iterator iter;

  cerr << "copy " << src_bucket << ":" << src_obj << " => " << dest_bucket << ":" << dest_obj << std::endl;

  void *handle = NULL;

  map<string, bufferlist> attrset;
  ret = prepare_get_obj(src_bucket, src_obj, ofs, &end, &attrset,
                mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &handle, err);

  if (ret < 0)
    return ret;

  do {
    ret = get_obj(&handle, src_bucket, src_obj, &data, ofs, end);
    if (ret < 0)
      return ret;

    r = put_obj_data(id, dest_bucket, dest_obj, data, ofs, ret, NULL);
    free(data);
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    attrset[iter->first] = iter->second;
  }
  attrs = attrset;

  ret = put_obj_meta(id, dest_bucket, dest_obj, mtime, attrs);

  finish_get_obj(&handle);

  return ret;
done_err:
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
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0) return r;

  r = rados->delete_pool(pool);
  if (r < 0) return r;
  r = delete_obj(id, root_bucket, bucket);
  return r;
}

/**
 * Delete an object.
 * id: unused
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_obj(std::string& id, std::string& bucket, std::string& oid)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  r = rados->remove(pool, oid);
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
int RGWRados::get_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& dest)
{
  rados_pool_t pool;
  string actual_bucket = bucket;
  string actual_obj = obj;

  if (actual_obj.size() == 0) {
    actual_obj = bucket;
    actual_bucket = root_bucket;
  }

  int r = open_pool(actual_bucket, &pool);
  if (r < 0)
    return r;

  r = rados->getxattr(pool, actual_obj, name, dest);

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
int RGWRados::set_attr(std::string& bucket, std::string& oid,
                       const char *name, bufferlist& bl)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  r = rados->setxattr(pool, oid, name, bl);

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
int RGWRados::prepare_get_obj(std::string& bucket, std::string& oid, 
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
  int r = -EINVAL;
  uint64_t size;
  bufferlist etag;
  time_t mtime;

  map<string, bufferlist>::iterator iter;

  *handle = NULL;

  GetObjState *state = new GetObjState;
  if (!state)
    return -ENOMEM;

  *handle = state;

  r = open_pool(bucket, &state->pool);
  if (r < 0)
    goto done_err;

  r = rados->stat(state->pool, oid, &size, &mtime);
  if (r < 0)
    goto done_err;

  if (attrs) {
    r = rados->getxattrs(state->pool, oid, *attrs);
    for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
      cerr << "xattr: " << iter->first << std::endl;
    }
    if (r < 0)
      goto done_err;
  }


  r = -ECANCELED;
  if (mod_ptr) {
    if (mtime < *mod_ptr) {
      err->num = "304";
      err->code = "NotModified";
      goto done_err;
    }
  }

  if (unmod_ptr) {
    if (mtime >= *mod_ptr) {
      err->num = "412";
      err->code = "PreconditionFailed";
      goto done_err;
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(bucket, oid, RGW_ATTR_ETAG, etag);
    if (r < 0)
      goto done_err;

    r = -ECANCELED;
    if (if_match) {
      cerr << "etag=" << etag << " " << " if_match=" << if_match << endl;
      if (strcmp(if_match, etag.c_str())) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done_err;
      }
    }

    if (if_nomatch) {
      cerr << "etag=" << etag << " " << " if_nomatch=" << if_nomatch << endl;
      if (strcmp(if_nomatch, etag.c_str()) == 0) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done_err;
      }
    }
  }

  if (*end < 0)
    *end = size - 1;

  *total_size = (ofs <= *end ? *end + 1 - ofs : 0);
  *lastmod = mtime;

  return 0;

done_err:
  delete state;
  *handle = NULL;
  return r;
}

int RGWRados::get_obj(void **handle,
            std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end)
{
  uint64_t len;
  bufferlist bl;

  GetObjState *state = *(GetObjState **)handle;

  if (end <= 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (len > RGW_MAX_CHUNK_SIZE)
    len = RGW_MAX_CHUNK_SIZE;

  cout << "rados->read ofs=" << ofs << " len=" << len << std::endl;
  int r = rados->read(state->pool, oid, ofs, bl, len);
  cout << "rados->read r=" << r << std::endl;

  if (r > 0) {
    *data = (char *)malloc(r);
    memcpy(*data, bl.c_str(), bl.length());
  }

  if (r < 0 || !len || ((off_t)(ofs + len - 1) == end)) {
    rados->close_pool(state->pool);
    delete state;
    *handle = NULL;
  }

  return r;

}

void RGWRados::finish_get_obj(void **handle)
{
  if (*handle) {
    GetObjState *state = *(GetObjState **)handle;
    rados->close_pool(state->pool);
    delete state;
    *handle = NULL;
  }
}

