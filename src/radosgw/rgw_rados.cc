#include <errno.h>
#include <stdlib.h>

#include "rgw_access.h"
#include "rgw_rados.h"

#include "include/librados.h"

#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>

using namespace std;

static Rados *rados = NULL;

#define ROOT_BUCKET ".s3"

static string root_bucket(ROOT_BUCKET);
static rados_pool_t root_pool;

int S3Rados::initialize(int argc, char *argv[])
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

int S3Rados::open_root_pool(rados_pool_t *pool)
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

class S3RadosListState {
public:
  vector<string> list;
  unsigned int pos;
  S3RadosListState() : pos(0) {}
};

int S3Rados::list_buckets_init(std::string& id, S3AccessHandle *handle)
{
  S3RadosListState *state = new S3RadosListState();

  if (!state)
    return -ENOMEM;

  int r = rados->list_pools(state->list);
  if (r < 0)
    return r;

  *handle = (S3AccessHandle)state;

  return 0;
}

int S3Rados::list_buckets_next(std::string& id, S3ObjEnt& obj, S3AccessHandle *handle)
{
  S3RadosListState *state = (S3RadosListState *)*handle;

  if (state->pos == state->list.size()) {
    delete state;
    return -ENOENT;
  }



  obj.name = state->list[state->pos++];

  /* FIXME: should read mtime/size vals for bucket */

  return 0;
}

static int open_pool(string& bucket, rados_pool_t *pool)
{
  return rados->open_pool(bucket.c_str(), pool);
}

int S3Rados::list_objects(string& id, string& bucket, int max, string& prefix, string& delim,
                          string& marker, vector<S3ObjEnt>& result, map<string, bool>& common_prefixes)
{
  rados_pool_t pool;
  map<string, object_t> dir_map;

  int r = rados->open_pool(bucket.c_str(), &pool);
  if (r < 0)
    return r;

  list<object_t> entries;
  Rados::ListCtx ctx;

  r = rados->list(pool, INT_MAX, entries, ctx);
  if (r < 0)
    return r;

  list<object_t>::iterator iter;
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    string name = iter->name.c_str();

    if (prefix.empty() ||
        (name.compare(0, prefix.size(), prefix) == 0)) {
      dir_map[name] = *iter;
    }
  }

  map<string, object_t>::iterator map_iter;
  if (!marker.empty())
    map_iter = dir_map.lower_bound(marker);
  else
    map_iter = dir_map.begin();

  if (max < 0)
    max = INT_MAX;

  result.clear();
  int i, count = 0;
  for (i=0; i<max && map_iter != dir_map.end(); i++, ++map_iter) {
    S3ObjEnt obj;
    obj.name = map_iter->first;

    if (!delim.empty()) {
      int delim_pos = obj.name.find(delim, prefix.size());

      if (delim_pos >= 0) {
        common_prefixes[obj.name.substr(0, delim_pos + 1)] = true;
        continue;
      }
    }

    if (rados->stat(pool, map_iter->second, &obj.size, &obj.mtime) < 0)
      continue;

    bufferlist bl; 
    obj.etag[0] = '\0';
    if (rados->getxattr(pool, map_iter->second, S3_ATTR_ETAG, bl) >= 0) {
      strncpy(obj.etag, bl.c_str(), sizeof(obj.etag));
      obj.etag[sizeof(obj.etag)-1] = '\0';
    }
    result.push_back(obj);
  }
  rados->close_pool(pool);

  return count;
}


int S3Rados::create_bucket(std::string& id, std::string& bucket, map<nstring, bufferlist>& attrs)
{
  object_t bucket_oid(bucket.c_str());

  int ret = rados->create(root_pool, bucket_oid, true);
  if (ret < 0)
    return ret;

  map<nstring, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    nstring name = iter->first;
    bufferlist& bl = iter->second;
    
    if (bl.length()) {
      ret = rados->setxattr(root_pool, bucket_oid, name.c_str(), bl);
      if (ret < 0) {
        delete_bucket(id, bucket);
        return ret;
      }
    }
  }

  ret = rados->create_pool(bucket.c_str());

  return ret;
}

int S3Rados::put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size,
                  time_t *mtime,
                  map<nstring, bufferlist>& attrs)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());

  map<nstring, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    nstring name = iter->first;
    bufferlist& bl = iter->second;

    if (bl.length()) {
      r = rados->setxattr(pool, oid, name.c_str(), bl);
      if (r < 0)
        return r;
    }
  }

  bufferlist bl;
  bl.append(data, size);
  r = rados->write(pool, oid, 0, bl, size);
  if (r < 0)
    return r;

  if (mtime) {
    r = rados->stat(pool, oid, NULL, mtime);
    if (r < 0)
      return r;
  }

  return 0;
}

int S3Rados::copy_obj(std::string& id, std::string& dest_bucket, std::string& dest_obj,
               std::string& src_bucket, std::string& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               map<nstring, bufferlist>& attrs,  /* in/out */
               struct s3_err *err)
{
 /* FIXME! this should use a special rados->copy() method */
  int ret;
  char *data;

  cerr << "copy " << src_bucket << ":" << src_obj << " => " << dest_bucket << ":" << dest_obj << std::endl;

  map<nstring, bufferlist> attrset;
  ret = get_obj(src_bucket, src_obj, &data, 0, -1, &attrset,
                mod_ptr, unmod_ptr, if_match, if_nomatch, true, err);

  if (ret < 0)
    return ret;

  map<nstring, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    attrset[iter->first] = iter->second;
  }
  attrs = attrset;

  ret =  put_obj(id, dest_bucket, dest_obj, data, ret, mtime, attrs);

  return ret;
}


int S3Rados::delete_bucket(std::string& id, std::string& bucket)
{
  /* TODO! */
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s", DIR_NAME, bucket.c_str());

  if (rmdir(buf) < 0)
    return -errno;
#endif
  return 0;
}


int S3Rados::delete_obj(std::string& id, std::string& bucket, std::string& obj)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());

  r = rados->remove(pool, oid);
  if (r < 0)
    return r;

  return 0;
}

int S3Rados::get_attr(std::string& bucket, std::string& obj,
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

  object_t oid(actual_obj.c_str());
  r = rados->getxattr(pool, oid, name, dest);

  if (r < 0)
    return r;

  return 0;
}

int S3Rados::set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());
  r = rados->setxattr(pool, oid, name, bl);

  if (r < 0)
    return r;

  return 0;
}

int S3Rados::get_obj(std::string& bucket, std::string& obj, 
            char **data, off_t ofs, off_t end,
            map<nstring, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            const char *if_match,
            const char *if_nomatch,
            bool get_data,
            struct s3_err *err)
{
  int r = -EINVAL;
  size_t size, len;
  bufferlist etag;
  time_t mtime;
  bufferlist bl;

  rados_pool_t pool;
  map<nstring, bufferlist>::iterator iter;

  r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());

  r = rados->stat(pool, oid, &size, &mtime);
  if (r < 0)
    return r;

  if (attrs) {
    r = rados->getxattrs(pool, oid, *attrs);
    for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
      cerr << "xattr: " << iter->first << std::endl;
    }
    if (r < 0)
      return r;
  }


  r = -ECANCELED;
  if (mod_ptr) {
    if (mtime < *mod_ptr) {
      err->num = "304";
      err->code = "PreconditionFailed";
      goto done;
    }
  }

  if (unmod_ptr) {
    if (mtime >= *mod_ptr) {
      err->num = "412";
      err->code = "PreconditionFailed";
      goto done;
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(bucket, obj, S3_ATTR_ETAG, etag);
    if (r < 0)
      goto done;

    r = -ECANCELED;
    if (if_match) {
      cerr << "etag=" << etag << " " << " if_match=" << if_match << endl;
      if (strcmp(if_match, etag.c_str())) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done;
      }
    }

    if (if_nomatch) {
      cerr << "etag=" << etag << " " << " if_nomatch=" << if_nomatch << endl;
      if (strcmp(if_nomatch, etag.c_str()) == 0) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done;
      }
    }
  }

  if (!get_data) {
    r = size;
    goto done;
  }

  if (end <= 0)
    len = 0;
  else
    len = end - ofs + 1;

  cout << "rados->read ofs=" << ofs << " len=" << len << std::endl;
  r = rados->read(pool, oid, ofs, bl, len);
  cout << "rados->read r=" << r << std::endl;
  if (r < 0)
    return r;

  if (r > 0) {
    *data = (char *)malloc(r);
    memcpy(*data, bl.c_str(), bl.length());
  }

done:

  return r;
}

