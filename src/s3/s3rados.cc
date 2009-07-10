#include <errno.h>
#include <stdlib.h>

#include "s3access.h"
#include "s3rados.h"

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

int S3Rados::list_buckets_init(std::string& id, S3AccessHandle *handle)
{
  return -ENOSYS;
}

int S3Rados::list_buckets_next(std::string& id, S3ObjEnt& obj, S3AccessHandle *handle)
{
  return -ENOSYS;
}

static int open_pool(string& bucket, rados_pool_t *pool)
{
  return rados->open_pool(bucket.c_str(), pool);
}

int S3Rados::list_objects(string& id, string& bucket, int max, string& prefix, string& marker, vector<S3ObjEnt>& result)
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
  cerr << "JJJ entries.size()=" << entries.size() << std::endl;
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    const char *name = iter->name.c_str();

    if (prefix.empty() ||
        (prefix.compare(0, prefix.size(), name) == 0)) {
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

    if (rados->stat(pool, map_iter->second, &obj.size, &obj.mtime) < 0)
      continue;

    bufferlist bl; 
    if (rados->getxattr(pool, map_iter->second, S3_ATTR_ETAG, bl) < 0)
      continue;

    strncpy(obj.etag, bl.c_str(), sizeof(obj.etag));
    obj.etag[sizeof(obj.etag)-1] = '\0';
    result.push_back(obj);
  }
  rados->close_pool(pool);

  return count;
}


int S3Rados::create_bucket(std::string& id, std::string& bucket, std::vector<std::pair<std::string, bufferlist> >& attrs)
{
  object_t bucket_oid(bucket.c_str());

  int ret = rados->create(root_pool, bucket_oid, true);
  if (ret < 0)
    return ret;

  vector<pair<string, bufferlist> >::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    pair<string, bufferlist>& attr = *iter;
    string& name = attr.first;
    bufferlist& bl = attr.second;
    
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
                  std::vector<std::pair<std::string, bufferlist> >& attrs)
{
  rados_pool_t pool;

  int r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());

  vector<pair<string, bufferlist> >::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    pair<string, bufferlist>& attr = *iter;
    string& name = attr.first;
    bufferlist& bl = attr.second;
    
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
               char **petag,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               std::vector<std::pair<std::string, bufferlist> >& attrs,
               struct s3_err *err)
{
 /* FIXME! this should use a special rados->copy() method */
  int ret;
  char *data;

  ret = get_obj(src_bucket, src_obj, &data, 0, -1, petag,
                mod_ptr, unmod_ptr, if_match, if_nomatch, true, err);
  if (ret < 0)
    return ret;

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
            char **petag,
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

  r = open_pool(bucket, &pool);
  if (r < 0)
    return r;

  object_t oid(obj.c_str());

  r = rados->stat(pool, oid, &size, &mtime);
  if (r < 0)
    return r;

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
  if (if_match || if_nomatch || petag) {
    r = get_attr(bucket, obj, S3_ATTR_ETAG, etag);
    if (r < 0)
      goto done;
    if (petag) {
      *petag = (char *)malloc(etag.length());
      memcpy(*petag, etag.c_str(), etag.length());
    }
  }


  if (if_match || if_nomatch) {
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
  if (r > 0) {
    *data = (char *)malloc(r);
    memcpy(*data, bl.c_str(), bl.length());
  }
done:

  return r;
}

