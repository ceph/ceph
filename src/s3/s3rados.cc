#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/xattr.h>

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

#define ROOT_POOL ".s3"

int S3Rados::initialize()
{
  return 0;
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

  r = rados->list(pool, -1, entries, ctx);
  if (r < 0)
    return r;

  list<object_t>::iterator iter;
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
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s", DIR_NAME, bucket.c_str());

  if (mkdir(buf, 0755) < 0)
    return -errno;

  vector<pair<string, bufferlist> >::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    pair<string, bufferlist>& attr = *iter;
    string& name = attr.first;
    bufferlist& bl = attr.second;
    
    if (bl.length()) {
      int r = setxattr(buf, name.c_str(), bl.c_str(), bl.length(), 0);
      if (r < 0) {
        r = -errno;
        rmdir(buf);
        return r;
      }
    }
  }
#endif
  return 0;
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

  bufferlist bl;
  bl.append(data, size);
  r = rados->write(pool, oid, 0, bl, size);
  if (r < 0)
    return r;
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());
  int fd;

  fd = open(buf, O_CREAT | O_WRONLY, 0755);
  if (fd < 0)
    return -errno;

  int r;
  vector<pair<string, bufferlist> >::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    pair<string, bufferlist>& attr = *iter;
    string& name = attr.first;
    bufferlist& bl = attr.second;
    
    if (bl.length()) {
      r = fsetxattr(fd, name.c_str(), bl.c_str(), bl.length(), 0);
      if (r < 0) {
        r = -errno;
        close(fd);
        return r;
      }
    }
  }

  r = write(fd, data, size);
  if (r < 0) {
    r = -errno;
    close(fd);
    unlink(buf);
    return r;
  }

  if (mtime) {
    struct stat st;
    r = fstat(fd, &st);
    if (r < 0) {
      r = -errno;
      close(fd);
      unlink(buf);
      return -errno;
    }
    *mtime = st.st_mtime;
  }

  r = close(fd);
  if (r < 0)
    return -errno;
#endif
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
#if 0
  int ret;
  char *data;

  ret = get_obj(src_bucket, src_obj, &data, 0, -1, petag,
                mod_ptr, unmod_ptr, if_match, if_nomatch, true, err);
  if (ret < 0)
    return ret;

  ret =  put_obj(id, dest_bucket, dest_obj, data, ret, mtime, attrs);

  return ret;
#endif
  return 0;
}


int S3Rados::delete_bucket(std::string& id, std::string& bucket)
{
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
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());

  if (unlink(buf) < 0)
    return -errno;
#endif
  return 0;
}

int S3Rados::get_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& dest)
{
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  int r = -EINVAL;
  char *data = NULL;

  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());

  r = get_attr(name, buf, &data);
  if (r < 0)
      goto done;
  dest.append(data, r);
done:
  free(data);

  return r;
#endif
  return 0;
}

int S3Rados::set_attr(std::string& bucket, std::string& obj,
                       const char *name, bufferlist& bl)
{
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  int r;

  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());

  r = setxattr(buf, name, bl.c_str(), bl.length(), 0);

  int ret = (r < 0 ? -errno : 0);
  cerr << "setxattr: path=" << buf << " ret=" << ret << std::endl;

  return ret;
#endif
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
#if 0
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  int fd;
  struct stat st;
  int r = -EINVAL;
  size_t max_len, pos;
  char *etag = NULL;

  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());

  fd = open(buf, O_RDONLY, 0755);

  if (fd < 0)
    return -errno;

  r = fstat(fd, &st);
  if (r < 0)
    return -errno;

  if (end < 0)
    end = st.st_size - 1;

  max_len = end - ofs + 1;

  r = -ECANCELED;
  if (mod_ptr) {
    if (st.st_mtime < *mod_ptr) {
      err->num = "304";
      err->code = "PreconditionFailed";
      goto done;
    }
  }

  if (unmod_ptr) {
    if (st.st_mtime >= *mod_ptr) {
      err->num = "412";
      err->code = "PreconditionFailed";
      goto done;
    }
  }
  if (if_match || if_nomatch || petag) {
    r = get_attr(S3_ATTR_ETAG, fd, &etag);
    if (r < 0)
      goto done;
    if (petag)
      *petag = etag;
  }


  if (if_match || if_nomatch) {
    r = -ECANCELED;
    if (if_match) {
      cerr << "etag=" << etag << " " << " if_match=" << if_match << endl;
      if (strcmp(if_match, etag)) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done;
      }
    }

    if (if_nomatch) {
      cerr << "etag=" << etag << " " << " if_nomatch=" << if_nomatch << endl;
      if (strcmp(if_nomatch, etag) == 0) {
        err->num = "412";
        err->code = "PreconditionFailed";
        goto done;
      }
    }
  }

  if (!get_data) {
    r = max_len;
    goto done;
  }
  *data = (char *)malloc(max_len);
  if (!*data) {
    r = -ENOMEM;
    goto done;
  }

  pos = 0;
  while (pos < max_len) {
    r = read(fd, (*data) + pos, max_len);
    if (r > 0) {
      pos += r;
    } else {
      if (!r) {
        cerr << "pos=" << pos << " r=" << r << " max_len=" << max_len << endl;
        r = -EIO; /* should not happen as we validated file size earlier */
        goto done;
      }
      switch (errno) {
      case EINTR:
        break;
      default:
        r = -errno;
        goto done;
      }
    }
  } 

  r = max_len;
done:
  if (etag && !petag)
    free(etag);
  close(fd);  

  return r;
#endif
  return 0;
}

