#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include "rgw_access.h"
#include <string>
#include <map>
#include "include/types.h"

static string data_space = "data";
static string attr_space = "attr";
static string stat_space = "stat";

class ObjectCache {
  std::map<string, bufferlist> cache_map;

public:
  ObjectCache() { }
  int get(std::string& name, bufferlist& bl);
  void put(std::string& name, bufferlist& bl);
};

template <class T>
class RGWCache  : public T
{
  ObjectCache cache;

  string normal_name(std::string& space, std::string& bucket, std::string& oid) {
    char buf[space.size() + 1 + bucket.size() + 1 + oid.size() + 1];
    const char *space_str = space.c_str();
    const char *bucket_str = bucket.c_str();
    const char *oid_str = oid.c_str();
    sprintf(buf, "%s+%s+%s", space_str, bucket_str, oid_str);
    return string(buf);
  }

public:
  RGWCache() {}

  int put_obj_data(std::string& id, std::string& bucket, std::string& obj, const char *data,
              off_t ofs, size_t len, time_t *mtime);

  int get_obj(void **handle, std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end);

  int obj_stat(std::string& bucket, std::string& obj, uint64_t *psize, time_t *pmtime);
};


template <class T>
int RGWCache<T>::get_obj(void **handle, std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end)
{
  string name = normal_name(data_space, bucket, oid);
  if (bucket[0] != '.' || ofs != 0)
    return T::get_obj(handle, bucket, oid, data, ofs, end);

  bufferlist bl;
  if (cache.get(name, bl) == 0) {
    *data = (char *)malloc(bl.length());
    memcpy(*data, bl.c_str(), bl.length());
    return bl.length();
  }
  int r = T::get_obj(handle, bucket, oid, data, ofs, end);
  if (r < 0)
    return r;

  bufferptr p(r);
  memcpy(p.c_str(), *data, r);
  bl.clear();
  bl.append(p);
  cache.put(name, bl);
  return r;
}

template <class T>
int RGWCache<T>::put_obj_data(std::string& id, std::string& bucket, std::string& obj, const char *data,
              off_t ofs, size_t len, time_t *mtime)
{
  string name = normal_name(data_space, bucket, obj);
  if (bucket[0] == '.' && ofs == 0) {
    bufferptr p(len);
    memcpy(p.c_str(), data, len);
    bufferlist bl;
    bl.append(p);
    cache.put(name, bl);
  }
  return T::put_obj_data(id, bucket, obj, data, ofs, len, mtime);
}

template <class T>
int RGWCache<T>::obj_stat(std::string& bucket, std::string& obj, uint64_t *psize, time_t *pmtime)
{
  if (bucket[0] != '.')
    return T::obj_stat(bucket, obj, psize, pmtime);

  string name = normal_name(stat_space, bucket, obj);

  bufferlist bl;
  uint64_t size;
  time_t mtime;

  int r = cache.get(name, bl);
  if (r == 0) {
    bufferlist::iterator iter = bl.begin();
    ::decode(size, iter);
    int64_t t;
    ::decode(t, iter);
    mtime = (time_t)t;
    goto done;
  }
  r = T::obj_stat(bucket, obj, &size, &mtime);
  if (r < 0)
    return r;
  bl.clear();
  ::encode(size, bl);
  int64_t t = (int64_t)mtime;
  ::encode(t, bl);
  cache.put(name, bl);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  return 0;
}

#endif
