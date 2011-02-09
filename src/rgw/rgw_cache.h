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
  ObjectCache() {}
  int get(std::string& name, bufferlist& bl);
  void put(std::string& name, bufferlist& bl);
};

template <class T>
class RGWCache  : public T
{
  ObjectCache cache;

  string normal_name(std::string& space, std::string& bucket, std::string& oid) {
    char buf[space.size() + 1 + bucket.size() + 1 + oid.size() + 1];
    sprintf("%s+%s+%s", space.c_str(), bucket.c_str(), oid.c_str());
    return string(buf);
  }

public:
  RGWCache() {}

  int put_obj_data(std::string& id, std::string& bucket, std::string& obj, const char *data,
              off_t ofs, size_t len, time_t *mtime);

  int get_obj(void **handle, std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end);

  int obj_stat(std::string& bucket, std::string& obj, size_t *psize, time_t *pmtime);
};


template <class T>
int RGWCache<T>::get_obj(void **handle, std::string& bucket, std::string& oid, 
            char **data, off_t ofs, off_t end)
{
  string name = normal_name(data_space, bucket, oid);
  cout << "bucket=" << bucket << " bucket[0]=" << bucket[0] << " ofs=" << ofs << " end=" << end << std::endl;
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
  cout << "bucket=" << bucket << " bucket[0]=" << bucket[0] << " ofs=" << ofs << " len=" << len << std::endl;
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
int RGWCache<T>::obj_stat(std::string& bucket, std::string& obj, size_t *psize, time_t *pmtime)
{
  if (bucket[0] != '.')
    return T::obj_stat(bucket, obj, psize, pmtime);

  string name = normal_name(stat_space, bucket, obj);

  bufferlist bl;
  size_t size;
  time_t mtime;

  int r = cache.get(name, bl);
  if (r == 0) {
    bufferlist::iterator iter = bl.begin();
    ::decode(size, iter);
    ::decode(mtime, iter);
    goto done;
  }
  r = T::obj_stat(bucket, obj, &size, &mtime);
  if (r < 0)
    return r;
  bl.clear();
  ::encode(size, bl);
  ::encode(mtime, bl);
  cache.put(name, bl);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  return 0;
}

#endif
