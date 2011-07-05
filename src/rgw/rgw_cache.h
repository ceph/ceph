#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include "rgw_access.h"
#include <string>
#include <map>
#include "include/types.h"

static string data_space = "data";
static string attr_space = "attr";
static string stat_space = "stat";

enum {
  UPDATE_OBJ_DATA,
  UPDATE_OBJ_INFO,
};

struct RGWCacheNotifyInfo {
  uint32_t op;
  rgw_obj obj;
  bufferlist bl;
  off_t ofs;

  void encode(bufferlist& obl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, obl);
    ::encode(op, obl);
    ::encode(obj, obl);
    ::encode(bl, obl);
    ::encode(ofs, obl);
  }
  void decode(bufferlist::iterator& ibl) {
    __u8 struct_v;
    ::decode(struct_v, ibl);
    ::decode(op, ibl);
    ::decode(obj, ibl);
    ::decode(bl, ibl);
    ::decode(ofs, ibl);
  }
};
WRITE_CLASS_ENCODER(RGWCacheNotifyInfo)

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

  int distribute(rgw_obj& obj, const char *data, off_t ofs, size_t len);
  int watch_cb(int opcode, uint64_t ver, bufferlist& bl);
public:
  RGWCache() {}

  int put_obj_data(std::string& id, rgw_obj& obj, const char *data,
              off_t ofs, size_t len);

  int get_obj(void **handle, rgw_obj& obj, char **data, off_t ofs, off_t end);

  int obj_stat(std::string& bucket, std::string& obj, uint64_t *psize, time_t *pmtime);
};


template <class T>
int RGWCache<T>::get_obj(void **handle, rgw_obj& obj, char **data, off_t ofs, off_t end)
{
  string& bucket = obj.bucket;
  string& oid = obj.object;
  string name = normal_name(data_space, bucket, oid);
  if (bucket[0] != '.' || ofs != 0)
    return T::get_obj(handle, obj, data, ofs, end);

  bufferlist bl;
  if (cache.get(name, bl) == 0) {
    *data = (char *)malloc(bl.length());
    memcpy(*data, bl.c_str(), bl.length());
    return bl.length();
  }
  int r = T::get_obj(handle, obj, data, ofs, end);
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
int RGWCache<T>::put_obj_data(std::string& id, rgw_obj& obj, const char *data,
              off_t ofs, size_t len)
{
  string& bucket = obj.bucket;
  string& oid = obj.object;
  string name = normal_name(data_space, bucket, oid);
  bool cacheable = false;
  if ((bucket[0] == '.') && ((ofs == 0) || (ofs == -1))) {
    cacheable = true;
    bufferptr p(len);
    memcpy(p.c_str(), data, len);
    bufferlist bl;
    bl.append(p);
    cache.put(name, bl);
  }
  int ret = T::put_obj_data(id, obj, data, ofs, len);
  if (cacheable && ret >= 0) {
    int r = distribute(obj, data, ofs, len);
    if (r < 0)
      RGW_LOG(0) << "ERROR: failed to distribute cache for " << obj << dendl;
  }

  return ret;
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
  int64_t t;

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
  t = (int64_t)mtime;
  ::encode(t, bl);
  cache.put(name, bl);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  return 0;
}

template <class T>
int RGWCache<T>::distribute(rgw_obj& obj, const char *data, off_t ofs, size_t len)
{
  RGWCacheNotifyInfo info;
  bufferlist bl;

  bufferptr p(data, len);
  info.bl.push_back(p);
  info.obj = obj;
  info.op = UPDATE_OBJ_DATA;
  ::encode(info, bl);
  int ret = T::distribute(bl);
  return ret;
}

template <class T>
int RGWCache<T>::watch_cb(int opcode, uint64_t ver, bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(info, iter);
  } catch (buffer::end_of_buffer *err) {
    RGW_LOG(0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  }

  string& bucket = info.obj.bucket;
  string& oid = info.obj.object;
  string name = normal_name(data_space, bucket, oid);

  switch (info.op) {
  case UPDATE_OBJ_DATA:
    cache.put(name, info.bl);
    break;
  case UPDATE_OBJ_INFO:
    break; // TODO
  default:
    RGW_LOG(0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

#endif
