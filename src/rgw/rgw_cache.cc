#include "rgw_cache.h"

#include <errno.h>

#define dout_subsys ceph_subsys_rgw

using namespace std;


void ObjectMetaInfo::generate_test_instances(list<ObjectMetaInfo*>& o)
{
  ObjectMetaInfo *m = new ObjectMetaInfo;
  m->size = 1024 * 1024;
  o.push_back(m);
  o.push_back(new ObjectMetaInfo);
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
}

void ObjectCacheInfo::generate_test_instances(list<ObjectCacheInfo*>& o)
{
  ObjectCacheInfo *i = new ObjectCacheInfo;
  i->status = 0;
  i->flags = CACHE_FLAG_MODIFY_XATTRS;
  string s = "this is a string";
  string s2 = "this is a another string";
  bufferlist data, data2;
  ::encode(s, data);
  ::encode(s2, data2);
  i->data = data;
  i->xattrs["x1"] = data;
  i->xattrs["x2"] = data2;
  i->rm_xattrs["r2"] = data2;
  i->rm_xattrs["r3"] = data;
  i->meta.size = 512 * 1024;
  o.push_back(i);
  o.push_back(new ObjectCacheInfo);
}

void ObjectCacheInfo::dump(Formatter *f) const
{
  f->dump_int("status", status);
  f->dump_unsigned("flags", flags);
  f->open_object_section("data");
  f->dump_unsigned("length", data.length());
  f->close_section();

  map<string, bufferlist>::const_iterator iter = xattrs.begin();
  f->open_array_section("xattrs");
  for (; iter != xattrs.end(); ++iter) {
    f->dump_string("name", iter->first);
    f->open_object_section("value");
    f->dump_unsigned("length", iter->second.length());
    f->close_section();
  }
  f->close_section();

  f->open_array_section("rm_xattrs");
  for (iter = rm_xattrs.begin(); iter != rm_xattrs.end(); ++iter) {
    f->dump_string("name", iter->first);
    f->open_object_section("value");
    f->dump_unsigned("length", iter->second.length());
    f->close_section();
  }
  f->close_section();
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();

}

void RGWCacheNotifyInfo::generate_test_instances(list<RGWCacheNotifyInfo*>& o)
{
  o.push_back(new RGWCacheNotifyInfo);
}

void RGWCacheNotifyInfo::dump(Formatter *f) const
{
  f->dump_unsigned("op", op);
  f->open_object_section("obj");
  obj.dump(f);
  f->close_section();
  f->open_object_section("obj_info");
  obj_info.dump(f);
  f->close_section();
  f->dump_unsigned("ofs", ofs);
  f->dump_string("ns", ns);
}

int ObjectCache::get(string& name, ObjectCacheInfo& info, uint32_t mask)
{
  Mutex::Locker l(lock);

  map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
  if (iter == cache_map.end()) {
    ldout(cct, 10) << "cache get: name=" << name << " : miss" << dendl;
    if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
    return -ENOENT;
  }

  touch_lru(name, iter->second.lru_iter);

  ObjectCacheInfo& src = iter->second.info;
  if ((src.flags & mask) != mask) {
    ldout(cct, 10) << "cache get: name=" << name << " : type miss (requested=" << mask << ", cached=" << src.flags << ")" << dendl;
    if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
    return -ENOENT;
  }
  ldout(cct, 10) << "cache get: name=" << name << " : hit" << dendl;

  info = src;
  if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

  return 0;
}

void ObjectCache::put(string& name, ObjectCacheInfo& info)
{
  Mutex::Locker l(lock);

  ldout(cct, 10) << "cache put: name=" << name << dendl;
  map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
  if (iter == cache_map.end()) {
    ObjectCacheEntry entry;
    entry.lru_iter = lru.end();
    cache_map.insert(pair<string, ObjectCacheEntry>(name, entry));
    iter = cache_map.find(name);
  }
  ObjectCacheEntry& entry = iter->second;
  ObjectCacheInfo& target = entry.info;

  touch_lru(name, entry.lru_iter);

  target.status = info.status;

  if (info.status < 0) {
    target.flags = 0;
    target.xattrs.clear();
    target.data.clear();
    return;
  }

  target.flags |= info.flags;

  if (info.flags & CACHE_FLAG_META)
    target.meta = info.meta;
  else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
    target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

  if (info.flags & CACHE_FLAG_XATTRS) {
    target.xattrs = info.xattrs;
    map<string, bufferlist>::iterator iter;
    for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
      ldout(cct, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
    }
  } else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
    map<string, bufferlist>::iterator iter;
    for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
      ldout(cct, 10) << "removing xattr: name=" << iter->first << dendl;
      target.xattrs.erase(iter->first);
    }
    for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
      ldout(cct, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
      target.xattrs[iter->first] = iter->second;
    }
  }

  if (info.flags & CACHE_FLAG_DATA)
    target.data = info.data;
}

void ObjectCache::remove(string& name)
{
  Mutex::Locker l(lock);

  map<string, ObjectCacheEntry>::iterator iter = cache_map.find(name);
  if (iter == cache_map.end())
    return;

  ldout(cct, 10) << "removing " << name << " from cache" << dendl;

  remove_lru(name, iter->second.lru_iter);
  cache_map.erase(iter);
}

void ObjectCache::touch_lru(string& name, std::list<string>::iterator& lru_iter)
{
  while (lru.size() > (size_t)cct->_conf->rgw_cache_lru_size) {
    list<string>::iterator iter = lru.begin();
    if ((*iter).compare(name) == 0) {
      /*
       * if the entry we're touching happens to be at the lru end, don't remove it,
       * lru shrinking can wait for next time
       */
      break;
    }
    map<string, ObjectCacheEntry>::iterator map_iter = cache_map.find(*iter);
    ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
    if (map_iter != cache_map.end())
      cache_map.erase(map_iter);
    lru.pop_front();
  }

  if (lru_iter == lru.end()) {
    lru.push_back(name);
    lru_iter--;
    ldout(cct, 10) << "adding " << name << " to cache LRU end" << dendl;
  } else {
    ldout(cct, 10) << "moving " << name << " to cache LRU end" << dendl;
    lru.erase(lru_iter);
    lru.push_back(name);
    lru_iter = lru.end();
    --lru_iter;
  }
}

void ObjectCache::remove_lru(string& name, std::list<string>::iterator& lru_iter)
{
  if (lru_iter == lru.end())
    return;

  lru.erase(lru_iter);
  lru_iter = lru.end();
}


