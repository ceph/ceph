#include "rgw_cache.h"

#include <errno.h>

#define dout_subsys ceph_subsys_rgw

using namespace std;

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

  if (info.flags & CACHE_FLAG_OBJV)
    target.version = info.version;
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
  while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
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
    lru_size--;
  }

  if (lru_iter == lru.end()) {
    lru.push_back(name);
    lru_size++;
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
  lru_size--;
  lru_iter = lru.end();
}


