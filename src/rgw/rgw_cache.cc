#include "rgw_cache.h"

#include <errno.h>

using namespace std;


int ObjectCache::get(string& name, ObjectCacheInfo& info, uint32_t mask)
{
  map<string, ObjectCacheInfo>::iterator iter = cache_map.find(name);
  if (iter == cache_map.end())
    return -ENOENT;

  ObjectCacheInfo& src = iter->second;
  if ((src.flags & mask) != mask)
    return -ENOENT;

  info = src;

  return 0;
}

void ObjectCache::put(string& name, ObjectCacheInfo& info)
{
  map<string, ObjectCacheInfo>::iterator iter;
  ObjectCacheInfo& target = cache_map[name];

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
  else
    target.flags &= ~CACHE_FLAG_META; // any non-meta change should reset meta

  if (info.flags & CACHE_FLAG_XATTRS)
    target.xattrs = info.xattrs;

  if (info.flags & CACHE_FLAG_DATA)
    target.data = info.data;
}

void ObjectCache::remove(string& name)
{
  map<string, ObjectCacheInfo>::iterator iter = cache_map.find(name);
  if (iter != cache_map.end())
    cache_map.erase(iter);
}


