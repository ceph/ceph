#include "rgw_cache.h"

#include <errno.h>

using namespace std;


int ObjectCache::get(string& name, bufferlist& bl)
{
  map<string, bufferlist>::iterator iter = cache_map.find(name);
  if (iter == cache_map.end())
    return -ENOENT;

  bl = iter->second;

  return 0;
}

void ObjectCache::put(string& name, bufferlist& bl)
{
  map<string, bufferlist>::iterator iter;
  cache_map[name] = bl;
}


