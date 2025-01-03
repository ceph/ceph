// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_cache.h"
#include "rgw_perf_counters.h"

#include <errno.h>

#define dout_subsys ceph_subsys_rgw

using namespace std;

int ObjectCache::get(const DoutPrefixProvider *dpp, const string& name, ObjectCacheInfo& info, uint32_t mask, rgw_cache_entry_info *cache_info)
{

  std::shared_lock rl{lock};
  std::unique_lock wl{lock, std::defer_lock}; // may be promoted to write lock
  if (!enabled) {
    return -ENOENT;
  }
  auto iter = cache_map.find(name);
  if (iter == cache_map.end()) {
    ldpp_dout(dpp, 10) << "cache get: name=" << name << " : miss" << dendl;
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  if (expiry.count() &&
       (ceph::coarse_mono_clock::now() - iter->second.info.time_added) > expiry) {
    ldpp_dout(dpp, 10) << "cache get: name=" << name << " : expiry miss" << dendl;
    rl.unlock();
    wl.lock(); // write lock for expiration
    // check that wasn't already removed by other thread
    iter = cache_map.find(name);
    if (iter != cache_map.end()) {
      for (auto &kv : iter->second.chained_entries)
        kv.first->invalidate(kv.second);
      remove_lru(name, iter->second.lru_iter);
      cache_map.erase(iter);
    }
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  ObjectCacheEntry *entry = &iter->second;

  if (lru_counter - entry->lru_promotion_ts > lru_window) {
    ldpp_dout(dpp, 20) << "cache get: touching lru, lru_counter=" << lru_counter
                   << " promotion_ts=" << entry->lru_promotion_ts << dendl;
    rl.unlock();
    wl.lock(); // write lock for touch_lru()
    /* need to redo this because entry might have dropped off the cache */
    iter = cache_map.find(name);
    if (iter == cache_map.end()) {
      ldpp_dout(dpp, 10) << "lost race! cache get: name=" << name << " : miss" << dendl;
      if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
      return -ENOENT;
    }

    entry = &iter->second;
    /* check again, we might have lost a race here */
    if (lru_counter - entry->lru_promotion_ts > lru_window) {
      touch_lru(dpp, name, *entry, iter->second.lru_iter);
    }
  }

  ObjectCacheInfo& src = iter->second.info;
  if(src.status == -ENOENT) {
    ldpp_dout(dpp, 10) << "cache get: name=" << name << " : hit (negative entry)" << dendl;
    if (perfcounter) perfcounter->inc(l_rgw_cache_hit);
    return -ENODATA;
  }
  if ((src.flags & mask) != mask) {
    ldpp_dout(dpp, 10) << "cache get: name=" << name << " : type miss (requested=0x"
                   << std::hex << mask << ", cached=0x" << src.flags
                   << std::dec << ")" << dendl;
    if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
    return -ENOENT;
  }
  ldpp_dout(dpp, 10) << "cache get: name=" << name << " : hit (requested=0x"
                 << std::hex << mask << ", cached=0x" << src.flags
                 << std::dec << ")" << dendl;

  info = src;
  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry->gen;
  }
  if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

  return 0;
}

bool ObjectCache::chain_cache_entry(const DoutPrefixProvider *dpp,
                                    std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
				    RGWChainedCache::Entry *chained_entry)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  std::vector<ObjectCacheEntry*> entries;
  entries.reserve(cache_info_entries.size());
  /* first verify that all entries are still valid */
  for (auto cache_info : cache_info_entries) {
    ldpp_dout(dpp, 10) << "chain_cache_entry: cache_locator="
		   << cache_info->cache_locator << dendl;
    auto iter = cache_map.find(cache_info->cache_locator);
    if (iter == cache_map.end()) {
      ldpp_dout(dpp, 20) << "chain_cache_entry: couldn't find cache locator" << dendl;
      return false;
    }

    auto entry = &iter->second;

    if (entry->gen != cache_info->gen) {
      ldpp_dout(dpp, 20) << "chain_cache_entry: entry.gen (" << entry->gen
		     << ") != cache_info.gen (" << cache_info->gen << ")"
		     << dendl;
      return false;
    }
    entries.push_back(entry);
  }


  chained_entry->cache->chain_cb(chained_entry->key, chained_entry->data);

  for (auto entry : entries) {
    entry->chained_entries.push_back(make_pair(chained_entry->cache,
					       chained_entry->key));
  }

  return true;
}

void ObjectCache::put(const DoutPrefixProvider *dpp, const string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return;
  }

  ldpp_dout(dpp, 10) << "cache put: name=" << name << " info.flags=0x"
                 << std::hex << info.flags << std::dec << dendl;

  auto [iter, inserted] = cache_map.emplace(name, ObjectCacheEntry{});
  ObjectCacheEntry& entry = iter->second;
  entry.info.time_added = ceph::coarse_mono_clock::now();
  if (inserted) {
    entry.lru_iter = lru.end();
  }
  ObjectCacheInfo& target = entry.info;

  invalidate_lru(entry);

  entry.chained_entries.clear();
  entry.gen++;

  touch_lru(dpp, name, entry, entry.lru_iter);

  target.status = info.status;

  if (info.status < 0) {
    target.flags = 0;
    target.xattrs.clear();
    target.data.clear();
    return;
  }

  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry.gen;
  }

  // put() must include the latest version if we're going to keep caching it
  target.flags &= ~CACHE_FLAG_OBJV;

  target.flags |= info.flags;

  if (info.flags & CACHE_FLAG_META)
    target.meta = info.meta;
  else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
    target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

  if (info.flags & CACHE_FLAG_XATTRS) {
    target.xattrs = info.xattrs;
    map<string, bufferlist>::iterator iter;
    for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
      ldpp_dout(dpp, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
    }
  } else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
    map<string, bufferlist>::iterator iter;
    for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
      ldpp_dout(dpp, 10) << "removing xattr: name=" << iter->first << dendl;
      target.xattrs.erase(iter->first);
    }
    for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
      ldpp_dout(dpp, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
      target.xattrs[iter->first] = iter->second;
    }
  }

  if (info.flags & CACHE_FLAG_DATA)
    target.data = info.data;

  if (info.flags & CACHE_FLAG_OBJV)
    target.version = info.version;
}

// WARNING: This function /must not/ be modified to cache a
// negative lookup. It must only invalidate.
bool ObjectCache::invalidate_remove(const DoutPrefixProvider *dpp, const string& name)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  auto iter = cache_map.find(name);
  if (iter == cache_map.end())
    return false;

  ldpp_dout(dpp, 10) << "removing " << name << " from cache" << dendl;
  ObjectCacheEntry& entry = iter->second;

  for (auto& kv : entry.chained_entries) {
    kv.first->invalidate(kv.second);
  }

  remove_lru(name, iter->second.lru_iter);
  cache_map.erase(iter);
  return true;
}

void ObjectCache::touch_lru(const DoutPrefixProvider *dpp, const string& name, ObjectCacheEntry& entry,
			    std::list<string>::iterator& lru_iter)
{
  while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
    auto iter = lru.begin();
    if ((*iter).compare(name) == 0) {
      /*
       * if the entry we're touching happens to be at the lru end, don't remove it,
       * lru shrinking can wait for next time
       */
      break;
    }
    auto map_iter = cache_map.find(*iter);
    ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
    if (map_iter != cache_map.end()) {
      ObjectCacheEntry& entry = map_iter->second;
      invalidate_lru(entry);
      cache_map.erase(map_iter);
    }
    lru.pop_front();
    lru_size--;
  }

  if (lru_iter == lru.end()) {
    lru.push_back(name);
    lru_size++;
    lru_iter--;
    ldpp_dout(dpp, 10) << "adding " << name << " to cache LRU end" << dendl;
  } else {
    ldpp_dout(dpp, 10) << "moving " << name << " to cache LRU end" << dendl;
    lru.erase(lru_iter);
    lru.push_back(name);
    lru_iter = lru.end();
    --lru_iter;
  }

  lru_counter++;
  entry.lru_promotion_ts = lru_counter;
}

void ObjectCache::remove_lru(const string& name,
			     std::list<string>::iterator& lru_iter)
{
  if (lru_iter == lru.end())
    return;

  lru.erase(lru_iter);
  lru_size--;
  lru_iter = lru.end();
}

void ObjectCache::invalidate_lru(ObjectCacheEntry& entry)
{
  for (auto iter = entry.chained_entries.begin();
       iter != entry.chained_entries.end(); ++iter) {
    RGWChainedCache *chained_cache = iter->first;
    chained_cache->invalidate(iter->second);
  }
}

void ObjectCache::set_enabled(bool status)
{
  std::unique_lock l{lock};

  enabled = status;

  if (!enabled) {
    do_invalidate_all();
  }
}

void ObjectCache::invalidate_all()
{
  std::unique_lock l{lock};

  do_invalidate_all();
}

void ObjectCache::do_invalidate_all()
{
  cache_map.clear();
  lru.clear();

  lru_size = 0;
  lru_counter = 0;
  lru_window = 0;

  for (auto& cache : chained_cache) {
    cache->invalidate_all();
  }
}

void ObjectCache::chain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};
  chained_cache.push_back(cache);
}

void ObjectCache::unchain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};

  auto iter = chained_cache.begin();
  for (; iter != chained_cache.end(); ++iter) {
    if (cache == *iter) {
      chained_cache.erase(iter);
      cache->unregistered();
      return;
    }
  }
}

ObjectCache::~ObjectCache()
{
  for (auto cache : chained_cache) {
    cache->unregistered();
  }
}

void ObjectMetaInfo::generate_test_instances(list<ObjectMetaInfo*>& o)
{
  ObjectMetaInfo *m = new ObjectMetaInfo;
  m->size = 1024 * 1024;
  o.push_back(m);
  o.push_back(new ObjectMetaInfo);
}

void ObjectMetaInfo::dump(Formatter *f) const
{
  encode_json("size", size, f);
  encode_json("mtime", utime_t(mtime), f);
}

void ObjectCacheInfo::generate_test_instances(list<ObjectCacheInfo*>& o)
{
  using ceph::encode;
  ObjectCacheInfo *i = new ObjectCacheInfo;
  i->status = 0;
  i->flags = CACHE_FLAG_MODIFY_XATTRS;
  string s = "this is a string";
  string s2 = "this is a another string";
  bufferlist data, data2;
  encode(s, data);
  encode(s2, data2);
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
  encode_json("status", status, f);
  encode_json("flags", flags, f);
  encode_json("data", data, f);
  encode_json_map("xattrs", "name", "value", "length", xattrs, f);
  encode_json_map("rm_xattrs", "name", "value", "length", rm_xattrs, f);
  encode_json("meta", meta, f);

}

void RGWCacheNotifyInfo::generate_test_instances(list<RGWCacheNotifyInfo*>& o)
{
  o.push_back(new RGWCacheNotifyInfo);
}

void RGWCacheNotifyInfo::dump(Formatter *f) const
{
  encode_json("op", op, f);
  encode_json("obj", obj, f);
  encode_json("obj_info", obj_info, f);
  encode_json("ofs", ofs, f);
  encode_json("ns", ns, f);
}

