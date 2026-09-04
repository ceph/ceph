// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_perf_counters.h"
#include <boost/redis/config.hpp>
#include <boost/version.hpp>
#include <memory>
#include "rgw_sal_d4n.h"
#include "d4n_policy.h"
#include "d4n_directory.h"
#include "d4n_connection.h"
#include "d4n_directory_redis.h"
#include "d4n_directory_fdb.h"
#include "rgw_ssd_driver.h"

namespace rgw { namespace sal {

static constexpr uint8_t OBJ_INSTANCE_LEN = 32;

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;
  
  return dynamic_cast<FilterObject*>(t)->get_next();
}

D4NFilterDriver::D4NFilterDriver(Driver* _next, boost::asio::io_context& io_context, bool admin) : FilterDriver(_next),
												   io_context(io_context), 
												   y(null_yield)
{
  rgw::cache::Partition partition_info;
  partition_info.location = g_conf()->rgw_d4n_l1_datacache_persistent_path;
  partition_info.name = "d4n";
  partition_info.type = "read-cache";
  partition_info.reserve_size = g_conf()->rgw_d4n_l1_datacache_disk_reserve;
  cacheDriver = std::make_unique<rgw::cache::SSDDriver>(partition_info, io_context, admin);
}

D4NFilterDriver::~D4NFilterDriver() = default;

int D4NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  namespace net = boost::asio;
  using boost::redis::config;

  std::string address = cct->_conf->rgw_d4n_l1_datacache_address;
  config cfg;
  cfg.addr.host = address.substr(0, address.find(":"));
  cfg.addr.port = address.substr(address.find(":") + 1, address.length());
  cfg.clientname = "D4N.Filter";

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 0) << "D4NFilterDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
    return -EDESTADDRREQ;
  }


  directory_type = dpp->get_cct()->_conf->rgw_d4n_directory_type;
  if (directory_type == "redis"){

    auto redis_native = std::make_shared<boost::redis::connection>(boost::asio::make_strand(io_context));
    conn = std::make_shared<rgw::d4n::RedisConnection>(redis_native);
    auto redis_conn = std::dynamic_pointer_cast<rgw::d4n::RedisConnection>(conn);
    if (!redis_conn) {
      ldpp_dout(dpp, 1) << "Wrong directory type: Redis " << dendl;
      return -1;
    }

    dir = std::make_unique<rgw::d4n::RedisDirectory>(redis_conn);
    objDir = std::make_unique<rgw::d4n::RedisObjectDirectory>(redis_conn);
    blockDir = std::make_unique<rgw::d4n::RedisBlockDirectory>(redis_conn);
    bucketDir = std::make_unique<rgw::d4n::RedisBucketDirectory>(redis_conn);

#if BOOST_VERSION >= 108900
    redis_conn->get_redis_conn()->async_run(cfg, net::consign(net::detached, redis_conn->get_redis_conn()));
#else
    redis_conn->get_redis_conn()->async_run(cfg, {}, net::consign(net::detached, redis_conn->get_redis_conn()));
#endif

    //setting the connection pool size and other parameters
    uint64_t rgw_redis_connection_pool_size = dpp->get_cct()->_conf->rgw_redis_connection_pool_size;
    std::shared_ptr<rgw::d4n::RedisPool>redis_pool = nullptr;
    if(rgw_redis_connection_pool_size>0){
      redis_pool = std::make_shared<rgw::d4n::RedisPool>(&io_context, cfg, rgw_redis_connection_pool_size);
      ldpp_dout(dpp, 10) << "redis connection pool created with " << rgw_redis_connection_pool_size << " connections "  << dendl;
    }

	auto redisObjDir =
    	dynamic_cast<rgw::d4n::RedisObjectDirectory*>(objDir.get());
	auto redisBlockDir =
    	dynamic_cast<rgw::d4n::RedisBlockDirectory*>(blockDir.get());
	auto redisBucketDir =
    	dynamic_cast<rgw::d4n::RedisBucketDirectory*>(bucketDir.get());

	if (redisObjDir) {
    	  redisObjDir->set_redis_pool(redis_pool);
	}
	if (redisBlockDir) {
    	  redisBlockDir->set_redis_pool(redis_pool);
	}
	if (redisBucketDir) {
    	  redisBucketDir->set_redis_pool(redis_pool);
	}

  }
  else if (directory_type == "fdb") {
    auto fdb_db = lfdb::create_database();

    dir = std::make_unique<rgw::d4n::FDBDirectory>(fdb_db); 
    objDir = std::make_unique<rgw::d4n::FDBObjectDirectory>(fdb_db);
    blockDir = std::make_unique<rgw::d4n::FDBBlockDirectory>(fdb_db);
    bucketDir = std::make_unique<rgw::d4n::FDBBucketDirectory>(fdb_db);
  }

  //since we are using references here, it is important to initialize policyDriver after the directories.
  policyDriver = std::make_unique<rgw::d4n::PolicyDriver>(*dir, *blockDir, *objDir, *bucketDir, directory_type, cacheDriver.get(), "lfuda", this->y);
  if (auto ret = FilterDriver::initialize(cct, dpp); ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to initialize filter driver: " << ret << dendl;
    return ret;
  }

  if (auto ret = cacheDriver->initialize(dpp); ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to initialize cache driver: " << ret << dendl;
    return ret;
  }
  if (auto ret = policyDriver->get_cache_policy()->init(cct, dpp, io_context, next); ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to initialize policy driver: " << ret << dendl;
    return ret;
  }
 
  if (dpp->get_cct()->_conf->rgw_d4n_async_remote_put) {
    int thread_pool_size = dpp->get_cct()->_conf->rgw_d4n_thread_pool_size;
    d4n_thread_pool = std::make_unique<boost::asio::thread_pool>(thread_pool_size);
    auto executor = get_d4n_executor();
    initialize_pool(dpp, executor, thread_pool_size);
  }

  return 0;
}

std::unique_ptr<User> D4NFilterDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<D4NFilterUser>(std::move(user), this);
}

std::unique_ptr<Object> D4NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this, filter);
}

std::unique_ptr<Bucket> D4NFilterDriver::get_bucket(const RGWBucketInfo& i)
{
  return std::make_unique<D4NFilterBucket>(next->get_bucket(i), this);
}

int D4NFilterDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
				 std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  const int ret = next->load_bucket(dpp, b, &nb, y);
  *bucket = std::make_unique<D4NFilterBucket>(std::move(nb), this);
  return ret;
}

int D4NFilterBucket::create(const DoutPrefixProvider* dpp,
                            const CreateParams& params,
                            optional_yield y)
{
  return next->create(dpp, params, y);
}

int D4NFilterBucket::fetch_objects_batch(const DoutPrefixProvider* dpp, const ListParams& params, int batch_size,
                            std::string& cursor_or_start, std::string& marker, bool is_first_batch, FetchContext& fetch_ctx, optional_yield y)
{
  auto bucketDir = this->filter->get_bucket_dir();
  // Only relevant on the very first batch: are we resuming mid-way
  // through a specific object's version list? If so, that object's key
  // itself must still be included in this fetch.
  bool marker_needs_inclusion =
      is_first_batch && params.list_versions && !params.marker.instance.empty();
  std::string continuation_token;

  auto ret = bucketDir->list_objects(
    dpp,
    this->get_bucket_id(),
    cursor_or_start, //backend specific start token (needed for Redis)
    params.prefix,
    marker,
    batch_size,
    marker_needs_inclusion,
    fetch_ctx.objects,
    continuation_token,
    y);

  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " scan_objects failed: " << ret << dendl;
    return ret;
  }
  for (const auto& obj : fetch_ctx.objects) {
    ldpp_dout(dpp, 20) << "obj.objName: " << obj.objName << dendl;
    ldpp_dout(dpp, 20) << "obj.bucketId: " << obj.bucketId << dendl;
    ldpp_dout(dpp, 20) << "obj.etag: " << obj.etag << dendl;
    ldpp_dout(dpp, 20) << "obj.size: " << obj.size << dendl;
    ldpp_dout(dpp, 20) << "obj.creationTime: " << obj.creationTime << dendl;
    ldpp_dout(dpp, 20) << "obj.deleteMarker: " << obj.deleteMarker << dendl;
  }

  fetch_ctx.has_more = !continuation_token.empty();
  cursor_or_start = continuation_token;
  marker = continuation_token;

  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " fetch_ctx.has_more: " << fetch_ctx.has_more << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " marker: " << marker << dendl;
  return 0;
}

int D4NFilterBucket::build_versioned_entries(const DoutPrefixProvider* dpp, const rgw::d4n::CacheObject& obj,
                              const ListParams& params, std::vector<rgw_bucket_dir_entry>& entries,
                              std::string& last_version, int& num_objs, bool& object_exhausted, int max, optional_yield y) {
  std::vector<rgw::d4n::CacheObjectVersion> versions;
  std::string objName = obj.objName;
  if (objName[0] == '_') {
    objName = "_" + obj.objName;
  }
  if (params.list_versions) {
    uint64_t count = (max > num_objs) ? static_cast<uint64_t>(max - num_objs) : 0;
    if (count == 0) {
        return 0;   // nothing to add, page already at capacity
    }
    auto objDir = this->filter->get_obj_dir();
    std::string bucket_id = this->get_bucket_id();
    std::string start_version;
    if (params.marker.instance.empty() || params.marker.name != obj.objName) {
      start_version = "";
    } else {
      start_version = params.marker.instance;
    }

    std::string continuation_token;
    auto ret = objDir->list_versions(dpp, bucket_id, obj.objName, start_version, count, versions, continuation_token, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " list_versions failed: " << ret << dendl;
      return ret;
    }
    // Empty continuation_token == this object's version history is
    // fully drained; non-empty == more versions remain beyond this page.
    object_exhausted = continuation_token.empty();
    for (const auto& version : versions) {
      ldpp_dout(dpp, 20) << "version.objName: " << version.objName << dendl;
      ldpp_dout(dpp, 20) << "version.bucketId: " << version.bucketId << dendl;
      ldpp_dout(dpp, 20) << "version.version: " << version.version << dendl;
      ldpp_dout(dpp, 20) << "version.user_id: " << version.user_id << dendl;
      ldpp_dout(dpp, 20) << "version.display_name: " << version.display_name << dendl;
    }
  }

  rgw_bucket_dir_entry entry;
  entry.key.name = objName;
  if (obj.deleteMarker) {
    entry.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
  }

  entry.meta.storage_class = "CACHE";
  entry.meta.size = obj.size;
  entry.meta.accounted_size = obj.size;
  entry.meta.etag = obj.etag;

  if (!obj.creationTime.empty()) {
    try {
      auto ns = std::stoll(obj.creationTime);
      entry.meta.mtime = ceph::real_time(std::chrono::nanoseconds(ns));
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " Invalid time value: "
                        << obj.creationTime << dendl;
    }
  }
  if (!versions.empty()) {
    for (size_t i = 0; i < versions.size(); i++) {
      const auto& version = versions[i].version;
      last_version = version;
      entry.flags = rgw_bucket_dir_entry::FLAG_VER;
      if (i == 0) {
        entry.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
      } else {
        entry.flags |= rgw_bucket_dir_entry::FLAG_VER_MARKER;
      }
      entry.key.instance = version;
      entry.meta.owner = versions[i].user_id;
      entry.meta.owner_display_name = versions[i].display_name;
      entries.emplace_back(entry);
      num_objs++;  // each version counts separately toward max
      if (num_objs == max) {
        break;  // caller checks num_objs to decide truncation
      }
    }
  } else if (!params.list_versions) {
    // Single version (current object)
    entries.emplace_back(entry);
    num_objs++;
  }

  return 0;
}

int D4NFilterBucket::process_objects_batch(const DoutPrefixProvider* dpp,
                              const std::vector<rgw::d4n::CacheObject>& input_objects,
                              const ListParams& params,
                              ListResults& cache_results,
                              ListResults& store_results,
                              std::string& last_version,
                              int& num_objs, int max,
                              bool is_truncated, //is input_objects truncated
                              bool& stopped_early,
                              optional_yield y)
{
  ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " is_truncated: " << is_truncated << dendl;
  stopped_early = false;
  for (size_t idx = 0; idx < input_objects.size(); ++idx) {
    const std::string& obj = input_objects[idx].objName;
    const rgw::d4n::CacheObject& cache_obj = input_objects[idx];
    //if the current batch being processed has more objects
    bool more_objs_in_cur_batch = (idx + 1 < input_objects.size());
    // 1. Marker filtering
    // Skipping/filtering is natively performed by FDB
    // But is needed for Redis zscan method
    if (!params.marker.name.empty() && obj <= params.marker.name) {
      if (obj != params.marker.name || !params.list_versions || params.marker.instance.empty()) {
          continue;
      }
    }

    // 2. Delimiter grouping -- consumes exactly 1 unit of budget, checked immediately
    if (!params.delim.empty()) {
      size_t pos = obj.find(params.delim, params.prefix.length());
      if (pos != std::string::npos) {
        std::string delim_str = obj.substr(0, pos + 1);
        if (cache_results.common_prefixes.find(delim_str) == cache_results.common_prefixes.end()) {
          cache_results.common_prefixes.emplace(std::make_pair(delim_str, true));
          num_objs++;
          if (num_objs == max) {
            cache_results.is_truncated = true;
            cache_results.next_marker.name = delim_str;
            stopped_early = true;
            ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.is_truncated: " << cache_results.is_truncated << dendl;
            ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.next_marker.name: " << cache_results.next_marker.name << dendl;
            return 0;   // stop immediately -- lex order preserved
          }
        }
        continue;  // grouped objects aren't individually expanded
      }
    }

    // 3. Not grouped -- expand versions (or single entry) against remaining budget
    bool object_exhausted = true; // all versions listed
    int ret = build_versioned_entries(dpp, cache_obj, params, cache_results.objs, last_version, num_objs, object_exhausted, max, y);
    if (ret < 0) return ret;

    if (num_objs == max) {
      //more versions still remaining
      if (!object_exhausted || more_objs_in_cur_batch || is_truncated) {
        cache_results.is_truncated = true;
        cache_results.next_marker.name = cache_results.objs.empty() ? obj : cache_results.objs.back().key.name;
        if (params.list_versions && !last_version.empty()) {
          cache_results.next_marker.instance = last_version;
        }
      }
      stopped_early = true;
      return 0;   // stop immediately -- next object (if any) not touched
    }
  }
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.is_truncated: " << cache_results.is_truncated << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.next_marker.name: " << cache_results.next_marker.name << dendl;
  return 0;
}

int D4NFilterBucket::populate_cache_results(const DoutPrefixProvider* dpp, std::vector<rgw_bucket_dir_entry>& entries,
                                            optional_yield y)
{
  if (entries.empty()) {
    return 0;
  }

  auto directory_type = this->filter->get_directory_type();
  if (directory_type == "fdb") {
    return 0;
  }

  auto blockDir = this->filter->get_block_dir();
  size_t batch_size = 100;  // Process blocks in batches

  for (size_t start = 0; start < entries.size(); start += batch_size) {
    size_t end = std::min(start + batch_size, entries.size());
    std::vector<rgw::d4n::CacheBlock> blocks(end - start);

    // Prepare cache block requests
    for (size_t i = start; i < end; i++) {
      const auto& entry = entries[i];
      size_t block_idx = i - start;

      std::string obj_name = entry.key.name;
      if (obj_name[0] == '_') {
        obj_name = "_" + entry.key.name;
      }

      if (entry.key.instance == "null") {
        blocks[block_idx].cacheObj.objName = "_:null_" + entry.key.name;
      } else {
        rgw_obj_key key{entry.key.name, entry.key.instance};
        blocks[block_idx].cacheObj.objName = key.get_oid();
      }
      blocks[block_idx].cacheObj.bucketName = this->get_bucket_id();
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " bucketName: " << blocks[block_idx].cacheObj.bucketName << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " objName: " << blocks[block_idx].cacheObj.objName << dendl;
    }

    // Fetch metadata from cache
    auto ret = blockDir->get(dpp, blocks, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " blockDir->get() failed: " << ret << dendl;
      return ret;
    }

    // Convert to result entries
    for (size_t i = 0; i < blocks.size(); i++) {
      const auto& block = blocks[i];
      if (block.cacheObj.objName.empty()) {
        continue;
      }

      size_t entry_idx = start + i;
      auto& source_entry = entries[entry_idx];
      if (block.deleteMarker) {
        source_entry.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
      }

      source_entry.meta.storage_class = "CACHE";
      source_entry.meta.size = block.cacheObj.size;
      source_entry.meta.accounted_size = block.cacheObj.size;

      try {
        auto ns = std::stoll(block.cacheObj.creationTime);
        source_entry.meta.mtime = ceph::real_time(std::chrono::nanoseconds(ns));
      } catch (const std::exception& e) {
        ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " Invalid time value: "
                          << block.cacheObj.creationTime << dendl;
      }

      source_entry.meta.etag = block.cacheObj.etag;
      source_entry.meta.owner = block.cacheObj.user_id;
      source_entry.meta.owner_display_name = block.cacheObj.display_name;
    }
  }

  return 0;
}

void D4NFilterBucket::merge_results(const DoutPrefixProvider* dpp, const ListParams& params,
                                    ListResults& cache_results, ListResults& store_results,
                                    int max, ListResults& results)
{
  if (cache_results.objs.empty() && cache_results.common_prefixes.empty()) {
    results = std::move(store_results);
    return;
  }
  if (store_results.objs.empty() && store_results.common_prefixes.empty()) {
    results = std::move(cache_results);
    return;
  }

  // Union of common prefixes from both sources, computed explicitly here
  std::map<std::string, bool> all_prefixes = store_results.common_prefixes;
  for (auto& kv : cache_results.common_prefixes) {
      all_prefixes.emplace(kv.first, kv.second);
  }

  std::vector<rgw_bucket_dir_entry> merged;
  std::map<std::string, bool> out_prefixes;
  merged.reserve(max);

  size_t i = 0, j = 0;
  auto p_it = all_prefixes.begin();
  int count = 0;

  while (count < max &&
          (i < cache_results.objs.size() || j < store_results.objs.size() || p_it != all_prefixes.end())) {
    bool has_c = i < cache_results.objs.size();
    bool has_s = j < store_results.objs.size();
    bool has_p = p_it != all_prefixes.end();

    std::optional<std::string> smallest;
    if (has_c) smallest = cache_results.objs[i].key.name;
    if (has_s && (!smallest || store_results.objs[j].key.name < *smallest))
        smallest = store_results.objs[j].key.name;
    if (has_p && (!smallest || p_it->first < *smallest))
        smallest = p_it->first;

    bool take_p = has_p && p_it->first == *smallest;
    bool take_c = !take_p && has_c && cache_results.objs[i].key.name == *smallest;
    bool take_s = !take_p && has_s && store_results.objs[j].key.name == *smallest;

    if (take_p) {
      out_prefixes.emplace(p_it->first, p_it->second);
      ++p_it;
    } else if (take_c && take_s) {
      // Both streams currently point at the same object name. Rather than
      // resolving just this one pair, walk the FULL run of entries sharing
      // this name in both streams -- necessary when list_versions is set
      // and only some versions of this key are cached(e.g. cache has 1 cached
      // version, store has all 3).
      const std::string name = *smallest;
      while (count < max &&
        i < cache_results.objs.size() && j < store_results.objs.size() &&
        cache_results.objs[i].key.name == name &&
        store_results.objs[j].key.name == name) {
        const auto& c_inst = cache_results.objs[i].key.instance;
        const auto& s_inst = store_results.objs[j].key.instance;
        if (!params.list_versions || c_inst == s_inst) {
          // Same exact version (or non-versioned listing)
          merged.push_back(cache_results.objs[i]);
          i++; j++;
        } else {
          // Differing versions of the same key present in both streams.
          // Both streams are newest-first for a given key, so emit
          // cache's copy here and let store's remaining run for this name
          // (if any) be picked up by the plain "remaining" loop below,
          // once i moves past this name or the outer loop exits.
          merged.push_back(cache_results.objs[i]);
          i++;
        }
        count++;
      }
      // Any leftover same-name entries still in EXACTLY ONE of the two
      // streams (this can happen when list_versions is set and the two
      // sources have different numbers of versions for this key) are
      // picked up on the next outer-loop iteration
    } else if (take_c) {
      merged.push_back(cache_results.objs[i]);
      i++;
    } else if (take_s) {
      merged.push_back(store_results.objs[j]);
      j++;
    }
    count++;
  }

  results.objs = std::move(merged);
  results.common_prefixes = std::move(out_prefixes);

  //Leftover: items were already fetched into cache_results.objs,
  //store_results.objs, all_prefixes but didn't fit within max.

  //Cache/store truncation: everything fetched was fully consumed, but
  //the cache backend fetch/ store backend fetch indicates more data exist

  bool leftover = (i < cache_results.objs.size()) ||
                    (j < store_results.objs.size()) ||
                    (p_it != all_prefixes.end());
  if (leftover) {
    results.is_truncated = true;
    if (!results.objs.empty() && !results.common_prefixes.empty()) {
      const std::string& last_obj = results.objs.back().key.name;
      const std::string& last_pfx = results.common_prefixes.rbegin()->first;
      results.next_marker = (last_pfx > last_obj)
          ? rgw_obj_key(last_pfx) : results.objs.back().key;
    } else if (!results.common_prefixes.empty()) {
      results.next_marker = rgw_obj_key(results.common_prefixes.rbegin()->first);
    } else if (!results.objs.empty()) {
      results.next_marker = results.objs.back().key;
    }
  } else if (cache_results.is_truncated || store_results.is_truncated) {
      results.is_truncated = true;
    if (cache_results.is_truncated && store_results.is_truncated) {
      results.next_marker = (cache_results.next_marker <= store_results.next_marker)
          ? std::move(cache_results.next_marker) : std::move(store_results.next_marker);
    } else if (store_results.is_truncated) {
      results.next_marker = std::move(store_results.next_marker);
    } else {
      results.next_marker = std::move(cache_results.next_marker);
    }
  }
 
}

int D4NFilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
                          ListResults& results, optional_yield y)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.marker.name: " << params.marker.name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.marker.instance: " << params.marker.instance << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " params.end_marker.key: " << params.end_marker.name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " max: " << max << dendl;

  if (max == 0) {
    return 0;
  }
  ListResults cache_results;
  ListResults store_results;
  cache_results.is_truncated = false;

  if (g_conf()->d4n_writecache_enabled) {
    std::string cursor_or_start;  // Empty to start
    std::string marker = params.marker.name;
    int num_objs = 0;
    std::string last_version;
    bool is_first_batch = true;

    while (num_objs < max) {
      FetchContext fetch_ctx;
      // 1. Fetch from cache using list_objects method that performs prefix matching or range requests
      int ret = fetch_objects_batch(dpp, params, max,
                                      cursor_or_start, marker, is_first_batch, fetch_ctx, y);
      if (ret < 0) {
        return ret;
      }
      is_first_batch = false;
      // Distinguish "genuinely exhausted" from "this page was empty but
      // scanning should continue" (Redis ZSCAN can return 0 matches on
      // a page while still having more to scan).
      if (fetch_ctx.objects.empty() && fetch_ctx.has_more) {
        continue;
      }
      if (fetch_ctx.objects.empty()) {
        break;
      }
      bool stopped_early = false;
      // 2. Filter(if needed), group by delimiter and build versioned entries
      ret = process_objects_batch(dpp, fetch_ctx.objects, params, cache_results,
                                  store_results, last_version, num_objs, max,
                                  fetch_ctx.has_more, stopped_early, y);
      if (ret < 0) {
        return ret;
      }
      // 3. Fetch block metadata and populate cache_results.objs
      ret = populate_cache_results(dpp, cache_results.objs, y);
      if (ret < 0) {
        return ret;
      }

      if (!fetch_ctx.has_more || stopped_early) {
        break;
      }
    } //end while
  } // d4n_writecache_enabled

  if (cache_request) {
    results = std::move(cache_results);
    return 0;
  }

  // 4. Call list method of backend store
  int ret = next->list(dpp, params, max, store_results, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__
                      << " store list failed: " << ret << dendl;
    return ret;
  }

  // 5. Merge cache and store results
  merge_results(dpp, params, cache_results, store_results, max, results);

  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.is_truncated: " << results.is_truncated << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.next_marker.name: " << results.next_marker.name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " cache_results.next_marker.instance: " << results.next_marker.instance << dendl;
  return 0;
}


constexpr int OBJECT_LIST_VAL = 1000;
constexpr int PIPELINE_MAX = 10000;
int D4NFilterBucket::remove(const DoutPrefixProvider* dpp,
			    bool delete_children,
			    optional_yield y)
{
  ListParams params;
  params.list_versions = true;
  ListResults results;
  int ret;

  return_blocks = true; 
  auto blockDir = this->filter->get_block_dir();
  auto objDir = this->filter->get_obj_dir();
  std::vector<rgw::d4n::CacheBlock> blocks; 
  std::vector<rgw::d4n::CacheObj> objects; 

  do {
    results.objs.clear();

    ret = list(dpp, params, OBJECT_LIST_VAL, results, y);
    if (ret < 0) {
      return ret;
    }

    if (!results.objs.empty() && !delete_children) {
      ldpp_dout(dpp, 10) << "ERROR: could not remove non-empty bucket " << this->get_name() << dendl;
      return -ENOTEMPTY;
    }

    

    for (const auto& obj : results.objs) { 
      if (((PIPELINE_MAX - blocks.size()) <= OBJECT_LIST_VAL) || (blocks.size() > (PIPELINE_MAX - 1000))) {
	for (auto& block : blocks) {
          if ((ret = blockDir->del(dpp, &block, y)) < 0) {
            ldpp_dout(dpp, 10)
              << "D4NFilterBucket::" << __func__
              << "(): Failed to delete cached object in block directory, ret="
              << ret << dendl;
            return ret;
          }
        }
        blocks.clear();
      }

      // Handle head objects
      std::unique_ptr<rgw::sal::Object> c_obj = this->get_object(obj.key);
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): handling object=" << obj.key << dendl;

      rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = c_obj->get_name(),
        .bucketName = this->get_bucket_id(),
      };

      rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
        .cacheObj = object,
        .blockID = 0,
        .size = 0,
      };
      
      blocks.push_back(block);
      objects.push_back(object);

      std::string oid_version;
      if (c_obj->have_instance()) {
        oid_version = c_obj->get_instance();
      } else {
        oid_version = "null";
      }
      off_t lst = obj.meta.size;
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): Obj size=" << lst << dendl;
      block.cacheObj.objName = "_:" + oid_version + "_" + block.cacheObj.objName;
      auto it = dir_blocks.find(block.cacheObj.objName);
      if (it != dir_blocks.end() && it->second.cacheObj.dirty) {
	    if (!this->filter->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, get_cache_block_prefix(c_obj.get(), it->second.version))) {
	      ldpp_dout(dpp, 10) << "D4NFilterBucket::" << __func__ << "(): Failed to invalidate obj=" << c_obj->get_name() << " in cache" << dendl;
	      return -EINVAL;
	    }
      /* For clean objects in the cache, inline deletes are avoided in favor of lazy deletes that occur through
       * later eviction calls. */
      } else {
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): Listing retrieved from backend for object " << c_obj->get_name() << dendl;
      }

      // Handle versioned head objects
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): versioned oid: " << block.cacheObj.objName << dendl;
      blocks.push_back(block);

      // Handle data blocks
      ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): Object size=" << lst << dendl;
      off_t fst = 0;
      do {
        /* The addition of data blocks to the blocks structure may push its size over PIPELINE_MAX, so
	     * pipelined calls must also made during this loop. */
	if (((PIPELINE_MAX - blocks.size()) <= OBJECT_LIST_VAL) || (blocks.size() > (PIPELINE_MAX - 1000))) { 
	  for (auto& block : blocks) {
            if ((ret = blockDir->del(dpp, &block, y)) < 0) {
              ldpp_dout(dpp, 10)
                << "D4NFilterBucket::" << __func__
                << "(): Failed to delete cached object in block directory, ret="
                << ret << dendl;
              return ret;
            }
          }
          blocks.clear();
	}

        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): handling object=" << obj.key << dendl;
        rgw::d4n::CacheBlock data_block;
        if (fst >= lst) {
          break;
        }
        off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
        off_t cur_len = cur_size - fst;
        data_block.cacheObj.bucketName = this->get_bucket_id();
        data_block.cacheObj.objName = c_obj->get_oid();
        ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): data_block=" << data_block.cacheObj.objName << dendl;
        data_block.size = cur_len;
        data_block.blockID = fst;

        fst += cur_len;
        blocks.push_back(data_block);

      } while (fst < lst); // end - do
    }

    /* Use pipelining for batches of ~10k commands since that is the max suggested
     * in redis docs */
    if (((PIPELINE_MAX - blocks.size()) <= OBJECT_LIST_VAL) || (blocks.size() > (PIPELINE_MAX - 1000))) { 
      for (auto& block : blocks) {
        if ((ret = blockDir->del(dpp, &block, y)) < 0) {
          ldpp_dout(dpp, 10)
            << "D4NFilterBucket::" << __func__
            << "(): Failed to delete cached object in block directory, ret="
            << ret << dendl;
          return ret;
        }
      }
      blocks.clear();
    }
    if ((PIPELINE_MAX - objects.size()) <= OBJECT_LIST_VAL) {
      for (auto& object : objects) {
        if ((ret = objDir->del(dpp, &object, y)) < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterBucket::" << __func__ << "(): Failed to delete bucket in bucket directory, ret=" << ret << dendl;
          return ret;
        }
      }
      objects.clear();
    }
  } while (results.is_truncated);

  // One more delete to clean up remaining blocks if present
  if (blocks.size()) {
    for (auto& block : blocks) {
      if ((ret = blockDir->del(dpp, &block, y)) < 0) {
        ldpp_dout(dpp, 10)
          << "D4NFilterBucket::" << __func__
          << "(): Failed to delete cached object in block directory, ret="
          << ret << dendl;
        return ret;
      }
    }
  }
  if (objects.size()) {
    for (auto& object : objects) {
      if ((ret = objDir->del(dpp, &object, y)) < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterBucket::" << __func__ << "(): Failed to delete bucket in bucket directory, ret=" << ret << dendl;
        return ret;
      }
    }
  }
  if ((ret = this->filter->get_bucket_dir()->del(dpp, this->get_bucket_id(), y)) < 0 && (ret != -ENOENT)) {
    ldpp_dout(dpp, 10) << "D4NFilterBucket::" << __func__ << "(): Failed to delete bucket in bucket directory, ret=" << ret << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): calling next->remove" << dendl;
  return next->remove(dpp, delete_children, y);
}

int D4NFilterBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  // if the bucket exists in the bucket directory, then there are objects in the local cache
  int ret;
  if ((ret = this->filter->get_bucket_dir()->exist_key(dpp, this->get_bucket_id(), y)) < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterBucket::" << __func__ << "(): Failed to retrieve bucket in bucket directory, ret=" << ret << dendl;
    return ret;
  } if (ret == 0) {
    ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << "(): calling next->check_empty" << dendl;
    return next->check_empty(dpp, y);
  } else {
    return -ENOTEMPTY;
  }
}

std::unique_ptr<MultipartUpload> D4NFilterBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu =
    next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<D4NFilterMultipartUpload>(std::move(nmu), this, this->filter);
}

int D4NFilterObject::copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              rgw::sal::DataProcessorFactory* dp_factory,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  bool write_to_cache = g_conf()->d4n_writecache_enabled;
  bool dirty{false};
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(this->get_read_op());
  read_op->params.mod_ptr = mod_ptr;
  read_op->params.unmod_ptr = unmod_ptr;
  read_op->params.high_precision_time = high_precision_time;
  read_op->params.if_match = if_match;
  read_op->params.if_nomatch = if_nomatch;
  if (auto ret = read_op->prepare(y, dpp); ret < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): prepare method failed with ret: " << ret << dendl;
    if (ret == -ERR_NOT_MODIFIED) {
      ret = ERR_PRECONDITION_FAILED;
    }
    return ret;
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): is_multipart: " << is_multipart() << dendl;
  //for multipart objects or for read only cache, write to backend store
  if (is_multipart() || !write_to_cache) {
    write_to_cache = false;
    auto ret = next->copy_object(owner, remote_user, info, source_zone,
                           nextObject(dest_object),
                           nextBucket(dest_bucket),
                           nextBucket(src_bucket),
                           dest_placement, src_mtime, mtime,
                           mod_ptr, unmod_ptr, high_precision_time, if_match,
                           if_nomatch, attrs_mod, copy_if_newer, attrs,
                           category, olh_epoch, delete_at, version_id, tag,
                           etag, progress_cb, progress_data, dp_factory, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): next->copy_object failed with ret: " << ret << dendl;
      return ret;
    }
  }

  this->dest_object = dest_object;
  this->dest_bucket = dest_bucket;
  D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(dest_object);

  rgw::sal::Attrs baseAttrs;
  //ATTRSMOD_NONE - the attributes of the source object will be copied without modifications, attrs parameter is ignored
  if (attrs_mod == rgw::sal::ATTRSMOD_NONE) {
    baseAttrs = this->get_attrs();
    baseAttrs.erase(RGW_CACHE_ATTR_VERSION_ID); //delete source version id
    if (version_id) {
      bufferlist bl_val;
      bl_val.append(*version_id);
      baseAttrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val); //populate destination version id
    }
    auto titer = attrs.find(RGW_ATTR_TAGS);
    if (titer != attrs.end()) {
      baseAttrs[RGW_ATTR_TAGS] = titer->second;
    }
  }

  //ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes are overwritten by values contained in attrs parameter.
  if (attrs_mod == rgw::sal::ATTRSMOD_MERGE) { /* Merge */
    rgw::sal::Attrs::iterator iter;

    for (const auto& pair : attrs) {
      iter = baseAttrs.find(pair.first);

      if (iter != baseAttrs.end()) {
        iter->second = pair.second;
      } else {
        baseAttrs.insert({pair.first, pair.second});
      }
    }
  } else if (attrs_mod == rgw::sal::ATTRSMOD_REPLACE) { /* Replace */
    //ATTRSMOD_REPLACE - new object will have the attributes provided by attrs parameter, source object attributes are not copied;
    baseAttrs.insert(attrs.begin(), attrs.end());
  }


  ceph::real_time creationTime;
  std::string dest_version;
  if (write_to_cache) {
    dirty = true;
    if (!dest_object->have_instance()) {
      if (dest_object->get_bucket()->versioned() && !dest_object->get_bucket()->versioning_enabled()) { //if versioning is suspended
        dest_version = "null";
      } else {
        char buf[OBJ_INSTANCE_LEN + 1];
        gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
        dest_version = buf; //version for non-versioned objects, using gen_rand_alphanumeric_no_underscore for the time being
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): generating version: " << version << dendl;
      }
    } else {
      dest_version = dest_object->get_instance();
    }
    d4n_dest_object->set_object_version(dest_version);
    if (auto ret = read_op->iterate(dpp, 0, (this->get_size() - 1), nullptr, y); ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): iterate method failed with ret: " << ret << dendl;
      return ret;
    }

    ceph::real_time dest_mtime;
    if (mtime) {
      if (real_clock::is_zero(*mtime)) {
        *mtime = real_clock::now();
      }
      dest_mtime = *mtime;
    } else {
      dest_mtime = real_clock::now();
    }
    creationTime = dest_mtime;
    dest_object->set_mtime(dest_mtime);
    dest_object->set_obj_size(this->get_size());
    dest_object->set_accounted_size(this->get_accounted_size());
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " size is: " << dest_object->get_size() << dendl;
    d4n_dest_object->set_attrs_from_obj_state(dpp, y, baseAttrs, dirty);
  } else {
    auto o_attrs = baseAttrs; 
    dest_object->load_obj_state(dpp, y);
    baseAttrs = dest_object->get_attrs();
    d4n_dest_object->set_attrs_from_obj_state(dpp, y, baseAttrs, dirty);
    d4n_dest_object->calculate_version(dpp, y, dest_version, o_attrs);
    if (dest_version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
  }
  bufferlist bl_val;
  bl_val.append(std::to_string(this->is_multipart()));
  baseAttrs[RGW_CACHE_ATTR_MULTIPART] = std::move(bl_val);
  bl_val.append(*etag);
  baseAttrs[RGW_ATTR_ETAG] = std::move(bl_val);
  baseAttrs[RGW_ATTR_ACL] = std::move(attrs[RGW_ATTR_ACL]);

  bufferlist bl_data;
  dest_version = d4n_dest_object->get_object_version();

  std::string key = get_cache_block_prefix(dest_object, dest_version);
  d4n_dest_object->set_object_version(dest_version);
  auto ret = d4n_dest_object->set_head_block_dir_entry(dpp, y, baseAttrs, true, dirty);
  baseAttrs.erase(RGW_CACHE_ATTR_MTIME);
  baseAttrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
  baseAttrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
  baseAttrs.erase(RGW_CACHE_ATTR_EPOCH);
  baseAttrs.erase(RGW_CACHE_ATTR_MULTIPART);
  baseAttrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
  baseAttrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
  baseAttrs.erase(RGW_CACHE_ATTR_DIRTY);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
    return ret;
  }
  if (dirty) {
    driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, dest_version, false, this->get_size(), creationTime, std::get<rgw_user>(dest_object->get_bucket()->get_owner()), *etag, dest_object->get_bucket()->get_name(), dest_object->get_bucket()->get_bucket_id(), dest_object->get_key(), rgw::d4n::RefCount::NOOP, y);
  }

  return 0;
}

int D4NFilterObject::load_obj_state(const DoutPrefixProvider *dpp, optional_yield y,
                             bool follow_olh)
{
  if (load_from_store) {
    if (cache_request) {
      return -ENOENT;
    }
    return next->load_obj_state(dpp, y, follow_olh);
  }
  bool has_instance = false;
  if (!this->get_instance().empty()) {
    has_instance = true;
  }
  int ret = get_obj_attrs_from_cache(dpp, y);
  if (ret) {
    /* clearing instance if not present in object before
       calling get_obj_attrs_from_cache as it incorrectly
       causes delete obj to be invoked for an instance
       even though a simple delete request has been issued
       (after load_obj_state is invoked) */
    if (!has_instance) {
      this->clear_instance();
    }
    return 0;
  }
  if (cache_request) {
    return -ENOENT;
  }
  return next->load_obj_state(dpp, y, follow_olh);
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  rgw::d4n::CacheBlock block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    if (setattrs != nullptr) {
      /* Ensure setattrs and delattrs do not overlap */
      if (delattrs != nullptr) {
        for (const auto& attr : *delattrs) {
          if (std::find(setattrs->begin(), setattrs->end(), attr) != setattrs->end()) {
            delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
          }
        }
      }
      for (const auto& attr : *setattrs) {
        block.cacheObj.attrs[attr.first] = attr.second;
      }
    } //if setattrs != nullptr

    if (delattrs != nullptr) {
      Attrs::iterator attr;
      Attrs currentattrs = this->get_attrs();

      /* Ensure all delAttrs exist */
      for (const auto& attr : *delattrs) {
        if (std::find(currentattrs.begin(), currentattrs.end(), attr) == currentattrs.end()) {
          delattrs->erase(std::find(delattrs->begin(), delattrs->end(), attr));
        }
      }
      for (const auto& attr : *delattrs) {
        block.cacheObj.attrs.erase(attr.first);
      }
    } //if delattrs != nullptr
    auto ret = driver->get_block_dir()->set(dpp, &block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed with ret: " << ret << dendl;
      return ret;
    }
  } else {
    if (block.deleteMarker || cache_request) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }
    auto ret = next->set_obj_attrs(dpp, setattrs, delattrs, y, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): set_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }

  return 0;
}

int D4NFilterObject::get_obj_attrs_from_cache(const DoutPrefixProvider* dpp, optional_yield y)
{
  //if attrs have already been set due to a previous call, do not read again.
  if (attrs_read_from_cache) {
    return true;
  }

  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlock block;
  bool found_in_cache = check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y);

  if (block.deleteMarker) {
    return -ENOENT;
  } else if (found_in_cache) {
    /* Set metadata locally */

    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): obj is: " << this->get_obj().key.name << dendl;
    std::string instance;
    for (auto& attr : attrs) {
      if (attr.second.length() > 0) {
        if (attr.first == RGW_CACHE_ATTR_MTIME) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting mtime." << dendl;
          auto ns = std::stoll(attr.second.to_str());
          auto mtime = ceph::real_time(ceph::timespan(ns));
          this->set_mtime(mtime);
        } else if (attr.first == RGW_CACHE_ATTR_OBJECT_SIZE) {
          auto size = std::stoull(attr.second.to_str());
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting object_size to: " << size << dendl;
          this->set_obj_size(size);
        } else if (attr.first == RGW_CACHE_ATTR_ACCOUNTED_SIZE) {
          auto accounted_size = std::stoull(attr.second.to_str());
          this->set_accounted_size(accounted_size);
        } else if (attr.first == RGW_CACHE_ATTR_EPOCH) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting epoch." << dendl;
          auto epoch = std::stoull(attr.second.to_str());
          this->set_epoch(epoch);
        } else if (attr.first == RGW_CACHE_ATTR_VERSION_ID) {
          instance = attr.second.to_str();
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting version_id to: " << instance << dendl;
        } else if (attr.first == RGW_CACHE_ATTR_SOURC_ZONE) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): setting source zone id." << dendl;
          auto short_zone_id = static_cast<uint32_t>(std::stoul(attr.second.to_str()));
          this->set_short_zone_id(short_zone_id);
        } else if (attr.first == RGW_CACHE_ATTR_MULTIPART) {
          std::string multipart = attr.second.to_str();
          this->multipart = (multipart == "1") ? true : false;
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): is_multipart: " << this->multipart << " multipart: " << multipart << dendl;
        } else {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Unexpected attribute; not locally set, attr name: " << attr.first << dendl;
        }
      }//end-if
    }//end-for
    if (!instance.empty()) {
      this->set_instance(instance); //set this only after setting object state else it won't take effect
    }
    attrs.erase(RGW_CACHE_ATTR_MTIME);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    attrs.erase(RGW_CACHE_ATTR_EPOCH);
    attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    /* Set attributes locally */
    auto ret = this->set_attrs(attrs);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): D4NFilterObject set_attrs method failed." << dendl;
    }
    attrs_read_from_cache = true;
  } // if found_in_cache = true

  return found_in_cache;
}

int D4NFilterObject::set_attr_crypt_parts(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs)
{
  if (attrs.count(RGW_ATTR_CRYPT_MODE)) {
    std::vector<size_t> parts_len;
    uint64_t obj_size = this->get_size();
    uint64_t obj_max_chunk_size = dpp->get_cct()->_conf->rgw_max_chunk_size;
    uint64_t num_parts = (obj_size%obj_max_chunk_size) == 0 ? obj_size/obj_max_chunk_size : (obj_size/obj_max_chunk_size) + 1;
    size_t remainder_size = obj_size;
    for (uint64_t part = 0; part < num_parts; part++) {
      size_t part_len;
      if (part == (num_parts - 1)) { //last part
        part_len = remainder_size;
      } else {
        part_len = obj_max_chunk_size;
      }
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): part_num: " << part << " part_len: " << part_len << dendl;
      parts_len.emplace_back(part_len);
      remainder_size -= part_len;
    }

    bufferlist parts_bl;
    ceph::encode(parts_len, parts_bl);
    attrs[RGW_ATTR_CRYPT_PARTS] = std::move(parts_bl);
  }
  return 0;
}

void D4NFilterObject::set_attrs_from_obj_state(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool dirty)
{
  bufferlist bl_val;
  bl_val.append(std::to_string(this->get_size()));
  attrs[RGW_CACHE_ATTR_OBJECT_SIZE] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_epoch()));
  attrs[RGW_CACHE_ATTR_EPOCH] = std::move(bl_val);

  bl_val.append(std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
      this->get_mtime().time_since_epoch()).count()));
  attrs[RGW_CACHE_ATTR_MTIME] = std::move(bl_val);

  if(this->have_instance()) {
    bl_val.append(this->get_instance());
    attrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val);
  }

  bl_val.append(std::to_string(this->get_short_zone_id()));
  attrs[RGW_CACHE_ATTR_SOURC_ZONE] = std::move(bl_val);

  bl_val.append(std::to_string(this->get_accounted_size()));
  attrs[RGW_CACHE_ATTR_ACCOUNTED_SIZE] = std::move(bl_val); // will this get updated?

  bl_val.append(this->get_key().ns);
  attrs[RGW_CACHE_ATTR_OBJECT_NS] = std::move(bl_val);

  bl_val.append(this->get_bucket()->get_name());
  attrs[RGW_CACHE_ATTR_BUCKET_NAME] = std::move(bl_val);
  
  if (dirty) {
    bl_val.append("1"); // only set xattr if dirty
    attrs[RGW_CACHE_ATTR_DIRTY] = std::move(bl_val);
  }

  return;
}

int D4NFilterObject::calculate_version(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, rgw::sal::Attrs& attrs)
{
  //versioned objects have instance set to versionId, and get_oid() returns oid containing instance, hence using id tag as version for non versioned objects only
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " instance: " << this->have_instance() << dendl;
  if (! this->have_instance() && version.empty()) {
    bufferlist bl = attrs[RGW_ATTR_ID_TAG];
    if (bl.length()) {
      version = bl.c_str();
      if (!version.empty()) {
	ldpp_dout(dpp, 20) << __func__ << " id tag version is: " << version << dendl;
      }
    }
  }
  if (this->have_instance()) {
    version = this->get_instance();
  }

  this->set_object_version(version);

  return 0;
}

int D4NFilterObject::write_if_space_available(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const Attrs& attrs, 
                                               uint64_t ofs, const std::string& version, const bool& dirty, const rgw_user user, const std::string& bucketName, 
                                               uint8_t op, optional_yield y, rgw::d4n::CacheBlock* block) {
  // Fast path: enough space exists, reserve and return
  auto cacheDriver = driver->get_cache_driver();
  uint64_t size = len;
  if (attrs.size()) {
    size += rgw::cache::XATTR_OVERHEAD_ESTIMATE;
  }

  auto rollback_reservation = [&]() {
    cacheDriver->release_reservation(dpp, size, y);
  };

  int ret = cacheDriver->check_and_reserve_space(dpp, size, y);
  if (ret == -ENOSPC) {
    // Not enough space — reserve upfront unconditionally and evict to free space
    cacheDriver->reserve_space(dpp, len, y);  
    ret = driver->get_policy_driver()->get_cache_policy()->eviction(dpp, size, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): ERROR: Eviction call failed, ret=" << ret << dendl;
      rollback_reservation();
      return ret;
    }
  } else if (ret < 0) {
    return ret;
  }

  ret = driver->get_cache_driver()->put(dpp, key, bl, len, attrs, y);
  if (ret == 0) {
    driver->get_policy_driver()->get_cache_policy()->update(dpp, key, ofs, len, version, dirty, user, bucketName, op, y, block);
  } else {
    ldpp_dout(dpp, 0) << __func__ << "(): ERROR: Cache driver put call failed, ret=" << ret << dendl;
    rollback_reservation();
    return ret;
  }
  return 0;
}

/* This method creates a delete marker for dirty objects:
1. creates a head block entry in cache driver - so that data can be restored from this when rgw goes down
2. calls set_head_block_dir_entry to set block entries for a delete marker */
int D4NFilterObject::create_delete_marker(const DoutPrefixProvider* dpp, optional_yield y)
{
  this->delete_marker = true;
  char buf[OBJ_INSTANCE_LEN + 1];
  gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf, OBJ_INSTANCE_LEN);
  this->version = buf;
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): generating delete marker: " << version << dendl;
  if (this->get_bucket()->versioned() && !this->get_bucket()->versioning_enabled()) { //if versioning is suspended
    this->set_instance("null");
  } else {
    this->set_instance(version);
  }

  auto m_time = real_clock::now();

  this->set_mtime(m_time);
  this->set_accounted_size(0); //setting 0 as this is a delete marker
  this->set_obj_size(0); // setting 0 as this is a delete marker
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " size is: " << this->get_size() << dendl;
  rgw::sal::Attrs attrs;
  this->set_attrs_from_obj_state(dpp, y, attrs, true);
  bufferlist bl_val;
  bl_val.append(std::to_string(this->delete_marker));
  attrs[RGW_CACHE_ATTR_DELETE_MARKER] = std::move(bl_val);
  std::string key = get_cache_block_prefix(this, this->version);

  bufferlist bl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): key is: " << key << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): version stored in update method is: " << version << dendl;
  auto ret = write_if_space_available(dpp, key, bl, bl.length(), attrs, 0, version, true, std::get<rgw_user>(this->get_bucket()->get_owner()),
                                       this->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, y, nullptr); // bl.length() is equal to 0
  if (ret == 0) {
	ret = this->set_head_block_dir_entry(dpp, y, attrs, true, true);
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
	  return ret;
	}
	auto creationTime = this->get_mtime();
	ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): key=" << key << dendl;
	std::string objEtag;
	driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, version, true, this->get_accounted_size(), creationTime, std::get<rgw_user>(this->get_bucket()->get_owner()), objEtag, this->get_bucket()->get_name(), this->get_bucket()->get_bucket_id(), this->get_key(), rgw::d4n::RefCount::NOOP, y);
  } else {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Write failed for key, ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

/*This method maintains adds the following entries:
1. A hash entry that maintains the latest version for dirty objects (versioned and non-versioned) and non-versioned clean objects.
2. A "null" hash entry that maintains the same version as the latest hash entry - this is used when get/delete requests are received
for "null" versions, when bucket is non-versioned.
3. The "null" hash entry is overwritten when we have a "null" instance when bucket versioning is suspended.
4. A versioned hash entry for every version for a version enabled bucket - this helps in get/delete requests with version-id specified
5. Redis ordered set to maintain the order of dirty objects added for a version enabled bucket. Even when the bucket is non-versioned, this set maintains a "null" entry
6. Another ordered set to maintain a lexicographically sorted order of objects for a bucket - used for bucket listing */
int D4NFilterObject::set_head_block_dir_entry(const DoutPrefixProvider* dpp, optional_yield y, rgw::sal::Attrs& attrs, bool is_latest_version, bool dirty)
{
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object name: " << this->get_name() << " bucket name: " << this->get_bucket()->get_name() << dendl;
  rgw::d4n::CacheBlock block; 
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  std::string directory_type = this->driver->get_directory_type();
  bufferlist bl_etag;
  auto etag_it = attrs.find(RGW_ATTR_ETAG);
  if (etag_it != attrs.end()) {
    bl_etag = etag_it->second;
  }
  auto etag = bl_etag.to_str();
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): etag: " << etag << dendl;

  RGWAccessControlPolicy policy = this->get_acl();
  std::string user_id, display_name;
  bufferlist bl_acl;
  auto acl_it = attrs.find(RGW_ATTR_ACL);
  if (acl_it != attrs.end()) {
    bl_acl = acl_it->second;
    auto iter = bl_acl.cbegin();
    try {
      policy.decode(iter);
      ACLOwner owner = policy.get_owner();
      rgw_user user = std::get<rgw_user>(owner.id);
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: user_id: " << user.to_str() << dendl;
      ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: display_name: " << owner.display_name << dendl;
      user_id = user.to_str();
      display_name = owner.display_name;
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    }
  }
  if (is_latest_version) {
    std::string objName = this->get_name();
    // special handling for name starting with '_'
    if (objName[0] == '_') {
      objName = "_" + this->get_name();
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): objName after special Handling: " << objName << dendl;
    rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
      .objName = objName,
      .bucketName = this->get_bucket()->get_bucket_id(),
      .creationTime = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(this->get_mtime().time_since_epoch()).count()),
      .dirty = dirty,
      .hostsList = { dpp->get_cct()->_conf->rgw_d4n_local_rgw_address },
      .attrs = attrs,
      };
    if (directory_type == "redis") {
      object.etag = etag;
      object.size = this->get_accounted_size();
      object.user_id = user_id;
      object.display_name = display_name;
      object.acl = bl_acl.to_str();
    }
    block.cacheObj = object;
    block.blockID = 0;
    block.version = this->get_object_version();
    block.size = 0;
    block.deleteMarker = this->delete_marker;

    /* adding an entry to maintain latest version, to serve simple get requests (without any version)
       but not for a clean object that belongs to a versioned bucket, as we will get the latest version from backend store
       to simplify delete object (maintaining correct order of versions) */

    //dirty objects
    if (dirty) {
      auto d4n_conn = this->driver->get_conn();
      if (directory_type == "redis"){
		auto redis_conn = std::static_pointer_cast<connection>(d4n_conn->get_conn());
	    auto redis_pool = this->driver->get_redis_pool();
      	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(redis_conn, redis_pool);
      	p.start();
        auto ret = blockDir->set(dpp, &block, y, &p);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
          return ret;
        }

        /* bucket is non versioned, set a null instance
           even when the bucket is non versioned, a get with "null" version-id returns the latest version, similarly
           delete-obj with "null" as version-id deletes the latest version */
        if (!(this->get_bucket()->versioned())) {
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y, &p);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
	        return ret;
    	  }
        }
        std::string object_version;
        //add an entry to ordered set for both versioned and non versioned bucket
        if (!this->get_bucket()->versioned() || !this->get_bucket()->versioning_enabled()) {
          object_version = "null";
        } else {
          object_version = this->get_object_version();
        }
        auto mtime = this->get_mtime();
        rgw::d4n::ObjectDirectory* objDir = this->driver->get_obj_dir();
        ret = objDir->add_version(dpp, this->get_bucket()->get_bucket_id(), objName, object_version, mtime, std::nullopt, y, &p);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add version to ordered set with error: " << ret << dendl;
          return ret;
        }
        //Redis - add an entry to ordered set containing objects for bucket listing, set score to 0 always to lexicographically order the objects
        rgw::d4n::BucketDirectory* bucketDir = this->driver->get_bucket_dir();
        ret = bucketDir->add_object(dpp, this->get_bucket()->get_bucket_id(), this->get_name(), std::nullopt, y, &p);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add object to ordered set with error: " << ret << dendl;
          return ret;
        }
        p.execute(dpp, y);
	  }
      else if (directory_type == "fdb"){
        auto ret = blockDir->set(dpp, &block, y, nullptr);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
          return ret;
        }
        
	/* bucket is non versioned, set a null instance
        even when the bucket is non versioned, a get with "null" version-id returns the latest version, similarly
        delete-obj with "null" as version-id deletes the latest version */
        if (!(this->get_bucket()->versioned())) {
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y, nullptr);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
	    return ret;
    	    }
          }
          std::string object_version;
          //add an entry to ordered set for both versioned and non versioned bucket
          if (!this->get_bucket()->versioned() || !this->get_bucket()->versioning_enabled()) {
            object_version = "null";
          } else {
            object_version = this->get_object_version();
          }
          auto mtime = this->get_mtime();
          rgw::d4n::CacheObjectVersion obj_version_info {
            .objName = objName,
            .bucketId = this->get_bucket()->get_bucket_id(),
            .version = object_version,
            .user_id = user_id,
            .display_name = display_name
          };
          rgw::d4n::ObjectDirectory* objDir = this->driver->get_obj_dir();
          ret = objDir->add_version(dpp, this->get_bucket()->get_bucket_id(), objName, object_version, mtime, obj_version_info, y, nullptr);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add version to ordered set with error: " << ret << dendl;
            return ret;
          }
          //FDB - add the entry with values, the entry is naturally sorted lexicographically
          rgw::d4n::BucketDirectory* bucketDir = this->driver->get_bucket_dir();
          rgw::d4n::CacheObject  obj_info {
            .objName = this->get_name(),
            .bucketId = this->get_bucket()->get_bucket_id(),
            .etag = etag,
            .size = this->get_accounted_size(),
            .creationTime = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(this->get_mtime().time_since_epoch()).count()),
            .deleteMarker = this->delete_marker
          };
          ret = bucketDir->add_object(dpp, this->get_bucket()->get_bucket_id(), this->get_name(), obj_info, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to add object to ordered set with error: " << ret << dendl;
            return ret;
          }
      }
    } else { //for clean/non-dirty objects
      rgw::d4n::CacheBlock latest = block;
      auto ret = blockDir->get(dpp, &latest, y);
      if (ret == -ENOENT) {
        if (!(this->get_bucket()->versioned())) {
      	  auto d4n_conn = this->driver->get_conn();
	  if (directory_type == "redis"){
	    auto redis_conn = std::static_pointer_cast<connection>(d4n_conn->get_conn());
            auto redis_pool = this->driver->get_redis_pool();
            rgw::d4n::Pipeline p = rgw::d4n::Pipeline(redis_conn, redis_pool);
            p.start();
            //we can explore pipelining to send the two 'HSET' commands together
            ret = blockDir->set(dpp, &block, y, &p);
            if (ret < 0) {
                ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
              return ret;
            }
            //bucket is non versioned, set a null instance
            block.cacheObj.objName = "_:null_" + this->get_name();
            ret = blockDir->set(dpp, &block, y, &p);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
              return ret;
            }
            p.execute(dpp, y);
          }
	  else if (directory_type == "fdb"){
            ret = blockDir->set(dpp, &block, y, nullptr);
            if (ret < 0) {
                ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
              return ret;
            }
            //bucket is non versioned, set a null instance
            block.cacheObj.objName = "_:null_" + this->get_name();
            ret = blockDir->set(dpp, &block, y, nullptr);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
              return ret;
            }

	  }
	}
      } else if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed for head object with ret: " << ret << dendl;
      } else { //head block is found
        /* for clean objects belonging to versioned buckets we will fetch the latest entry from backend store, hence removing latest head entry
           once a bucket transitions to a versioned state */
        if (this->get_bucket()->versioned()) {
          ret = blockDir->del(dpp, &block, y);
          //Ignore a racing delete that could have deleted the latest block
          if (ret < 0 && ret != -ENOENT) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory del method failed for head object with ret: " << ret << dendl;
          }
        }
        /* even if the head block is found, overwrite existing values with new version in case of non-versioned bucket, clean objects
           and versioned and non-versioned buckets dirty objects */
        if (!(this->get_bucket()->versioned())) {
          auto d4n_conn = this->driver->get_conn();
	  if (directory_type == "redis"){
	    auto redis_conn = std::static_pointer_cast<connection>(d4n_conn->get_conn());
            auto redis_pool = this->driver->get_redis_pool();
            rgw::d4n::Pipeline p = rgw::d4n::Pipeline(redis_conn, redis_pool);
            p.start();
            ret = blockDir->set(dpp, &block, y, &p);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
              return ret;
            }
            //bucket is non versioned, set a null instance
            block.cacheObj.objName = "_:null_" + this->get_name();
            ret = blockDir->set(dpp, &block, y, &p);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
              return ret;
            }
            p.execute(dpp, y);
	  }
	  else if (directory_type == "fdb"){
            ret = blockDir->set(dpp, &block, y, nullptr);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
              return ret;
            }
            //bucket is non versioned, set a null instance
            block.cacheObj.objName = "_:null_" + this->get_name();
            ret = blockDir->set(dpp, &block, y, nullptr);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
              return ret;
            }
	  }
        }//end-if !(this->get_bucket()->versioned())
      } //end-if ret = 0
    } //end-else
  }//end-if latest-version

  /* An entry corresponding to each instance will be needed to locate the head block
     this will also be needed for deleting an object from a version enabled bucket. */
  if (this->get_bucket()->versioned()) {
    std::string objName = this->get_oid();
    /* for null version, creating a "null" block specifically to differentiate between the latest entry and the null entry
       since oid does not take "null" into account */
    if (this->get_instance() == "null" || !this->get_bucket()->versioning_enabled()) {
      objName = "_:null_" + this->get_name();
    }
    rgw::d4n::CacheObj version_object = rgw::d4n::CacheObj{
    .objName = objName,
    .bucketName = this->get_bucket()->get_bucket_id(),
    .creationTime = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(this->get_mtime().time_since_epoch()).count()),
    .dirty = dirty,
    .attrs = attrs,
    };

    if (directory_type == "redis") {
      version_object.etag = etag;
      version_object.size = this->get_accounted_size();
      version_object.user_id = user_id;
      version_object.display_name = display_name;
      version_object.acl = bl_acl.to_str();
    }

    version_object.hostsList.insert({ dpp->get_cct()->_conf->rgw_d4n_local_rgw_address });

    rgw::d4n::CacheBlock version_block = rgw::d4n::CacheBlock{
      .cacheObj = version_object,
      .blockID = 0,
      .version = this->get_object_version(),
      .deleteMarker = this->delete_marker,
      .size = 0,
    };

    auto ret = blockDir->set(dpp, &version_block, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for versioned head object with ret: " << ret << dendl;
      return ret;
    }
  }//end-if get_bucket_versioned()

  return 0;
}

int D4NFilterObject::update_head_block_hostslist(const DoutPrefixProvider* dpp, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = driver->get_block_dir();
  auto redis_conn = this->driver->get_conn();
  auto redis_pool = this->driver->get_redis_pool();

  std::string objName = this->get_name();
  // special handling for name starting with '_'
  if (objName[0] == '_') {
    objName = "_" + this->get_name();
  }
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): objName after special Handling: " << objName << dendl;

  rgw::d4n::CacheBlock block {
    .cacheObj = {
      .objName    = objName,
      .bucketName = this->get_bucket()->get_bucket_id(),
    },
    .blockID = 0,
    .size    = 0,
  };

  //get block that contains latest version
  auto ret = blockDir->get(dpp, &block, y);
  if (ret == 0) {
    //if found, check if version matches with block's existing version
    if(this->version == block.version) {
      //only then update hostsList to contain local cache address
      block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
      if (!(this->get_bucket()->versioned())) {
        auto d4n_conn = this->driver->get_conn();
        std::string directory_type = this->driver->get_directory_type();
        if (directory_type == "redis"){
	  auto redis_conn = std::static_pointer_cast<connection>(d4n_conn->get_conn());
	  auto redis_pool = this->driver->get_redis_pool();
          //for non-versioned bucket, update latest version block and null version block
          rgw::d4n::Pipeline p = rgw::d4n::Pipeline(redis_conn, redis_pool);
          p.start();
          ret = blockDir->set(dpp, &block, y, &p);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
            return ret;
          }
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y, &p);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
            return ret;
          }
          p.execute(dpp, y);
	}
        else if (directory_type == "fdb"){
          ret = blockDir->set(dpp, &block, y, nullptr);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
            return ret;
          }
          block.cacheObj.objName = "_:null_" + this->get_name();
          ret = blockDir->set(dpp, &block, y, nullptr);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head object with ret: " << ret << dendl;
            return ret;
          }

	}
      }
    }
  } else if (ret < 0) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed for head block with ret: " << ret << dendl;
    if (ret != -ENOENT) {
      return ret;
    }
  }

  //save latest block get result to be used later in a pipeline if needed
  auto latest_block_ret = ret;
  //check if bucket is versioned
  if (this->get_bucket()->versioned()) {
    std::string objName = this->get_oid();
    if (this->get_instance() == "null" || !this->get_bucket()->versioning_enabled()) {
      objName = "_:null_" + this->get_name();
    }
    rgw::d4n::CacheObj versioned_object;
    versioned_object.objName = objName;
    versioned_object.bucketName = this->get_bucket()->get_bucket_id();

    rgw::d4n::CacheBlock versioned_block;
    versioned_block.cacheObj = versioned_object;
    versioned_block.blockID = 0;
    versioned_block.size = 0;
    //get versioned block
    ret = blockDir->get(dpp, &versioned_block, y);
    if (ret == 0) {
      //verify versions match for the versioned block
      if(this->version == versioned_block.version) {
        versioned_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
        //verify versions match for the latest block
        if (latest_block_ret == 0 && block.version == version) {
          block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
	  auto d4n_conn = this->driver->get_conn();
          std::string directory_type = this->driver->get_directory_type();
          if (directory_type == "redis"){
  	    auto redis_conn = std::static_pointer_cast<connection>(d4n_conn->get_conn());
	    auto redis_pool = this->driver->get_redis_pool();

            rgw::d4n::Pipeline p = rgw::d4n::Pipeline(redis_conn, redis_pool);
            p.start();
            ret = blockDir->set(dpp, &versioned_block, y, &p);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head block with ret: " << ret << dendl;
              return ret;
            }
            ret = blockDir->set(dpp, &block, y, &p);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head block with ret: " << ret << dendl;
              return ret;
            }
            p.execute(dpp, y);
	  }
	  else if (directory_type == "fdb"){
	     ret = blockDir->set(dpp, &versioned_block, y, nullptr);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head block with ret: " << ret << dendl;
              return ret;
            }
            ret = blockDir->set(dpp, &block, y, nullptr);
            if (ret < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for null head block with ret: " << ret << dendl;
              return ret;
            }
	  }
        } else {
          //case when latest block version does not match with existing version, update only version block
          ret = blockDir->set(dpp, &versioned_block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head block with ret: " << ret << dendl;
            return ret;
          }
        }
      } else {
        //case when only latest block version matches existing version
        if (latest_block_ret == 0 && block.version == version) {
          block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
          ret = blockDir->set(dpp, &block, y);
          if (ret < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head block with ret: " << ret << dendl;
            return ret;
          }
        }
      }
    } else if (ret < 0) {//ret ==0 for versioned block
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

/*
 This method updates the hostslist, version and dirty flag for data block directory entries
*/
int D4NFilterObject::set_data_block_dir_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty)
{
  rgw::d4n::BlockDirectory* blockDir = driver->get_block_dir();

  //update data block entries in directory
  const off_t lst = is_remote_cache_request() ? this->get_remote_block_len() : this->get_size();
  off_t fst = is_remote_cache_request() ? this->get_remote_block_offset() : 0;
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Object/Block size =" << lst << dendl;
  ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): offset =" << fst << dendl;

  std::vector<rgw::d4n::CacheBlock> blocks;
  while (fst < lst) {
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;
    rgw::d4n::CacheBlock block;
    block.cacheObj.bucketName = this->get_bucket()->get_bucket_id();
    block.cacheObj.objName = this->get_key().get_oid();
    block.size = cur_len;
    block.blockID = fst;
    fst += cur_len;
    blocks.emplace_back(block);
  }

  auto ret = blockDir->get(dpp, blocks, y);
  if (ret == -ENOENT) {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory get() no entry exists in directory." << dendl;
  }
  else if (ret < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory get() method failed, ret=" << ret << dendl;
    return ret;
  }

  //in case of a remote request, the flag in the directory should updated only by the local cache and/or the cleaning cache.
  bool update_dirty_flag = !remote_cache_request;
  for (auto& block : blocks) {
    if (block.cacheObj.objName.empty()) {
      continue;
    }
    if (update_dirty_flag) {
      block.cacheObj.dirty = dirty;
    }
    block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
    block.version = version;
  }
  if ((ret = blockDir->set(dpp, blocks, y)) < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory pipelined set() method failed, ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int D4NFilterObject::delete_cache_entry(const DoutPrefixProvider* dpp, const std::string key, optional_yield y) {
  int ret;
  if ((ret = driver->get_cache_driver()->delete_data(dpp, key, y)) == 0) { // Inline cache delete
    if (!(ret = driver->get_policy_driver()->get_cache_policy()->erase(dpp, key, y))) {
      ldpp_dout(dpp, 10) << "Failed to delete policy entry for: " << key << ", ret=" << ret << dendl;
      return ret;
    }
  } else {
    ldpp_dout(dpp, 10) << "Failed to delete object in cache for: " << key << ", ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int D4NFilterObject::delete_data_block_cache_entries(const DoutPrefixProvider* dpp, optional_yield y, std::string& version, bool dirty)
{
  //delete cache entries
  off_t lst = this->get_size();
  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Object size =" << lst << dendl;
  off_t fst = 0;
  do {
    if (fst >= lst){
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;

    std::string key =  get_key_in_cache(get_cache_block_prefix(this, version), std::to_string(fst), std::to_string(cur_len));
    int ret;
    if ((ret = delete_cache_entry(dpp, key, y)) < 0) {
      return ret;
    }
    fst += cur_len;
  } while(fst < lst);

  return 0;
}

bool D4NFilterObject::check_head_exists_in_cache_get_oid(const DoutPrefixProvider* dpp, std::string& head_oid_in_cache, rgw::sal::Attrs& attrs, rgw::d4n::CacheBlock& blk, optional_yield y)
{
  rgw::d4n::BlockDirectory* blockDir = this->driver->get_block_dir();
  std::string objName = this->get_oid();
  //object oid does not contain "null" in case the instance is "null", so explicitly populating that
  if (this->have_instance() && this->get_instance() == "null") {
    objName = "_:null_" + this->get_name();
  }
  ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): objName: " << objName << dendl;
  rgw::d4n::CacheObj object = rgw::d4n::CacheObj{
        .objName = objName, //version-enabled buckets will not have version for latest version, so this will work even when version is not provided in input
        .bucketName = this->get_bucket()->get_bucket_id(),
        };

  rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
          .cacheObj = object,
          .blockID = 0,
          .size = 0
          };

  bool found_in_cache = true;
  int ret;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if ((ret = blockDir->get(dpp, &block, y)) == 0) {
    if (this->is_remote_cache_request()) {
      if (block.version != get_object_version()) {
        ldpp_dout(dpp, 10) << "D4NFilterObject:: " << __func__ << "(): Error: Version mismatch" << dendl;
        return -EINVAL;
      }
    }
    blk = block;

    std::string version;
    version = block.version;
    this->set_object_version(version);

    head_oid_in_cache = get_cache_block_prefix(this, version); //check if still needed
    attrs = block.cacheObj.attrs;
    this->exists_in_cache = true;
    found_in_cache = true;
  } else if (ret == -ENOENT) { //if blockDir->get
    found_in_cache = false;
  } else {
    found_in_cache = false;
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory get method failed, ret=" << ret << dendl;
  }

  if (block.deleteMarker) {
    found_in_cache = false;
  }
  return found_in_cache;
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp)
{
  bool is_latest_version = true;
  if (this->have_instance()) {
    is_latest_version = false;
  }
  
  int ret;
  if ((ret = get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): " << " object " << this->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    if (cache_request) {
      return -ENOENT;
    }
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Fetching attrs from backend store." << dendl;
    auto ret = next->get_obj_attrs(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to fetching attrs from backend store with ret: " << ret << dendl;
      return ret;
    }
  
    this->load_obj_state(dpp, y);
    this->obj = next->get_obj();
    if (!this->obj.key.instance.empty()) {
      this->set_instance(this->obj.key.instance);
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): this->obj oid is: " << this->obj.key.name << "instance is: " << this->obj.key.instance << dendl;
    attrs = this->get_attrs();
    this->set_attrs_from_obj_state(dpp, y, attrs);

    calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
    ret = set_head_block_dir_entry(dpp, y, attrs, is_latest_version);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
    }
  } else {
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  }

  return 0;
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,  uint32_t flags)
{
  Attrs update;
  update[(std::string)attr_name] = attr_val;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  rgw::d4n::CacheBlock block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    block.cacheObj.attrs[attr_name] = attr_val;
    if (auto ret = driver->get_block_dir()->set(dpp, &block, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed with ret: " << ret << dendl;
      return ret;
    }
  } else {
    if (cache_request) {
      return -ENOENT;
    }

    if (block.deleteMarker) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }

    auto ret = next->modify_obj_attrs(attr_name, attr_val, y, dpp, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): modify_obj_attrs of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  buffer::list bl;
  std::string head_oid_in_cache;
  rgw::sal::Attrs attrs;
  Attrs delattr;
  rgw::d4n::CacheBlock block;
  if (check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y)) {
    auto it = block.cacheObj.attrs.find(attr_name);
    if (it != block.cacheObj.attrs.end()) {
      block.cacheObj.attrs.erase(it);

      auto ret = driver->get_block_dir()->set(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed with ret: " << ret << dendl;
        return ret;
      }
    }
  } else {
    if (block.deleteMarker) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object " << this->get_name() << " does not exist." << dendl;
      return -ENOENT;
    }
    if (cache_request) {
      return -ENOENT;
    }
    if (auto ret = next->delete_obj_attrs(dpp, attr_name, y); ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): delete_obj_attrs method of backend store failed with ret: " << ret << dendl;
      return ret;
    }
  }

  return 0;
}

std::unique_ptr<Object> D4NFilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

std::unique_ptr<Writer> D4NFilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, obj, dpp, true, y);
}

void D4NFilterDriver::shutdown()
{
  if (d4n_thread_pool) {
    if(d4n_coroutine_pool) {
      d4n_coroutine_pool->stop();
    }
    if(d4n_coroutine_get_pool) {
      d4n_coroutine_get_pool->stop();
    }
    if(d4n_thread_pool) {
      d4n_thread_pool->stop();
    }
  }

  // call cancel() on the connection's executor
  if (directory_type == "redis"){
    auto redis_conn = std::dynamic_pointer_cast<rgw::d4n::RedisConnection>(conn);
    boost::asio::dispatch(redis_conn->get_redis_conn()->get_executor(), [c = redis_conn->get_redis_conn()] { c->cancel(); });
  }
  else if (directory_type == "fdb"){
  	ceph::libfdb::shutdown_libfdb(); 
  }

  cacheDriver.reset();
  objDir.reset();
  blockDir.reset();
  bucketDir.reset();
  policyDriver.reset();

  next->shutdown();
}

std::unique_ptr<Object::ReadOp> D4NFilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<D4NFilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> D4NFilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<D4NFilterDeleteOp>(std::move(d), this);
}

int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  //set a flag to show that incoming instance has no version specified
  bool is_latest_version = true;
  if (source->have_instance()) {
    is_latest_version = false; 
  }

  int ret;
  if ((ret = source->get_obj_attrs_from_cache(dpp, y)) == -ENOENT) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::D4NFilterReadOp::" << __func__ << "(): object " << source->get_name() << " does not exist." << dendl;
    return -ENOENT;
  } else if (!ret) {
    if (source->is_cache_request()) {
      return -ENOENT;
    }
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_misses);
    }
    std::string head_oid_in_cache;
    rgw::sal::Attrs attrs;
    std::string version;
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): fetching head object from backend store" << dendl;
    next->params = params;
    auto ret = next->prepare(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): next->prepare method failed, ret=" << ret << dendl;
      return ret;
    }

    params.parts_count = next->params.parts_count;
    this->source->load_obj_state(dpp, y);
    attrs = source->get_attrs();
    source->set_attrs_from_obj_state(dpp, y, attrs);
    source->calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }

    this->source->set_attr_crypt_parts(dpp, y, attrs);
    ret = source->set_head_block_dir_entry(dpp, y, attrs, is_latest_version);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): set_head_block_dir_entry method failed for head object, ret=" << ret << dendl;
    }
  } else {
    /* 
      The following if statement handles the following:
      1. When part_num is given: if it is anything other than 1 and if source is not multipart, then return error
      2. When part_num is 0 and source is multipart
      In both the cases the head is fetched from the backend store.
    */
    if (params.part_num || (!params.part_num && source->is_multipart())) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): source->is_multipart()= " << source->is_multipart() << dendl;
      if (params.part_num) { 
	ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): *(params.part_num)= " << *(params.part_num) << dendl;
      }
      if (!source->is_multipart()) {
        if (params.part_num && *(params.part_num) != 1) {
          return -ERR_INVALID_PART;
        }
      } else {
        next->params = params;
        auto ret = next->prepare(y, dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): next->prepare failed, ret=" << ret << dendl;
          return ret;
        }
        params.parts_count = next->params.parts_count;
        return 0;
      }
    }
    bufferlist etag_bl;
    if (get_attr(dpp, RGW_ATTR_ETAG, etag_bl, y) < 0) {
      return -EINVAL;
    }

    if (params.mod_ptr || params.unmod_ptr) {
      if (params.mod_ptr && !params.if_nomatch) {
	ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.mod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
	if (!(*params.mod_ptr < source->get_mtime())) {
	  return -ERR_NOT_MODIFIED;
	}
      }

      if (params.unmod_ptr && !params.if_match) {
	ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.unmod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
	if (*params.unmod_ptr < source->get_mtime()) {
	  return -ERR_PRECONDITION_FAILED;
	}
      }
    }

    if (params.if_match) {
      std::string if_match_str = rgw_string_unquote(params.if_match);
      ldpp_dout(dpp, 10) << "If-Match: " << if_match_str << " ETAG: " << etag_bl.c_str() << dendl;

      if (if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
	return -ERR_PRECONDITION_FAILED;
      }
    }
    if (params.if_nomatch) {
      std::string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
      ldpp_dout(dpp, 10) << "If-No-Match: " << if_nomatch_str << " ETAG: " << etag_bl.c_str() << dendl;
      if (if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
	return -ERR_NOT_MODIFIED;
      }
    }

    if (params.lastmod) {
      *params.lastmod = source->get_mtime();
    }

    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  }
  
  return 0;
}

void D4NFilterObject::D4NFilterReadOp::cancel() {
  aio->drain();
}

int D4NFilterObject::D4NFilterReadOp::drain(const DoutPrefixProvider* dpp, optional_yield y) {
  auto c = aio->drain();
  int r = flush(dpp, std::move(c), y);
  std::string version = source->get_object_version();
  std::string prefix = source->get_prefix();
  for (auto it : blocks_info) {
    auto [id, ofs, len, read_ofs, read_len, is_remote] = it.second;
    if(!is_remote) {
      std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(ofs), std::to_string(len));
      source->driver->get_policy_driver()->get_cache_policy()->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::DECR, y);
    }
  }
  if (r < 0) {
    cancel();
    return r;
  }
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, optional_yield y) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  while (!results.empty()) {
    rgw::AioResultEntry* entry_ptr = &results.front();
    results.pop_front();
    std::unique_ptr<rgw::AioResultEntry> entry(entry_ptr);
    uint64_t id = entry->id;
    completed_map.try_emplace(id, std::move(entry));
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: " << dendl;

  while (true) {
    bufferlist bl;
    uint64_t cur_ofs;
    auto map_it = completed_map.find(offset);
    if (map_it == completed_map.end() || map_it->first != offset) {
      break;
    }
    ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: map_it->first:" << map_it->first << dendl;
    bl = std::move(map_it->second->data);
    completed_map.erase(map_it);
    cur_ofs = offset;
    offset += bl.length();
    std::optional<BlockMeta> block_meta;
    auto it = blocks_info.find(cur_ofs);
    if (it != blocks_info.end()) {
      block_meta = it->second;
      blocks_info.erase(it);
    }
    ldpp_dout(dpp, 20) << "D4NFilterObject::flush:: calling handle_data for offset: " << cur_ofs << " bufferlist length: " << bl.length() << dendl;
    if (client_cb) {
      int r;
      //We read an entire block from the remote and then send the data needed for a range request
      //and then cache the entire block locally (instead of just the range)
      if (block_meta && block_meta->is_remote) {
        r = client_cb->handle_data(bl, block_meta->read_offset, block_meta->read_len);
        if (r < 0) {
          return r;
        }
      } else {
        r = client_cb->handle_data(bl, 0, bl.length());
      }
    }
    if (block_meta) {
      std::string version = source->get_object_version();
      std::string prefix = source->get_prefix();
      uint64_t ofs = block_meta->offset;
      uint64_t len = block_meta->len;
      bool is_remote = block_meta->is_remote;
      std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(ofs), std::to_string(len));

      if(!is_remote) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " calling update for offset: " << cur_ofs << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;
        ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " version stored in update method is: " << version << " " << source->get_object_version() << dendl;
        source->driver->get_policy_driver()->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, std::nullopt, std::get<rgw_user>(source->get_bucket()->get_owner()), source->get_bucket()->get_name(), rgw::d4n::RefCount::DECR, y, nullptr);
      }
      if ((source->dest_object && source->dest_bucket) || is_remote) {
        std::string dest_version;
        rgw::d4n::CacheBlock dest_block;
        dest_block.blockID = ofs;
        dest_block.size = len;
        dest_block.version = dest_version;
        std::string key;
        bool write_to_local_cache{true};
        rgw::sal::Attrs attrs;
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        bufferlist bl_val;
        if (is_remote) {
          dest_version = source->get_object_version();
          dest_block.cacheObj.objName = source->get_oid();
          dest_block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();
          key = get_key_in_cache(get_cache_block_prefix(source, dest_version), std::to_string(ofs), std::to_string(len));
          if (auto ret = source->driver->get_block_dir()->get(dpp, &dest_block, y); ret < 0) {
            ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " BlockDirectory get failed with ret: " << ret << dendl;
            //should we return from here?
          }
          //if a new version has been written, then do not cache old data locally
          if (dest_version == dest_block.version) {
            dest_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
          } else {
            write_to_local_cache = false;
          }
          // TODO: Add DIRTY attr as well
          bufferlist bl_val;
          if (source->have_instance()) {
            bl_val.append(source->get_instance());
            attrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val);
          }
          bl_val.clear();
          bl_val.append(source->get_key().ns);
          attrs[RGW_CACHE_ATTR_OBJECT_NS] = std::move(bl_val);
        } else { // for copy object
          bl_val.append("1");
          attrs[RGW_CACHE_ATTR_DIRTY] = std::move(bl_val);
          bl_val.clear();
          if (d4n_dest_object->have_instance()) {
            bufferlist bl_val;
            bl_val.append(d4n_dest_object->get_instance());
            attrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val);
          }
          bl_val.append(d4n_dest_object->get_key().ns);
          attrs[RGW_CACHE_ATTR_OBJECT_NS] = std::move(bl_val);
          dest_version = d4n_dest_object->get_object_version();
          dest_block.cacheObj.objName = source->dest_object->get_oid();
          dest_block.cacheObj.bucketName = source->dest_bucket->get_bucket_id();
          dest_block.cacheObj.dirty = true;
          key = get_key_in_cache(get_cache_block_prefix(source->dest_object, dest_version), std::to_string(ofs), std::to_string(len));
          dest_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
        }

        if (write_to_local_cache) {
          ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " object version in update method is: " << dest_version << dendl;
          int ret;
          // destination key is the same as key
          if (is_remote) {
            ret = source->write_if_space_available(dpp, key, bl, bl.length(), attrs, ofs, dest_version, true, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                    source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, y, &dest_block);
          } else {
            ret = source->write_if_space_available(dpp, key, bl, bl.length(), attrs, ofs, dest_version, true, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                    source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, y, nullptr);
          }
          if (ret == 0) {
            if (ret = source->driver->get_block_dir()->set(dpp, &dest_block, y); ret < 0) {
              ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << " BlockDirectory set failed with ret: " << ret << dendl;
            }
          } else {
			ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Write failed for key, ret=" << ret << dendl;
          }
        }
      }
    } else {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << " offset not found: " << cur_ofs << dendl;
    }
    if(perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_hits);
    }
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::returning from flush:: " << dendl;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::process_remote_results(const DoutPrefixProvider* dpp, std::shared_ptr<TaskGroup> group, optional_yield y)
{
  if (remote_task_results.empty()) {
    return 0;
  }

  try {
    group->wait(dpp, y.get_yield_context());
    for (auto& task_result : remote_task_results) {
      while (!task_result->result_list.empty()) {
        auto& entry = task_result->result_list.front();
        auto id = entry.id;
        task_result->result_list.pop_front();
        completed_map.emplace(id, std::unique_ptr<rgw::AioResultEntry>(&entry));
      }
      for (auto& meta : task_result->blocks) {
        blocks_info.emplace(
          meta.id,
          BlockMeta(meta.id, meta.offset, meta.len, meta.read_offset, meta.read_len, meta.is_remote)
        );
      }
    }
    remote_task_results.clear();
    return 0;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__
                      << "(): Error processing remote task results: "
                      << e.what() << dendl;
    remote_task_results.clear();
    return -EIO;
  }
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) 
{
  //special handling in case object size is zero
  if (source->get_size() == 0) {
    return 0;
  }

  const uint64_t window_size = g_conf()->rgw_get_obj_window_size;
  std::string version = source->get_object_version();
  std::string prefix = get_cache_block_prefix(source, version);

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "prefix: " << prefix << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "oid: " << source->get_key().get_oid() << " ofs: " << ofs << " end: " << end << dendl;

  this->client_cb = cb;
  this->cb->set_client_cb(cb, dpp, &y);
  source->set_prefix(prefix);

  uint64_t max_chunk_size = std::min(g_conf()->rgw_max_chunk_size, source->get_size());
  uint64_t start_part_num = 0;
  uint64_t part_num = ofs/max_chunk_size; //part num of ofs wrt start of the object
  uint64_t adjusted_start_ofs = part_num*max_chunk_size; //in case of ranged request, adjust the start offset to the beginning of a chunk/ part
  uint64_t start_diff_ofs = ofs - adjusted_start_ofs; //difference between actual start offset and adjusted start offset
  off_t len = (end - adjusted_start_ofs) + 1;
  uint64_t num_parts = (len%max_chunk_size) == 0 ? len/max_chunk_size : (len/max_chunk_size) + 1; //calculate num parts based on adjusted offset
  uint64_t last_part_num = end/max_chunk_size;
  uint64_t adjusted_end_ofs = std::min(((last_part_num + 1)*max_chunk_size - 1), (source->get_size() - 1)); //align end offset to max_chunk_size boundary in case of ranged request
  uint64_t end_diff_ofs = adjusted_end_ofs - end; //difference between actual end offset and adjusted end offset
  uint64_t adjusted_len = (adjusted_end_ofs - adjusted_start_ofs) + 1;
  //len_to_read is the actual length read from a part/ chunk in cache, while part_len is the length of the chunk/ part in cache 
  uint64_t cost = 0, len_to_read = 0, part_len = 0;

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " adjusted_start_offset: " << adjusted_start_ofs << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " adjusted_end_ofs: " << adjusted_end_ofs << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " adjusted_len: " << adjusted_len << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << " len: " << len << dendl;

  auto group = std::make_shared<TaskGroup>(y.get_yield_context().get_executor());
  if ((params.part_num && !source->is_multipart()) || !params.part_num) {
    aio = rgw::make_throttle(window_size, y);

    ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << "max_chunk_size " << max_chunk_size << " num_parts " << num_parts << dendl;

    this->offset = ofs;

    rgw::d4n::CacheBlock block;
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();

    do {
      uint64_t id = adjusted_start_ofs, read_ofs = 0; //read_ofs is the actual offset to start reading from the current part/ chunk
      if (start_part_num == (num_parts - 1)) {
        len_to_read = adjusted_len - end_diff_ofs;
        part_len = adjusted_len;
        cost = adjusted_len;
      } else {
        len_to_read = max_chunk_size;
        cost = max_chunk_size;
        part_len = max_chunk_size;
      }
      if (start_part_num == 0) {
        len_to_read -= start_diff_ofs;
        id += start_diff_ofs;
        read_ofs = start_diff_ofs;
      }

      block.blockID = adjusted_start_ofs;
      block.size = part_len;

      ceph::bufferlist bl;
      std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(part_len));

      ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ <<  " " << __LINE__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << len_to_read << " part num: " << start_part_num << 
      " read_ofs: " << read_ofs << " part len: " << part_len << dendl;

      int ret;
      auto policy = source->driver->get_policy_driver()->get_cache_policy();
      auto cache_driver = source->driver->get_cache_driver();
      auto block_dir = source->driver->get_block_dir();
      if (policy->update_refcount_if_key_exists(dpp, oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): " << __LINE__ << ": READ FROM CACHE: oid_in_cache=" << oid_in_cache << dendl;
        // Read From Cache
        auto completed = cache_driver->get_async(dpp, y, aio.get(), oid_in_cache, read_ofs, len_to_read, cost, id);
        this->blocks_info.insert(std::make_pair(id, BlockMeta{id, adjusted_start_ofs, part_len, read_ofs, len_to_read, false}));
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
        auto r = flush(dpp, std::move(completed), y);
        if (r < 0) {
          process_remote_results(dpp, group, y);
          drain(dpp, y);
          ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, ret=" << r << dendl;
          return r;
        }
      } else { // else - if update_refcount_if_key_exists
        int r = -1;
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: Fetching from remote cache! " << dendl;
        if ((ret = block_dir->get(dpp, &block, y)) == 0) {
          if (block.version != version) {
            // TODO: If data has already been returned for any older versioned block, then return ‘retry’ error
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: Version mismatch, draining data for oid: " << oid_in_cache << dendl;
            process_remote_results(dpp, group, y);
            auto r = drain(dpp, y);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
            break;
          } //end if block.version != version
          auto it = block.cacheObj.hostsList.find(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
          auto hostsListSize = block.cacheObj.hostsList.size();
          if (it != block.cacheObj.hostsList.end()) {
            if ((r = block_dir->remove_host(dpp, &block, dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, y)) < 0) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to remove incorrect host from block with oid=" << oid_in_cache <<", ret=" << r << dendl;
              hostsListSize = hostsListSize - 1;
            }
          }
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): hostsListSize=" << hostsListSize << dendl;
          if (hostsListSize > 0) { /* Remote copy */
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Block with oid=" << oid_in_cache << " found in remote cache." << dendl;
            auto& user = source->get_bucket()->get_owner();
            std::string instance_id = "";
            if (source->have_instance()) {
              instance_id = source->get_instance(); 
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: populating remote op instance ID with " << instance_id << dendl;
            }
            std::string remote_addr = *(block.cacheObj.hostsList.begin());
            if (!dpp->get_cct()->_conf->rgw_d4n_remote_async_get) {
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: remote_addr: " << remote_addr << dendl;
              rgw::d4n::RemoteCacheGetOp::RemoteCacheGetOpData op {
                  source->get_bucket()->get_name(),
                  source->get_name(),
                  adjusted_start_ofs,
                  part_len,
                  version,
                  block.cacheObj.dirty,
                  std::get<rgw_user>(user),
                  remote_addr,
                  block.cacheObj.size,
                  instance_id
                };
              std::unique_ptr<rgw::d4n::RemoteCacheGetOp> remote_get = std::make_unique<rgw::d4n::RemoteCacheGetOp>(source->driver, op);
              auto completed = remote_get->send_and_complete_request(dpp, aio.get(), cost, id, y);
              this->blocks_info.insert(std::make_pair(id, BlockMeta{id, adjusted_start_ofs, part_len, read_ofs, len_to_read, true}));
              ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: flushing data for oid: " << oid_in_cache << dendl;
              auto r = flush(dpp, std::move(completed), y);
              if (r < 0) {
                process_remote_results(dpp, group, y);
                drain(dpp, y);
                ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to flush, ret=" << r << dendl;
                return r;
              }
            } else {
              CephContext* cct = dpp->get_cct();
              group->in_flight++;
              auto task_result = std::make_shared<RemoteTaskResult>();
              remote_task_results.push_back(task_result);
              auto executor = y.get_yield_context().get_executor();
              boost::asio::spawn(executor,
                [cct,
                task_result,
                group,
                bucket_name = source->get_bucket()->get_name(),
                obj_name = source->get_name(),
                offset = adjusted_start_ofs,
                len = part_len,
                read_offset = read_ofs,
                read_len = len_to_read,
                cost,
                id,
                start_part_num,
                num_parts,
                remote_addr,
                version,
                usr = std::get<rgw_user>(user),
                driver = source->driver,
                aio_shared = aio,
                dirty = block.cacheObj.dirty,
                objSize = block.cacheObj.size,
                instance_id = instance_id,
                this]
                (boost::asio::yield_context yield) mutable {
                optional_yield y(yield);
                std::string prefix = "async_read_from_remote_cache: ";
                auto dpp_local = std::make_shared<D4NFilterDPP>(cct, prefix);
                try {
                  auto guard = make_scope_guard([group, dpp_local]() {
                    int old_value = group->in_flight.fetch_sub(1, std::memory_order_release);
                    ldpp_dout(dpp_local.get(), 0) << "DEBUG: Task complete, in_flight was=" << old_value 
                                                    << ", now=" << (old_value - 1) << dendl;
                    if (old_value == 1 && group->timer_set.load(std::memory_order_acquire)) {
                      ldpp_dout(dpp_local.get(), 0) << "DEBUG: Last task, cancelling timer" << dendl;
                      group->signal_timer.cancel();
                    }
                  });
                  ldpp_dout(dpp_local.get(), 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: remote_addr: " << remote_addr << dendl;
                  rgw::d4n::RemoteCacheGetOp::RemoteCacheGetOpData op {
                      bucket_name,
                      obj_name,
                      offset,
                      len,
                      version,
                      dirty,
                      usr,
                      remote_addr,
                      objSize,
                      instance_id
                    };
                  std::unique_ptr<rgw::d4n::RemoteCacheGetOp> remote_get = std::make_unique<rgw::d4n::RemoteCacheGetOp>(driver, op);
                  task_result->result_list = remote_get->send_and_complete_request(dpp_local.get(), aio_shared.get(), cost, id, y);
                  ldpp_dout(dpp_local.get(), 20) << "D4NFilterObject::iterate:: " << __func__ << "(): After send_and complete: " << dendl;
                  task_result->blocks.push_back({ id, offset, len, read_offset, read_len, true });
                  ldpp_dout(dpp_local.get(), 10)
                      << "Successfully completed remote cache get for "
                      << obj_name << dendl;
                  return 0;
                } catch (const std::exception& e) {
                  ldpp_dout(dpp_local.get(), 0)
                      << "Error in remote cache get: "
                      << e.what() << dendl;
                  return -EIO;
                }
              },
              boost::asio::detached);
            }
          } else {
            ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
            auto r = process_remote_results(dpp, group, y);
            if (r < 0) {
              return r;
            }
            r = drain(dpp, y);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
            break;
          }
        } else { // else - if source->driver->get_block_dir()->get
          ldpp_dout(dpp, 5) << "Failed to fetch block for: " << block.cacheObj.objName << " blockID: " << block.blockID << " block size: " << block.size << ", ret=" << ret << dendl;
          ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
          auto r = process_remote_results(dpp, group, y);
          if (r < 0) {
            return r;
          }
          r = drain(dpp, y);
          if (r < 0) {
            ldpp_dout(dpp, 10) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
            return r;
          }
          break;
        }
      } //end - else
      if (start_part_num == (num_parts - 1)) {
        ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Info: draining data for oid: " << oid_in_cache << dendl;
        auto r = process_remote_results(dpp, group, y);
        if (r < 0) {
          return r;
        }
        return drain(dpp, y);
      } else {
        adjusted_start_ofs += max_chunk_size;
      }

      start_part_num += 1;
      adjusted_len -= max_chunk_size;
    } while (start_part_num < num_parts);
  }

  if (source->cache_request) {
    return -ENOENT;
  }

  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): Fetching object from backend store" << dendl;

  Attrs obj_attrs;
  if (source->has_attrs()) {
    obj_attrs = source->get_attrs();
  }

  this->cb->set_start_ofs(start_diff_ofs);
  this->cb->set_len(len);
  this->cb->set_adjusted_start_ofs(adjusted_start_ofs);
  this->cb->set_part_num(start_part_num);
  this->cb->set_num_parts(num_parts);
  ldpp_dout(dpp, 20) << "D4NFilterObject::iterate:: " << __func__ << "(): adjusted_start_ofs: " << adjusted_start_ofs << " end: " << end << dendl;
  auto r = next->iterate(dpp, adjusted_start_ofs, adjusted_end_ofs, this->cb.get(), y);
  //calculate the number of blocks read from backend store, and increment the perfcounter using that
  if(perfcounter) {
    uint64_t len_to_read_from_store = ((adjusted_end_ofs - adjusted_start_ofs) + 1);
    uint64_t num_blocks = (len_to_read_from_store%max_chunk_size) == 0 ? len_to_read_from_store/max_chunk_size : (len_to_read_from_store/max_chunk_size) + 1;
    perfcounter->inc(l_rgw_d4n_cache_misses, num_blocks);
  }
  
  if (r < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::iterate:: " << __func__ << "(): Error: failed to fetch object from backend store, ret=" << r << dendl;
    return r;
  }
  /* Copy params out of next */
  params = next->params;
  return this->cb->flush_last_part();
}

int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  rgw::sal::Attrs& attrs = source->get_attrs();
  if (attrs.empty()) {
    auto ret = source->get_obj_attrs(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Error: failed to fetch attrs, ret=" << ret << dendl;
      return ret;
    }
    //get_obj_attrs() calls set_attrs() internally, hence get_attrs() can be invoked to get the latest attrs.
    attrs = source->get_attrs();
  }
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Attribute value NOT found for attr name= " << name << dendl;
    return next->get_attr(dpp, name, dest, y);
  }

  dest = it->second;
  return 0;
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::flush_last_part()
{
  last_part = true;
  return handle_data(bl_rem, 0, bl_rem.length());
}

int D4NFilterObject::D4NFilterReadOp::D4NFilterGetCB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  auto rgw_max_chunk_size = g_conf()->rgw_max_chunk_size;
  ldpp_dout(dpp, 20) << __func__ << ": bl_ofs is: " << bl_ofs << " bl_len is: " << bl_len << " part_num: " << part_num << dendl;
  ldpp_dout(dpp, 20) << __func__ << ": start_ofs is: " << start_ofs << " end_ofs is: " << end_ofs << " part_num: " << part_num << dendl;
  if (!last_part && bl.length() <= rgw_max_chunk_size) {
    if (client_cb) {
      int r = 0;
      //ranged request
      if (bl_ofs != start_ofs && part_num == 0) {
        if (start_ofs < bl_len) { // this can happen in case of multipart where each chunk returned is not always of size rgw_max_chunk_size
          off_t bl_part_len = bl_len - start_ofs;
          ldpp_dout(dpp, 20) << __func__ << ": bl_part_len is: " << bl_part_len << dendl;
          bufferlist bl_part;
          bl.begin(start_ofs).copy(bl_part_len, bl_part);
          ldpp_dout(dpp, 20) << __func__ << ": bl_part.length() is: " << bl_part.length() << dendl;
          r = client_cb->handle_data(bl_part, 0, bl_part_len);
          part_num += 1;
          len_sent += bl_part_len;
        } else {
          start_ofs = start_ofs - bl_len; //re-adjust the offset
          ldpp_dout(dpp, 20) << __func__ << ": New value ofs is: " << start_ofs << dendl;
        }
      } else if (part_num == (num_parts - 1) && (len_sent + bl_len) > len) {
        uint64_t extra = (len_sent + bl_len) - len;
        uint64_t len_to_send = bl_len - extra;
        bufferlist bl_part;
        bl.begin(bl_ofs).copy(len_to_send, bl_part);
        ldpp_dout(dpp, 20) << __func__ << ": last part bl_part.length() is: " << bl_part.length() << dendl;
        r = client_cb->handle_data(bl_part, 0, bl_part.length());
      } else {
        r = client_cb->handle_data(bl, bl_ofs, bl_len);
        part_num += 1;
        len_sent += bl_len;
      }

      if (r < 0) {
        ldpp_dout(dpp, 20) << __func__ << ": error returned is: " << r << dendl;
        return r;
      }
    }
  }

  //Accumulating data from backend store into rgw_max_chunk_size sized chunks and then writing to cache
  if (write_to_cache) {
    Attrs attrs; // empty attrs for cache sets
    std::string version = source->get_object_version();
    std::string prefix = source->get_prefix();
    std::string dest_prefix;

    rgw::d4n::CacheBlock block, dest_block;
    rgw::d4n::BlockDirectory* blockDir = source->driver->get_block_dir();
    auto policy = filter->get_policy_driver()->get_cache_policy();
    block.cacheObj.objName = source->get_key().get_oid();
    block.cacheObj.bucketName = source->get_bucket()->get_bucket_id();
    std::stringstream s;
    block.cacheObj.creationTime = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(source->get_mtime().time_since_epoch()).count());
    bool dirty = false; //Reading from the backend, data is clean

    if (source->dest_object && source->dest_bucket) {
      D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
      std::string dest_version = d4n_dest_object->get_object_version();
      dest_prefix = get_cache_block_prefix(source->dest_object, dest_version);
      dest_block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
      dest_block.cacheObj.objName = source->dest_object->get_key().get_oid();
      dest_block.cacheObj.bucketName = source->dest_object->get_bucket()->get_bucket_id();
      //dest_block.cacheObj.creationTime = std::to_string(ceph::real_clock::to_time_t(source->get_mtime()));
      dest_block.cacheObj.dirty = false;
      dest_block.version = dest_version;
    }

    ldpp_dout(dpp, 20) << __func__ << ": version stored in update method is: " << version << dendl;

    if (bl.length() > 0 && last_part) { // if bl = bl_rem has data and this is the last part, write it to cache
      std::string oid = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
      if (!policy->exist_key(oid)) {
        block.blockID = adjusted_start_ofs;
        block.size = bl.length();

        auto ret = source->write_if_space_available(dpp, oid, bl, bl.length(), attrs, adjusted_start_ofs, version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                           source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
        if (ret == 0) {
          std::string objEtag;
          blocks.emplace_back(block);
        } //end-if ret == 0
      } //end-if exist_key
      if (source->dest_object && source->dest_bucket) {
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        std::string dest_version = d4n_dest_object->get_object_version();
        std::string dest_oid = get_key_in_cache(dest_prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
        dest_block.blockID = adjusted_start_ofs;
        dest_block.size = bl.length();
        auto ret = source->write_if_space_available(dpp, dest_oid, bl, bl.length(), attrs, adjusted_start_ofs, dest_version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                                 source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
        if (ret == 0) {
          dest_blocks.emplace_back(dest_block);
        }
      }
    } else if (bl.length() == rgw_max_chunk_size && bl_rem.length() == 0) { // if bl is the same size as rgw_max_chunk_size, write it to cache
      std::string oid = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
      block.blockID = adjusted_start_ofs;
      block.size = bl.length();
      if (!policy->exist_key(oid)) {
        auto ret = source->write_if_space_available(dpp, oid, bl, bl.length(), attrs, adjusted_start_ofs, version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                           source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
        if (ret == 0) {
          blocks.emplace_back(block);
        }
      }
      if (source->dest_object && source->dest_bucket) {
        D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
        std::string dest_version = d4n_dest_object->get_object_version();
        std::string dest_oid = get_key_in_cache(dest_prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_len));
        dest_block.blockID = adjusted_start_ofs;
        dest_block.size = bl.length();
        auto ret = source->write_if_space_available(dpp, dest_oid, bl, bl.length(), attrs, adjusted_start_ofs, dest_version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                           source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
        if (ret == 0) {
          dest_blocks.emplace_back(dest_block);
        }
      }
      adjusted_start_ofs += bl_len;
    } else { //copy data from incoming bl to bl_rem till it is rgw_max_chunk_size, and then write it to cache
      uint64_t rem_space = rgw_max_chunk_size - bl_rem.length();
      uint64_t len_to_copy = rem_space > bl.length() ? bl.length() : rem_space;
      bufferlist bl_copy;

      bl.splice(0, len_to_copy, &bl_copy);
      bl_rem.claim_append(bl_copy);

      if (bl_rem.length() == rgw_max_chunk_size) {
        std::string oid = get_key_in_cache(prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_rem.length()));
          if (!policy->exist_key(oid)) {
          block.blockID = adjusted_start_ofs;
          block.size = bl_rem.length();
          
          auto ret = source->write_if_space_available(dpp, oid, bl_rem, bl_rem.length(), attrs, adjusted_start_ofs, version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                           source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
          if (ret == 0) {
            blocks.emplace_back(block);
          } else {
            ldpp_dout(dpp, 0) << "D4N Filter: " << __func__ << " An error occurred during writing, ret=" << ret << dendl;
          }
        }

        if (source->dest_object && source->dest_bucket) {
          D4NFilterObject* d4n_dest_object = dynamic_cast<D4NFilterObject*>(source->dest_object);
          std::string dest_version = d4n_dest_object->get_object_version();
          std::string dest_oid = get_key_in_cache(dest_prefix, std::to_string(adjusted_start_ofs), std::to_string(bl_rem.length()));
          dest_block.blockID = adjusted_start_ofs;
          dest_block.size = bl_rem.length();
          auto ret = source->write_if_space_available(dpp, dest_oid, bl_rem, bl_rem.length(), attrs, adjusted_start_ofs, dest_version, dirty, std::get<rgw_user>(source->get_bucket()->get_owner()), 
                                                                                           source->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, *y, nullptr);
          if (ret == 0) {
            dest_blocks.emplace_back(dest_block);
          }
        }
        adjusted_start_ofs += bl_rem.length();
        bl_rem.clear();
        bl_rem = std::move(bl);
      }//bl_rem.length()
    }
    if (last_part) {
      auto ret = blockDir->get(dpp, blocks, *y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory pipelined get() method failed, ret=" << ret << dendl;
      }

      for (auto& block : blocks) {
        block.cacheObj.dirty = false;
        block.cacheObj.hostsList.insert(dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
        block.version = version;
      }
      if ((ret = blockDir->set(dpp, blocks, *y)) < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory pipelined set() method failed, ret=" << ret << dendl;
      }
      if (source->dest_object && source->dest_bucket) {
        if ((ret = blockDir->set(dpp, dest_blocks, *y)) < 0) {
          ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): BlockDirectory pipelined set() method for dest blocks failed, ret=" << ret << dendl;
        }
      }
    }// if last_part
  }//if write_to_cache

  /* Clean-up:
  1. do we need to clean up keys belonging to older versions (the last blocks), in case the size of newer version is different
  2. do we need to revert the cache ops, in case the directory ops fail
  */

  return 0;
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                                   optional_yield y, uint32_t flags)
{

  rgw::sal::Attrs attrs;
  std::string head_oid_in_cache;
  rgw::d4n::CacheBlock block;
  int ret = -1;
  bool cache_request = source->cache_request;

  /* check_head_exists_in_cache_get_oid also returns false if the head object is in the cache, but is a delete marker.
     As a result, the below check guarantees the head object is not in the cache. */
  if (!source->check_head_exists_in_cache_get_oid(dpp, head_oid_in_cache, attrs, block, y) && !block.deleteMarker) {
    /* for a dirty object, if the first call is a simple delete after versioning is enabled, the call will go to the backend store and create a delete marker there
       since no object with source->get_name() will be found in the cache (and this is correct) */
    if (cache_request) {
      return -ENOENT;
    }
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): head object not found; calling next->delete_obj" << dendl;
    next->params = params;
    ret = next->delete_obj(dpp, y, flags);
    result = next->result;
    return ret;
  } else {
    bool objDirty = block.cacheObj.dirty;
    auto blockDir = source->driver->get_block_dir();
    auto objDir = source->driver->get_obj_dir();
    auto bucketDir = source->driver->get_bucket_dir();
    auto cacheDriver = source->driver->get_cache_driver();
    std::string version = source->get_object_version();
    std::string objName = source->get_name();
    bool remote_cache_request = source->is_remote_cache_request();

	if (dpp->get_cct()->_conf->rgw_d4n_remote_delete_enabled) {
	  if (remote_cache_request) {
		objDirty = source->get_remote_dirty_flag();
	    if (objDirty){
	  	  ret = source->driver->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, head_oid_in_cache);
	  	  if (ret < 0)
			return ret;
		  objDirty = false;
	    }
	    //check if the cache has enough space, if yes, we will wait for cleaning.
	    if (source->driver->get_cache_driver()->get_free_space(dpp, y) > dpp->get_cct()->_conf->rgw_d4n_l1_datacache_free_threshold)
		  return 0;
	  }
      //send it to remote only if it is not a remote request from another rgw
	  // TODO: for better efficiency, it is better to check if the data is copied to the remote before sending the request
	  else{
          auto& user = source->get_bucket()->get_owner();
          std::string remote_addr = dpp->get_cct()->_conf->rgw_d4n_remote_cache_address;
          if (remote_addr.size()) {
			ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): remoteaddr =" << remote_addr << dendl;
			rgw::d4n::RemoteCacheDeleteOp::RemoteCacheDeleteOpData op {
				source->get_bucket()->get_name(),
				objName,
				0, 
				0,
				version,
				objDirty,
				std::get<rgw_user>(user),
				remote_addr,
				source->get_size()
			};
			std::unique_ptr<rgw::d4n::RemoteCacheDeleteOp> remote_delete = std::make_unique<rgw::d4n::RemoteCacheDeleteOp>(source->driver, op);
			auto ret = remote_delete->send_and_complete_request(dpp, y);
			if (ret < 0) {
			  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): send_and_complete_request failed for remote cache: " << remote_addr <<  "ret= " << ret << dendl;
			}
          }
      } //end - if else (remote_cache_request)
	} //if (dpp->get_cct()->_conf->rgw_d4n_remote_delete_enabled)

    // special handling for name starting with '_'
    if (objName[0] == '_') {
      objName = "_" + source->get_name();
    }

    if (objDirty && !cache_request) { // head object dirty flag represents object dirty flag
      //for versioned buckets, for a simple delete we need to create a delete marker (and not invalidate/delete any object)
      if (!source->get_bucket()->versioned() || (block.cacheObj.objName != source->get_name())) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): calling invalidate_dirty_object for: " << head_oid_in_cache << dendl;
        if (!source->driver->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, head_oid_in_cache)) {
          objDirty = false;
        }
      }
    }

    // Versioned buckets - this will delete the head object indexed by version-id (even null) and latest en
    if (source->get_bucket()->versioned()) {
        /* 1. clean objects - no latest head entry as latest entry to be retrieved from backend now
           hence delete only versioned head object */
        if (!objDirty) {
          if (source->have_instance()) {
            if ((ret = blockDir->del(dpp, &block, y)) < 0) {
              ldpp_dout(dpp, 0) << "Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl; 
              return ret;
            }
            if (cache_request) {
			  if ((ret = source->delete_cache_entry(dpp, get_cache_block_prefix(source, version), y)) < 0) {
				return ret;
			  }
            }
          }
          /* if versioning is suspended, we might have a latest head entry created from when bucket was non-versioned
             don't return error as that could already be deleted by set_head_block_dir_entry */
          if (!source->get_bucket()->versioning_enabled()) {
            block.cacheObj.objName = objName;
            if ((ret = blockDir->del(dpp, &block, y)) < 0) {
              ldpp_dout(dpp, 0) << "Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
            }
            if (cache_request) {
			  if ((ret = source->delete_cache_entry(dpp, head_oid_in_cache, y)) < 0) {
				return ret;
			  }
            }
          }
        } else if (objDirty) { //2. dirty objects - 1. add delete marker for simple request 2. delete version if given and correctly promote latest version if needed
          bool transaction_success = false;
          //add watch on latest entry, as it can be modified by a put or another del
          rgw::d4n::CacheBlock latest_block = block;
          latest_block.cacheObj.objName = objName;
          int retry = 3;
          while(retry) {
            retry--;
            //get latest entry
            ret = blockDir->get(dpp, &latest_block, y);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get latest entry in block directory for: " << latest_block.cacheObj.objName << ", ret=" << ret << dendl;
              return ret;
            }
            //simple delete request with no version id - create a delete marker
            if (block.cacheObj.objName == objName) {
              /* we are checking for latest_block and not block because latest_block has the most updated value of latest hash entry
                 if existing latest entry is already a delete marker, do not create a new one and simply return */
              if (!latest_block.deleteMarker) {
                ret = source->create_delete_marker(dpp, y);
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to create a delete marker for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                  //ERR_INTERNAL_ERROR is returned when exec_responses are empty which means the watched key has been modified, hence retry
                  if (ret == -ERR_INTERNAL_ERROR) {
                    continue;
                  } else {
                    return ret;
                  }
                }
                if (ret >= 0) {
                  result.delete_marker = true;
                  result.version_id = source->get_instance();
                  transaction_success = true;
                  return 0;
                }
              }
              transaction_success = true;
              return 0;
            } else { //not a simple request, delete version requested
              //get latest entry ret is 0
              if (ret == 0) {
                rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
                  .objName = objName,
                  .bucketName = source->get_bucket()->get_bucket_id(),
                };
                //check if version to be deleted is the same as latest version
                if (latest_block.version == block.version) {
                  std::vector<std::string> members;
                  std::vector<rgw::d4n::CacheObjectVersion> obj_versions;
                  //get the second latest version
                  std::string continuation_token;
                  ret = objDir->list_versions(dpp, source->get_bucket()->get_bucket_id(), objName, "", 2, obj_versions, continuation_token, y);
                  if (ret < 0) {
                    ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get the second latest version for: " << dir_obj.objName << ", ret=" << ret << dendl;
                    return ret;
                  }
                  //if there is a second latest version
                  if (obj_versions.size() == 2) {
                    rgw::d4n::CacheBlock version_block = latest_block;
                    version_block.cacheObj.objName = "_:" + obj_versions[1].version + "_" + source->get_name();
                    //get versioned entry
                    ret = blockDir->get(dpp, &version_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to get the versioned entry for: " << version_block.cacheObj.objName << ", ret=" << ret << dendl;
                      return 0;
                    }
                    //set versioned entry as the latest entry
                    version_block.cacheObj.objName = latest_block.cacheObj.objName;
                    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): INFO: promoting latest version entry to version: " << version_block.version << ", ret=" << ret << dendl;
                    ret = blockDir->set(dpp, &version_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to set new latest entry for: " << version_block.cacheObj.objName << ", ret=" << ret << dendl;
                      return 0;
                    }
                  } else { // there are no more versions left
                    //delete latest block entry
                    ret = blockDir->del(dpp, &latest_block, y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete latest entry in block directory, when it is the same as version requested, for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                      return ret;
                    }
                    //delete entry from ordered set of objects
                    ret = bucketDir->remove_object(dpp, source->get_bucket()->get_bucket_id(), source->get_name(), y);
                    if (ret < 0) {
                      ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue remove_object request in bucket directory for: " << source->get_name() << ", ret=" << ret << dendl;
                      return ret;
                    }
					if (cache_request) {
					  std::string req_oid_in_cache = get_key_in_cache(head_oid_in_cache + "#0#0", std::to_string(0), std::to_string(0));
					  if ((ret = source->delete_cache_entry(dpp, req_oid_in_cache, y)) < 0) {
						return ret;
					  }
					}
                  }
                } //end-if latest_block.version == block.version
                //delete versioned entry (handles delete markers also)
                if ((ret = blockDir->del(dpp, &block, y)) < 0 && ret != -ENOENT) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
                  return ret;
                }
                //delete entry from ordered set of versions
                std::string version = source->get_instance();
                ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Version to be deleted is: " << version << dendl;
                ret = objDir->remove_version(dpp, dir_obj.bucketName, dir_obj.objName, version, y);
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue remove_version request in object directory for: " << source->get_name() << ", ret=" << ret << dendl;
                  return ret;
                }
                if (ret < 0) {
                  ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to execute exec in block directory: " << "ret= " << ret << dendl;
                  return ret;
                }
				if (cache_request) {
				  std::string req_oid_in_cache = get_key_in_cache(get_cache_block_prefix(source, version), std::to_string(0), std::to_string(0));
				  if ((ret = source->delete_cache_entry(dpp, req_oid_in_cache, y)) < 0) {
					return ret;
				  }
				}
                result.delete_marker = block.deleteMarker;
                result.version_id = version;
                //success, hence break from loop
                transaction_success = true;
                break;
              }
            } //end-else (simple request)
          } //end-while retry
          if (!transaction_success) {
            ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Redis transaction failed after retrying! " << dendl;
            return -ERR_INTERNAL_ERROR;
          }
        } //end-if objDirty
    } //end-if versioned buckets

    /* Non-versioned buckets - we will delete the latest entry and the "null" entry
       dirty objects - delete "null" entry from ordered set also */
    if (!source->get_bucket()->versioned()) {
      //explore redis pipelining to send the two 'DEL' commands together in a single request
      ret = blockDir->del(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue delete head object op in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return ret;
      }
      if (cache_request) {
		if ((ret = source->delete_cache_entry(dpp, head_oid_in_cache, y)) < 0) {
		  return ret;
		}
      }
      //if we get request for latest head entry, delete the null block and vice versa
      if (block.cacheObj.objName == objName) {
        block.cacheObj.objName = "_:null_" + source->get_name();
      } else {
        block.cacheObj.objName = source->get_name();
      }
      ret = blockDir->del(dpp, &block, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue delete head object in block directory for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return ret;
      }
      //dirty objects - delete from ordered set of versions and objects
      if (objDirty) {
        rgw::d4n::CacheObj dir_obj = rgw::d4n::CacheObj{
          .objName = source->get_name(),
          .bucketName = source->get_bucket()->get_bucket_id(),
        };
        //delete entry from ordered set of object versions
        ret = objDir->remove_version(dpp, dir_obj.bucketName, dir_obj.objName, "null", y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue remove_version request in object directory for: " << source->get_name() << ", ret=" << ret << dendl;
          return ret;
        }
        //delete entry from ordered set of objects
        ret = bucketDir->remove_object(dpp, source->get_bucket()->get_bucket_id(), source->get_name(), y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to Queue remove_object request in bucket directory for: " << source->get_name() << ", ret=" << ret << dendl;
          return ret;
        }
      }
    } //end-if non-versioned buckets

    int size;
    if (objDirty) {
      std::string size_str;

      if (attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE) != attrs.end()) {
        size_str = attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE)->second.to_str();
      } else {
        ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to retrieve size for for: " << block.cacheObj.objName << ", ret=" << ret << dendl;
        return -EINVAL;
      }
      size = stoi(size_str);
    } else { //for clean objects
      size = this->source->get_size();
    }
    ldpp_dout(dpp, 20) << "D4NFilterObject::" << __func__ << "(): Size of object is: " << size << dendl;

    /* delete data blocks directory entries, when,
       1. object is clean, bucket is versioned and there is an instance in the request
       2. object is clean, bucket is non-versioned
       3. object is dirty - except for delete markers */
    if ((!objDirty && source->get_bucket()->versioned() && source->have_instance()) ||
        (!objDirty && !source->get_bucket()->versioned()) ||
        (objDirty && !block.deleteMarker)) {
        const off_t lst = size;
        off_t fst = 0;

      while (fst < lst) { // loop through the data blocks
        //data blocks have cacheObj.objName set to oid always
        block.cacheObj.objName = source->get_oid();
        off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
        off_t cur_len = cur_size - fst;
        block.blockID = static_cast<uint64_t>(fst);
        block.size = static_cast<uint64_t>(cur_len);

          if ((ret = blockDir->get(dpp, &block, y)) < 0) {
            if (ret == -ENOENT) {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Directory entry for: " << source->get_oid() << " blockid: " << fst << " block size: " << cur_len << " does not exist; continuing" << dendl;
              fst += cur_len;
              continue;
            } else {
              ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): Failed to retrieve directory entry for: " << source->get_oid() << " blockid: " << fst << " block size: " << cur_len << ", ret=" << ret << dendl;
              return ret;
            }
          }

          if ((ret = blockDir->del(dpp, &block, y)) == -ENOENT) { 
            continue;
          } else if (ret < 0) {
            ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to delete directory entry for: " << source->get_name() << " blockid: " << fst << " block size: " << cur_len << ", ret=" << ret << dendl;
            return ret;
          }

	    std::string req_oid_in_cache = get_key_in_cache(get_cache_block_prefix(source, version), std::to_string(block.blockID), std::to_string(block.size));
		if (cache_request || (source->driver->get_cache_driver()->get_free_space(dpp, y) <= dpp->get_cct()->_conf->rgw_d4n_l1_datacache_free_threshold)) {
		  if ((ret = source->delete_cache_entry(dpp, req_oid_in_cache, y)) < 0) {
			return ret;
		  }
		}
          //set invalid flag for dirty data blocks
          if (objDirty) {
            std::string key = get_key_in_cache(get_cache_block_prefix(source, version), std::to_string(fst), std::to_string(cur_len));
            int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_INVALID, "1", y);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << "D4NFilterObject::" << __func__ << "(): Failed to set xattr, ret=" << ret << dendl;
            }
          }
        fst += cur_len;
      }
    }

    if (!objDirty) {
      if (cache_request) {
        return 0;
      }
      next->params = params;
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): object is not dirty; calling next->delete_obj" << dendl;
      ret = next->delete_obj(dpp, y, flags);
      result = next->result;
      return ret;
    }
    return 0;
  }
}

int D4NFilterWriter::prepare(optional_yield y) 
{
  d4n_writecache = g_conf()->d4n_writecache_enabled;

  if (!d4n_writecache) {
    if (object->is_cache_request()) {
      return -EINVAL;
    }
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): calling next->prepare" << dendl;
    return next->prepare(y);
  } else {
    //for non-versioned buckets or version suspended buckets, we need to delete the older dirty blocks of the object from the cache as dirty blocks do not get evicted
    if (!object->get_bucket()->versioned() || (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled())) {
      rgw::d4n::CacheBlock block;
      rgw::sal::Attrs attrs;
      if (object->check_head_exists_in_cache_get_oid(dpp, prev_oid_in_cache, attrs, block, y)) {
        ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): found in cache, prev_oid_in_cache=" << prev_oid_in_cache << dendl;
      }
      object->clear_instance();
    }
  }

  std::string version;
  bool remote_cache_request = object->is_remote_cache_request();
  if (remote_cache_request) {
    version = object->get_object_version();
  }
  if (!object->have_instance()) {
    if (object->get_bucket()->versioned() && !object->get_bucket()->versioning_enabled()) { //if versioning is suspended
      object->set_instance("null");
    }
    if (version.empty()) {
      std::array<char, OBJ_INSTANCE_LEN + 1> buf;
      gen_rand_alphanumeric_no_underscore(dpp->get_cct(), buf.data(), OBJ_INSTANCE_LEN);
      version = buf.data(); // using gen_rand_alphanumeric_no_underscore for the time being
      ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): generating version: " << version << dendl;
      object->set_object_version(version);
    }
  } else {
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): version is: " << object->get_instance() << dendl;
    /* If version is empty, that means it is not a remote request,
     * hence we use the instance sent by the upper layer (rgw_op.cc)
     */
    if (version.empty()) {
      version = object->get_instance();
    } else {
     /* If version is non-empty then it is a remote request
      * and we overwrite the version-id generated by the upper layer
      */
      object->set_instance(version);
    }
    object->set_object_version(version);
  }
  this->version = version;

  return 0;
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
  bufferlist bl = data;
  off_t bl_len = bl.length();
  off_t ofs = offset;
  bool remote_cache_request = object->is_remote_cache_request();
  const bool dirty = !(remote_cache_request || object->is_cache_request());
  if (remote_cache_request) {
    ofs = object->get_remote_block_offset();
    ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): ofs is: " << ofs << dendl;
  }
  std::string version = object->get_object_version();
  std::string prefix = get_cache_block_prefix(obj, version);

  if (!d4n_writecache) {
    if (object->is_cache_request() || remote_cache_request) {
      return -EINVAL;
    }
    ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): calling next process" << dendl;
    return next->process(std::move(data), offset);
  }

  if (bl.length() == 0) {
    return 0;
  }

  rgw::sal::Attrs attrs;
  std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(ofs), std::to_string(bl_len));
  ldpp_dout(dpp, 10) << "D4NFilterWriter::" << __func__ << "(): oid_in_cache is: " << oid_in_cache << dendl;

  if (dirty) {
    bufferlist bl_val;
    bl_val.append("1");
    attrs[RGW_CACHE_ATTR_DIRTY] = std::move(bl_val);
  }
  if (object->have_instance()) {
    bufferlist bl_val;
    bl_val.append(object->get_instance());
    attrs[RGW_CACHE_ATTR_VERSION_ID] = std::move(bl_val);
  }
  bufferlist bl_val;
  bl_val.append(object->get_key().ns);
  attrs[RGW_CACHE_ATTR_OBJECT_NS] = std::move(bl_val);

  auto local_cache_ret = object->write_if_space_available(dpp, oid_in_cache, bl, bl_len, attrs, ofs, version, dirty, std::get<rgw_user>(object->get_bucket()->get_owner()), 
												   object->get_bucket()->get_name(), rgw::d4n::RefCount::NOOP, y, nullptr);
  if (local_cache_ret < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): adding block to local cache failed with ret= " << local_cache_ret << dendl;
  }
  //send it to remote only if it is not a remote request from another rgw
  if (!remote_cache_request && dpp->get_cct()->_conf->rgw_d4n_remote_put && !dpp->get_cct()->_conf->rgw_d4n_async_remote_put) {
    auto& user = obj->get_bucket()->get_owner();
    std::string remote_addr = dpp->get_cct()->_conf->rgw_d4n_remote_cache_address;
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): remoteaddr =" << remote_addr << dendl;
    uint64_t offset = ofs;
    rgw::d4n::RemoteCachePutOp::RemoteCachePutOpData op {
        obj->get_bucket()->get_name(),
        obj->get_name(),
        offset,
        bl.length(),
        version,
        dirty,
        std::get<rgw_user>(user),
        remote_addr
    };
    std::unique_ptr<rgw::d4n::RemoteCachePutOp> remote_put = std::make_unique<rgw::d4n::RemoteCachePutOp>(driver, op);
    auto ret = remote_put->send_and_complete_request(dpp, y, &bl);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): send_and_complete_request failed for remote cache: " << remote_addr << " ret= " << ret << dendl;
    }
  }
  if (local_cache_ret < 0) {
    return local_cache_ret;
  }
  return 0;
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  bool dirty = false;
  std::unordered_set<std::string> hostsList = {};
  std::string objEtag = etag;
  auto size = object->get_size();
  std::string instance;
  if (object->have_instance()) {
    instance = object->get_instance();
  }
  bool remote_cache_request = object->is_remote_cache_request();
  int ret;
  
  /* Return early if the put request was only for the data block. This occurs when a remote RGW evicts a block and
   * a different, qualifying RGW is available to keep the block instead. This logic is part of the LFUDA algorithm. */ 
  if (object->get_remote_block_only()) {
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << " Skipping head object write." << dendl;

    //update data block entries in directory
    ret = object->set_data_block_dir_entries(dpp, y, this->version, object->get_remote_dirty_flag());
    if (ret < 0) {
      return ret;
    }

    return 0;
  }

  /* for cache coherence, we are going to cache the head even in case when read-only cache is enabled, just that
     the head will not be marked dirty and the entire object will written to backend store also. In case write-back
     cache is enabled, the head will be cached as dirty. */
  if (d4n_writecache) {
    auto ret = object->get_obj_attrs(y, dpp);
    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        if (ret == -ENOENT) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        rgw::sal::Attrs attrs = object->get_attrs();
        bufferlist bl;
        auto iter = attrs.find(RGW_ATTR_ETAG);
        if (iter == attrs.end()) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        } else {
          bl = iter->second;
        }
        if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        if (ret != -ENOENT) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        rgw::sal::Attrs attrs = object->get_attrs();
        bufferlist bl;
        auto iter = attrs.find(RGW_ATTR_ETAG);
        if (iter == attrs.end()) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        } else {
          bl = iter->second;
        }
        if (strncmp(if_nomatch, bl.c_str(), bl.length()) == 0) {
          object->delete_data_block_cache_entries(dpp, y, this->version, true);
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    //get_obj_attrs will override object version and size with older values, hence setting it here again
    object->set_object_version(this->version);
    object->set_instance(instance);
    object->set_obj_size(size);

    //update data block entries in directory
    ret = object->set_data_block_dir_entries(dpp, y, this->version, true);
    if (ret < 0) {
      return ret;
    }

    dirty = true;
    if (remote_cache_request || object->is_cache_request()) {
      dirty = false;
    }
    ceph::real_time m_time;
    if (mtime) {
      if (real_clock::is_zero(*mtime)) {
        *mtime = real_clock::now();
      }
      m_time = *mtime;
    } else {
      m_time = real_clock::now();
    }
    object->set_mtime(m_time);
    object->set_accounted_size(accounted_size);
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << " size is: " << object->get_size() << dendl;
    object->set_attr_crypt_parts(dpp, y, attrs);
    object->set_attrs(attrs);
    object->set_attrs_from_obj_state(dpp, y, attrs, dirty);
  } else {
    if (object->is_cache_request()) {
      return -EINVAL;
    }
    // we need to call next->complete here so that we are able to correctly get the object state needed for caching head
    ret = next->complete(accounted_size, etag, mtime, set_mtime, attrs, cksum,
                            delete_at, if_match, if_nomatch, user_data, zones_trace,
                            canceled, rctx, flags);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): writing to backend store failed, ret=" << ret << dendl;
      return ret;
    }
    /* we want to always load latest object state from store
       to avoid reading stale state in case of object overwrites. */
    object->set_load_obj_from_store(true);
    object->load_obj_state(dpp, y);
    attrs = object->get_attrs();
    object->set_attrs_from_obj_state(dpp, y, attrs, dirty);

    std::string version;
    object->calculate_version(dpp, y, version, attrs);
    if (version.empty()) {
      ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
    }
  }

  std::string version = object->get_object_version();
  std::string key = get_cache_block_prefix(obj, version);

  object->set_object_version(version);
  //don't update directory head block entry for a remote request
  if (!remote_cache_request) {
    ret = object->set_head_block_dir_entry(dpp, y, attrs, true, dirty);
    attrs.erase(RGW_CACHE_ATTR_MTIME);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_SIZE);
    attrs.erase(RGW_CACHE_ATTR_ACCOUNTED_SIZE);
    attrs.erase(RGW_CACHE_ATTR_EPOCH);
    attrs.erase(RGW_CACHE_ATTR_MULTIPART);
    attrs.erase(RGW_CACHE_ATTR_OBJECT_NS);
    attrs.erase(RGW_CACHE_ATTR_BUCKET_NAME);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterWriter::" << __func__ << "(): set_head_block_dir_entry set method failed for head object, ret=" << ret << dendl;
      return ret;
    }
  }
  if (dirty) {
    auto creationTime = object->get_mtime();
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): key=" << key << dendl;
    ldpp_dout(dpp, 20) << "D4NFilterWriter::" << __func__ << "(): obj->get_key()=" << obj->get_key() << dendl;
    driver->get_policy_driver()->get_cache_policy()->update_dirty_object(dpp, key, version, false, accounted_size, creationTime, std::get<rgw_user>(obj->get_bucket()->get_owner()), objEtag, obj->get_bucket()->get_name(), obj->get_bucket()->get_bucket_id(), obj->get_key(), rgw::d4n::RefCount::NOOP, y);
    if (!prev_oid_in_cache.empty()) {
      driver->get_policy_driver()->get_cache_policy()->invalidate_dirty_object(dpp, prev_oid_in_cache);
    }
    if (dpp->get_cct()->_conf->rgw_d4n_remote_put && !remote_cache_request) {
      auto& user = obj->get_bucket()->get_owner();
      std::string remote_addr = dpp->get_cct()->_conf->rgw_d4n_remote_cache_address;
      if (dpp->get_cct()->_conf->rgw_d4n_async_remote_put) {
        CephContext* cct = dpp->get_cct();
        auto pool = driver->get_pool();
        if (pool) {
          pool->submit(
            [cct,
            prefix = key,
            size = obj->get_size(),
            usr = std::get<rgw_user>(user),
            remote_addr,
            bucket_name = obj->get_bucket()->get_name(),
            oid = obj->get_key().get_oid(),
            version,
    dirty,
            driver = this->driver]
            (boost::asio::yield_context yield) {

            std::string dpp_prefix = "async_write_to_remote_cache: ";
            auto dpp_local = std::make_shared<D4NFilterDPP>(cct, dpp_prefix);
            try {
              auto ret = D4NFilterWriter::write_to_remote_cache(
                  dpp_local.get(),
                  prefix,
                  size,
                  usr,
                  remote_addr,
                  bucket_name,
                  oid,
                  version,
                  dirty,
                  driver,
                  yield
              );
              if (ret < 0) {
                ldpp_dout(dpp_local.get(), 0)
                    << "write_to_remote_cache failed for "
                    << oid << " ret=" << ret << dendl;
              } else {
                ldpp_dout(dpp_local.get(), 10)
                    << "Successfully completed remote cache write for "
                    << oid << dendl;
              }
            } catch (const std::exception& e) {
                ldpp_dout(dpp_local.get(), 0)
                    << "Error in remote cache write: "
                    << e.what() << dendl;
            }
          });
        }
      } //end-if rgw_d4n_async_remote_put
    }
  }
  return 0;
}

int D4NFilterWriter::write_to_remote_cache(const DoutPrefixProvider* dpp_o, const std::string& prefix, uint64_t size, const rgw_user& user, const std::string& remote_addr, const std::string& bucket_name, const std::string& obj_name, const std::string& version, bool dirty, D4NFilterDriver* driver, optional_yield y)
{
  //Read data blocks from cache, and send remote requests
  const uint64_t lst = size;
  uint64_t fst = 0;
  auto policy = driver->get_policy_driver()->get_cache_policy();
  auto cache_driver = driver->get_cache_driver();

  while (fst < lst) {
    uint64_t chunk_size = std::min<off_t>(fst + dpp_o->get_cct()->_conf->rgw_max_chunk_size, lst);
    uint64_t cur_len = chunk_size - fst;
    std::string oid_in_cache = get_key_in_cache(prefix, std::to_string(fst), std::to_string(cur_len));

    ldpp_dout(dpp_o, 20) << "D4NFilterWriter:: " << __func__ << "(): READ FROM CACHE: oid=" << oid_in_cache << " length to read is: " << cur_len << " read_ofs: " << fst << dendl;

    if (policy->update_refcount_if_key_exists(dpp_o, oid_in_cache, rgw::d4n::RefCount::INCR, y)) {
      // Read From Cache
      bufferlist bl;
      rgw::sal::Attrs attrs;
      auto ret = cache_driver->get(dpp_o, oid_in_cache, 0, cur_len, bl, attrs, y);
      if (ret < 0) {
        ldpp_dout(dpp_o, 20) << "D4NFilterWriter:: " << __func__ << " get failed with ret: " << ret << dendl;
        return ret;
      }

      rgw::d4n::RemoteCachePutOp::RemoteCachePutOpData op {
          bucket_name,
          obj_name,
          fst,
          cur_len,
          version,
          dirty,
          user,
          remote_addr,
          size
      };
      std::unique_ptr<rgw::d4n::RemoteCachePutOp> remote_put = std::make_unique<rgw::d4n::RemoteCachePutOp>(driver, op);
      ret = remote_put->send_and_complete_request(dpp_o, y, &bl);
      if (ret < 0) {
        ldpp_dout(dpp_o, 0) << "D4NFilterWriter::" << __func__ << "(): send_and_complete_request failed for remote cache: " << remote_addr << " ret= " << ret << dendl;
        return ret;
      }
    }
    fst += cur_len;
  }
  if (driver->get_pool()) {
    auto stats = driver->get_pool()->get_stats();
    ldpp_dout(dpp_o, 20) << "D4NFilterWriter::" << __func__
                          << " Pool stats:"
                          << " Queue: " << stats.queue_size
                          << " Active: " << stats.active_workers
                          << " Idle: " << stats.idle_workers
                          << " Total submitted: " << stats.queued_tasks
                          << dendl;
  }
  return 0;
}

int D4NFilterMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
            prefix_map_t& processed_prefixes,
            const char *if_match,
            const char *if_nomatch)
{
  //call next->complete to complete writing the object to the backend store
  auto ret = next->complete(dpp, y, cct, part_etags, remove_objs, accounted_size,
			compressed, cs_info, ofs, tag, owner, olh_epoch,
			nextObject(target_obj), processed_prefixes);
  if (ret < 0) {
    return ret;
  }

  //Cache only the head block for multipart objects
  D4NFilterObject* d4n_target_obj = dynamic_cast<D4NFilterObject*>(target_obj);
  /* we want to always load latest object state from store
     to avoid reading stale state in case of object overwrites. */
  d4n_target_obj->set_load_obj_from_store(true);
  d4n_target_obj->load_obj_state(dpp, y);
  rgw::sal::Attrs attrs = d4n_target_obj->get_attrs();
  d4n_target_obj->set_attrs_from_obj_state(dpp, y, attrs);
  bufferlist bl_val;
  bool is_multipart = true;
  bl_val.append(std::to_string(is_multipart));
  attrs[RGW_CACHE_ATTR_MULTIPART] = std::move(bl_val);

  std::string version;
  d4n_target_obj->calculate_version(dpp, y, version, attrs);
  if (version.empty()) {
    ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): version could not be calculated." << dendl;
  }

  ret = d4n_target_obj->set_head_block_dir_entry(dpp, y, attrs, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "D4NFilterMultipartUpload::" << __func__ << "(): BlockDirectory set method failed for head object, ret=" << ret << dendl;
  }

  return 0;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, void* io_context, bool admin)
{
  rgw::sal::D4NFilterDriver* driver = new rgw::sal::D4NFilterDriver(next, *static_cast<boost::asio::io_context*>(io_context), admin);

  return driver;
}

}
