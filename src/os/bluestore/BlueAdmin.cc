// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "BlueAdmin.h"
#include "Compression.h"
#include "common/pretty_binary.h"
#include "common/debug.h"
#include <asm-generic/errno-base.h>
#include <vector>
#include <limits>

#define dout_subsys ceph_subsys_bluestore
#define dout_context store.cct

using ceph::bufferlist;
using ceph::Formatter;
using ceph::common::cmd_getval;

//To help in short term tracking for cache stats
static void take_cache_snapshot(BlueStore& store) {
  std::lock_guard l(store.cache_stats_lock);
  
  BlueStore::CacheStatsSnapshot snap;
  snap.timestamp = ceph::mono_clock::now();
  snap.onode_hits = store.logger->get(l_bluestore_onode_hits);
  snap.onode_misses = store.logger->get(l_bluestore_onode_misses);
  snap.onode_miss_latency_sum = store.logger->get(l_bluestore_onode_miss_lat);
  snap.onode_shard_hits = store.logger->get(l_bluestore_onode_shard_hits);
  snap.onode_shard_misses = store.logger->get(l_bluestore_onode_shard_misses);
  snap.onode_shard_miss_latency_sum = store.logger->get(l_bluestore_onode_shard_miss_lat);
  snap.buffer_hits = store.logger->get(l_bluestore_buffer_hits);
  snap.buffer_miss_count = store.logger->get(l_bluestore_buffer_miss_lat + 1);
  snap.buffer_miss_latency_sum = store.logger->get(l_bluestore_buffer_miss_lat);
  
  store.cache_stats_snapshots.push_back(snap);
  
  //Remove old ones
  while (store.cache_stats_snapshots.size() > BlueStore::MAX_CACHE_SNAPSHOTS) {
    store.cache_stats_snapshots.pop_front();
  }
}

static bool get_snapshot_windows(BlueStore& store,
                                 BlueStore::CacheStatsSnapshot& current,
                                 BlueStore::CacheStatsSnapshot& most_recent,
                                 BlueStore::CacheStatsSnapshot& oldest) {
  std::lock_guard l(store.cache_stats_lock);
  
  if (store.cache_stats_snapshots.empty()) {
    return false;
  }
  
  // Get current values 
  current.timestamp = ceph::mono_clock::now();
  current.onode_hits = store.logger->get(l_bluestore_onode_hits);
  current.onode_misses = store.logger->get(l_bluestore_onode_misses);
  current.onode_miss_latency_sum = store.logger->get(l_bluestore_onode_miss_lat);
  current.onode_shard_hits = store.logger->get(l_bluestore_onode_shard_hits);
  current.onode_shard_misses = store.logger->get(l_bluestore_onode_shard_misses);
  current.onode_shard_miss_latency_sum = store.logger->get(l_bluestore_onode_shard_miss_lat);
  current.buffer_hits = store.logger->get(l_bluestore_buffer_hits);
  current.buffer_miss_count = store.logger->get(l_bluestore_buffer_miss_lat + 1);
  current.buffer_miss_latency_sum = store.logger->get(l_bluestore_buffer_miss_lat);
  
  if(store.cache_stats_snapshots.size() >= 2) {
    most_recent = store.cache_stats_snapshots[store.cache_stats_snapshots.size() - 2];
  } else {
  most_recent = store.cache_stats_snapshots.back();
  }
  oldest = store.cache_stats_snapshots.front();
  
  return true;
}


BlueStore::SocketHook::SocketHook(BlueStore& store)
  : store(store)
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    int r = admin_socket->register_command(
      "bluestore collections",
      this,
      "list all collections");
    if (r != 0) {
      dout(1) << __func__ << " cannot register SocketHook" << dendl;
      return;
    }
    r = admin_socket->register_command(
      "bluestore list "
      "name=collection,type=CephString,req=true "
      "name=start,type=CephString,req=false "
      "name=max,type=CephInt,req=false",
      this,
      "list objects in specific collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore onode metadata "
      "name=object_name,type=CephString,req=true",
      this,
      "print object internals");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore compression stats "
      "name=collection,type=CephString,req=false",
      this,
      "print compression stats, per collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore runtime frag score "
      "name=collection,type=CephString,req=false",
      this,
      "print runtime fragmentation score, per collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore clear runtime frag "
      "name=collection,type=CephString,req=false",
      this,
      "clear runtime fragmentation score, per collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore static frag score "
      "name=collection,type=CephString,req=false",
      this,
      "print static fragmentation score, per collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore clear static frag "
      "name=collection,type=CephString,req=false",
      this,
      "clear static fragmentation score, per collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore cache stats",
      this,
      "print cache performance stats");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore show sharding ",
      this,
      "print RocksDB sharding");
    ceph_assert(r == 0);
  }
}

BlueStore::SocketHook::~SocketHook()
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    admin_socket->unregister_commands(this);
  }
}

int BlueStore::SocketHook::call(
  std::string_view command,
  const cmdmap_t& cmdmap,
  const bufferlist& inbl,
  Formatter *f,
  std::ostream& ss,
  bufferlist& out)
{
  int r = 0;
  if (command == "bluestore collections") {
    std::vector<coll_t> collections;
    store.list_collections(collections);
    std::stringstream result;
    for (const auto& c : collections) {
      result << c << std::endl;
    }
    out.append(result.str());
    return 0;
  } else if (command == "bluestore list") {
    std::string coll;
    std::string start;
    int64_t max;
    cmd_getval(cmdmap, "collection", coll);
    cmd_getval(cmdmap, "start", start);
    if (!cmd_getval(cmdmap, "max", max)) {
      max = 100;
    }
    if (max == 0) {
      max = std::numeric_limits<int>::max();
    }
    coll_t c;
    if (c.parse(coll) == false) {
      ss << "Cannot parse collection" << std::endl;
      return -EINVAL;
    }
    BlueStore::CollectionRef col = store._get_collection(c);
    if (!col) {
      ss << "No such collection" << std::endl;
      return -ENOENT;
    }
    ghobject_t start_object;
    if (start.length() > 0) {
      if (start_object.parse(start) == false) {
        ss << "Cannot parse start object";
	return -EINVAL;
      }
    }
    std::vector<ghobject_t> list;
    {
      std::shared_lock l(col->lock);
      r = store._collection_list(col.get(), start_object, ghobject_t::get_max(),
        max, false, &list, nullptr);
    }
    if (r != 0) {
      return 0;
    }
    std::stringstream result;
    for (auto& obj : list) {
      result << obj << std::endl;
    }
    out.append(result.str());
    return 0;
  } else if (command == "bluestore onode metadata") {
    std::string object_name;
    cmd_getval(cmdmap, "object_name", object_name);
    ghobject_t object;
    if (!object.parse(object_name)) {
      ss << "Cannot parse object" << std::endl;
      return -EINVAL;
    }
    std::shared_lock l(store.coll_lock);
    for (const auto& cp : store.coll_map) {
      if (cp.second->contains(object)) {
        std::shared_lock l(cp.second->lock);
        OnodeRef o = cp.second->get_onode(object, false);
        if (!o || !o->exists) {
          ss << "Object not found" << std::endl;
          return -ENOENT;
        }
        o->extent_map.fault_range(store.db, 0, 0xffffffff);
        using P = BlueStore::printer;
        std::stringstream result;
        result << o->print(P::PTR + P::DISK + P::USE + P::BUF + P::CHK + P::ATTRS) << std::endl;
        out.append(result.str());
        return 0;
      }
    }
    r = -ENOENT;
    ss << "No collection that can hold such object" << std::endl;
  } else if (command == "bluestore compression stats") {
    std::vector<CollectionRef> copied;
    {
      std::shared_lock l(store.coll_lock);
      copied.reserve(store.coll_map.size());
      for (const auto& c : store.coll_map) {
        copied.push_back(c.second);
      }
    }
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    f->open_array_section("compression");
    for (const auto& c : copied) {
      std::shared_lock l(c->lock);
      if ((coll.empty() && bool(c->estimator))
        || coll == c->get_cid().c_str()) {
        f->open_object_section("collection");
        f->dump_string("cid", c->get_cid().c_str());
        f->open_object_section("estimator");
        if (c->estimator) {
          c->estimator->dump(f);
        }
        f->close_section();
        f->close_section();
      }
    }
    f->close_section();
    return 0;
  } else if (command == "bluestore show sharding") {
    int r = 0;
    std::string sharding;
    if (store.get_db_sharding(sharding)) {
      out.append(sharding + '\n');
    } else {
      r = -EFAULT;
      ss << "Failed to get sharding" << std::endl;
    }
    return r;
  } else if (command == "bluestore runtime frag score") {
    std::shared_lock l(store.coll_lock);
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    f->open_array_section("runtime_frag_score");
    for (const auto& it : store.coll_map) {
      auto c = it.second;
      std::shared_lock l(c->lock);
      if (coll.empty() || coll == c->get_cid().c_str()) {
        f->open_object_section("collection");
        f->dump_string("cid", c->get_cid().c_str());
        auto samples = c->runtime_read_samples.load(std::memory_order_relaxed);
        auto sum = c->runtime_frag_count.load(std::memory_order_relaxed);
        f->dump_unsigned("object_read_samples", samples);
        f->dump_unsigned("runtime_frag_count", sum);
        if (samples == 0) {
          f->dump_int("runtime_frag_score", 0);
        } else {
          f->dump_float("runtime_frag_score", (float)sum / samples);
        }
        f->close_section();
      }
    }
    f->close_section();
    return 0;
  } else if (command == "bluestore clear runtime frag") {
    std::shared_lock l(store.coll_lock);
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    for (const auto& it : store.coll_map) {
      auto c = it.second;
      std::shared_lock l(c->lock);
      if (coll.empty() || coll == c->get_cid().c_str()) {
        c->runtime_frag_count.store(0, std::memory_order_relaxed);
        c->runtime_read_samples.store(0, std::memory_order_relaxed);
      }
    }
    return 0;
  } else if (command == "bluestore static frag score") {
    std::shared_lock l(store.coll_lock);
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    f->open_array_section("static_frag_score");
    for (const auto& it : store.coll_map) {
      auto c = it.second;
      std::shared_lock l(c->lock);
      if (coll.empty() || coll == c->get_cid().c_str()) {
        f->open_object_section("collection");
        f->dump_string("cid", c->get_cid().c_str());
        auto score = c->static_frag_score.load(std::memory_order_relaxed);
        auto count = c->object_read_samples.load(std::memory_order_relaxed);
        f->dump_unsigned("static_frag_score", score);
        f->dump_unsigned("object_read_samples", count);
        f->close_section();
      }
    }
    f->close_section();
    return 0;
  } else if (command == "bluestore cache stats") {
    //Store stats at this time for short term tracking
    take_cache_snapshot(store);
    
    f->open_object_section("cache_stats");
    
    f->open_object_section("since_startup");
    
    f->open_object_section("onode_cache");
    
    uint64_t onode_hits = store.logger->get(l_bluestore_onode_hits);
    f->dump_unsigned("hits", onode_hits);
    
   
    f->open_object_section("onode_cache_miss_latency");
    auto onode_miss_tavg = store.logger->get_tavg_ns(l_bluestore_onode_miss_lat);
    uint64_t onode_miss_sum_ns = onode_miss_tavg.first;
    uint64_t onode_miss_count = onode_miss_tavg.second;
    f->dump_unsigned("avgcount", onode_miss_count);
    f->dump_format_unquoted("sum", "%" PRId64 ".%09" PRId64,
                            onode_miss_sum_ns / 1000000000ull,
                            onode_miss_sum_ns % 1000000000ull);
    if (onode_miss_count) {
      uint64_t avg_ns = onode_miss_sum_ns / onode_miss_count;
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64,
                              avg_ns / 1000000000ull,
                              avg_ns % 1000000000ull);
    } else {
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64, 0, 0);
    }
    f->close_section();
    
    
    f->open_object_section("onode_shard_miss_latency");
    auto shard_miss_tavg = store.logger->get_tavg_ns(l_bluestore_onode_shard_miss_lat);
    uint64_t shard_miss_sum_ns = shard_miss_tavg.first;
    uint64_t shard_miss_count = shard_miss_tavg.second;
    f->dump_unsigned("avgcount", shard_miss_count);
    f->dump_format_unquoted("sum", "%" PRId64 ".%09" PRId64,
                            shard_miss_sum_ns / 1000000000ull,
                            shard_miss_sum_ns % 1000000000ull);
    if (shard_miss_count) {
      uint64_t avg_ns = shard_miss_sum_ns / shard_miss_count;
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64,
                              avg_ns / 1000000000ull,
                              avg_ns % 1000000000ull);
    } else {
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64, 0, 0);
    }
    f->close_section();
    
    uint64_t total_accesses = onode_hits + onode_miss_count;
    f->dump_unsigned("total_accesses", total_accesses);
    if (total_accesses > 0) {
      double hit_ratio = static_cast<double>(onode_hits) / static_cast<double>(total_accesses);
      f->dump_float("hit_ratio", hit_ratio);
    } else {
      f->dump_float("hit_ratio", 0.0);
    }
    
    f->close_section();
    
    f->open_object_section("onode_shard");
    uint64_t shard_hits = store.logger->get(l_bluestore_onode_shard_hits);
    uint64_t shard_misses = store.logger->get(l_bluestore_onode_shard_misses);
    f->dump_unsigned("hits", shard_hits);
    f->dump_unsigned("misses", shard_misses);
    uint64_t shard_total = shard_hits + shard_misses;
    if (shard_total > 0) {
      f->dump_float("hit_ratio", (double)shard_hits / (double)shard_total);
    } else {
      f->dump_float("hit_ratio", 0.0);
    }
    
    auto shard_miss_lat_tavg = store.logger->get_tavg_ns(l_bluestore_onode_shard_miss_lat);
    uint64_t shard_miss_lat_sum_ns = shard_miss_lat_tavg.first;
    uint64_t shard_miss_lat_count = shard_miss_lat_tavg.second;
    if (shard_miss_lat_count > 0) {
      uint64_t avg_ns = shard_miss_lat_sum_ns / shard_miss_lat_count;
      f->dump_format_unquoted("avg_miss_latency", "%" PRId64 ".%09" PRId64,
                              avg_ns / 1000000000ull,
                              avg_ns % 1000000000ull);
    }
    f->close_section();
    
    f->open_object_section("object_data_cache");
    
    f->open_object_section("buffer_miss_latency");
    auto buffer_miss_tavg = store.logger->get_tavg_ns(l_bluestore_buffer_miss_lat);
    uint64_t buffer_miss_sum_ns = buffer_miss_tavg.first;
    uint64_t buffer_miss_count = buffer_miss_tavg.second;
    f->dump_unsigned("avgcount", buffer_miss_count);
    f->dump_format_unquoted("sum", "%" PRId64 ".%09" PRId64,
                            buffer_miss_sum_ns / 1000000000ull,
                            buffer_miss_sum_ns % 1000000000ull);
    if (buffer_miss_count) {
      uint64_t avg_ns = buffer_miss_sum_ns / buffer_miss_count;
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64,
                              avg_ns / 1000000000ull,
                              avg_ns % 1000000000ull);
    } else {
      f->dump_format_unquoted("avgtime", "%" PRId64 ".%09" PRId64, 0, 0);
    }
    f->close_section();
    
    uint64_t buffer_hits = store.logger->get(l_bluestore_buffer_hits);
    f->dump_unsigned("hits", buffer_hits);
    f->dump_unsigned("misses", buffer_miss_count);
    uint64_t buffer_total = buffer_hits + buffer_miss_count;
    f->dump_unsigned("total_accesses", buffer_total);
    if (buffer_total > 0) {
      double buffer_hit_ratio = static_cast<double>(buffer_hits) / static_cast<double>(buffer_total);
      f->dump_float("hit_ratio", buffer_hit_ratio);
    } else {
      f->dump_float("hit_ratio", 0.0);
    }
    
    f->close_section();
    
    f->close_section();// since_startup
    
    // Short-term stat
    BlueStore::CacheStatsSnapshot current, most_recent, oldest;
    if (get_snapshot_windows(store, current, most_recent, oldest)) {
      
      auto recent_duration = std::chrono::duration<double>(current.timestamp - most_recent.timestamp).count();
      if (recent_duration > 0) {
        f->open_object_section("since_most_recent_snapshot");
        f->dump_float("window_seconds", recent_duration);
        
        f->open_object_section("onode_cache");
        uint64_t delta_onode_hits = current.onode_hits - most_recent.onode_hits;
        uint64_t delta_onode_misses = current.onode_misses - most_recent.onode_misses;
        uint64_t delta_onode_lat = current.onode_miss_latency_sum - most_recent.onode_miss_latency_sum;
        f->dump_unsigned("hits", delta_onode_hits);
        f->dump_unsigned("misses", delta_onode_misses);
        uint64_t delta_onode_total = delta_onode_hits + delta_onode_misses;
        if (delta_onode_total > 0) {
          f->dump_float("hit_ratio", (double)delta_onode_hits / (double)delta_onode_total);
        } else {
          f->dump_float("hit_ratio", 0.0);
        }
        if (delta_onode_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_onode_lat / (double)delta_onode_misses / 1000.0);
        }
        f->close_section();
        
        f->open_object_section("onode_shard");
        uint64_t delta_shard_hits = current.onode_shard_hits - most_recent.onode_shard_hits;
        uint64_t delta_shard_misses = current.onode_shard_misses - most_recent.onode_shard_misses;
        uint64_t delta_shard_lat = current.onode_shard_miss_latency_sum - most_recent.onode_shard_miss_latency_sum;
        f->dump_unsigned("hits", delta_shard_hits);
        f->dump_unsigned("misses", delta_shard_misses);
        uint64_t delta_shard_total = delta_shard_hits + delta_shard_misses;
        if (delta_shard_total > 0) {
          f->dump_float("hit_ratio", (double)delta_shard_hits / (double)delta_shard_total);
        } else {
          f->dump_float("hit_ratio", 0.0);
        }
        if (delta_shard_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_shard_lat / (double)delta_shard_misses / 1000.0);
        }
        f->close_section();
        
        f->open_object_section("object_data_cache");
        uint64_t delta_buffer_hits = current.buffer_hits - most_recent.buffer_hits;
        uint64_t delta_buffer_misses = current.buffer_miss_count - most_recent.buffer_miss_count;
        uint64_t delta_buffer_lat = current.buffer_miss_latency_sum - most_recent.buffer_miss_latency_sum;
        uint64_t delta_buffer_total = delta_buffer_hits + delta_buffer_misses;
        f->dump_unsigned("hits", delta_buffer_hits);
        f->dump_unsigned("misses", delta_buffer_misses);
        f->dump_unsigned("total_accesses", delta_buffer_total);
        if (delta_buffer_total > 0) {
          f->dump_float("hit_ratio", (double)delta_buffer_hits / (double)delta_buffer_total);
        } else {
          f->dump_float("hit_ratio", 0.0);
        }
        if (delta_buffer_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_buffer_lat / (double)delta_buffer_misses / 1000.0);
        }
        f->close_section();
        
        f->close_section();
      }
      
      auto oldest_duration = std::chrono::duration<double>(current.timestamp - oldest.timestamp).count();
      
      if (oldest_duration > 0) {
        f->open_object_section("since_oldest_snapshot");
        f->dump_float("window_seconds", oldest_duration);
        
        f->open_object_section("onode_cache");
        uint64_t delta_onode_hits = current.onode_hits - oldest.onode_hits;
        uint64_t delta_onode_misses = current.onode_misses - oldest.onode_misses;
        uint64_t delta_onode_lat = current.onode_miss_latency_sum - oldest.onode_miss_latency_sum;
        uint64_t delta_onode_total = delta_onode_hits + delta_onode_misses;
        
        f->dump_unsigned("hits", delta_onode_hits);
        f->dump_unsigned("misses", delta_onode_misses);
        f->dump_unsigned("total_accesses", delta_onode_total);
        if (delta_onode_total > 0) {
          f->dump_float("hit_ratio", (double)delta_onode_hits / (double)delta_onode_total);
          f->dump_float("accesses_per_second", (double)delta_onode_total / oldest_duration);
        } else {
          f->dump_float("hit_ratio", 0.0);
          f->dump_float("accesses_per_second", 0.0);
        }
        if (delta_onode_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_onode_lat / (double)delta_onode_misses / 1000.0);
        }
        f->close_section();
        
        f->open_object_section("onode_shard");
        uint64_t delta_shard_hits = current.onode_shard_hits - oldest.onode_shard_hits;
        uint64_t delta_shard_misses = current.onode_shard_misses - oldest.onode_shard_misses;
        uint64_t delta_shard_lat = current.onode_shard_miss_latency_sum - oldest.onode_shard_miss_latency_sum;
        f->dump_unsigned("hits", delta_shard_hits);
        f->dump_unsigned("misses", delta_shard_misses);
        uint64_t delta_shard_total = delta_shard_hits + delta_shard_misses;
        if (delta_shard_total > 0) {
          f->dump_float("hit_ratio", (double)delta_shard_hits / (double)delta_shard_total);
        } else {
          f->dump_float("hit_ratio", 0.0);
        }
        if (delta_shard_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_shard_lat / (double)delta_shard_misses / 1000.0);
        }
        f->close_section();
        
        f->open_object_section("object_data_cache");
        uint64_t delta_buffer_hits = current.buffer_hits - oldest.buffer_hits;
        uint64_t delta_buffer_misses = current.buffer_miss_count - oldest.buffer_miss_count;
        uint64_t delta_buffer_lat = current.buffer_miss_latency_sum - oldest.buffer_miss_latency_sum;
        uint64_t delta_buffer_total = delta_buffer_hits + delta_buffer_misses;
        
        f->dump_unsigned("hits", delta_buffer_hits);
        f->dump_unsigned("misses", delta_buffer_misses);
        f->dump_unsigned("total_accesses", delta_buffer_total);
        if (delta_buffer_total > 0) {
          f->dump_float("hit_ratio", (double)delta_buffer_hits / (double)delta_buffer_total);
          f->dump_float("accesses_per_second", (double)delta_buffer_total / oldest_duration);
        } else {
          f->dump_float("hit_ratio", 0.0);
          f->dump_float("accesses_per_second", 0.0);
        }
        if (delta_buffer_misses > 0) {
          f->dump_float("avg_miss_latency_us", (double)delta_buffer_lat / (double)delta_buffer_misses / 1000.0);
        }
        f->close_section();
        
        f->close_section();
      }
    }
    
    // RocksDB performance statistics (I think the hits and misses is useful, latency is not, commented out for now)
    //f->open_object_section("rocksdb_perf_stats");
    
    //auto collection = store.cct->get_perfcounters_collection();
    
    //if (collection) {
    //  collection->dump_formatted(f, false, static_cast<select_labeled_t>(0), "rocksdb", "");
    //} else {
    //  f->dump_string("status", "perf counters collection not available");
    //}
    
    //f->close_section(); // rocksdb_perf_stats
    
    f->close_section(); // cache_stats
    
    return 0;
  } else if (command == "bluestore clear static frag") {
    std::shared_lock l(store.coll_lock);
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    for (const auto& it : store.coll_map) {
      auto c = it.second;
      std::shared_lock l(c->lock);
      if (coll.empty() || coll == c->get_cid().c_str()) {
        c->static_frag_score.store(0, std::memory_order_relaxed);
        c->object_read_samples.store(0, std::memory_order_relaxed);
      }
    }
    return 0;
  } else {
    ss << "Invalid command" << std::endl;
    r = -ENOSYS;
  }
  return r;
}
