// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "BlueAdmin.h"
#include "Compression.h"
#include "common/errno.h"
#include "common/pretty_binary.h"
#include "os/bluestore/BlueStore.h"
#include "common/debug.h"
#include <asm-generic/errno-base.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <limits>
#include <optional>

#define dout_subsys ceph_subsys_bluestore
#define dout_context store.cct

using ceph::bufferlist;
using ceph::Formatter;
using ceph::common::cmd_getval;

static void dump_avg_latency(Formatter *f, const char *name, uint64_t sum_ns, uint64_t count) {
  f->open_object_section(name);
  f->dump_unsigned("avgcount", count);
  f->dump_float("sum", sum_ns / 1000000000.0);
  if (count) {
    f->dump_float("avgtime", (sum_ns / (double)count) / 1000000000.0);
  } else {
    f->dump_float("avgtime", 0.0);
  }
  f->close_section();
}

static void dump_cache_section(Formatter *f, const char *name,
                               uint64_t hits, uint64_t misses,
                               uint64_t lat_sum, uint64_t lat_count,
                               bool is_byte_cache,
                               std::optional<double> duration = std::nullopt) {
  f->open_object_section(name);
  dump_avg_latency(f, is_byte_cache ? "buffer_miss_latency" : "miss_latency", lat_sum, lat_count);

  f->dump_unsigned(is_byte_cache ? "hit_bytes" : "hits", hits);
  f->dump_unsigned(is_byte_cache ? "miss_bytes" : "misses", misses);
  uint64_t total = hits + misses;
  f->dump_unsigned(is_byte_cache ? "total_byte_accesses" : "total", total);
  if (total > 0) {
    f->dump_float(is_byte_cache ? "byte_hit_ratio" : "hit_ratio", (double)hits / (double)total);
  } else {
    f->dump_float(is_byte_cache ? "byte_hit_ratio" : "hit_ratio", 0.0);
  }

  if (duration && *duration > 0) {
    f->dump_float("accesses_per_second", (double)total / *duration);
  }

  f->close_section();
}

static void dump_snapshot_section(Formatter *f, const char *name,
                                  const BlueStore::CacheStatsSnapshot& snap,
                                  std::optional<double> duration = std::nullopt) {
  f->open_object_section(name);
  if (duration) {
    f->dump_float("seconds elapsed", *duration);
  }
  dump_cache_section(f, "onode_cache", snap.onode_hits, snap.onode_misses,
                     snap.onode_miss_latency_sum, snap.onode_misses,
                     false, duration);
  dump_cache_section(f, "onode_shard", snap.onode_shard_hits, snap.onode_shard_misses,
                     snap.onode_shard_miss_latency_sum, snap.onode_shard_misses,
                     false, duration);
  dump_cache_section(f, "object_data_cache", snap.buffer_hit_bytes, snap.buffer_miss_bytes,
                     snap.buffer_miss_latency_sum, snap.buffer_miss_lat_count,
                     true, duration);
  f->close_section();
}

static void get_snapshot_windows(BlueStore& store,
                                 BlueStore::CacheStatsSnapshot& current,
                                 BlueStore::CacheStatsSnapshot& most_recent,
                                 BlueStore::CacheStatsSnapshot& oldest) {
  std::lock_guard l(store.cache_stats_lock);

  // Get current values
  current.timestamp = ceph::mono_clock::now();
  current.onode_hits = store.logger->get(l_bluestore_onode_hits);
  current.onode_misses = store.logger->get(l_bluestore_onode_misses);
  current.onode_miss_latency_sum = store.logger->get_tavg_ns(l_bluestore_onode_miss_lat).first;
  current.onode_shard_hits = store.logger->get(l_bluestore_onode_shard_hits);
  current.onode_shard_misses = store.logger->get(l_bluestore_onode_shard_misses);
  current.onode_shard_miss_latency_sum = store.logger->get_tavg_ns(l_bluestore_onode_shard_miss_lat).first;
  current.buffer_hit_bytes = store.logger->get(l_bluestore_buffer_hit_bytes);
  current.buffer_miss_bytes = store.logger->get(l_bluestore_buffer_miss_bytes);
  auto buffer_miss_tavg = store.logger->get_tavg_ns(l_bluestore_buffer_miss_lat);
  current.buffer_miss_latency_sum = buffer_miss_tavg.first;
  current.buffer_miss_lat_count = buffer_miss_tavg.second;

  store.cache_stats_snapshots.push_back(current);
  while (store.cache_stats_snapshots.size() > BlueStore::MAX_CACHE_SNAPSHOTS) {
    store.cache_stats_snapshots.pop_front();
  }

  if (store.cache_stats_snapshots.size() >= 2) {
    most_recent = store.cache_stats_snapshots[store.cache_stats_snapshots.size() - 2];
  } else {
    most_recent = store.cache_stats_snapshots.back();
  }
  oldest = store.cache_stats_snapshots.front();
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
    r = admin_socket->register_command("bluestore bluefs-bdev-expand",
                                       this,
                                       "Instruct BlueFS to check the size of its block devices"
                                       " and, if they have expanded, make use of the additional space.");
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
    f->open_object_section("cache_stats");

    BlueStore::CacheStatsSnapshot current, most_recent, oldest;
    get_snapshot_windows(store, current, most_recent, oldest);
    dump_snapshot_section(f, "since_startup", current);

    double recent_duration = std::chrono::duration<double>(
      current.timestamp - most_recent.timestamp).count();
    if (recent_duration > 0) {
      dump_snapshot_section(f, "since_most_recent_snapshot",
                            current.delta(most_recent), recent_duration);
    }

    double oldest_duration = std::chrono::duration<double>(
      current.timestamp - oldest.timestamp).count();
    if (oldest_duration > 0) {
      dump_snapshot_section(f, "since_oldest_snapshot",
                            current.delta(oldest), oldest_duration);
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
  } else if (command == "bluestore bluefs-bdev-expand"){
    std::stringstream result;
    int ret = store.expand_devices(result);
    if (ret < 0) {
      ss << "expand device failed: " << cpp_strerror(ret) << std::endl;
    } else {
      out.append(result.str());
    }
    return ret;
  } else {
    ss << "Invalid command" << std::endl;
    r = -ENOSYS;
  }
  return r;
}
