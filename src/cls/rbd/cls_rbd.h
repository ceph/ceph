// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_CLS_RBD_H
#define __CEPH_CLS_RBD_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "include/rbd_types.h"
#include "common/Formatter.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/Clock.h"

/// information about our parent image, if any
struct cls_rbd_parent {
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_id;
  snapid_t snap_id = CEPH_NOSNAP;
  std::optional<uint64_t> head_overlap = std::nullopt;

  cls_rbd_parent() {
  }
  cls_rbd_parent(const cls::rbd::ParentImageSpec& parent_image_spec,
                 const std::optional<uint64_t>& head_overlap)
    : pool_id(parent_image_spec.pool_id),
      pool_namespace(parent_image_spec.pool_namespace),
      image_id(parent_image_spec.image_id), snap_id(parent_image_spec.snap_id),
      head_overlap(head_overlap) {
  }

  inline bool exists() const {
    return (pool_id >= 0 && !image_id.empty() && snap_id != CEPH_NOSNAP);
  }

  inline bool operator==(const cls_rbd_parent& rhs) const {
    return (pool_id == rhs.pool_id &&
            pool_namespace == rhs.pool_namespace &&
            image_id == rhs.image_id &&
            snap_id == rhs.snap_id);
  }
  inline bool operator!=(const cls_rbd_parent& rhs) const {
    return !(*this == rhs);
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    // NOTE: remove support for version 1 after Nautilus EOLed
    uint8_t version = 1;
    if ((features & CEPH_FEATURE_SERVER_NAUTILUS) != 0ULL) {
      // break backwards compatability when using nautilus or later OSDs
      version = 2;
    }

    ENCODE_START(version, version, bl);
    encode(pool_id, bl);
    if (version >= 2) {
      encode(pool_namespace, bl);
    }
    encode(image_id, bl);
    encode(snap_id, bl);
    if (version == 1) {
      encode(head_overlap.value_or(0ULL), bl);
    } else {
      encode(head_overlap, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(pool_id, bl);
    if (struct_v >= 2) {
      decode(pool_namespace, bl);
    }
    decode(image_id, bl);
    decode(snap_id, bl);
    if (struct_v == 1) {
      uint64_t overlap;
      decode(overlap, bl);
      head_overlap = overlap;
    } else {
      decode(head_overlap, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_int("pool_id", pool_id);
    f->dump_string("pool_namespace", pool_namespace);
    f->dump_string("image_id", image_id);
    f->dump_unsigned("snap_id", snap_id);
    if (head_overlap) {
      f->dump_unsigned("head_overlap", *head_overlap);
    }
  }

  static void generate_test_instances(std::list<cls_rbd_parent*>& o) {
    o.push_back(new cls_rbd_parent{});
    o.push_back(new cls_rbd_parent{{1, "", "image id", 234}, {}});
    o.push_back(new cls_rbd_parent{{1, "", "image id", 234}, {123}});
    o.push_back(new cls_rbd_parent{{1, "ns", "image id", 234}, {123}});
  }
};
WRITE_CLASS_ENCODER_FEATURES(cls_rbd_parent)

struct cls_rbd_snap {
  snapid_t id = CEPH_NOSNAP;
  std::string name;
  uint64_t image_size = 0;
  uint8_t protection_status = RBD_PROTECTION_STATUS_UNPROTECTED;
  cls_rbd_parent parent;
  uint64_t flags = 0;
  utime_t timestamp;
  cls::rbd::SnapshotNamespace snapshot_namespace = {
    cls::rbd::UserSnapshotNamespace{}};
  uint32_t child_count = 0;
  std::optional<uint64_t> parent_overlap = std::nullopt;

  cls_rbd_snap() {
  }
  cls_rbd_snap(snapid_t id, const std::string& name, uint64_t image_size,
               uint8_t protection_status, const cls_rbd_parent& parent,
               uint64_t flags, utime_t timestamp,
               const cls::rbd::SnapshotNamespace& snapshot_namespace,
               uint32_t child_count,
               const std::optional<uint64_t>& parent_overlap)
    : id(id), name(name), image_size(image_size),
      protection_status(protection_status), parent(parent), flags(flags),
      timestamp(timestamp), snapshot_namespace(snapshot_namespace),
      child_count(child_count), parent_overlap(parent_overlap) {
  }

  bool migrate_parent_format(uint64_t features) const {
    return (((features & CEPH_FEATURE_SERVER_NAUTILUS) != 0) &&
            (parent.exists()));
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    // NOTE: remove support for versions < 8 after Nautilus EOLed
    uint8_t min_version = 1;
    if ((features & CEPH_FEATURE_SERVER_NAUTILUS) != 0ULL) {
      // break backwards compatability when using nautilus or later OSDs
      min_version = 8;
    }

    ENCODE_START(8, min_version, bl);
    encode(id, bl);
    encode(name, bl);
    encode(image_size, bl);
    if (min_version < 8) {
      uint64_t image_features = 0;
      encode(image_features, bl); // unused -- preserve ABI
      encode(parent, bl, features);
    }
    encode(protection_status, bl);
    encode(flags, bl);
    encode(snapshot_namespace, bl);
    encode(timestamp, bl);
    encode(child_count, bl);
    encode(parent_overlap, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(8, p);
    decode(id, p);
    decode(name, p);
    decode(image_size, p);
    if (struct_compat < 8) {
      uint64_t features;
      decode(features, p); // unused -- preserve ABI
    }
    if (struct_v >= 2 && struct_compat < 8) {
      decode(parent, p);
    }
    if (struct_v >= 3) {
      decode(protection_status, p);
    }
    if (struct_v >= 4) {
      decode(flags, p);
    }
    if (struct_v >= 5) {
      decode(snapshot_namespace, p);
    }
    if (struct_v >= 6) {
      decode(timestamp, p);
    }
    if (struct_v >= 7) {
      decode(child_count, p);
    }
    if (struct_v >= 8) {
      decode(parent_overlap, p);
    }
    DECODE_FINISH(p);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("id", id);
    f->dump_string("name", name);
    f->dump_unsigned("image_size", image_size);
    if (parent.exists()) {
      f->open_object_section("parent");
      parent.dump(f);
      f->close_section();
    }
    switch (protection_status) {
    case RBD_PROTECTION_STATUS_UNPROTECTED:
      f->dump_string("protection_status", "unprotected");
      break;
    case RBD_PROTECTION_STATUS_UNPROTECTING:
      f->dump_string("protection_status", "unprotecting");
      break;
    case RBD_PROTECTION_STATUS_PROTECTED:
      f->dump_string("protection_status", "protected");
      break;
    default:
      ceph_abort();
    }
    f->dump_unsigned("child_count", child_count);
    if (parent_overlap) {
      f->dump_unsigned("parent_overlap", *parent_overlap);
    }
  }

  static void generate_test_instances(std::list<cls_rbd_snap*>& o) {
    o.push_back(new cls_rbd_snap{});
    o.push_back(new cls_rbd_snap{1, "snap", 123456,
                                 RBD_PROTECTION_STATUS_PROTECTED,
                                 {{1, "", "image", 123}, 234}, 31, {},
                                 cls::rbd::UserSnapshotNamespace{}, 543, {}});
    o.push_back(new cls_rbd_snap{1, "snap", 123456,
                                 RBD_PROTECTION_STATUS_PROTECTED,
                                 {{1, "", "image", 123}, 234}, 31, {},
                                 cls::rbd::UserSnapshotNamespace{}, 543, {0}});
    o.push_back(new cls_rbd_snap{1, "snap", 123456,
                                 RBD_PROTECTION_STATUS_PROTECTED,
                                 {{1, "ns", "image", 123}, 234}, 31, {},
                                 cls::rbd::UserSnapshotNamespace{}, 543,
                                 {123}});
  }
};
WRITE_CLASS_ENCODER_FEATURES(cls_rbd_snap)

struct cls_rbd_rwlcache_map {
  epoch_t current_cache_id;

  struct Daemon {
    uint64_t id;

    std::string rdma_address;
    int32_t rdma_port;

    uint64_t total_size;
    uint64_t free_size;

    // Primary or other replicated already flush data into osd.
    // So those caches can directly deleted.
    std::set<epoch_t> need_free_caches;

    utime_t expiration;

    struct entity_addr_t daemon_addr;

    Daemon() {
    }

    Daemon(const cls::rbd::RwlCacheDaemonInfo &info,
	   const entity_inst_t &inst) :
      id(info.id),
      rdma_address(info.rdma_address),
      rdma_port(info.rdma_port),
      total_size(info.total_size),
      free_size(info.total_size),
      expiration(ceph_clock_now() + utime_t(RBD_RWLCACHE_DAEMON_PING_TIMEOUT, 0)),
      daemon_addr(inst.addr) {
    }

    void encode(ceph::buffer::list &bl, uint64_t features) const {
      ENCODE_START(1, 1, bl);
      encode(id, bl);
      encode(rdma_address, bl);
      encode(rdma_port, bl);
      encode(total_size, bl);
      encode(free_size, bl);
      encode(need_free_caches, bl);
      encode(expiration, bl);
      encode(daemon_addr, bl, features);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &it) {
      DECODE_START(1, it);
      decode(id, it);
      decode(rdma_address, it);
      decode(rdma_port, it);
      decode(total_size, it);
      decode(free_size, it);
      decode(need_free_caches, it);
      decode(expiration, it);
      decode(daemon_addr, it);
      DECODE_FINISH(it);
    }
  };

  std::map<uint64_t, struct Daemon> daemons;

  struct Cache {
    epoch_t cache_id;

    // infos of primary
    uint64_t primary_id;
    struct entity_addr_t primary_addr;

    uint64_t cache_size;
    uint32_t copies;

    // unfree space daemons. Before free, daemons.size() == copies
    std::set<uint64_t> daemons;

    Cache() {
    }

    Cache(epoch_t cache_id,
	  const cls::rbd::RwlCacheRequest &req,
	  const struct entity_addr_t &addr,
	  const std::set<uint64_t> &daemons) :
      cache_id(cache_id),
      primary_id(req.id),
      primary_addr(addr),
      cache_size(req.size),
      copies(req.copies),
      daemons(daemons) {
    }

    void encode(ceph::buffer::list &bl, uint64_t features) const {
      ENCODE_START(1, 1, bl);
      encode(cache_id, bl);
      encode(primary_id, bl);
      encode(primary_addr, bl, features);
      encode(cache_size, bl);
      encode(copies, bl);
      encode(daemons, bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &it) {
      DECODE_START(1, it);
      decode(cache_id, it);
      decode(primary_id, it);
      decode(primary_addr, it);
      decode(cache_size, it);
      decode(copies, it);
      decode(daemons, it);
      DECODE_FINISH(it);
    }
  };

  // cache allocated but not receive ack in uncommitted_caches
  std::map<epoch_t, std::pair<utime_t, struct Cache>> uncommitted_caches;

  // uncommitted_cache recive ack in time and result is ok. Move cahe from uncommitted_cache
  // to caches.
  std::map<epoch_t, struct Cache> caches;

  // Primary can't free related daemon's cache-file, etc rdma-disconnect.
  // So cache move uncommitted_caches or caches to free_daemon_space_caches;
  std::map<epoch_t, struct Cache> free_daemon_space_caches;

  cls_rbd_rwlcache_map() {
    current_cache_id = 0;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    ENCODE_START(1, 1, bl);
    encode(current_cache_id, bl);
    encode(daemons, bl, features);
    encode(uncommitted_caches, bl, features);
    encode(caches, bl, features);
    encode(free_daemon_space_caches, bl, features);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &it) {
    DECODE_START(1, it);
    decode(current_cache_id, it);
    decode(daemons, it);
    decode(uncommitted_caches, it);
    decode(caches, it);
    decode(free_daemon_space_caches, it);
    DECODE_FINISH(it);
  }

  void dump(ceph::Formatter *f) const {
    f->open_array_section("librbd rwlcache infos");
    f->close_section();
  }
};
WRITE_CLASS_ENCODER_FEATURES(cls_rbd_rwlcache_map)
WRITE_CLASS_ENCODER_FEATURES(cls_rbd_rwlcache_map::Daemon)
WRITE_CLASS_ENCODER_FEATURES(cls_rbd_rwlcache_map::Cache)

#endif // __CEPH_CLS_RBD_H
