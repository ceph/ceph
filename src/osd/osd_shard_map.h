// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <map>
#include <ostream>
#include <vector>

#include "common/Formatter.h"
#include "include/encoding.h"
#include "osd/osd_types.h"

/**
 * osd_shard_map_t
 *
 * Placement of an OSD's PGs on its reactors (Crimson). OSD-local: it never
 * enters the OSDMap. Placement = default_core(pgid) unless overridden.
 *
 * Shared by the OSD (authoritative copy) and clients (cached copy, pulled
 * when an MOSDOpReply carries a different version). Header-only so that
 * librados can use it without new link dependencies.
 *
 * Store layout assumed by default_placement() mirrors PGShardMapping's
 * constructor (crimson/osd/pg_map.h):
 *  - n_cores == n_store_shards: core c owns store shard c.
 *  - n_cores <  n_store_shards: store shard s lives on core s % n_cores at
 *    local index s / n_cores.
 *  - n_cores >  n_store_shards: core c uses store shard c % n_store_shards
 *    (index 0); several cores share one store shard.
 */
struct osd_shard_map_t {
  static constexpr uint16_t NO_CORE = UINT16_MAX;

  uint32_t n_cores = 0;          ///< reactor count; 0 == not initialised
  uint32_t n_store_shards = 0;   ///< store shard count
  uint64_t version = 0;          ///< monotonic per OSD, persisted
  std::map<spg_t, uint16_t> overrides;  ///< exceptions to default_core()
  /// Per-reactor client ports (crimson_osd_client_shard_ports), indexed by
  /// core; 0 = none. Bound from the ms_bind_port range at startup, so they
  /// may change across restarts (and the version always does).
  std::vector<uint16_t> shard_ports;

  bool empty() const { return n_cores == 0; }

  /// Placement key of a PG: ps + pool, taken modulo the slot count
  /// (n_cores, or n_store_shards when there are more store shards).
  /// Deliberately not hashed:
  ///  - when an OSD holds every PG of a pool, PGs spread exactly evenly;
  ///  - with pg_num a multiple of the slot count, a split child
  ///    (ps + k * old_pg_num) keeps its parent's slot, so a split moves
  ///    nothing between reactors and adds no overrides;
  ///  - every OSD with the same layout computes the same slot, so the
  ///    primary and replicas of a PG run on the same reactor index.
  /// The EC shard is not part of the key: an OSD normally holds a single
  /// shard of a PG, and leaving it out keeps the key equal across OSDs.
  static uint32_t pg_key(const spg_t &pgid) {
    return pgid.pgid.ps() + static_cast<uint32_t>(pgid.pgid.pool());
  }

  struct placement_t {
    uint16_t core;
    uint16_t store_index;
  };

  /// Default (core, local store index) for pgid. Requires !empty().
  placement_t default_placement(const spg_t &pgid) const {
    const uint32_t h = pg_key(pgid);
    if (n_cores >= n_store_shards) {
      return {static_cast<uint16_t>(h % n_cores), 0};
    }
    const uint32_t s = h % n_store_shards;
    return {static_cast<uint16_t>(s % n_cores),
            static_cast<uint16_t>(s / n_cores)};
  }

  uint16_t default_core(const spg_t &pgid) const {
    return default_placement(pgid).core;
  }

  /// Core serving pgid. Clients use this to pick a shard connection.
  uint16_t get_core(const spg_t &pgid) const {
    if (empty()) {
      return NO_CORE;
    }
    if (auto it = overrides.find(pgid); it != overrides.end()) {
      return it->second;
    }
    return default_core(pgid);
  }

  /// Record that pgid lives on core; returns true if the table changed.
  /// Does not bump version; the owner decides when to publish.
  bool set_core(const spg_t &pgid, uint16_t core) {
    if (core == default_core(pgid)) {
      return overrides.erase(pgid) > 0;
    }
    auto [it, inserted] = overrides.emplace(pgid, core);
    if (!inserted && it->second == core) {
      return false;
    }
    it->second = core;
    return true;
  }

  bool clear_core(const spg_t &pgid) {
    return overrides.erase(pgid) > 0;
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(n_cores, bl);
    encode(n_store_shards, bl);
    encode(version, bl);
    encode(overrides, bl);
    encode(shard_ports, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &p) {
    DECODE_START(1, p);
    decode(n_cores, p);
    decode(n_store_shards, p);
    decode(version, p);
    decode(overrides, p);
    decode(shard_ports, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("n_cores", n_cores);
    f->dump_unsigned("n_store_shards", n_store_shards);
    f->dump_unsigned("version", version);
    f->open_array_section("overrides");
    for (const auto &[pgid, core] : overrides) {
      f->open_object_section("override");
      f->dump_stream("pgid") << pgid;
      f->dump_unsigned("core", core);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("shard_ports");
    for (const auto port : shard_ports) {
      f->dump_unsigned("port", port);
    }
    f->close_section();
  }

  /// Client port of reactor `core`, or 0 if it has none (no reactor ports,
  /// the port could not be bound, or an older map).
  uint16_t get_shard_port(uint32_t core) const {
    return core < shard_ports.size() ? shard_ports[core] : 0;
  }
};
WRITE_CLASS_ENCODER(osd_shard_map_t)

inline std::ostream &operator<<(std::ostream &out, const osd_shard_map_t &m) {
  return out << "osd_shard_map(v" << m.version << " cores " << m.n_cores
             << " store_shards " << m.n_store_shards
             << " overrides " << m.overrides.size()
             << " ports " << m.shard_ports.size() << ")";
}
