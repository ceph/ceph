// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef SNAPMAPPER_H
#define SNAPMAPPER_H

#include <string>
#include <set>
#include <utility>
#include <cstring>

#include "common/map_cacher.hpp"
#include "common/hobject.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "os/ObjectStore.h"
#include "osd/OSDMap.h"

class OSDriver : public MapCacher::StoreDriver<std::string, ceph::buffer::list> {
  ObjectStore *os;
  ObjectStore::CollectionHandle ch;
  ghobject_t hoid;

public:
  class OSTransaction : public MapCacher::Transaction<std::string, ceph::buffer::list> {
    friend class OSDriver;
    coll_t cid;
    ghobject_t hoid;
    ObjectStore::Transaction *t;
    OSTransaction(
      const coll_t &cid,
      const ghobject_t &hoid,
      ObjectStore::Transaction *t)
      : cid(cid), hoid(hoid), t(t) {}
  public:
    void set_keys(
      const std::map<std::string, ceph::buffer::list> &to_set) override {
      t->omap_setkeys(cid, hoid, to_set);
    }
    void remove_keys(
      const std::set<std::string> &to_remove) override {
      t->omap_rmkeys(cid, hoid, to_remove);
    }
    void add_callback(
      Context *c) override {
      t->register_on_applied(c);
    }
  };

  OSTransaction get_transaction(
    ObjectStore::Transaction *t) {
    return OSTransaction(ch->cid, hoid, t);
  }

  OSDriver(ObjectStore *os, const coll_t& cid, const ghobject_t &hoid) :
    os(os),
    hoid(hoid) {
    ch = os->open_collection(cid);
  }
  int get_keys(
    const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out) override;
  int get_next(
    const std::string &key,
    std::pair<std::string, ceph::buffer::list> *next) override;
};

/**
 * SnapMapper
 *
 * Manages two mappings:
 *  1) hobject_t -> {snapid}
 *  2) snapid -> {hobject_t}
 *
 * We accomplish this using two sets of keys:
 *  1) OBJECT_PREFIX + obj.str() -> encoding of object_snaps
 *  2) MAPPING_PREFIX + poolid + snapid_t + obj.str() -> encoding of std::pair<snapid_t, obj>
 *
 * The on disk strings and encodings are implemented in to_raw, to_raw_key,
 * from_raw, to_object_key.
 *
 * The object -> {snapid} mapping is primarily included so that the
 * SnapMapper state can be verified against the external PG state during
 * scrub etc.
 *
 * The 2) mapping is arranged such that all objects in a particular
 * snap will sort together, and so that all objects in a pg for a
 * particular snap will group under up to 8 prefixes.
 */
class SnapMapper {
  friend class MapperVerifier;
public:
  CephContext* cct;
  struct object_snaps {
    hobject_t oid;
    std::set<snapid_t> snaps;
    object_snaps(hobject_t oid, const std::set<snapid_t> &snaps)
      : oid(oid), snaps(snaps) {}
    object_snaps() {}
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &bp);
  };

  struct Mapping {
    snapid_t snap;
    hobject_t hoid;
    explicit Mapping(const std::pair<snapid_t, hobject_t> &in)
      : snap(in.first), hoid(in.second) {}
    Mapping() : snap(0) {}
    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(1, 1, bl);
      encode(snap, bl);
      encode(hoid, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      DECODE_START(1, bl);
      decode(snap, bl);
      decode(hoid, bl);
      DECODE_FINISH(bl);
    }
  };

  static const std::string LEGACY_MAPPING_PREFIX;
  static const std::string MAPPING_PREFIX;
  static const std::string OBJECT_PREFIX;
  static const char *PURGED_SNAP_EPOCH_PREFIX;
  static const char *PURGED_SNAP_PREFIX;

  struct Scrubber {
    CephContext *cct;
    ObjectStore *store;
    ObjectStore::CollectionHandle ch;
    ghobject_t mapping_hoid;
    ghobject_t purged_snaps_hoid;

    ObjectMap::ObjectMapIterator psit;
    int64_t pool;
    snapid_t begin, end;

    bool _parse_p();   ///< advance the purged_snaps pointer

    ObjectMap::ObjectMapIterator mapit;
    Mapping mapping;
    shard_id_t shard;

    bool _parse_m();   ///< advance the (object) mapper pointer

    std::vector<std::tuple<int64_t, snapid_t, uint32_t, shard_id_t>> stray;

    Scrubber(
      CephContext *cct,
      ObjectStore *store,
      ObjectStore::CollectionHandle& ch,
      ghobject_t mapping_hoid,
      ghobject_t purged_snaps_hoid)
      : cct(cct),
	store(store),
	ch(ch),
	mapping_hoid(mapping_hoid),
	purged_snaps_hoid(purged_snaps_hoid) {}

    void run();
  };

  static std::string convert_legacy_key(
    const std::string& old_key,
    const bufferlist& value);

  static int convert_legacy(
    CephContext *cct,
    ObjectStore *store,
    ObjectStore::CollectionHandle& ch,
    ghobject_t hoid,
    unsigned max);

  static void record_purged_snaps(
    CephContext *cct,
    ObjectStore *store,
    ObjectStore::CollectionHandle& ch,
    ghobject_t hoid,
    ObjectStore::Transaction *t,
    std::map<epoch_t,mempool::osdmap::map<int64_t,snap_interval_set_t>> purged_snaps);
  static void scrub_purged_snaps(
    CephContext *cct,
    ObjectStore *store,
    ObjectStore::CollectionHandle& ch,
    ghobject_t mapper_hoid,
    ghobject_t purged_snaps_hoid);

private:
  static int _lookup_purged_snap(
    CephContext *cct,
    ObjectStore *store,
    ObjectStore::CollectionHandle& ch,
    const ghobject_t& hoid,
    int64_t pool, snapid_t snap,
    snapid_t *begin, snapid_t *end);
  static void make_purged_snap_key_value(
    int64_t pool, snapid_t begin,
    snapid_t end, std::map<std::string,ceph::buffer::list> *m);
  static std::string make_purged_snap_key(int64_t pool, snapid_t last);


  MapCacher::MapCacher<std::string, ceph::buffer::list> backend;

  static std::string get_legacy_prefix(snapid_t snap);
  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map);
  static bool is_legacy_mapping(const std::string &to_test);

  static std::string get_prefix(int64_t pool, snapid_t snap);
  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map);

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map);

  static bool is_mapping(const std::string &to_test);

  static std::pair<snapid_t, hobject_t> from_raw(
    const std::pair<std::string, ceph::buffer::list> &image);

  std::string to_object_key(const hobject_t &hoid);

  int get_snaps(const hobject_t &oid, object_snaps *out);

  void set_snaps(
    const hobject_t &oid,
    const object_snaps &out,
    MapCacher::Transaction<std::string, ceph::buffer::list> *t);

  void clear_snaps(
    const hobject_t &oid,
    MapCacher::Transaction<std::string, ceph::buffer::list> *t);

  // True if hoid belongs in this mapping based on mask_bits and match
  bool check(const hobject_t &hoid) const;

  int _remove_oid(
    const hobject_t &oid,    ///< [in] oid to remove
    MapCacher::Transaction<std::string, ceph::buffer::list> *t ///< [out] transaction
    );

public:
  static std::string make_shard_prefix(shard_id_t shard) {
    if (shard == shard_id_t::NO_SHARD)
      return std::string();
    char buf[20];
    int r = snprintf(buf, sizeof(buf), ".%x", (int)shard);
    ceph_assert(r < (int)sizeof(buf));
    return std::string(buf, r) + '_';
  }
  uint32_t mask_bits;
  const uint32_t match;
  std::string last_key_checked;
  const int64_t pool;
  const shard_id_t shard;
  const std::string shard_prefix;
  SnapMapper(
    CephContext* cct,
    MapCacher::StoreDriver<std::string, ceph::buffer::list> *driver,
    uint32_t match,  ///< [in] pgid
    uint32_t bits,   ///< [in] current split bits
    int64_t pool,    ///< [in] pool
    shard_id_t shard ///< [in] shard
    )
    : cct(cct), backend(driver), mask_bits(bits), match(match), pool(pool),
      shard(shard), shard_prefix(make_shard_prefix(shard)) {
    update_bits(mask_bits);
  }

  std::set<std::string> prefixes;
  /// Update bits in case of pg split or merge
  void update_bits(
    uint32_t new_bits  ///< [in] new split bits
    ) {
    mask_bits = new_bits;
    std::set<std::string> _prefixes = hobject_t::get_prefixes(
      mask_bits,
      match,
      pool);
    prefixes.clear();
    for (auto i = _prefixes.begin(); i != _prefixes.end(); ++i) {
      prefixes.insert(shard_prefix + *i);
    }
  }

  /// Update snaps for oid, empty new_snaps removes the mapping
  int update_snaps(
    const hobject_t &oid,       ///< [in] oid to update
    const std::set<snapid_t> &new_snaps, ///< [in] new snap std::set
    const std::set<snapid_t> *old_snaps, ///< [in] old snaps (for debugging)
    MapCacher::Transaction<std::string, ceph::buffer::list> *t ///< [out] transaction
    ); ///@ return error, 0 on success

  /// Add mapping for oid, must not already be mapped
  void add_oid(
    const hobject_t &oid,       ///< [in] oid to add
    const std::set<snapid_t>& new_snaps, ///< [in] snaps
    MapCacher::Transaction<std::string, ceph::buffer::list> *t ///< [out] transaction
    );

  /// Returns first object with snap as a snap
  int get_next_objects_to_trim(
    snapid_t snap,              ///< [in] snap to check
    unsigned max,               ///< [in] max to get
    std::vector<hobject_t> *out      ///< [out] next objects to trim (must be empty)
    );  ///< @return error, -ENOENT if no more objects

  /// Remove mapping for oid
  int remove_oid(
    const hobject_t &oid,    ///< [in] oid to remove
    MapCacher::Transaction<std::string, ceph::buffer::list> *t ///< [out] transaction
    ); ///< @return error, -ENOENT if the object is not mapped

  /// Get snaps for oid
  int get_snaps(
    const hobject_t &oid,     ///< [in] oid to get snaps for
    std::set<snapid_t> *snaps ///< [out] snaps
    ); ///< @return error, -ENOENT if oid is not recorded
};
WRITE_CLASS_ENCODER(SnapMapper::object_snaps)
WRITE_CLASS_ENCODER(SnapMapper::Mapping)

#endif
