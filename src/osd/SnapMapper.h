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
#include <string.h>

#include "common/map_cacher.hpp"
#include "common/hobject.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "os/ObjectStore.h"

class OSDriver : public MapCacher::StoreDriver<std::string, bufferlist> {
  ObjectStore *os;
  coll_t cid;
  ghobject_t hoid;

public:
  class OSTransaction : public MapCacher::Transaction<std::string, bufferlist> {
    friend class OSDriver;
    coll_t cid;
    ghobject_t hoid;
    ObjectStore::Transaction *t;
    OSTransaction(
      coll_t cid,
      const ghobject_t &hoid,
      ObjectStore::Transaction *t)
      : cid(cid), hoid(hoid), t(t) {}
  public:
    void set_keys(
      const std::map<std::string, bufferlist> &to_set) {
      t->omap_setkeys(cid, hoid, to_set);
    }
    void remove_keys(
      const std::set<std::string> &to_remove) {
      t->omap_rmkeys(cid, hoid, to_remove);
    }
    void add_callback(
      Context *c) {
      t->register_on_applied(c);
    }
  };

  OSTransaction get_transaction(
    ObjectStore::Transaction *t) {
    return OSTransaction(cid, hoid, t);
  }

  OSDriver(ObjectStore *os, coll_t cid, const ghobject_t &hoid) :
    os(os), cid(cid), hoid(hoid) {}
  int get_keys(
    const std::set<std::string> &keys,
    std::map<std::string, bufferlist> *out);
  int get_next(
    const std::string &key,
    pair<std::string, bufferlist> *next);
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
 *  2) MAPPING_PREFIX + snapid_t + obj.str() -> encoding of pair<snapid_t, obj>
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
public:
  struct object_snaps {
    hobject_t oid;
    std::set<snapid_t> snaps;
    object_snaps(hobject_t oid, const std::set<snapid_t> &snaps)
      : oid(oid), snaps(snaps) {}
    object_snaps() {}
    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &bp);
  };

private:
  MapCacher::MapCacher<std::string, bufferlist> backend;

  static const std::string MAPPING_PREFIX;
  static const std::string OBJECT_PREFIX;

  static std::string get_prefix(snapid_t snap);

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map);

  std::pair<std::string, bufferlist> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map);

  static bool is_mapping(const std::string &to_test);

  std::pair<snapid_t, hobject_t> from_raw(
    const std::pair<std::string, bufferlist> &image);

  std::string to_object_key(const hobject_t &hoid);

  int get_snaps(const hobject_t &oid, object_snaps *out);

  void set_snaps(
    const hobject_t &oid,
    const object_snaps &out,
    MapCacher::Transaction<std::string, bufferlist> *t);

  void clear_snaps(
    const hobject_t &oid,
    MapCacher::Transaction<std::string, bufferlist> *t);

  // True if hoid belongs in this mapping based on mask_bits and match
  bool check(const hobject_t &hoid) const {
    return hoid.match(mask_bits, match);
  }

  int _remove_oid(
    const hobject_t &oid,    ///< [in] oid to remove
    MapCacher::Transaction<std::string, bufferlist> *t ///< [out] transaction
    );

public:
  static string make_shard_prefix(shard_id_t shard) {
    if (shard == shard_id_t::NO_SHARD)
      return string();
    char buf[20];
    int r = snprintf(buf, sizeof(buf), ".%x", (int)shard);
    assert(r < (int)sizeof(buf));
    return string(buf, r) + '_';
  }
  uint32_t mask_bits;
  const uint32_t match;
  string last_key_checked;
  const int64_t pool;
  const shard_id_t shard;
  const string shard_prefix;
  SnapMapper(
    MapCacher::StoreDriver<std::string, bufferlist> *driver,
    uint32_t match,  ///< [in] pgid
    uint32_t bits,   ///< [in] current split bits
    int64_t pool,    ///< [in] pool
    shard_id_t shard ///< [in] shard
    )
    : backend(driver), mask_bits(bits), match(match), pool(pool),
      shard(shard), shard_prefix(make_shard_prefix(shard)) {
    update_bits(mask_bits);
  }

  set<string> prefixes;
  /// Update bits in case of pg split
  void update_bits(
    uint32_t new_bits  ///< [in] new split bits
    ) {
    assert(new_bits >= mask_bits);
    mask_bits = new_bits;
    set<string> _prefixes = hobject_t::get_prefixes(
      mask_bits,
      match,
      pool);
    prefixes.clear();
    for (set<string>::iterator i = _prefixes.begin();
	 i != _prefixes.end();
	 ++i) {
      prefixes.insert(shard_prefix + *i);
    }
  }

  /// Update snaps for oid, empty new_snaps removes the mapping
  int update_snaps(
    const hobject_t &oid,       ///< [in] oid to update
    const std::set<snapid_t> &new_snaps, ///< [in] new snap set
    const std::set<snapid_t> *old_snaps, ///< [in] old snaps (for debugging)
    MapCacher::Transaction<std::string, bufferlist> *t ///< [out] transaction
    ); ///@ return error, 0 on success

  /// Add mapping for oid, must not already be mapped
  void add_oid(
    const hobject_t &oid,       ///< [in] oid to add
    const std::set<snapid_t>& new_snaps, ///< [in] snaps
    MapCacher::Transaction<std::string, bufferlist> *t ///< [out] transaction
    );

  /// Returns first object with snap as a snap
  int get_next_objects_to_trim(
    snapid_t snap,              ///< [in] snap to check
    unsigned max,               ///< [in] max to get
    vector<hobject_t> *out      ///< [out] next objects to trim (must be empty)
    );  ///< @return error, -ENOENT if no more objects

  /// Remove mapping for oid
  int remove_oid(
    const hobject_t &oid,    ///< [in] oid to remove
    MapCacher::Transaction<std::string, bufferlist> *t ///< [out] transaction
    ); ///< @return error, -ENOENT if the object is not mapped

  /// Get snaps for oid
  int get_snaps(
    const hobject_t &oid,     ///< [in] oid to get snaps for
    std::set<snapid_t> *snaps ///< [out] snaps
    ); ///< @return error, -ENOENT if oid is not recorded
};
WRITE_CLASS_ENCODER(SnapMapper::object_snaps)

#endif
