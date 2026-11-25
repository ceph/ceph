// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <functional>
#include <map>
#include <string>
#include <vector>
#include <seastar/core/future.hh>
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/purged_snaps.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace ceph::os {
  class Transaction;
}

namespace crimson::os {
  class FuturizedCollection;
  class FuturizedStore;
}

using read_errorator = crimson::os::FuturizedStore::Shard::read_errorator;

/// metadata shared across PGs, or put in another way,
/// metadata not specific to certain PGs.
class OSDMeta {
  template<typename T> using Ref = boost::intrusive_ptr<T>;

  crimson::os::BackendStore store;
  Ref<crimson::os::FuturizedCollection> coll;

public:
  OSDMeta(Ref<crimson::os::FuturizedCollection> coll,
          crimson::os::BackendStore store)
    : store{store}, coll{coll}
  {}

  auto collection() {
    return coll;
  }
  void create(ceph::os::Transaction& t);

  void store_map(ceph::os::Transaction& t,
                 epoch_t e, const bufferlist& m);
  void store_inc_map(ceph::os::Transaction& t,
                 epoch_t e, const bufferlist& m);
  void remove_map(ceph::os::Transaction& t, epoch_t e);
  void remove_inc_map(ceph::os::Transaction& t, epoch_t e);

  seastar::future<bufferlist> load_map(epoch_t e);
  read_errorator::future<ceph::bufferlist> load_inc_map(epoch_t e);

  void store_superblock(ceph::os::Transaction& t,
                        const OSDSuperblock& sb);

  using load_superblock_ertr = crimson::os::FuturizedStore::Shard::read_errorator;
  using load_superblock_ret = load_superblock_ertr::future<OSDSuperblock>;
  load_superblock_ret load_superblock();

  using ec_profile_t = std::map<std::string, std::string>;
  seastar::future<std::tuple<pg_pool_t,
			     std::string,
			     ec_profile_t>> load_final_pool_info(int64_t pool);
  void store_final_pool_info(
    ceph::os::Transaction&,
    LocalOSDMapRef lastmap,
    std::map<epoch_t, LocalOSDMapRef>&);

  // --- the purged_snaps object (see osd/purged_snaps.h)

  /// the object holding the purged snaps of all pools. Same as Classic's.
  static ghobject_t purged_snaps_oid();

  void touch_purged_snaps(ceph::os::Transaction& t);
  void update_purged_snaps(
    ceph::os::Transaction& t,
    const ceph::osd::purged_snaps::row_update_t& upd);

  /// all the purged_snaps rows, in key order. None if there is no object.
  seastar::future<std::vector<ceph::osd::purged_snaps::interval_t>>
  load_purged_snaps();

  /**
   * the rows of iv.pool that overlap or touch 'iv'
   *
   * Relies on the rows of the pool being disjoint and non-adjacent (as
   * written by Crimson), i.e. on their key order (by 'end') being their
   * 'begin' order as well.
   */
  seastar::future<std::vector<ceph::osd::purged_snaps::interval_t>>
  load_purged_snaps_touching(const ceph::osd::purged_snaps::interval_t& iv);

private:
  /// the rows starting at the key 'from', as long as their keys
  /// have 'prefix' and they are not 'is_past'
  seastar::future<std::vector<ceph::osd::purged_snaps::interval_t>>
  load_purged_rows(
    std::string from,
    std::string prefix,
    std::function<bool(const ceph::osd::purged_snaps::interval_t&)> is_past);

  static ghobject_t osdmap_oid(epoch_t epoch);
  static ghobject_t inc_osdmap_oid(epoch_t epoch);
  static ghobject_t final_pool_info_oid(int64_t pool);
  static ghobject_t superblock_oid();
};
