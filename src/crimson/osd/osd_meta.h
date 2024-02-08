// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <string>
#include <seastar/core/future.hh>
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
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

  crimson::os::FuturizedStore::Shard& store;
  Ref<crimson::os::FuturizedCollection> coll;

public:
  OSDMeta(Ref<crimson::os::FuturizedCollection> coll,
          crimson::os::FuturizedStore::Shard& store)
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
private:
  static ghobject_t osdmap_oid(epoch_t epoch);
  static ghobject_t inc_osdmap_oid(epoch_t epoch);
  static ghobject_t final_pool_info_oid(int64_t pool);
  static ghobject_t superblock_oid();
};
