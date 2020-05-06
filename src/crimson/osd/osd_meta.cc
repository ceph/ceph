// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_meta.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "os/Transaction.h"

using read_errorator = crimson::os::FuturizedStore::read_errorator;

void OSDMeta::create(ceph::os::Transaction& t)
{
  t.create_collection(coll->get_cid(), 0);
}

void OSDMeta::store_map(ceph::os::Transaction& t,
                        epoch_t e, const bufferlist& m)
{
  t.write(coll->get_cid(), osdmap_oid(e), 0, m.length(), m);
}

seastar::future<bufferlist> OSDMeta::load_map(epoch_t e)
{
  return store->read(coll,
                     osdmap_oid(e), 0, 0,
                     CEPH_OSD_OP_FLAG_FADVISE_WILLNEED).handle_error(
    read_errorator::all_same_way([e] {
      throw std::runtime_error(fmt::format("read gave enoent on {}",
                                           osdmap_oid(e)));
    }));
}

void OSDMeta::store_superblock(ceph::os::Transaction& t,
                               const OSDSuperblock& superblock)
{
  bufferlist bl;
  encode(superblock, bl);
  t.write(coll->get_cid(), superblock_oid(), 0, bl.length(), bl);
}

seastar::future<OSDSuperblock> OSDMeta::load_superblock()
{
  return store->read(coll, superblock_oid(), 0, 0).safe_then(
    [this] (bufferlist&& bl) {
      auto p = bl.cbegin();
      OSDSuperblock superblock;
      decode(superblock, p);
      return seastar::make_ready_future<OSDSuperblock>(std::move(superblock));
    }, read_errorator::all_same_way([] {
      throw std::runtime_error(fmt::format("read gave enoent on {}",
                                           superblock_oid()));
    }));
}

seastar::future<std::tuple<pg_pool_t,
			   std::string,
			   OSDMeta::ec_profile_t>>
OSDMeta::load_final_pool_info(int64_t pool) {
  return store->read(coll, final_pool_info_oid(pool),
                     0, 0).safe_then([this] (bufferlist&& bl) {
    auto p = bl.cbegin();
    pg_pool_t pi;
    string name;
    ec_profile_t ec_profile;
    decode(pi, p);
    decode(name, p);
    decode(ec_profile, p);
    return seastar::make_ready_future<std::tuple<pg_pool_t,
						 string,
						 ec_profile_t>>(
      std::make_tuple(std::move(pi),
		      std::move(name),
		      std::move(ec_profile)));
  },read_errorator::all_same_way([pool] {
    throw std::runtime_error(fmt::format("read gave enoent on {}",
                                         final_pool_info_oid(pool)));
  }));
}

ghobject_t OSDMeta::osdmap_oid(epoch_t epoch)
{
  string name = fmt::format("osdmap.{}", epoch);
  return ghobject_t(hobject_t(sobject_t(object_t(name), 0)));
}

ghobject_t OSDMeta::final_pool_info_oid(int64_t pool)
{
  string name = fmt::format("final_pool_{}", pool);
  return ghobject_t(hobject_t(sobject_t(object_t(name), CEPH_NOSNAP)));
}

ghobject_t OSDMeta::superblock_oid()
{
  return ghobject_t(hobject_t(sobject_t(object_t("osd_superblock"), 0)));
}
