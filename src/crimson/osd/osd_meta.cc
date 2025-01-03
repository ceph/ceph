// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_meta.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "os/Transaction.h"
#include "osd/OSDMap.h"

using std::string;

void OSDMeta::create(ceph::os::Transaction& t)
{
  t.create_collection(coll->get_cid(), 0);
}

void OSDMeta::store_map(ceph::os::Transaction& t,
                        epoch_t e, const bufferlist& m)
{
  t.write(coll->get_cid(), osdmap_oid(e), 0, m.length(), m);
}

void OSDMeta::store_inc_map(ceph::os::Transaction& t,
                        epoch_t e, const bufferlist& m)
{
  t.write(coll->get_cid(), inc_osdmap_oid(e), 0, m.length(), m);
}

void OSDMeta::remove_map(ceph::os::Transaction& t, epoch_t e)
{
  t.remove(coll->get_cid(), osdmap_oid(e));
}

void OSDMeta::remove_inc_map(ceph::os::Transaction& t, epoch_t e)
{
  t.remove(coll->get_cid(), inc_osdmap_oid(e));
}

seastar::future<bufferlist> OSDMeta::load_map(epoch_t e)
{
  return store.read(coll,
                    osdmap_oid(e), 0, 0,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED).handle_error(
    read_errorator::assert_all_func([e](const auto&) {
      ceph_abort_msg(fmt::format("{} read gave enoent on {}",
                                 __func__, osdmap_oid(e)));
    }));
}

read_errorator::future<ceph::bufferlist> OSDMeta::load_inc_map(epoch_t e)
{
  return store.read(coll,
                    inc_osdmap_oid(e), 0, 0,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
}

void OSDMeta::store_superblock(ceph::os::Transaction& t,
                               const OSDSuperblock& superblock)
{
  bufferlist bl;
  encode(superblock, bl);
  t.write(coll->get_cid(), superblock_oid(), 0, bl.length(), bl);
}

OSDMeta::load_superblock_ret OSDMeta::load_superblock()
{
  return store.read(
    coll, superblock_oid(), 0, 0
  ).safe_then([] (bufferlist&& bl) {
    auto p = bl.cbegin();
    OSDSuperblock superblock;
    decode(superblock, p);
    return seastar::make_ready_future<OSDSuperblock>(std::move(superblock));
  });
}

seastar::future<std::tuple<pg_pool_t,
			   std::string,
			   OSDMeta::ec_profile_t>>
OSDMeta::load_final_pool_info(int64_t pool) {
  return store.read(coll, final_pool_info_oid(pool),
                     0, 0).safe_then([] (bufferlist&& bl) {
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
  },read_errorator::assert_all_func([pool](const auto&) {
    throw std::runtime_error(fmt::format("read gave enoent on {}",
                                         final_pool_info_oid(pool)));
  }));
}

void OSDMeta::store_final_pool_info(
  ceph::os::Transaction &t,
  LocalOSDMapRef previous,
  std::map<epoch_t, LocalOSDMapRef> &added_map)
{
  for (auto [e, map] : added_map) {
    if (!previous) {
      previous = map;
      continue;
    }
    for (auto &[pool_id, pool] : previous->get_pools()) {
      if (!map->have_pg_pool(pool_id)) {
	ghobject_t obj = final_pool_info_oid(pool_id);
	bufferlist bl;
	encode(pool, bl, CEPH_FEATURES_ALL);
	string name = previous->get_pool_name(pool_id);
	encode(name, bl);
	std::map<string, string> profile;
	if (pool.is_erasure()) {
	  profile = previous->get_erasure_code_profile(
	    pool.erasure_code_profile);
	}
	encode(profile, bl);
	t.write(coll->get_cid(), obj, 0, bl.length(), bl);
      }
    }
    previous = map;
  }
}

ghobject_t OSDMeta::osdmap_oid(epoch_t epoch)
{
  string name = fmt::format("osdmap.{}", epoch);
  return ghobject_t(hobject_t(sobject_t(object_t(name), 0)));
}

ghobject_t OSDMeta::inc_osdmap_oid(epoch_t epoch)
{
  string name = fmt::format("inc_osdmap.{}", epoch);
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
