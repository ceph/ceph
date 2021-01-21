// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "poseidonstore.h"

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"

#include "crimson/os/futurized_collection.h"
#include "poseidonstore.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using crimson::common::local_conf;

namespace crimson::os::poseidonstore {

PoseidonStore::PoseidonStore(const std::string& path) :
  device_manager(create_memory(device_manager::DEFAULT_TEST_MEMORY)),
  wal(*device_manager), /// TODO: PoseidonStore has multiple wals
  cache(*device_manager)
{
  /* TODO */
}

PoseidonStore::~PoseidonStore() = default;

seastar::future<> PoseidonStore::stop()
{
  return seastar::now();
}

seastar::future<> PoseidonStore::mount()
{
  return seastar::now();
}

seastar::future<> PoseidonStore::umount()
{
  return seastar::now();
}

seastar::future<> PoseidonStore::mkfs(uuid_d new_osd_fsid)
{
  return seastar::now();
}

seastar::future<store_statfs_t> PoseidonStore::stat() const
{
  /* TODO */
  logger().debug("{}", __func__);
  store_statfs_t st;
  return seastar::make_ready_future<store_statfs_t>(st);
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
PoseidonStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  /* TODO */
  return seastar::make_ready_future<std::tuple<std::vector<ghobject_t>, ghobject_t>>(
    std::make_tuple(std::vector<ghobject_t>(), end));
}

seastar::future<CollectionRef> PoseidonStore::create_new_collection(const coll_t& cid)
{
  /* TODO */
  return seastar::make_ready_future<CollectionRef>();
}

seastar::future<CollectionRef> PoseidonStore::open_collection(const coll_t& cid)
{
  /* TODO */
  return seastar::make_ready_future<CollectionRef>();
}

seastar::future<std::vector<coll_t>> PoseidonStore::list_collections()
{
  /* TODO */
  return seastar::make_ready_future<std::vector<coll_t>>();
}

PoseidonStore::read_errorator::future<ceph::bufferlist> PoseidonStore::read(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  uint32_t op_flags)
{
  /* TODO */
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

PoseidonStore::read_errorator::future<ceph::bufferlist> PoseidonStore::readv(
  CollectionRef ch,
  const ghobject_t& oid,
  interval_set<uint64_t>& m,
  uint32_t op_flags)
{
  /* TODO */
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

PoseidonStore::get_attr_errorator::future<ceph::bufferptr> PoseidonStore::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name) const
{
  /* TODO */
  return crimson::ct_error::enoent::make();
}

PoseidonStore::get_attrs_ertr::future<PoseidonStore::attrs_t> PoseidonStore::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid)
{
  /* TODO */
  return crimson::ct_error::enoent::make();
}

seastar::future<struct stat> stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  /* TODO */
  return seastar::make_ready_future<struct stat>();
}


seastar::future<struct stat> PoseidonStore::stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  struct stat st;
  return seastar::make_ready_future<struct stat>(st);
}

seastar::future<ceph::bufferlist> omap_get_header(
  CollectionRef c,
  const ghobject_t& oid)
{
  /* TODO */
  return seastar::make_ready_future<bufferlist>();
}

auto
PoseidonStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t& oid,
  const omap_keys_t& keys)
  -> read_errorator::future<omap_values_t>
{
  /* TODO */
  return seastar::make_ready_future<omap_values_t>();
}

auto
PoseidonStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start)
  -> read_errorator::future<std::tuple<bool, PoseidonStore::omap_values_t>>
{
  /* TODO */
  return seastar::make_ready_future<std::tuple<bool, omap_values_t>>(
    std::make_tuple(false, omap_values_t()));
}

seastar::future<FuturizedStore::OmapIteratorRef> get_omap_iterator(
  CollectionRef ch,
  const ghobject_t& oid)
{
  /* TODO */
  return seastar::make_ready_future<FuturizedStore::OmapIteratorRef>();
}

seastar::future<std::map<uint64_t, uint64_t>> fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  /* TODO */
  return seastar::make_ready_future<std::map<uint64_t, uint64_t>>();
}

seastar::future<> PoseidonStore::do_transaction(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  /* TODO */
  // 1. get onode info
  // 2. make transaction
  // 3. add the transaction to the list
  // 4. write the data if large
  // 5. do_flush_works.then( remove the committed item in the list )
  // 5. submit_transaction.then( complete commit )

  auto t = make_transaction();
  auto record = cache.try_construct_record(*t);
  if (!record) {
    return seastar::make_ready_future<>();
  }
  // TODO: write first if the data is large
  wal.do_defered_works();
  return wal.submit_entry(*record
      ).safe_then([this](auto p) {
	return seastar::make_ready_future<>();
      }).handle_error(crimson::ct_error::assert_all{});
}

seastar::future<> PoseidonStore::write_meta(const std::string& key,
					const std::string& value)
{
  /* TODO */
  return seastar::make_ready_future<>();
}

seastar::future<std::tuple<int, std::string>> PoseidonStore::read_meta(const std::string& key)
{
  /* TODO */
  return seastar::make_ready_future<std::tuple<int, std::string>>(
    std::make_tuple(0, ""s));
}

uuid_d PoseidonStore::get_fsid() const
{
  return osd_fsid;
}


}
