// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>

#include "common/ceph_context.h"
#include "os/ObjectStore.h"
#include "osd/osd_types.h"

#include "crimson/os/alienstore/thread_pool.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class AlienStore final : public FuturizedStore {
public:
  class AlienOmapIterator final : public OmapIterator {
  public:
    AlienOmapIterator(ObjectMap::ObjectMapIterator& it,
	AlienStore* store) : iter(it), store(store) {}
    seastar::future<int> seek_to_first();
    seastar::future<int> upper_bound(const std::string& after);
    seastar::future<int> lower_bound(const std::string& to);
    bool valid() const;
    seastar::future<int> next();
    std::string key();
    seastar::future<std::string> tail_key();
    ceph::buffer::list value();
    int status() const;
  private:
    ObjectMap::ObjectMapIterator iter;
    AlienStore* store;
  };
  AlienStore(const std::string& path, const ConfigValues& values);
  ~AlienStore() final;

  seastar::future<> start() final;
  seastar::future<> stop() final;
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  read_errorator::future<ceph::bufferlist> read(CollectionRef c,
                                   const ghobject_t& oid,
                                   uint64_t offset,
                                   size_t len,
                                   uint32_t op_flags = 0) final;
  read_errorator::future<ceph::bufferlist> readv(CollectionRef c,
						 const ghobject_t& oid,
						 interval_set<uint64_t>& m,
						 uint32_t op_flags = 0) final;
					      

  get_attr_errorator::future<ceph::bufferptr> get_attr(CollectionRef c,
                                            const ghobject_t& oid,
                                            std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(CollectionRef c,
                                     const ghobject_t& oid) final;

  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  /// Retrieves paged set of values > start (if present)
  seastar::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(CollectionRef c,
                                   ceph::os::Transaction&& txn) final;

  seastar::future<> write_meta(const std::string& key,
                  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) final;
  uuid_d get_fsid() const final;
  seastar::future<store_statfs_t> stat() const final;
  unsigned get_max_attr_name_length() const final;
  seastar::future<struct stat> stat(
    CollectionRef,
    const ghobject_t&) final;
  seastar::future<ceph::bufferlist> omap_get_header(
    CollectionRef,
    const ghobject_t&) final;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef,
    const ghobject_t&,
    uint64_t off,
    uint64_t len) final;
  seastar::future<FuturizedStore::OmapIteratorRef> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& oid) final;

  static void configure_thread_memory();
private:
  constexpr static unsigned MAX_KEYS_PER_OMAP_GET_CALL = 32;
  mutable std::unique_ptr<crimson::os::ThreadPool> tp;
  const std::string path;
  uint64_t used_bytes = 0;
  std::unique_ptr<ObjectStore> store;
  std::unique_ptr<CephContext> cct;
  seastar::gate transaction_gate;
  std::unordered_map<coll_t, CollectionRef> coll_map;
  seastar::shared_mutex tp_mutex;
};
}
