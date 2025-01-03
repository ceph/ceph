// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>

#include "common/ceph_context.h"
#include "os/ObjectStore.h"
#include "osd/osd_types.h"

#include "crimson/common/gated.h"
#include "crimson/os/alienstore/thread_pool.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
using coll_core_t = FuturizedStore::coll_core_t;
class AlienStore final : public FuturizedStore,
                         public FuturizedStore::Shard {
public:
  AlienStore(const std::string& type,
             const std::string& path,
             const ConfigValues& values);
  ~AlienStore() final;

  seastar::future<> start() final;
  seastar::future<> stop() final;
  mount_ertr::future<> mount() final;
  seastar::future<> umount() final;

  base_errorator::future<bool> exists(
    CollectionRef c,
    const ghobject_t& oid) final;
  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;
  read_errorator::future<ceph::bufferlist> read(CollectionRef c,
                                   const ghobject_t& oid,
                                   uint64_t offset,
                                   size_t len,
                                   uint32_t op_flags = 0) final;
  read_errorator::future<ceph::bufferlist> readv(CollectionRef c,
						 const ghobject_t& oid,
						 interval_set<uint64_t>& m,
						 uint32_t op_flags = 0) final;
					      

  get_attr_errorator::future<ceph::bufferlist> get_attr(CollectionRef c,
                                            const ghobject_t& oid,
                                            std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(CollectionRef c,
                                     const ghobject_t& oid) final;

  read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  /// Retrieves paged set of values > start (if present)
  read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_core_t>> list_collections() final;
  seastar::future<> set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts) final;

  seastar::future<> do_transaction_no_callbacks(
    CollectionRef c,
    ceph::os::Transaction&& txn) final;

  // error injection
  seastar::future<> inject_data_error(const ghobject_t& o) final;
  seastar::future<> inject_mdata_error(const ghobject_t& o) final;

  seastar::future<> write_meta(const std::string& key,
                  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) final;
  uuid_d get_fsid() const final;
  seastar::future<store_statfs_t> stat() const final;
  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final;
  unsigned get_max_attr_name_length() const final;
  seastar::future<struct stat> stat(
    CollectionRef,
    const ghobject_t&) final;
  seastar::future<std::string> get_default_device_class() final;
  get_attr_errorator::future<ceph::bufferlist> omap_get_header(
    CollectionRef,
    const ghobject_t&) final;
  read_errorator::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef,
    const ghobject_t&,
    uint64_t off,
    uint64_t len) final;

  FuturizedStore::Shard& get_sharded_store() final {
    return *this;
  }

private:

  template <class... Args>
  auto do_with_op_gate(Args&&... args) const {
    return op_gates.simple_dispatch("AlienStore::do_with_op_gate",
      // perfect forwarding in lambda's closure isn't available in C++17
      // using tuple as workaround; see: https://stackoverflow.com/a/49902823
      [args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
      return std::apply([] (auto&&... args) {
        return seastar::do_with(std::forward<decltype(args)>(args)...);
      }, std::move(args));
    });
  }

  mutable std::unique_ptr<crimson::os::ThreadPool> tp;
  const std::string type;
  const std::string path;
  const ConfigValues values;
  uint64_t used_bytes = 0;
  std::unique_ptr<ObjectStore> store;
  std::unique_ptr<CephContext> cct;
  mutable crimson::common::gate_per_shard op_gates;

  /**
   * coll_map
   *
   * Contains a reference to every CollectionRef returned to the upper layer.
   * It's important that ObjectStore::CollectionHandle instances (in particular,
   * those from BlueStore) not be released from seastar reactor threads.
   * Keeping a reference here and breaking the
   * CollectionRef->ObjectStore::CollectionHandle links in AlienStore::stop()
   * ensures that all CollectionHandle's are released in the alien thread pool.
   *
   * Long term, we probably want to drop this map.  To do that two things need
   * to happen:
   * 1. ~AlienCollection() needs to submit the ObjectStore::CollectionHandle
   *    instance to the alien thread pool to be released.
   * 2. OSD shutdown needs to *guarantee* that all outstanding CollectionRefs
   *    are released before unmounting and stopping the store.
   *
   * coll_map is accessed exclusively from alien threadpool threads under the
   * coll_map_lock.
   */
  std::mutex coll_map_lock;
  std::unordered_map<coll_t, CollectionRef> coll_map;
  CollectionRef get_alien_coll_ref(ObjectStore::CollectionHandle c);
};
}
