// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>

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
class AlienStore final : public FuturizedStore {
  class Shard : public FuturizedStore::Shard {
  public:
    Shard(crimson::os::ThreadPool* _tp,
          ObjectStore* _store,
          crimson::common::gate_per_shard* _op_gates,
          std::unordered_map<coll_t, CollectionRef>* _coll_map,
          std::mutex& _coll_map_lock)
    : tp(_tp),
      store(_store),
      op_gates(_op_gates),
      coll_map(_coll_map),
      coll_map_lock(_coll_map_lock) {}

    ~Shard() = default;

    base_errorator::future<bool> exists(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

    read_errorator::future<ceph::bufferlist> read(CollectionRef c,
      const ghobject_t& oid,
      uint64_t offset,
      size_t len,
      uint32_t op_flags = 0) final;
    read_errorator::future<ceph::bufferlist> readv(CollectionRef c,
      const ghobject_t& oid,
      interval_set<uint64_t>& m,
      uint32_t op_flags = 0) final;

    get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name,
      uint32_t op_flags = 0) const final;
    get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

    read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys,
      uint32_t op_flags = 0) final;
    get_attr_errorator::future<ceph::bufferlist> omap_get_header(
      CollectionRef,
      const ghobject_t&,
      uint32_t) final;
    read_errorator::future<std::map<uint64_t, uint64_t>> fiemap(
      CollectionRef,
      const ghobject_t&,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags) final;

    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const final;

    read_errorator::future<ObjectStore::omap_iter_ret_t> omap_iterate(
      CollectionRef c,
      const ghobject_t &oid,
      ObjectStore::omap_iter_seek_t start_from,
      omap_iterate_cb_t callback,
      uint32_t op_flags = 0) final;

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
    seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
    seastar::future<std::vector<coll_core_t>> shard_list_collections();
    seastar::future<> set_collection_opts(CollectionRef c,
                                          const pool_opts_t& opts) final;
    seastar::future<store_statfs_t> shard_stat();
    seastar::future<store_statfs_t> shard_pool_statfs(int64_t pool_id);
    seastar::future<struct stat> stat(
      CollectionRef,
      const ghobject_t&,
      uint32_t op_flags = 0) final;

  seastar::future<> do_transaction_no_callbacks(
    CollectionRef c,
    ceph::os::Transaction&& txn) final;

  // error injection
  seastar::future<> inject_data_error(const ghobject_t& o) final;
  seastar::future<> inject_mdata_error(const ghobject_t& o) final;
  unsigned get_max_attr_name_length() const final;

  private:
  CollectionRef get_alien_coll_ref(ObjectStore::CollectionHandle c);

    template <class... Args>
    auto do_with_op_gate(Args&&... args) const {
      return (*op_gates).simple_dispatch("AlienStore::do_with_op_gate",
        // perfect forwarding in lambda's closure isn't available in C++17
        // using tuple as workaround; see: https://stackoverflow.com/a/49902823
        [args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
        return std::apply([] (auto&&... args) {
          return seastar::do_with(std::forward<decltype(args)>(args)...);
        }, std::move(args));
      });
    }
    crimson::os::ThreadPool* tp = nullptr; //for each shard
    ObjectStore* store = nullptr;  //for each shard
    crimson::common::gate_per_shard* op_gates = nullptr;  //for per shard
    std::unordered_map<coll_t, CollectionRef>* coll_map = nullptr;  // for per shard
    std::mutex& coll_map_lock;
  };
public:
  AlienStore(const std::string& type,
             const std::string& path,
             const ConfigValues& values);
  ~AlienStore() final;

  seastar::future<unsigned int> start() final;
  seastar::future<> stop() final;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;
  mount_ertr::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> write_meta(const std::string& key,
                  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) final;
  uuid_d get_fsid() const final;

  seastar::future<store_statfs_t> stat() const final;
  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final;

  seastar::future<std::string> get_default_device_class() final;

  seastar::future<std::vector<coll_core_t>> list_collections() final;

  FuturizedStore::StoreShardRef get_sharded_store(unsigned int store_index = 0) final {
    return make_local_shared_foreign(
      seastar::make_foreign(seastar::static_pointer_cast<FuturizedStore::Shard>(
        shard_stores.local().mshard_stores)));
  }

  std::vector<FuturizedStore::StoreShardRef> get_sharded_stores() final {
    std::vector<FuturizedStore::StoreShardRef> ret;
    ret.emplace_back(make_local_shared_foreign(
      seastar::make_foreign(seastar::static_pointer_cast<FuturizedStore::Shard>(
        shard_stores.local().mshard_stores))));
    return ret;
  }

private:
  mutable std::unique_ptr<crimson::os::ThreadPool> main_tp;
  const std::string type;
  const std::string path;
  const ConfigValues values;
  uint64_t used_bytes = 0;
  std::unique_ptr<ObjectStore> main_store;
  std::unique_ptr<CephContext> cct;
  mutable crimson::common::gate_per_shard main_op_gates;

  class MultiShardStores {
    public:
      seastar::shared_ptr<AlienStore::Shard> mshard_stores;

    public:
      MultiShardStores(crimson::os::ThreadPool* _tp,
                       ObjectStore* _store,
                       crimson::common::gate_per_shard* _op_gates,
                       std::unordered_map<coll_t, CollectionRef>* _coll_map,
                       std::mutex& _coll_map_lock)
      {
        mshard_stores = seastar::make_shared<AlienStore::Shard>(
            _tp, _store, _op_gates, _coll_map, _coll_map_lock);
      }

      ~MultiShardStores() {}
    };

  seastar::sharded<AlienStore::MultiShardStores> shard_stores;

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
  std::mutex main_coll_map_lock;
  std::unordered_map<coll_t, CollectionRef> main_coll_map;

};

}
