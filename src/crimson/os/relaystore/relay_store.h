// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "crimson/os/futurized_store.h"

#define TRAMPOLINE(method_name) \
  template <typename... Args> \
  decltype(auto) method_name(this auto&& self, Args&&... args) { \
    return std::forward_like<decltype(self)>(self.wrapped).method_name(std::forward<Args>(args)...); \
  }


namespace crimson::os {

class _RelayStore final : public FuturizedStore {
  FuturizedStore& decorated_store;

public:
  class Shard : public FuturizedStore::Shard {
    _RelayStore& store;

    template<auto MemberFunc, typename... Args>
    decltype(auto) RelayStore::with_store(Args&&... args)
    {
      return (store.*MemberFunc)(std::forward<Args>(args)...);
    }

  public:
    Shard(_RelayStore& store)
      : store(store)
    {}
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final
    {
      return with_store<&crimson::os::FuturizedStore::Shard::stat>(c, oid, op_flags);
    }

    base_errorator::future<bool> exists(
      CollectionRef ch,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

    read_errorator::future<ceph::bufferlist> read(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t offset,
      size_t len,
      uint32_t op_flags = 0) final;

    read_errorator::future<ceph::bufferlist> readv(
      CollectionRef c,
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

    read_errorator::future<ObjectStore::omap_iter_ret_t> omap_iterate(
      CollectionRef c,
      const ghobject_t &oid,
      ObjectStore::omap_iter_seek_t start_from,
      omap_iterate_cb_t callback,
      uint32_t op_flags = 0,
      omap_iterate_conf_t on_conflict = nullptr
    ) final;

    get_attr_errorator::future<ceph::bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
    list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const final;

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;

    seastar::future<CollectionRef> open_collection(const coll_t& cid) final;

    seastar::future<> set_collection_opts(
      CollectionRef c,
      const pool_opts_t& opts) final;

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) final;

    read_errorator::future<std::map<uint64_t, uint64_t>>
    fiemap(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags) final;

    unsigned get_max_attr_name_length() const final;

  public:
    // only exposed to RelayStore
    mount_ertr::future<> mount();

    seastar::future<> umount();

    seastar::future<> mkfs();

    mkfs_ertr::future<> mkcoll(uuid_d new_osd_fsid);

    using coll_core_t = FuturizedStore::coll_core_t;
    seastar::future<std::vector<coll_core_t>> list_collections();

    uint64_t get_used_bytes() const {
      return used_bytes;
    }

    unsigned int get_store_index() const {
      return store_shard_desc.local_index;
    }
  };
  friend Shard;

  RelayStore(FuturizedStore& decorated_store)
    : decorated_store(decorated_store)
  {}
  ~RelayStore() final;

  seastar::future<uint32_t> start() final;

  seastar::future<> stop() final;

  mount_ertr::future<> mount() final;

  seastar::future<> umount() final;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;

  seastar::future<store_statfs_t> stat() const final;

  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final;

  uuid_d get_fsid() const final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;

  FuturizedStore::Shard& get_sharded_store(store_index_t store_index = 0) final {
    return shard_stores.local(store_index);
  }

  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) final;

  seastar::future<std::vector<coll_core_t>> list_collections() final;

  seastar::future<std::string> get_default_device_class() final;

  seastar::future<uint32_t> get_storage_shard_count() final;
};

} // namespace crimson::os
