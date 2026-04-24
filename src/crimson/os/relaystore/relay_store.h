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
    using base_t = FuturizedStore::Shard;
    FuturizedStore::Shard& shard;

    template<auto MemberFunc, typename... Args>
    decltype(auto) with_store(this auto&& self, Args&&... args)
    {
      return (std::forward_like<decltype(self)>(self.shard).*MemberFunc)(std::forward<Args>(args)...);
    }

  public:
    Shard(FuturizedStore::Shard& shard)
      : shard(shard)
    {}
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::stat>(c, oid, op_flags);
    }

    base_errorator::future<bool> exists(
      CollectionRef ch,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::exists>(ch, oid, op_flags);
    }

    read_errorator::future<ceph::bufferlist> read(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t offset,
      size_t len,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::read>(c, oid, offset, len, op_flags);
    }


    read_errorator::future<ceph::bufferlist> readv(
      CollectionRef c,
      const ghobject_t& oid,
      interval_set<uint64_t>& m,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::readv>(c, oid, m, op_flags);
    }

    get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name,
      uint32_t op_flags = 0) const final
    {
      return with_store<&base_t::get_attr>(c, oid, name, op_flags);
    }

    get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::get_attrs>(c, oid, op_flags);
    }

    read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::omap_get_values>(c, oid, keys, op_flags);
    }

    read_errorator::future<ObjectStore::omap_iter_ret_t> omap_iterate(
      CollectionRef c,
      const ghobject_t &oid,
      ObjectStore::omap_iter_seek_t start_from,
      omap_iterate_cb_t callback,
      uint32_t op_flags = 0,
      omap_iterate_conf_t on_conflict = nullptr) final
    {
      return with_store<&base_t::omap_iterate>(c, oid, start_from, callback, op_flags,  on_conflict);
    }

    get_attr_errorator::future<ceph::bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final
    {
      return with_store<&base_t::omap_get_header>(c, oid, op_flags);
    }

    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
    list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const final
    {
      return with_store<&base_t::list_objects>(c, start, end, limit, op_flags);
    }

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final
    {
      return with_store<&base_t::create_new_collection>(cid);
    }

    seastar::future<CollectionRef> open_collection(const coll_t& cid) final
    {
      return with_store<&base_t::open_collection>(cid);
    }

    seastar::future<> set_collection_opts(
      CollectionRef c,
      const pool_opts_t& opts) final
    {
      return with_store<&base_t::set_collection_opts>(c, opts);
    }

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) final
    {
      return with_store<&base_t::do_transaction_no_callbacks>(ch, std::move(txn));
    }

    read_errorator::future<std::map<uint64_t, uint64_t>>
    fiemap(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags) final
    {
      return with_store<&base_t::fiemap>(c, oid, off, len, op_flags);
    }

    unsigned get_max_attr_name_length() const final
    {
      return with_store<&base_t::get_max_attr_name_length>();
    }

  public:
    // only exposed to RelayStore
    mount_ertr::future<> mount();

    seastar::future<> umount();

    seastar::future<> mkfs();

    mkfs_ertr::future<> mkcoll(uuid_d new_osd_fsid);

    using coll_core_t = FuturizedStore::coll_core_t;
    seastar::future<std::vector<coll_core_t>> list_collections();
  };
  friend Shard;

  _RelayStore(FuturizedStore& decorated_store)
    : decorated_store(decorated_store)
  {}
  ~_RelayStore() final;

  seastar::future<uint32_t> start() final
  {
    return decorated_store.start();
  }

  seastar::future<> stop() final
  {
    return decorated_store.stop();
  }

  mount_ertr::future<> mount() final
  {
    return decorated_store.mount();
  }

  seastar::future<> umount() final
  {
    return decorated_store.umount();
  }

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final
  {
    return decorated_store.mkfs(new_osd_fsid);
  }

  seastar::future<store_statfs_t> stat() const final
  {
    return decorated_store.stat();
  }

  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final
  {
    return decorated_store.pool_statfs(pool_id);
  }

  uuid_d get_fsid() const final
  {
    return decorated_store.get_fsid();
  }

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final
  {
    return decorated_store.write_meta(key, value);
  }

  FuturizedStore::Shard& get_sharded_store(store_index_t store_index = 0) final
  {
    seastar::sharded<std::vector<std::unique_ptr<Shard>>> decorated_shards;
    auto& local_shards = decorated_shards.local();
    if (store_index >= local_shards.size()) {
      local_shards.resize(store_index + 1);
    }
    if (!local_shards[store_index]) {
      auto& shard_to_decorate = decorated_store.get_sharded_store(store_index);
      local_shards[store_index] = std::make_unique<Shard>(shard_to_decorate);
    }
    return *local_shards[store_index];
  }

  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) final
  {
    return decorated_store.read_meta(key);
  }

  seastar::future<std::vector<coll_core_t>> list_collections() final
  {
    return decorated_store.list_collections();
  }

  seastar::future<std::string> get_default_device_class() final
  {
    return decorated_store.get_default_device_class();
  }

  seastar::future<uint32_t> get_storage_shard_count() final
  {
    return decorated_store.get_storage_shard_count();
  }
};

} // namespace crimson::os
