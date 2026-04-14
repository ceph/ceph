// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "osd/osd_types.h"
#include "include/uuid.h"

#include "crimson/os/cyanstore/cyan_object.h"
#include "crimson/os/cyanstore/cyan_collection.h"
#include "crimson/os/futurized_store.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class CyanStore final : public FuturizedStore {
public:
  class Shard : public FuturizedStore::Shard {
  public:
    Shard(std::string path,
      uint32_t store_shard_nums,
      store_index_t store_index);
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) override final;

    base_errorator::future<bool> exists(
      CollectionRef ch,
      const ghobject_t& oid,
      uint32_t op_flags = 0) override final;

    read_errorator::future<ceph::bufferlist> read(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t offset,
      size_t len,
      uint32_t op_flags = 0) override final;

    read_errorator::future<ceph::bufferlist> readv(
      CollectionRef c,
      const ghobject_t& oid,
      interval_set<uint64_t>& m,
      uint32_t op_flags = 0) override final;

    get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name,
      uint32_t op_flags = 0) const override final;

    get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) override final;

    read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys,
      uint32_t op_flags = 0) override final;

    read_errorator::future<ObjectStore::omap_iter_ret_t> omap_iterate(
      CollectionRef c,
      const ghobject_t &oid,
      ObjectStore::omap_iter_seek_t start_from,
      omap_iterate_cb_t callback,
      uint32_t op_flags = 0,
      omap_iterate_conf_t on_conflict = nullptr
    ) override final;

    get_attr_errorator::future<ceph::bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) override final;

    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
    list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const override final;

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) override final;

    seastar::future<CollectionRef> open_collection(const coll_t& cid) override final;

    seastar::future<> set_collection_opts(
      CollectionRef c,
      const pool_opts_t& opts) override final;

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) override final;

    read_errorator::future<std::map<uint64_t, uint64_t>>
    fiemap(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags) override final;

    unsigned get_max_attr_name_length() const override final;

  public:
    // only exposed to CyanStore
    mount_ertr::future<> mount();

    seastar::future<> umount();

    seastar::future<> mkfs();

    mkfs_ertr::future<> mkcoll(uuid_d new_osd_fsid);

    using coll_core_t = FuturizedStore::coll_core_t;
    seastar::future<std::vector<coll_core_t>> list_collections();

    uint64_t get_used_bytes() const {
      if (!store_active) {
        return 0;
      }
      return used_bytes;
    }

    unsigned int get_store_index() const {
      return store_index;
    }
    bool get_status() const {
      return store_active;
    }

  private:
    int _remove(const coll_t& cid, const ghobject_t& oid);
    int _touch(const coll_t& cid, const ghobject_t& oid);
    int _write(const coll_t& cid, const ghobject_t& oid,
	       uint64_t offset, size_t len, const ceph::bufferlist& bl,
	       uint32_t fadvise_flags);
    int _zero(const coll_t& cid, const ghobject_t& oid,
	      uint64_t offset, size_t len);
    int _omap_clear(
      const coll_t& cid,
      const ghobject_t& oid);
    int _omap_set_values(
      const coll_t& cid,
      const ghobject_t& oid,
      std::map<std::string, ceph::bufferlist> &&aset);
    int _omap_set_header(
      const coll_t& cid,
      const ghobject_t& oid,
      const ceph::bufferlist &header);
    int _omap_rmkeys(
      const coll_t& cid,
      const ghobject_t& oid,
      const omap_keys_t& aset);
    int _omap_rmkeyrange(
      const coll_t& cid,
      const ghobject_t& oid,
      const std::string &first,
      const std::string &last);
    int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
    int _clone(const coll_t& cid, const ghobject_t& oid,
	       const ghobject_t& noid);
    int _setattrs(const coll_t& cid, const ghobject_t& oid,
		  std::map<std::string,bufferlist>&& aset);
    int _rm_attr(const coll_t& cid, const ghobject_t& oid,
		 std::string_view name);
    int _rm_attrs(const coll_t& cid, const ghobject_t& oid);
    int _create_collection(const coll_t& cid, int bits);
    int _remove_collection(const coll_t& cid);
    boost::intrusive_ptr<Collection> _get_collection(const coll_t& cid);

  private:
    uint64_t used_bytes = 0;
    const std::string path;
    std::unordered_map<coll_t, boost::intrusive_ptr<Collection>> coll_map;
    std::map<coll_t, boost::intrusive_ptr<Collection>> new_coll_map;
    store_index_t store_index;
    bool store_active = true;
  };

  CyanStore(const std::string& path);
  ~CyanStore() override;

  seastar::future<uint32_t> start() override;

  seastar::future<> stop() override;

  mount_ertr::future<> mount() override;

  seastar::future<> umount() override;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) override;

  seastar::future<store_statfs_t> stat() const override;

  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const override;

  uuid_d get_fsid() const override;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) override;

  BackendStore get_backend_store(store_index_t store_index) override {
    assert(!shard_stores.local().mshard_stores.empty());
    if (store_index != NULL_STORE_INDEX) {
      assert(store_index < shard_stores.local().mshard_stores.size());
    }
    auto this_id = seastar::this_shard_id();
    if (this_id < store_shard_nums) {
      return BackendStore(*this, this_id, store_index);
    } else {
      auto shard_id = this_id % store_shard_nums;
      return BackendStore(*this, shard_id, store_index);
    }
  }

  FuturizedStore::Shard& get_sharded_store(store_index_t store_index = 0) override
  {
    assert(store_index < shard_stores.local().mshard_stores.size());
    auto &shard_store = *(shard_stores.local().mshard_stores[store_index]);
    assert(shard_store.get_status() == true);
    return shard_store;
  }

  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) override;

  seastar::future<std::vector<coll_core_t>> list_collections() override;

  seastar::future<std::string> get_default_device_class() override;

  seastar::future<> get_shard_nums();


private:
class MultiShardStores {
  public:
    std::vector<std::unique_ptr<CyanStore::Shard>> mshard_stores;

  public:
    MultiShardStores(size_t count,
                     const std::string path,
                     uint32_t store_shard_nums)
    : mshard_stores() {
      mshard_stores.reserve(count); // Reserve space for the shards
      for (size_t store_index = 0; store_index < count; ++store_index) {
        mshard_stores.emplace_back(std::make_unique<CyanStore::Shard>(
          path, store_shard_nums, store_index));
      }
    }
    ~MultiShardStores() {
      mshard_stores.clear();
    }
  };
  seastar::sharded<CyanStore::MultiShardStores> shard_stores;
  uint32_t store_shard_nums = 0;
  const std::string path;
  uuid_d osd_fsid;
};
}
