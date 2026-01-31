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
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

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
      unsigned int store_shard_nums,
      unsigned int store_index = 0);
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

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
      uint32_t op_flags = 0
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
    unsigned int store_index;
    bool store_active = true;
  };

  CyanStore(const std::string& path);
  ~CyanStore() final;

  seastar::future<unsigned int> start() final;

  seastar::future<> stop() final;

  mount_ertr::future<> mount() final;

  seastar::future<> umount() final;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;

  seastar::future<store_statfs_t> stat() const final;

  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final;

  uuid_d get_fsid() const final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;

  FuturizedStore::StoreShardRef get_sharded_store(unsigned int store_index = 0) final {
    assert(!shard_stores.local().mshard_stores.empty());
    assert(store_index < shard_stores.local().mshard_stores.size());
    assert(shard_stores.local().mshard_stores[store_index]->get_status() == true);
    return make_local_shared_foreign(
      seastar::make_foreign(seastar::static_pointer_cast<FuturizedStore::Shard>(
        shard_stores.local().mshard_stores[store_index])));
  }
  std::vector<FuturizedStore::StoreShardRef> get_sharded_stores() final{
    std::vector<FuturizedStore::StoreShardRef> ret;
    ret.reserve(shard_stores.local().mshard_stores.size());
    for (auto& mshard_store : shard_stores.local().mshard_stores) {
      if (mshard_store->get_status() == true) {
        ret.emplace_back(make_local_shared_foreign(
          seastar::make_foreign(seastar::static_pointer_cast<FuturizedStore::Shard>(mshard_store))));
      }
    }
    return ret;
  }

  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) final;

  seastar::future<std::vector<coll_core_t>> list_collections() final;

  seastar::future<std::string> get_default_device_class() final;

  seastar::future<> get_shard_nums();


private:
class MultiShardStores {
  public:
    std::vector<seastar::shared_ptr<CyanStore::Shard>> mshard_stores;

  public:
    MultiShardStores(size_t count,
                     const std::string path,
                     unsigned int store_shard_nums)
    : mshard_stores() {
      mshard_stores.reserve(count); // Reserve space for the shards
      for (size_t store_index = 0; store_index < count; ++store_index) {
        mshard_stores.emplace_back(seastar::make_shared<CyanStore::Shard>(
          path, store_shard_nums, store_index));
      }
    }
    ~MultiShardStores() {
      mshard_stores.clear();
    }
  };
  seastar::sharded<CyanStore::MultiShardStores> shard_stores;
  unsigned int store_shard_nums = 0;
  const std::string path;
  uuid_d osd_fsid;
};
}
