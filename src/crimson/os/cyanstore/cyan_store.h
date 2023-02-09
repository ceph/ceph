// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
using coll_core_t = FuturizedStore::coll_core_t;
class CyanStore final : public FuturizedStore {
  class ShardStores {
  public:
    ShardStores(std::string path)
      :path(path){}

    mount_ertr::future<> mount();

    seastar::future<> umount();

    seastar::future<> mkfs();

    mkfs_ertr::future<> mkcoll(uuid_d new_osd_fsid);

    seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid);

    read_errorator::future<ceph::bufferlist> read(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t offset,
      size_t len,
      uint32_t op_flags = 0);

    read_errorator::future<ceph::bufferlist> readv(
      CollectionRef c,
      const ghobject_t& oid,
      interval_set<uint64_t>& m,
      uint32_t op_flags = 0);

    get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name) const;

    get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid);

    read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys);

    read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
      CollectionRef c,           ///< [in] collection
      const ghobject_t &oid,     ///< [in] oid
      const std::optional<std::string> &start ///< [in] start, empty for begin
      );

    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
    list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit) const;

    get_attr_errorator::future<ceph::bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid);

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid);

    seastar::future<CollectionRef> open_collection(const coll_t& cid);

    seastar::future<std::vector<coll_core_t>> list_collections();

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn);

    read_errorator::future<std::map<uint64_t, uint64_t>>
    fiemap(
      CollectionRef c,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len);

    uint64_t get_used_bytes() const { return used_bytes; }

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
    boost::intrusive_ptr<Collection> _get_collection(const coll_t& cid);

  private:
    uint64_t used_bytes = 0;
    const std::string path;
    std::unordered_map<coll_t, boost::intrusive_ptr<Collection>> coll_map;
    std::map<coll_t, boost::intrusive_ptr<Collection>> new_coll_map;
  };

// public interfaces called by main OSD
public:
  CyanStore(const std::string& path);
  ~CyanStore() final;

  seastar::future<> stop() final {
    return shard_stores.stop();
  }
  seastar::future<> start() final;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;

  seastar::future<store_statfs_t> stat() const final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;

  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) final;

  uuid_d get_fsid() const final;

  mount_ertr::future<> mount() final {
    return shard_stores.invoke_on_all(
      [](auto &local_store) {
      return local_store.mount().handle_error(
      crimson::stateful_ec::handle([](const auto& ec) {
        crimson::get_logger(ceph_subsys_cyanstore).error(
	    "error mounting cyanstore: ({}) {}",
            ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
    });
  }

  seastar::future<> umount() final {
    return shard_stores.invoke_on_all(
      [](auto &local_store) {
      return local_store.umount();
    });
  }

  seastar::future<std::vector<coll_core_t>> list_collections() final;

// public interfaces called by each shard osd
public:
  unsigned get_max_attr_name_length() const final;

  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) final {
    return shard_stores.local().stat(
      c, oid);
  }

  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final {
    return shard_stores.local().read(
      c, oid, offset, len, op_flags);
  }

  read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    uint32_t op_flags = 0) final {
    return shard_stores.local().readv(
      c, oid, m, op_flags);
  }

  get_attr_errorator::future<ceph::bufferlist> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const final {
    return shard_stores.local().get_attr(
      c, oid, name);
  }

  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) final {
    return shard_stores.local().get_attrs(
      c, oid);
  }

  read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final {
    return shard_stores.local().omap_get_values(
      c, oid, keys);
  }

  /// Retrieves paged set of values > start (if present)
  read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final {
    return shard_stores.local().omap_get_values(
      c, oid, start);
  }

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
  list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final {
    return shard_stores.local().list_objects(
      c, start, end, limit);
  }

  get_attr_errorator::future<ceph::bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) final {
    return shard_stores.local().omap_get_header(
      c, oid);
  }

  seastar::future<CollectionRef>
  create_new_collection(const coll_t& cid) final {
    return shard_stores.local().create_new_collection(cid);
  }

  seastar::future<CollectionRef>
  open_collection(const coll_t& cid) final {
    return shard_stores.local().open_collection(cid);
  }

  seastar::future<> do_transaction_no_callbacks(
    CollectionRef ch,
    ceph::os::Transaction&& txn) final {
    return shard_stores.local().do_transaction_no_callbacks(
      ch, std::move(txn));
  }

  read_errorator::future<std::map<uint64_t, uint64_t>>
  fiemap(CollectionRef c,
         const ghobject_t& oid,
         uint64_t off,
         uint64_t len) final {
    return shard_stores.local().fiemap(
      c, oid, off, len);
  }

private:
  const std::string path;
  uuid_d osd_fsid;
  seastar::sharded<ShardStores> shard_stores;
};
}
