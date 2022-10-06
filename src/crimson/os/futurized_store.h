// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>
#include <optional>
#include <vector>

#include <seastar/core/future.hh>

#include "os/Transaction.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/osd/exceptions.h"
#include "include/buffer_fwd.h"
#include "include/uuid.h"
#include "osd/osd_types.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class FuturizedCollection;

class FuturizedStore {

public:
  static seastar::future<std::unique_ptr<FuturizedStore>> create(const std::string& type,
                                                const std::string& data,
                                                const ConfigValues& values);
  FuturizedStore() = default;
  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<> start() {
    return seastar::now();
  }
  virtual seastar::future<> stop() = 0;

  using mount_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mount_ertr::future<> mount() = 0;
  virtual seastar::future<> umount() = 0;

  using mkfs_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) = 0;
  virtual seastar::future<store_statfs_t> stat() const = 0;

  using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
  using read_errorator = crimson::errorator<crimson::ct_error::enoent,
                                            crimson::ct_error::input_output_error>;
  virtual read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) = 0;
  virtual read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    uint32_t op_flags = 0) = 0;

  using get_attr_errorator = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::enodata>;
  virtual get_attr_errorator::future<ceph::bufferlist> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const = 0;

  using get_attrs_ertr = crimson::errorator<
    crimson::ct_error::enoent>;
  using attrs_t = std::map<std::string, ceph::bufferlist, std::less<>>;
  virtual get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) = 0;
  virtual seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  using omap_values_t = std::map<std::string, ceph::bufferlist, std::less<>>;
  using omap_keys_t = std::set<std::string>;
  virtual read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) = 0;
  virtual seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const = 0;
  virtual read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) = 0; ///< @return <done, values> values.empty() only if done

  virtual get_attr_errorator::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  virtual seastar::future<CollectionRef> create_new_collection(const coll_t& cid) = 0;
  virtual seastar::future<CollectionRef> open_collection(const coll_t& cid) = 0;
  virtual seastar::future<std::vector<coll_t>> list_collections() = 0;

protected:
  virtual seastar::future<> do_transaction_no_callbacks(
    CollectionRef ch,
    ceph::os::Transaction&& txn) = 0;

public:
  seastar::future<> do_transaction(
    CollectionRef ch,
    ceph::os::Transaction&& txn) {
    std::unique_ptr<Context> on_commit(
      ceph::os::Transaction::collect_all_contexts(txn));
    return do_transaction_no_callbacks(
      std::move(ch), std::move(txn)
    ).then([on_commit=std::move(on_commit)]() mutable {
      auto c = on_commit.release();
      if (c) c->complete(0);
      return seastar::now();
    });
  }


  /**
   * flush
   *
   * Flushes outstanding transactions on ch, returned future resolves
   * after any previously submitted transactions on ch have committed.
   *
   * @param ch [in] collection on which to flush
   */
  virtual seastar::future<> flush(CollectionRef ch) {
    return do_transaction(ch, ceph::os::Transaction{});
  }

  // error injection
  virtual seastar::future<> inject_data_error(const ghobject_t& o) {
    return seastar::now();
  }
  virtual seastar::future<> inject_mdata_error(const ghobject_t& o) {
    return seastar::now();
  }

  virtual read_errorator::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len) = 0;

  virtual seastar::future<> write_meta(const std::string& key,
				       const std::string& value) = 0;
  virtual seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) = 0;
  virtual uuid_d get_fsid() const  = 0;
  virtual unsigned get_max_attr_name_length() const = 0;
};

/**
 * ShardedStoreProxy
 *
 * Simple helper to proxy FuturizedStore operations to the core on which
 * the store was initialized for implementations without support for multiple
 * reactors.
 */
template <typename T>
class ShardedStoreProxy : public FuturizedStore {
  const core_id_t core;
  std::unique_ptr<T> impl;
  uuid_d fsid;
  unsigned max_attr = 0;

  template <typename Method, typename... Args>
  decltype(auto) proxy(Method method, Args&&... args) const {
    return proxy_method_on_core(
      core, *impl, method, std::forward<Args>(args)...);
  }

  template <typename Method, typename... Args>
  decltype(auto) proxy(Method method, Args&&... args) {
    return proxy_method_on_core(
      core, *impl, method, std::forward<Args>(args)...);
  }

public:
  ShardedStoreProxy(T *t)
    : core(seastar::this_shard_id()),
      impl(t) {}
  template <typename... Args>
  ShardedStoreProxy(Args&&... args)
    : core(seastar::this_shard_id()),
      impl(std::make_unique<T>(std::forward<Args>(args)...)) {}
  ~ShardedStoreProxy() = default;

  // no copying
  explicit ShardedStoreProxy(const ShardedStoreProxy &o) = delete;
  const ShardedStoreProxy &operator=(const ShardedStoreProxy &o) = delete;

  seastar::future<> start() final { return proxy(&T::start); }
  seastar::future<> stop() final { return proxy(&T::stop); }
  mount_ertr::future<> mount() final {
    auto ret = seastar::smp::submit_to(
      core,
      [this] {
	auto ret = impl->mount(
	).safe_then([this] {
	  fsid = impl->get_fsid();
	  max_attr = impl->get_max_attr_name_length();
	  return seastar::now();
	});
	return std::move(ret).to_base();
      });
    return mount_ertr::future<>(std::move(ret));
  }
  seastar::future<> umount() final { return proxy(&T::umount); }
  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final {
    return proxy(&T::mkfs, new_osd_fsid);
  }
  seastar::future<store_statfs_t> stat() const final {
    return crimson::submit_to(core, [this] { return impl->stat(); });
  }
  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t &oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final {
    return proxy(&T::read, std::move(c), oid, offset, len, op_flags);
  }
  read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t &oid,
    interval_set<uint64_t> &m,
    uint32_t op_flags = 0) final {
    return crimson::submit_to(core, [this, c, oid, m, op_flags]() mutable {
      return impl->readv(c, oid, m, op_flags);
    });
  }
  get_attr_errorator::future<ceph::bufferlist> get_attr(
    CollectionRef c,
    const ghobject_t &oid,
    std::string_view name) const final {
    return proxy(&T::get_attr, std::move(c), oid, std::string(name));
  }
  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t &oid) final {
    return proxy(&T::get_attrs, std::move(c), oid);
  }
  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t &oid) final {
    return crimson::submit_to(
      core,
      [this, c, oid] {
	return impl->stat(c, oid);
      });
  }
  read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t &oid,
    const omap_keys_t &keys) final {
    return crimson::submit_to(core, [this, c, oid, keys] {
      return impl->omap_get_values(c, oid, keys);
    });
  }
  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
  list_objects(
    CollectionRef c,
    const ghobject_t &start,
    const ghobject_t &end,
    uint64_t limit) const final {
    return proxy(&T::list_objects, std::move(c), start, end, limit);
  }
  read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,
    const ghobject_t &oid,
    const std::optional<std::string> &start) final {
    return crimson::submit_to(core, [this, c, oid, start] {
      return impl->omap_get_values(c, oid, start);
    });
  }
  get_attr_errorator::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t &oid) final {
    return proxy(&T::omap_get_header, std::move(c), oid);
  }
  seastar::future<CollectionRef> create_new_collection(const coll_t &cid) final {
    return proxy(&T::create_new_collection, cid);
  }
  seastar::future<CollectionRef> open_collection(const coll_t &cid) final {
    return proxy(&T::open_collection, cid);
  }
  seastar::future<std::vector<coll_t>> list_collections() final {
    return proxy(&T::list_collections);
  }
  seastar::future<> do_transaction_no_callbacks(
    CollectionRef ch,
    ceph::os::Transaction &&txn) final {
    return proxy(&T::do_transaction_no_callbacks, std::move(ch), std::move(txn));
  }
  seastar::future<> flush(CollectionRef ch) final {
    return proxy(&T::flush, std::move(ch));
  }
  seastar::future<> inject_data_error(const ghobject_t &o) final {
    return proxy(&T::inject_data_error, o);
  }
  seastar::future<> inject_mdata_error(const ghobject_t &o) final {
    return proxy(&T::inject_mdata_error, o);
  }

  read_errorator::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t &oid,
    uint64_t off,
    uint64_t len) final {
    return proxy(&T::fiemap, std::move(ch), oid, off, len);
  }

  seastar::future<> write_meta(
    const std::string &key,
    const std::string &value) final {
    return proxy(&T::write_meta, key, value);
  }
  seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string &key) final {
    return proxy(&T::read_meta, key);
  }
  uuid_d get_fsid() const final {
    return fsid;
  }
  unsigned get_max_attr_name_length() const final {
    return max_attr;
  }
};

}
