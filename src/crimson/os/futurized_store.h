// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <map>
#include <optional>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "os/Transaction.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/local_shared_foreign_ptr.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/osd/exceptions.h"
#include "include/buffer_fwd.h"
#include "include/uuid.h"
#include "osd/osd_types.h"
#include "os/ObjectStore.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class FuturizedCollection;

class FuturizedStore {
public:
  class Shard {
  public:
    Shard() = default;
    virtual ~Shard() = default;
    // no copying
    explicit Shard(const Shard& o) = delete;
    const Shard& operator=(const Shard& o) = delete;

    bool is_shard_store_active(unsigned int store_index, unsigned int store_shard_nums) {
      if(seastar::this_shard_id() + seastar::smp::count * store_index >= store_shard_nums) {
        // store_index is out of range {} - inactivating this store shard
        return false;
      }
      return true;
    }

    using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
    using base_errorator = crimson::errorator<crimson::ct_error::input_output_error>;
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

    virtual base_errorator::future<bool> exists(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) = 0;

    using get_attr_errorator = crimson::errorator<
      crimson::ct_error::enoent,
      crimson::ct_error::enodata>;
    virtual get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name,
      uint32_t op_flags = 0) const = 0;

    using get_attrs_ertr = crimson::errorator<
      crimson::ct_error::enoent>;
    using attrs_t = std::map<std::string, ceph::bufferlist, std::less<>>;
    virtual get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) = 0;

    virtual seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) = 0;

    using omap_values_t = attrs_t;
    using omap_keys_t = std::set<std::string>;
    virtual read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys,
      uint32_t op_flags = 0) = 0;

    /**
     * Iterate over object map with user-provided callable
     *
     * Warning! f cannot block or perform IO and must not wait on a future.
     *
     * @param c collection
     * @param oid object
     * @param start_from where the iterator should point to at
     *                   the beginning
     * @param f callable that takes OMAP key and corresponding
     *          value as string_views and controls iteration
     *          by the return. It is executed for every object's
     *          OMAP entry from `start_from` till end of the
     *          object's OMAP or till the iteration is stopped
     *          by `STOP`. Please note that if there is no such
     *          entry, `visitor` will be called 0 times.
     * @return omap_iter_ret_t on success
     *         omap_iter_ret_t::STOP means omap_iterate() is stopped by f,
     *         omap_iter_ret_t::NEXT means omap_iterate() reaches the end of omap tree
     */
    using omap_iterate_cb_t = std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)>;
    virtual read_errorator::future<ObjectStore::omap_iter_ret_t> omap_iterate(
      CollectionRef c,   ///< [in] collection
      const ghobject_t &oid, ///< [in] object
      ObjectStore::omap_iter_seek_t start_from, ///< [in] where the iterator should point to at the beginning
      omap_iterate_cb_t callback,
      ///< [in] the callback function for each OMAP entry after start_from till end of the OMAP or
      /// till the iteration is stopped by `STOP`.
      uint32_t op_flags = 0
      ) = 0;

    virtual get_attr_errorator::future<bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) = 0;

    virtual seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const = 0;

    virtual seastar::future<CollectionRef> create_new_collection(const coll_t& cid) = 0;

    virtual seastar::future<CollectionRef> open_collection(const coll_t& cid) = 0;

    virtual seastar::future<> set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts) = 0;

  public:
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

    using fiemap_ret_t = std::map<uint64_t, uint64_t>;
    virtual read_errorator::future<fiemap_ret_t> fiemap(
      CollectionRef ch,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags = 0) = 0;

    virtual unsigned get_max_attr_name_length() const = 0;
  };

public:
  static std::unique_ptr<FuturizedStore> create(const std::string& type,
                                                const std::string& data,
                                                const ConfigValues& values);
  FuturizedStore()
  : primary_core(seastar::this_shard_id())
  {}

  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<unsigned int> start() = 0;

  virtual seastar::future<> stop() = 0;

  using mount_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mount_ertr::future<> mount() = 0;

  virtual seastar::future<> umount() = 0;

  using mkfs_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) = 0;

  virtual seastar::future<store_statfs_t> stat() const = 0;

  virtual seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const = 0;

  virtual seastar::future<> report_stats() { return seastar::now(); }

  virtual uuid_d get_fsid() const  = 0;

  virtual seastar::future<> write_meta(const std::string& key,
				       const std::string& value) = 0;

  using StoreShardLRef = seastar::shared_ptr<FuturizedStore::Shard>;
  using StoreShardFRef = seastar::foreign_ptr<StoreShardLRef>;
  using StoreShardRef = ::crimson::local_shared_foreign_ptr<StoreShardLRef>;
  using StoreShardFFRef = seastar::foreign_ptr<StoreShardRef>;
  using StoreShardXcoreRef = ::crimson::local_shared_foreign_ptr<StoreShardRef>;

  // called on the shard and get this FuturizedStore::shard;
  virtual StoreShardRef get_sharded_store(unsigned int store_index = 0) = 0;
  virtual std::vector<StoreShardRef> get_sharded_stores() = 0;

  virtual seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) = 0;

  using coll_core_t = std::pair<coll_t, std::pair<core_id_t, unsigned int>>;
  virtual seastar::future<std::vector<coll_core_t>> list_collections() = 0;

  virtual seastar::future<std::string> get_default_device_class() = 0;
protected:
  const core_id_t primary_core;
};

template<auto MemberFunc, typename... Args>
auto with_store(crimson::os::FuturizedStore::StoreShardRef store, Args&&... args)
{
  using raw_return_type = decltype((std::declval<crimson::os::FuturizedStore::Shard>().*MemberFunc)(std::forward<Args>(args)...));

  constexpr bool is_errorator = is_errorated_future_v<raw_return_type>;
  constexpr bool is_seastar_future = seastar::is_future<raw_return_type>::value && !is_errorator;
  constexpr bool is_plain = !is_errorator && !is_seastar_future;
  const auto original_core = seastar::this_shard_id();
  if(crimson::common::get_conf<bool>("seastore_require_partition_count_match_reactor_count")) {
    ceph_assert(store.get_owner_shard() == seastar::this_shard_id());
  }
  if (store.get_owner_shard() == seastar::this_shard_id()) {
    if constexpr (is_plain) {
      return seastar::make_ready_future<raw_return_type>(
        ((*store).*MemberFunc)(std::forward<Args>(args)...));
    } else {
      return ((*store).*MemberFunc)(std::forward<Args>(args)...);
    }
  } else {
    if constexpr (is_errorator) {
      auto fut = seastar::smp::submit_to(
        store.get_owner_shard(),
        [f_store=store.get(), args=std::make_tuple(std::forward<Args>(args)...)]() mutable {
          return std::apply([f_store](auto&&... args) {
            return ((*f_store).*MemberFunc)(std::forward<decltype(args)>(args)...).to_base();
          }, std::move(args));
        }).then([original_core] (auto&& result) {
          return seastar::smp::submit_to(original_core,
            [result = std::forward<decltype(result)>(result)]() mutable {
              return std::forward<decltype(result)>(result);
          });
        });
      return raw_return_type(std::move(fut));
    } else {
      auto fut = seastar::smp::submit_to(
        store.get_owner_shard(),
        [f_store=store.get(), args=std::make_tuple(std::forward<Args>(args)...)]() mutable {
        return std::apply([f_store](auto&&... args) {
          return ((*f_store).*MemberFunc)(std::forward<decltype(args)>(args)...);
        }, std::move(args));
      });
      if constexpr (std::is_same_v<raw_return_type, seastar::future<>>) {
        return fut.then([original_core] {
          return seastar::smp::submit_to(original_core, [] {
            return seastar::make_ready_future<>();
          });
        });
      } else {
        return fut.then([original_core](auto&& result) {
          return seastar::smp::submit_to(original_core,
            [result = std::forward<decltype(result)>(result)]() mutable {
              return std::forward<decltype(result)>(result);
          });
        });
      }
    }
  }
}

seastar::future<> with_store_do_transaction(
  crimson::os::FuturizedStore::StoreShardRef store,
  FuturizedStore::Shard::CollectionRef ch,
  ceph::os::Transaction&& txn);

}
