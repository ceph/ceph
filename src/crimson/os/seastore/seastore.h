// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <optional>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_types.hh>

#include "include/uuid.h"

#include "os/Transaction.h"
#include "crimson/common/throttle.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

#include "crimson/os/seastore/device.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/collection_manager.h"
#include "crimson/os/seastore/object_data_handler.h"

namespace crimson::os::seastore {

class Onode;
using OnodeRef = boost::intrusive_ptr<Onode>;
class TransactionManager;

enum class op_type_t : uint8_t {
    DO_TRANSACTION = 0,
    READ,
    WRITE,
    GET_ATTR,
    GET_ATTRS,
    STAT,
    OMAP_GET_VALUES,
    OMAP_GET_VALUES2,
    MAX
};

class SeastoreCollection final : public FuturizedCollection {
public:
  template <typename... T>
  SeastoreCollection(T&&... args) :
    FuturizedCollection(std::forward<T>(args)...) {}

  seastar::shared_mutex ordering_lock;
};

/**
 * col_obj_ranges_t
 *
 * Represents the two ghobject_t ranges spanned by a PG collection.
 * Temp objects will be within [temp_begin, temp_end) and normal objects
 * will be in [obj_begin, obj_end).
 */
struct col_obj_ranges_t {
  ghobject_t temp_begin;
  ghobject_t temp_end;
  ghobject_t obj_begin;
  ghobject_t obj_end;
};

class SeaStore final : public FuturizedStore {
public:
  using base_ertr = TransactionManager::base_ertr;
  using base_iertr = TransactionManager::base_iertr;

  class MDStore {
  public:
    using write_meta_ertr = base_ertr;
    using write_meta_ret = write_meta_ertr::future<>;
    virtual write_meta_ret write_meta(
      const std::string &key,
      const std::string &val
    ) = 0;

    using read_meta_ertr = base_ertr;
    using read_meta_ret = write_meta_ertr::future<std::optional<std::string>>;
    virtual read_meta_ret read_meta(const std::string &key) = 0;

    virtual ~MDStore() {}
  };
  using MDStoreRef = std::unique_ptr<MDStore>;

  class Shard : public FuturizedStore::Shard {
  public:
    Shard(
      std::string root,
      Device* device,
      bool is_test);
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
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

    base_errorator::future<bool> exists(
      CollectionRef c,
      const ghobject_t& oid,
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

    /// Retrieves paged set of values > start (if present)
    read_errorator::future<omap_values_paged_t> omap_get_values(
      CollectionRef c,           ///< [in] collection
      const ghobject_t &oid,     ///< [in] oid
      const std::optional<std::string> &start, ///< [in] start, empty for begin
      uint32_t op_flags = 0
      ) final; ///< @return <done, values> values.empty() iff done

    get_attr_errorator::future<bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) final;

    /// std::get<1>(ret) returns end if and only if the listing has listed all
    /// the items within the range, otherwise it returns the next key to be listed.
    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const final;

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
    seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
    seastar::future<> set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts) final;

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) final;

    /* Note, flush() machinery must go through the same pipeline
     * stages and locks as do_transaction. */
    seastar::future<> flush(CollectionRef ch) final;

    read_errorator::future<fiemap_ret_t> fiemap(
      CollectionRef ch,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags = 0) final;

    unsigned get_max_attr_name_length() const final {
      return 256;
    }

    const omap_root_le_t& select_log_omap_root(Onode& onode);

  // only exposed to SeaStore
  public:
    seastar::future<> umount();
    // init managers and mount transaction_manager
    seastar::future<> mount_managers();

    void set_secondaries(Device& sec_dev) {
      secondaries.emplace_back(&sec_dev);
    }

    seastar::future<std::vector<coll_core_t>> list_collections();

    seastar::future<> write_meta(const std::string& key,
                                 const std::string& value);

    seastar::future<std::string> get_default_device_class();

    store_statfs_t stat() const;

    uuid_d get_fsid() const;

    seastar::future<> mkfs_managers();

    void init_managers();

    double reset_report_interval() const;

    device_stats_t get_device_stats(bool report_detail, double seconds) const;

    shard_stats_t get_io_stats(bool report_detail, double seconds) const;

    cache_stats_t get_cache_stats(bool report_detail, double seconds) const;

  private:
    struct internal_context_t {
      CollectionRef ch;
      ceph::os::Transaction ext_transaction;

      internal_context_t(
        CollectionRef ch,
        ceph::os::Transaction &&_ext_transaction,
        TransactionRef &&transaction)
        : ch(ch), ext_transaction(std::move(_ext_transaction)),
          transaction(std::move(transaction)),
          iter(ext_transaction.begin()) {}

      TransactionRef transaction;

      ceph::os::Transaction::iterator iter;
      std::chrono::steady_clock::time_point begin_timestamp = std::chrono::steady_clock::now();

      void reset_preserve_handle(TransactionManager &tm) {
        tm.reset_transaction_preserve_handle(*transaction);
        iter = ext_transaction.begin();
      }
    };

    TransactionManager::read_extent_iertr::future<std::optional<unsigned>>
    get_coll_bits(CollectionRef ch, Transaction &t) const;

    static void on_error(ceph::os::Transaction &t);

    template <typename F>
    auto repeat_with_internal_context(
      CollectionRef ch,
      ceph::os::Transaction &&t,
      Transaction::src_t src,
      const char* tname,
      op_type_t op_type,
      F &&f) {
      // The below repeat_io_num requires MUTATE
      assert(src == Transaction::src_t::MUTATE);
      return seastar::do_with(
        internal_context_t(
          ch, std::move(t),
          transaction_manager->create_transaction(
	    src, tname, t.get_fadvise_flags())),
        std::forward<F>(f),
        [this, op_type](auto &ctx, auto &f) {
        assert(shard_stats.starting_io_num);
        --(shard_stats.starting_io_num);
        ++(shard_stats.waiting_collock_io_num);

	return ctx.transaction->get_handle().take_collection_lock(
	  static_cast<SeastoreCollection&>(*(ctx.ch)).ordering_lock
	).then([this] {
	  assert(shard_stats.waiting_collock_io_num);
	  --(shard_stats.waiting_collock_io_num);
	  ++(shard_stats.waiting_throttler_io_num);

	  return throttler.get(1);
	}).then([&, this] {
	  assert(shard_stats.waiting_throttler_io_num);
	  --(shard_stats.waiting_throttler_io_num);
	  ++(shard_stats.processing_inlock_io_num);

	  return repeat_eagain([&, this] {
	    ++(shard_stats.repeat_io_num);

	    ctx.reset_preserve_handle(*transaction_manager);
	    return std::invoke(f, ctx);
	  }).handle_error(
	    crimson::ct_error::all_same_way([&ctx](auto e) {
	      on_error(ctx.ext_transaction);
	      return seastar::now();
	    })
	  );
	}).then([this, op_type, &ctx] {
	  add_latency_sample(op_type,
	      std::chrono::steady_clock::now() - ctx.begin_timestamp);
	}).finally([this] {
	  throttler.put();
	});
      });
    }

    template <typename Ret, typename F>
    auto repeat_with_onode(
      CollectionRef ch,
      const ghobject_t &oid,
      Transaction::src_t src,
      const char* tname,
      op_type_t op_type,
      cache_hint_t cache_hint_flags,
      F &&f) const {
      auto begin_time = std::chrono::steady_clock::now();
      return seastar::do_with(
        oid, Ret{}, std::forward<F>(f),
        [this, ch, src, op_type, begin_time, tname, cache_hint_flags
        ](auto &oid, auto &ret, auto &f)
      {
        return repeat_eagain([&, this, ch, src, tname, cache_hint_flags] {
          assert(src == Transaction::src_t::READ);
          ++(shard_stats.repeat_read_num);

          return transaction_manager->with_transaction_intr(
            src,
            tname,
	    cache_hint_flags,
            [&, this, ch, tname](auto& t)
          {
            LOG_PREFIX(SeaStoreS::repeat_with_onode);
            SUBDEBUGT(seastore, "{} cid={} oid={} ...",
                      t, tname, ch->get_cid(), oid);
            return onode_manager->get_onode(t, oid
            ).si_then([&](auto onode) {
              return seastar::do_with(std::move(onode), [&](auto& onode) {
                return f(t, *onode);
              });
            }).si_then([&ret](auto _ret) {
              ret = _ret;
            });
          });
        }).safe_then([&ret, op_type, begin_time, this] {
          const_cast<Shard*>(this)->add_latency_sample(op_type,
                     std::chrono::steady_clock::now() - begin_time);
          return seastar::make_ready_future<Ret>(ret);
        });
      });
    }

    using omap_list_bare_ret = OMapManager::omap_list_bare_ret;
    using omap_list_ret = OMapManager::omap_list_ret;
    omap_list_ret omap_list(
      Onode& onode,
      const omap_root_le_t& omap_root,
      Transaction& t,
      const std::optional<std::string>& start,
      OMapManager::omap_list_config_t config) const;

    using _omap_get_value_iertr = base_iertr::extend<
      crimson::ct_error::enodata
      >;
    using _omap_get_value_ret = _omap_get_value_iertr::future<ceph::bufferlist>;
    _omap_get_value_ret _omap_get_value(
      Transaction &t,
      omap_root_t &&root,
      std::string_view key) const;

    base_iertr::future<omap_values_t> _omap_get_values(
      Transaction &t,
      omap_root_t &&root,
      const omap_keys_t &keys) const;

    friend class SeaStoreOmapIterator;

    base_iertr::future<ceph::bufferlist> _read( 
      Transaction& t,
      Onode& onode,
      uint64_t offset,
      std::size_t len,
      uint32_t op_flags);

    _omap_get_value_ret _get_attr(
      Transaction& t,
      Onode& onode,
      std::string_view name) const;

    base_iertr::future<attrs_t> _get_attrs(
      Transaction& t,
      Onode& onode);

    seastar::future<struct stat> _stat(
      Transaction& t,
      Onode& onode,
      const ghobject_t& oid);

    base_iertr::future<omap_values_t> do_omap_get_values(
      Transaction& t,
      Onode& onode,
      const omap_keys_t& keys);

    base_iertr::future<omap_values_paged_t> do_omap_get_values(
      Transaction& t,
      Onode& onode,
      const std::optional<std::string>& start,
      const omap_root_le_t& omap_root);

    base_iertr::future<fiemap_ret_t> _fiemap(
      Transaction &t,
      Onode &onode,
      uint64_t off,
      uint64_t len) const;

    using tm_iertr = base_iertr;
    using tm_ret = tm_iertr::future<>;
    tm_ret _do_transaction_step(
      internal_context_t &ctx,
      CollectionRef &col,
      std::vector<OnodeRef> &onodes,
      ceph::os::Transaction::iterator &i);

    tm_ret _remove_omaps(
      internal_context_t &ctx,
      OnodeRef &onode,
      omap_root_t &&omap_root);
    tm_ret _remove(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _touch(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _write(
      internal_context_t &ctx,
      OnodeRef &onode,
      uint64_t offset, size_t len,
      ceph::bufferlist &&bl,
      uint32_t fadvise_flags);
    tm_ret _clone_omaps(
      internal_context_t &ctx,
      OnodeRef &onode,
      OnodeRef &d_onode,
      const omap_type_t otype);
    tm_ret _clone(
      internal_context_t &ctx,
      OnodeRef &onode,
      OnodeRef &d_onode);
    tm_ret _rename(
      internal_context_t &ctx,
      OnodeRef &onode,
      OnodeRef &d_onode);
    tm_ret _zero(
      internal_context_t &ctx,
      OnodeRef &onode,
      objaddr_t offset, extent_len_t len);
    tm_ret _omap_set_values(
      internal_context_t &ctx,
      OnodeRef &onode,
      std::map<std::string, ceph::bufferlist> &&aset,
      const omap_root_le_t &omap_root);
    tm_ret _omap_set_header(
      internal_context_t &ctx,
      OnodeRef &onode,
      ceph::bufferlist &&header);
    tm_ret _omap_clear(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _omap_rmkeys(
      internal_context_t &ctx,
      OnodeRef &onode,
      omap_keys_t &&aset,
      const omap_root_le_t &_omap_root);
    tm_ret _omap_rmkeyrange(
      internal_context_t &ctx,
      OnodeRef &onode,
      std::string first,
      std::string last,
      const omap_root_le_t &_omap_root);
    tm_ret _truncate(
      internal_context_t &ctx,
      OnodeRef &onode, uint64_t size);
    tm_ret _setattrs(
      internal_context_t &ctx,
      OnodeRef &onode,
      std::map<std::string,bufferlist>&& aset);
    tm_ret _rmattr(
      internal_context_t &ctx,
      OnodeRef &onode,
      std::string name);
    tm_ret _rmattrs(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _xattr_rmattr(
      internal_context_t &ctx,
      OnodeRef &onode,
      std::string &&name);
    tm_ret _xattr_clear(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _create_collection(
      internal_context_t &ctx,
      const coll_t& cid, int bits);
    tm_ret _remove_collection(
      internal_context_t &ctx,
      const coll_t& cid);
    using omap_set_kvs_ret = tm_iertr::future<omap_root_t>;
    omap_set_kvs_ret _omap_set_kvs(
      const OnodeRef &onode,
      const omap_root_le_t& omap_root,
      Transaction& t,
      std::map<std::string, ceph::bufferlist>&& kvs);

    boost::intrusive_ptr<SeastoreCollection> _get_collection(const coll_t& cid);

    static constexpr auto LAT_MAX = static_cast<std::size_t>(op_type_t::MAX);

    struct {
      std::array<seastar::metrics::histogram, LAT_MAX> op_lat;
    } stats;

    seastar::metrics::histogram& get_latency(
      op_type_t op_type) {
      assert(static_cast<std::size_t>(op_type) < stats.op_lat.size());
      return stats.op_lat[static_cast<std::size_t>(op_type)];
    }

    void add_latency_sample(op_type_t op_type,
        std::chrono::steady_clock::duration dur) {
      seastar::metrics::histogram& lat = get_latency(op_type);
      lat.sample_count++;
      lat.sample_sum += std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
     }

  private:
    std::string root;
    Device* device;
    const uint32_t max_object_size;
    bool is_test;

    std::vector<Device*> secondaries;
    TransactionManagerRef transaction_manager;
    CollectionManagerRef collection_manager;
    OnodeManagerRef onode_manager;

    common::Throttle throttler;

    seastar::metrics::metric_group metrics;
    void register_metrics();

    mutable shard_stats_t shard_stats;
    mutable seastar::lowres_clock::time_point last_tp =
      seastar::lowres_clock::time_point::min();
    mutable shard_stats_t last_shard_stats;
  };

public:
  SeaStore(
    const std::string& root,
    MDStoreRef mdstore);
  ~SeaStore();

  seastar::future<> start() final;
  seastar::future<> stop() final;

  mount_ertr::future<> mount() final;
  seastar::future<> umount() final;

  mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<store_statfs_t> stat() const final;
  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const final;

  seastar::future<> report_stats() final;

  uuid_d get_fsid() const final {
    ceph_assert(seastar::this_shard_id() == primary_core);
    return shard_stores.local().get_fsid();
  }

  seastar::future<> write_meta(const std::string& key, const std::string& value) final;

  seastar::future<std::tuple<int, std::string>> read_meta(const std::string& key) final;

  seastar::future<std::vector<coll_core_t>> list_collections() final;

  seastar::future<std::string> get_default_device_class() final;

  FuturizedStore::Shard& get_sharded_store() final {
    return shard_stores.local();
  }

  static col_obj_ranges_t
  get_objs_range(CollectionRef ch, unsigned bits);

// for test
public:
  mount_ertr::future<> test_mount();
  mkfs_ertr::future<> test_mkfs(uuid_d new_osd_fsid);

  DeviceRef get_primary_device_ref() {
    return std::move(device);
  }

  seastar::future<> test_start(DeviceRef dev);

private:
  seastar::future<> write_fsid(uuid_d new_osd_fsid);

  seastar::future<> prepare_meta(uuid_d new_osd_fsid);

  seastar::future<> set_secondaries();

private:
  std::string root;
  MDStoreRef mdstore;
  DeviceRef device;
  std::vector<DeviceRef> secondaries;
  seastar::sharded<SeaStore::Shard> shard_stores;

  mutable seastar::lowres_clock::time_point last_tp =
    seastar::lowres_clock::time_point::min();
  mutable std::vector<device_stats_t> shard_device_stats;
  mutable std::vector<shard_stats_t> shard_io_stats;
  mutable std::vector<cache_stats_t> shard_cache_stats;
};

std::unique_ptr<SeaStore> make_seastore(
  const std::string &device);

std::unique_ptr<SeaStore> make_test_seastore(
  SeaStore::MDStoreRef mdstore);
}
