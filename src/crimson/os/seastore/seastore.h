// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "crimson/common/smp_helpers.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

#include "crimson/os/seastore/device.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/transaction_interruptor.h"
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
    GET_ATTR,
    GET_ATTRS,
    STAT,
    OMAP_GET_VALUES,
    OMAP_ITERATE,
    MAX
};

enum class txn_stage_t : uint8_t {
    COLLOCK_WAIT = 0,  // waiting on the collection ordering_lock
    THROTTLER_WAIT,    // waiting for a throttler slot
    BUILD,             // building the transaction (_do_transaction_step loop)
    BUILD_GET_ONODE,   // onode_manager get/get_or_create calls within BUILD
    SUBMIT_TOTAL,      // the whole submit_transaction (pipeline + journal write)
    // Sub-phases of submit_transaction:
    SUBMIT_RESERVE,        // enter(reserve_projected_usage) + epm reserve_projected_usage
    SUBMIT_OOL_WRITE,      // write_delayed + write_preallocated OOL extents (device I/O)
    SUBMIT_LBA_UPDATE,     // update_lba_mappings
    SUBMIT_PREPARE_ENTER,  // enter(prepare) pipeline stage (global OrderedExclusive wait)
    SUBMIT_PREPARE_RECORD, // prepare_record (record encoding)
    SUBMIT_JOURNAL,        // journal->submit_record -- POST-lock (not part of the hold)
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
      bool is_test,
      uint32_t store_shard_nums,
      store_index_t store_index = 0);
    ~Shard() = default;

    seastar::future<struct stat> stat(
      CollectionRef c,
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

    base_errorator::future<bool> exists(
      CollectionRef c,
      const ghobject_t& oid,
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
      omap_iterate_conf_t on_conflict = nullptr) override final;

    get_attr_errorator::future<bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid,
      uint32_t op_flags = 0) override final;

    /// std::get<1>(ret) returns end if and only if the listing has listed all
    /// the items within the range, otherwise it returns the next key to be listed.
    seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit,
      uint32_t op_flags = 0) const override final;

    seastar::future<CollectionRef> create_new_collection(const coll_t& cid) override final;
    seastar::future<CollectionRef> open_collection(const coll_t& cid) override final;
    seastar::future<> set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts) override final;

    seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) override final;

    /* Note, flush() machinery must go through the same pipeline
     * stages and locks as do_transaction. */
    seastar::future<> flush(CollectionRef ch) override final;

    read_errorator::future<fiemap_ret_t> fiemap(
      CollectionRef ch,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len,
      uint32_t op_flags = 0) override final;

    unsigned get_max_attr_name_length() const override final {
      return 256;
    }

    omap_root_t select_log_omap_root(Onode& onode) const;

  // only exposed to SeaStore
  public:
    base_ertr::future<> umount();
    seastar::future<> do_gc();
    // init managers and mount transaction_manager
    seastar::future<> mount_managers();

    void set_secondaries(Device& sec_dev) {
      secondaries.emplace_back(&sec_dev);
    }

    seastar::future<std::vector<coll_core_t>> list_collections();

    seastar::future<> write_meta(const std::string& key,
                                 const std::string& value);

    seastar::future<std::string> get_default_device_class();

    seastar::future<store_statfs_t> stat() const;

    uuid_d get_fsid() const;

    TransactionManager::alloc_extent_ertr::future<> mkfs_managers();

    void init_managers();

    double reset_report_interval() const;

    device_stats_t get_device_stats(bool report_detail, double seconds) const;

    shard_stats_t get_io_stats(bool report_detail, double seconds) const;

    cache_stats_t get_cache_stats(bool report_detail, double seconds) const;

    unsigned int get_store_index() const {
      return store_index;
    }

    bool get_status() const {
      return store_active;
    }
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

      std::chrono::steady_clock::duration build_time{0};
      std::chrono::steady_clock::duration get_onode_time{0};
      std::chrono::steady_clock::duration submit_time{0};

      void reset_preserve_handle(TransactionManager &tm) {
        tm.reset_transaction_preserve_handle(*transaction);
        iter = ext_transaction.begin();
      }
    };

    TransactionManager::read_extent_iertr::future<std::optional<unsigned>>
    get_coll_bits(CollectionRef ch, Transaction &t) const;

    static void transaction_dump(ceph::os::Transaction &t);

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

    friend class SeaStoreOmapIterator;

    base_iertr::future<ceph::bufferlist> _read( 
      Transaction& t,
      Onode& onode,
      uint64_t offset,
      std::size_t len,
      uint32_t op_flags);

    using omaptree_get_value_iertr = base_iertr::extend<
      crimson::ct_error::enodata
      >;
    using omaptree_get_value_ret = omaptree_get_value_iertr::future<ceph::bufferlist>;
    omaptree_get_value_ret _get_attr(
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

    base_iertr::future<fiemap_ret_t> _fiemap(
      Transaction &t,
      Onode &onode,
      uint64_t off,
      uint64_t len) const;

    using tm_iertr = base_iertr::extend<
      crimson::ct_error::value_too_large>;
    using tm_ret = tm_iertr::future<>;
    tm_ret _do_transaction_step(
      internal_context_t &ctx,
      CollectionRef &col,
      std::vector<OnodeRef> &onodes,
      ceph::os::Transaction::iterator &i);

    tm_ret _remove(
      internal_context_t &ctx,
      OnodeRef &onode);
    tm_ret _touch(
      internal_context_t &ctx,
      Onode &onode);
    tm_ret _write(
      internal_context_t &ctx,
      Onode &onode,
      uint64_t offset, size_t len,
      ceph::bufferlist &&bl,
      uint32_t fadvise_flags);
    tm_ret _clone(
      internal_context_t &ctx,
      Onode &onode,
      Onode &d_onode);
    tm_ret _maybe_copy_on_write(
      internal_context_t &ctx,
      Onode &onode,
      ObjectDataHandler &handler);
    tm_ret _rename(
      internal_context_t &ctx,
      OnodeRef &onode,
      OnodeRef &d_onode);
    tm_ret _clone_range(
      internal_context_t &ctx,
      OnodeRef &src_onode,
      OnodeRef &dst_onode,
      extent_len_t srcoff,
      extent_len_t length,
      extent_len_t dstoff);
    tm_ret _zero(
      internal_context_t &ctx,
      Onode &onode,
      objaddr_t offset, extent_len_t len);
    tm_ret _omap_set_header(
      internal_context_t &ctx,
      Onode &onode,
      ceph::bufferlist &&header);
    tm_ret _omap_clear(
      internal_context_t &ctx,
      Onode &onode);
    tm_ret _truncate(
      internal_context_t &ctx,
      Onode &onode, uint64_t size);
    tm_ret _setattrs(
      internal_context_t &ctx,
      Onode &onode,
      std::map<std::string,bufferlist>&& aset);
    tm_ret _rmattr(
      internal_context_t &ctx,
      Onode &onode,
      std::string name);
    tm_ret _rmattrs(
      internal_context_t &ctx,
      Onode &onode);
    tm_ret _create_collection(
      internal_context_t &ctx,
      const coll_t& cid, int bits);
    tm_ret _split_collection(
      internal_context_t &ctx,
      const coll_t& cid, int bits);
    tm_ret _merge_collection(
      internal_context_t &ctx,
      coll_t cid,
      coll_t dest_cid,
      int bits);
    tm_ret _remove_collection(
      internal_context_t &ctx,
      const coll_t& cid);

    boost::intrusive_ptr<SeastoreCollection> _get_collection(const coll_t& cid);

    static constexpr auto LAT_MAX = static_cast<std::size_t>(op_type_t::MAX);

    // Histogram bucket upper bounds in microseconds (0.25ms–20ms).
    // Ops above 20ms land in the last bucket as overflow.
    static constexpr std::array<double, 14> lat_hist_bounds_us = {
      250, 500, 1000,
      1500, 2000, 3000,
      5000,
      7500, 10000,
      15000, 20000,
      30000, 50000,
      100000
    };

    // Buckets for the per-transaction conflict/replay distribution.
    static constexpr std::size_t REPLAY_BUCKETS = 16;

    static constexpr auto STAGE_MAX = static_cast<std::size_t>(txn_stage_t::MAX);
    // Upper bounds (microseconds) for the per-stage do_transaction latency histograms
    static constexpr std::array<uint64_t, 14> STAGE_LAT_BUCKETS_US = {
      250, 500, 1000, 1500, 2000, 3000, 5000, 7500,
      10000, 15000, 20000, 30000, 50000, 100000
    };

    struct {
      std::array<seastar::metrics::histogram, LAT_MAX> op_lat;
      seastar::metrics::histogram conflict_replays;
      std::array<seastar::metrics::histogram, STAGE_MAX> stage_lat;
      uint64_t onode_lookups = 0;      // tree lookups
      uint64_t onode_lookup_nodes = 0; // nodes searched (~depth/lookup)
      uint64_t onode_str_cmp_count = 0;// ns/oid memcmp calls
      uint64_t onode_inserts = 0;
      uint64_t onode_updates = 0;
      uint64_t onode_erases = 0;
      int64_t  onode_extents_delta = 0;
    } stats;

    void add_onode_tree_sample(const Transaction::tree_stats_t& ts) {
      stats.onode_lookups        += ts.lookup_count;
      stats.onode_lookup_nodes   += ts.nodes_visited;
      stats.onode_str_cmp_count  += ts.string_cmp_count;
      stats.onode_inserts        += ts.num_inserts;
      stats.onode_updates        += ts.num_updates;
      stats.onode_erases         += ts.num_erases;
      stats.onode_extents_delta  += ts.extents_num_delta;
    }

    seastar::metrics::histogram& get_latency(
      op_type_t op_type) {
      assert(static_cast<std::size_t>(op_type) < stats.op_lat.size());
      return stats.op_lat[static_cast<std::size_t>(op_type)];
    }

    void add_latency_sample(op_type_t op_type,
        std::chrono::steady_clock::duration dur) {
      seastar::metrics::histogram& lat = get_latency(op_type);
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
      lat.sample_count++;
      lat.sample_sum += us;
      bool found = false;
      for (auto& b : lat.buckets) {
        if (static_cast<double>(us) <= b.upper_bound) {
          ++b.count;
          found = true;
          break;
        }
      }
      if (!found && !lat.buckets.empty()) {
        ++lat.buckets.back().count;
      }
    }

    // Record how many times a just-completed transaction was conflicted/replayed.
    // Called only from the do_transaction_no_callbacks() completion path.
    void add_conflict_replay_sample(std::size_t num_replays) {
      auto& hist = stats.conflict_replays;
      if (hist.buckets.empty()) {
        // register_metrics() did not run (store inactive); nothing to record.
        return;
      }
      std::size_t idx = num_replays < REPLAY_BUCKETS ?
        num_replays : REPLAY_BUCKETS - 1;
      ++hist.buckets[idx].count;
      ++hist.sample_count;
      hist.sample_sum += num_replays;
    }

    // Record the latency of one do_transaction stage (microseconds). Buckets are
    // non-cumulative (bucket = first upper_bound >= value); values above the top
    // bound aren't bucketed but still land in sample_count/sample_sum.
    void add_stage_latency_sample(txn_stage_t stage,
        std::chrono::steady_clock::duration dur) {
      auto& hist = stats.stage_lat[static_cast<std::size_t>(stage)];
      if (hist.buckets.empty()) {
        // register_metrics() did not run (store inactive); nothing to record.
        return;
      }
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
      for (auto& b : hist.buckets) {
        if (static_cast<double>(us) <= b.upper_bound) {
          ++b.count;
          break;
        }
      }
      ++hist.sample_count;
      hist.sample_sum += us;
    }

    /*
     * omaptree interfaces
     */

    omap_root_t get_omap_root(omap_type_t type, Onode& onode) const {
      return onode.get_root(type).get(
        onode.get_metadata_hint(device->get_block_size()));
    }

    omaptree_get_value_ret omaptree_get_value(
      Transaction& t,
      omap_root_t&& root,
      std::string_view key) const;

    using omaptree_list_bare_ret = OMapManager::omap_list_bare_ret;
    using omaptree_list_ret = OMapManager::omap_list_ret;
    omaptree_list_ret omaptree_list(
      Transaction& t,
      omap_root_t&& root,
      const std::optional<std::string>& start,
      OMapManager::omap_list_config_t config) const;

    using omaptree_iterate_ret = OMapManager::omap_iterate_ret;
    omaptree_iterate_ret omaptree_iterate(
      Transaction& t,
      omap_root_t&& root,
      ObjectStore::omap_iter_seek_t &start_from,
      omap_iterate_cb_t callback
      );

    base_iertr::future<omap_values_t> omaptree_get_values(
      Transaction& t,
      omap_root_t&& root,
      const omap_keys_t& keys) const;

    using omap_values_paged_t = std::tuple<bool, omap_values_t>;
    base_iertr::future<omap_values_paged_t> omaptree_get_values(
      Transaction& t,
      omap_root_t&& root,
      const std::optional<std::string>& start) const;

    using omaptree_set_keys_iertr = base_iertr::extend<
      crimson::ct_error::value_too_large>;
    omaptree_set_keys_iertr::future<> omaptree_set_keys(
      Transaction& t,
      omap_root_t&& root,
      Onode& onode,
      std::map<std::string, ceph::bufferlist>&& aset);

    base_iertr::future<> omaptree_rm_key(
      Transaction& t,
      omap_root_t&& root,
      Onode& onode,
      std::string&& name);

    base_iertr::future<> omaptree_rm_keys(
      Transaction& t,
      omap_root_t&& root,
      Onode& onode,
      omap_keys_t&& aset);

    base_iertr::future<> omaptree_rm_keyrange(
      Transaction& t,
      omap_root_t&& root,
      Onode& onode,
      std::string first,
      std::string last);

    base_iertr::future<> omaptree_clone(
      Transaction& t,
      omap_type_t type,
      Onode& onode,
      Onode& d_onode);

    base_iertr::future<omap_root_t> omaptree_do_clear(
      Transaction& t,
      omap_root_t&& root);

    base_iertr::future<> omaptree_clear_no_onode(
      Transaction& t,
      omap_root_t&& root);

    base_iertr::future<> omaptree_clear(
      Transaction& t,
      omap_root_t&& root,
      Onode& onode);

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
    store_index_t store_index;
    bool store_active = true;

    seastar::metrics::metric_group metrics;
    void register_metrics(store_index_t store_index);

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

  seastar::future<uint32_t> start() override;
  seastar::future<> stop() override;

  Device::access_ertr::future<> _mount();

  // FuturizedStore::mount_ertr/mkfs_ertr only supports a stateful_ec
  // to keep the interface intact, convert to stateful_ec.
  crimson::os::FuturizedStore::mount_ertr::future<> mount() override {
    return _mount().handle_error(
      Device::access_ertr::all_same_way([](auto& code) {
        return crimson::stateful_ec{code};
    }));
  }
  seastar::future<> umount() override;

  Device::access_ertr::future<> _mkfs(uuid_d new_osd_fsid);

  crimson::os::FuturizedStore::mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) override {
    return _mkfs(new_osd_fsid).handle_error(
      Device::access_ertr::all_same_way([](auto& code) {
        return crimson::stateful_ec{code};
    }));
  }

  seastar::future<store_statfs_t> stat() const override;
  seastar::future<store_statfs_t> pool_statfs(int64_t pool_id) const override;

  seastar::future<> report_stats() override;

  uuid_d get_fsid() const override {
    ceph_assert(seastar::this_shard_id() == primary_core);
    return shard_stores.local().mshard_stores[0]->get_fsid();
  }

  uint64_t get_max_object_size() const override final {
    return std::min<uint64_t>(
      crimson::common::local_conf()->osd_max_object_size,
      crimson::common::get_conf<uint64_t>("seastore_default_max_object_size"));
  }

  seastar::future<> write_meta(const std::string& key, const std::string& value) override;

  seastar::future<std::tuple<int, std::string>> read_meta(const std::string& key) override;

  seastar::future<std::vector<coll_core_t>> list_collections() override;

  seastar::future<std::string> get_default_device_class() final;

  seastar::future<> do_gc() override;

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

  seastar::future<> get_shard_nums();
  seastar::future<> shard_stores_start(bool is_test);
  seastar::future<> shard_stores_stop();

private:
class MultiShardStores {
  public:
    std::vector<std::unique_ptr<SeaStore::Shard>> mshard_stores;

  public:
    MultiShardStores(size_t count,
                     const std::string& root,
                     Device* dev,
                     bool is_test,
                     uint32_t store_shard_nums)
    : mshard_stores() {
      mshard_stores.reserve(count); // Reserve space for the shards
      for (size_t store_index = 0; store_index < count; ++store_index) {
        mshard_stores.emplace_back(std::make_unique<SeaStore::Shard>(
          root, dev, is_test, store_shard_nums, store_index));
      }
    }
    ~MultiShardStores() {
      mshard_stores.clear();
    }
  };
  std::string root;
  MDStoreRef mdstore;
  DeviceRef device;
  std::vector<DeviceRef> secondaries;
  seastar::sharded<SeaStore::MultiShardStores> shard_stores;
  uint32_t store_shard_nums = 0;

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
