// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

#include <sstream>
#include <string_view>

#include "crimson/os/seastore/logging.h"
#include "crimson/common/config_proxy.h"

// included for get_extent_by_type
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/object_data_handler.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "test/crimson/seastore/test_block.h"

using std::string_view;

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

Cache::Cache(
  ExtentReader &reader)
  : reader(reader),
    lru(crimson::common::get_conf<Option::size_t>(
	  "seastore_cache_lru_size"))
{
  register_metrics();
}

Cache::~Cache()
{
  LOG_PREFIX(Cache::~Cache);
  for (auto &i: extents) {
    ERROR("extent {} still alive", i);
  }
  ceph_assert(extents.empty());
}

Cache::retire_extent_ret Cache::retire_extent_addr(
  Transaction &t, paddr_t addr, extent_len_t length)
{
  assert(addr.is_real() && !addr.is_block_relative());

  LOG_PREFIX(Cache::retire_extent);
  CachedExtentRef ext;
  auto result = t.get_extent(addr, &ext);
  if (result == Transaction::get_extent_ret::PRESENT) {
    DEBUGT("found {} in t", t, addr);
    t.add_to_retired_set(CachedExtentRef(&*ext));
    return retire_extent_iertr::now();
  } else if (result == Transaction::get_extent_ret::RETIRED) {
    ERRORT("{} is already retired", t, addr);
    ceph_abort();
  }

  // any relative addr must have been on the transaction
  assert(!addr.is_relative());

  // absent from transaction
  // retiring is not included by the cache hit metrics
  ext = query_cache(addr, nullptr);
  if (ext) {
    if (ext->get_type() != extent_types_t::RETIRED_PLACEHOLDER) {
      t.add_to_read_set(ext);
      return trans_intr::make_interruptible(
        ext->wait_io()
      ).then_interruptible([&t, ext=std::move(ext)]() mutable {
        t.add_to_retired_set(ext);
        return retire_extent_iertr::now();
      });
    }
    // the retired-placeholder exists
  } else {
    // add a new placeholder to Cache
    ext = CachedExtent::make_cached_extent_ref<
      RetiredExtentPlaceholder>(length);
    ext->set_paddr(addr);
    ext->state = CachedExtent::extent_state_t::CLEAN;
    add_extent(ext);
  }

  // add the retired-placeholder to transaction
  t.add_to_read_set(ext);
  t.add_to_retired_set(ext);
  return retire_extent_iertr::now();
}

void Cache::dump_contents()
{
  LOG_PREFIX(Cache::dump_contents);
  DEBUG("enter");
  for (auto &&i: extents) {
    DEBUG("live {}", i);
  }
  DEBUG("exit");
}

void Cache::register_metrics()
{
  stats = {};

  namespace sm = seastar::metrics;
  using src_t = Transaction::src_t;

  auto src_label = sm::label("src");
  std::map<src_t, sm::label_instance> labels_by_src {
    {src_t::MUTATE,  src_label("MUTATE")},
    {src_t::READ,    src_label("READ")},
    {src_t::CLEANER_TRIM, src_label("CLEANER_TRIM")},
    {src_t::CLEANER_RECLAIM, src_label("CLEANER_RECLAIM")},
  };

  auto ext_label = sm::label("ext");
  std::map<extent_types_t, sm::label_instance> labels_by_ext {
    {extent_types_t::ROOT,                ext_label("ROOT")},
    {extent_types_t::LADDR_INTERNAL,      ext_label("LADDR_INTERNAL")},
    {extent_types_t::LADDR_LEAF,          ext_label("LADDR_LEAF")},
    {extent_types_t::OMAP_INNER,          ext_label("OMAP_INNER")},
    {extent_types_t::OMAP_LEAF,           ext_label("OMAP_LEAF")},
    {extent_types_t::ONODE_BLOCK_STAGED,  ext_label("ONODE_BLOCK_STAGED")},
    {extent_types_t::COLL_BLOCK,          ext_label("COLL_BLOCK")},
    {extent_types_t::OBJECT_DATA_BLOCK,   ext_label("OBJECT_DATA_BLOCK")},
    {extent_types_t::RETIRED_PLACEHOLDER, ext_label("RETIRED_PLACEHOLDER")},
    {extent_types_t::RBM_ALLOC_INFO,      ext_label("RBM_ALLOC_INFO")},
    {extent_types_t::TEST_BLOCK,          ext_label("TEST_BLOCK")},
    {extent_types_t::TEST_BLOCK_PHYSICAL, ext_label("TEST_BLOCK_PHYSICAL")}
  };

  /*
   * trans_created
   */
  for (auto& [src, src_label] : labels_by_src) {
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "trans_created",
          get_by_src(stats.trans_created_by_src, src),
          sm::description("total number of transaction created"),
          {src_label}
        ),
      }
    );
  }

  /*
   * cache_query: cache_access and cache_hit
   */
  for (auto& [src, src_label] : labels_by_src) {
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "cache_access",
          get_by_src(stats.cache_query_by_src, src).access,
          sm::description("total number of cache accesses"),
          {src_label}
        ),
        sm::make_counter(
          "cache_hit",
          get_by_src(stats.cache_query_by_src, src).hit,
          sm::description("total number of cache hits"),
          {src_label}
        ),
      }
    );
  }

  {
    /*
     * efforts discarded/committed
     */
    auto effort_label = sm::label("effort");

    // invalidated efforts
    using namespace std::literals::string_view_literals;
    const string_view invalidated_effort_names[] = {
      "READ"sv,
      "MUTATE"sv,
      "RETIRE"sv,
      "FRESH"sv,
      "FRESH_OOL_WRITTEN"sv,
    };
    for (auto& [src, src_label] : labels_by_src) {
      auto& efforts = get_by_src(stats.invalidated_efforts_by_src, src);
      for (auto& [ext, ext_label] : labels_by_ext) {
        auto& counter = get_by_ext(efforts.num_trans_invalidated, ext);
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "trans_invalidated",
              counter,
              sm::description("total number of transaction invalidated"),
              {src_label, ext_label}
            ),
          }
        );
      }

      if (src == src_t::READ) {
        // read transaction won't have non-read efforts
        auto read_effort_label = effort_label("READ");
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "invalidated_extents",
              efforts.read.extents,
              sm::description("extents of invalidated transactions"),
              {src_label, read_effort_label}
            ),
            sm::make_counter(
              "invalidated_extent_bytes",
              efforts.read.bytes,
              sm::description("extent bytes of invalidated transactions"),
              {src_label, read_effort_label}
            ),
          }
        );
        continue;
      }

      // non READ invalidated efforts
      for (auto& effort_name : invalidated_effort_names) {
        auto& effort = [&effort_name, &efforts]() -> effort_t& {
          if (effort_name == "READ") {
            return efforts.read;
          } else if (effort_name == "MUTATE") {
            return efforts.mutate;
          } else if (effort_name == "RETIRE") {
            return efforts.retire;
          } else if (effort_name == "FRESH") {
            return efforts.fresh;
          } else {
            assert(effort_name == "FRESH_OOL_WRITTEN");
            return efforts.fresh_ool_written;
          }
        }();
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "invalidated_extents",
              effort.extents,
              sm::description("extents of invalidated transactions"),
              {src_label, effort_label(effort_name)}
            ),
            sm::make_counter(
              "invalidated_extent_bytes",
              effort.bytes,
              sm::description("extent bytes of invalidated transactions"),
              {src_label, effort_label(effort_name)}
            ),
          }
        );
      } // effort_name

      metrics.add_group(
        "cache",
        {
          sm::make_counter(
            "invalidated_delta_bytes",
            efforts.mutate_delta_bytes,
            sm::description("delta bytes of invalidated transactions"),
            {src_label}
          ),
          sm::make_counter(
            "invalidated_ool_records",
            efforts.num_ool_records,
            sm::description("number of ool-records from invalidated transactions"),
            {src_label}
          ),
          sm::make_counter(
            "invalidated_ool_record_bytes",
            efforts.ool_record_bytes,
            sm::description("bytes of ool-record from invalidated transactions"),
            {src_label}
          ),
        }
      );
    } // src

    // committed efforts
    const string_view committed_effort_names[] = {
      "READ"sv,
      "MUTATE"sv,
      "RETIRE"sv,
      "FRESH_INVALID"sv,
      "FRESH_INLINE"sv,
      "FRESH_OOL"sv,
    };
    for (auto& [src, src_label] : labels_by_src) {
      if (src == src_t::READ) {
        // READ transaction won't commit
        continue;
      }
      auto& efforts = get_by_src(stats.committed_efforts_by_src, src);
      metrics.add_group(
        "cache",
        {
          sm::make_counter(
            "trans_committed",
            efforts.num_trans,
            sm::description("total number of transaction committed"),
            {src_label}
          ),
          sm::make_counter(
            "committed_ool_records",
            efforts.num_ool_records,
            sm::description("number of ool-records from committed transactions"),
            {src_label}
          ),
          sm::make_counter(
            "committed_ool_record_padding_bytes",
            efforts.ool_record_padding_bytes,
            sm::description("bytes of ool-record padding from committed transactions"),
            {src_label}
          ),
          sm::make_counter(
            "committed_ool_record_metadata_bytes",
            efforts.ool_record_metadata_bytes,
            sm::description("bytes of ool-record metadata from committed transactions"),
            {src_label}
          ),
          sm::make_counter(
            "committed_ool_record_data_bytes",
            efforts.ool_record_data_bytes,
            sm::description("bytes of ool-record data from committed transactions"),
            {src_label}
          ),
          sm::make_counter(
            "committed_inline_record_metadata_bytes",
            efforts.inline_record_metadata_bytes,
            sm::description("bytes of inline-record metadata from committed transactions"
                            "(excludes delta buffer)"),
            {src_label}
          ),
        }
      );
      for (auto& effort_name : committed_effort_names) {
        auto& effort_by_ext = [&efforts, &effort_name]()
            -> counter_by_extent_t<effort_t>& {
          if (effort_name == "READ") {
            return efforts.read_by_ext;
          } else if (effort_name == "MUTATE") {
            return efforts.mutate_by_ext;
          } else if (effort_name == "RETIRE") {
            return efforts.retire_by_ext;
          } else if (effort_name == "FRESH_INVALID") {
            return efforts.fresh_invalid_by_ext;
          } else if (effort_name == "FRESH_INLINE") {
            return efforts.fresh_inline_by_ext;
          } else {
            assert(effort_name == "FRESH_OOL");
            return efforts.fresh_ool_by_ext;
          }
        }();
        for (auto& [ext, ext_label] : labels_by_ext) {
          auto& effort = get_by_ext(effort_by_ext, ext);
          metrics.add_group(
            "cache",
            {
              sm::make_counter(
                "committed_extents",
                effort.extents,
                sm::description("extents of committed transactions"),
                {src_label, effort_label(effort_name), ext_label}
              ),
              sm::make_counter(
                "committed_extent_bytes",
                effort.bytes,
                sm::description("extent bytes of committed transactions"),
                {src_label, effort_label(effort_name), ext_label}
              ),
            }
          );
        } // ext
      } // effort_name

      auto& delta_by_ext = efforts.delta_bytes_by_ext;
      for (auto& [ext, ext_label] : labels_by_ext) {
        auto& value = get_by_ext(delta_by_ext, ext);
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "committed_delta_bytes",
              value,
              sm::description("delta bytes of committed transactions"),
              {src_label, ext_label}
            ),
          }
        );
      } // ext
    } // src

    // successful read efforts
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "trans_read_successful",
          stats.success_read_efforts.num_trans,
          sm::description("total number of successful read transactions")
        ),
        sm::make_counter(
          "successful_read_extents",
          stats.success_read_efforts.read.extents,
          sm::description("extents of successful read transactions")
        ),
        sm::make_counter(
          "successful_read_extent_bytes",
          stats.success_read_efforts.read.bytes,
          sm::description("extent bytes of successful read transactions")
        ),
      }
    );
  }

  /**
   * Cached extents (including placeholders)
   *
   * Dirty extents
   */
  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "cached_extents",
        [this] {
          return extents.size();
        },
        sm::description("total number of cached extents")
      ),
      sm::make_counter(
        "cached_extent_bytes",
        [this] {
          return extents.get_bytes();
        },
        sm::description("total bytes of cached extents")
      ),
      sm::make_counter(
        "dirty_extents",
        [this] {
          return dirty.size();
        },
        sm::description("total number of dirty extents")
      ),
      sm::make_counter(
        "dirty_extent_bytes",
        stats.dirty_bytes,
        sm::description("total bytes of dirty extents")
      ),
      sm::make_counter(
	"cache_lru_size_bytes",
	[this] {
	  return lru.get_current_contents_bytes();
	},
	sm::description("total bytes pinned by the lru")
      ),
      sm::make_counter(
	"cache_lru_size_extents",
	[this] {
	  return lru.get_current_contents_extents();
	},
	sm::description("total extents pinned by the lru")
      ),
    }
  );

  /**
   * tree stats
   */
  auto tree_label = sm::label("tree");
  auto onode_label = tree_label("ONODE");
  auto lba_label = tree_label("LBA");
  auto register_tree_metrics = [&labels_by_src, &onode_label, this](
      const sm::label_instance& tree_label,
      uint64_t& tree_depth,
      counter_by_src_t<tree_efforts_t>& committed_tree_efforts,
      counter_by_src_t<tree_efforts_t>& invalidated_tree_efforts) {
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "tree_depth",
          tree_depth,
          sm::description("the depth of tree"),
          {tree_label}
        ),
      }
    );
    for (auto& [src, src_label] : labels_by_src) {
      if (src == src_t::READ) {
        // READ transaction won't contain any tree inserts and erases
        continue;
      }
      if ((src == src_t::CLEANER_TRIM ||
           src == src_t::CLEANER_RECLAIM) &&
          tree_label == onode_label) {
        // CLEANER transaction won't contain any onode tree operations
        continue;
      }
      auto& committed_efforts = get_by_src(committed_tree_efforts, src);
      auto& invalidated_efforts = get_by_src(invalidated_tree_efforts, src);
      metrics.add_group(
        "cache",
        {
          sm::make_counter(
            "tree_inserts_committed",
            committed_efforts.num_inserts,
            sm::description("total number of committed insert operations"),
            {tree_label, src_label}
          ),
          sm::make_counter(
            "tree_erases_committed",
            committed_efforts.num_erases,
            sm::description("total number of committed erase operations"),
            {tree_label, src_label}
          ),
          sm::make_counter(
            "tree_inserts_invalidated",
            invalidated_efforts.num_inserts,
            sm::description("total number of invalidated insert operations"),
            {tree_label, src_label}
          ),
          sm::make_counter(
            "tree_erases_invalidated",
            invalidated_efforts.num_erases,
            sm::description("total number of invalidated erase operations"),
            {tree_label, src_label}
          ),
        }
      );
    }
  };
  register_tree_metrics(
      onode_label,
      stats.onode_tree_depth,
      stats.committed_onode_tree_efforts,
      stats.invalidated_onode_tree_efforts);
  register_tree_metrics(
      lba_label,
      stats.lba_tree_depth,
      stats.committed_lba_tree_efforts,
      stats.invalidated_lba_tree_efforts);

  /**
   * conflict combinations
   */
  auto srcs_label = sm::label("srcs");
  auto num_srcs = static_cast<std::size_t>(Transaction::src_t::MAX);
  std::size_t srcs_index = 0;
  for (uint8_t src2_int = 0; src2_int < num_srcs; ++src2_int) {
    auto src2 = static_cast<Transaction::src_t>(src2_int);
    for (uint8_t src1_int = src2_int; src1_int < num_srcs; ++src1_int) {
      ++srcs_index;
      auto src1 = static_cast<Transaction::src_t>(src1_int);
      // impossible combinations
      // should be consistent with checks in account_conflict()
      if ((src1 == Transaction::src_t::READ &&
           src2 == Transaction::src_t::READ) ||
          (src1 == Transaction::src_t::CLEANER_TRIM &&
           src2 == Transaction::src_t::CLEANER_TRIM) ||
          (src1 == Transaction::src_t::CLEANER_RECLAIM &&
           src2 == Transaction::src_t::CLEANER_RECLAIM) ||
          (src1 == Transaction::src_t::CLEANER_TRIM &&
           src2 == Transaction::src_t::CLEANER_RECLAIM)) {
        continue;
      }
      std::ostringstream oss;
      oss << src1 << "," << src2;
      metrics.add_group(
        "cache",
        {
          sm::make_counter(
            "trans_srcs_invalidated",
            stats.trans_conflicts_by_srcs[srcs_index - 1],
            sm::description("total number conflicted transactions by src pair"),
            {srcs_label(oss.str())}
          ),
        }
      );
    }
  }
  assert(srcs_index == NUM_SRC_COMB);
  srcs_index = 0;
  for (uint8_t src_int = 0; src_int < num_srcs; ++src_int) {
    ++srcs_index;
    auto src = static_cast<Transaction::src_t>(src_int);
    std::ostringstream oss;
    oss << "UNKNOWN," << src;
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "trans_srcs_invalidated",
          stats.trans_conflicts_by_unknown[srcs_index - 1],
          sm::description("total number conflicted transactions by src pair"),
          {srcs_label(oss.str())}
        ),
      }
    );
  }
}

void Cache::add_extent(CachedExtentRef ref)
{
  LOG_PREFIX(Cache::add_extent);
  assert(ref->is_valid());
  extents.insert(*ref);

  if (ref->is_dirty()) {
    add_to_dirty(ref);
  } else {
    touch_extent(*ref);
  }
  DEBUG("extent {}", *ref);
}

void Cache::mark_dirty(CachedExtentRef ref)
{
  LOG_PREFIX(Cache::mark_dirty);
  if (ref->is_dirty()) {
    assert(ref->primary_ref_list_hook.is_linked());
    return;
  }

  lru.remove_from_lru(*ref);
  add_to_dirty(ref);
  ref->state = CachedExtent::extent_state_t::DIRTY;

  DEBUG("extent: {}", *ref);
}

void Cache::add_to_dirty(CachedExtentRef ref)
{
  assert(ref->is_valid());
  assert(!ref->primary_ref_list_hook.is_linked());
  intrusive_ptr_add_ref(&*ref);
  dirty.push_back(*ref);
  stats.dirty_bytes += ref->get_length();
}

void Cache::remove_from_dirty(CachedExtentRef ref)
{
  if (ref->is_dirty()) {
    ceph_assert(ref->primary_ref_list_hook.is_linked());
    stats.dirty_bytes -= ref->get_length();
    dirty.erase(dirty.s_iterator_to(*ref));
    intrusive_ptr_release(&*ref);
  } else {
    ceph_assert(!ref->primary_ref_list_hook.is_linked());
  }
}

void Cache::remove_extent(CachedExtentRef ref)
{
  LOG_PREFIX(Cache::remove_extent);
  DEBUG("extent {}", *ref);
  assert(ref->is_valid());
  if (ref->is_dirty()) {
    remove_from_dirty(ref);
  } else {
    lru.remove_from_lru(*ref);
  }
  extents.erase(*ref);
}

void Cache::commit_retire_extent(
    Transaction& t,
    CachedExtentRef ref)
{
  LOG_PREFIX(Cache::commit_retire_extent);
  DEBUGT("extent {}", t, *ref);
  assert(ref->is_valid());

  // TODO: why does this duplicate remove_extent?
  if (ref->is_dirty()) {
    remove_from_dirty(ref);
  } else {
    lru.remove_from_lru(*ref);
  }
  ref->dirty_from_or_retired_at = JOURNAL_SEQ_MAX;

  invalidate_extent(t, *ref);
  extents.erase(*ref);
}

void Cache::commit_replace_extent(
    Transaction& t,
    CachedExtentRef next,
    CachedExtentRef prev)
{
  LOG_PREFIX(Cache::commit_replace_extent);
  DEBUGT("prev {}, next {}", t, *prev, *next);
  assert(next->get_paddr() == prev->get_paddr());
  assert(next->version == prev->version + 1);
  extents.replace(*next, *prev);

  if (prev->get_type() == extent_types_t::ROOT) {
    assert(prev->is_clean()
      || prev->primary_ref_list_hook.is_linked());
    if (prev->is_dirty()) {
      stats.dirty_bytes -= prev->get_length();
      dirty.erase(dirty.s_iterator_to(*prev));
      intrusive_ptr_release(&*prev);
    }
    add_to_dirty(next);
  } else if (prev->is_dirty()) {
    assert(prev->get_dirty_from() == next->get_dirty_from());
    assert(prev->primary_ref_list_hook.is_linked());
    auto prev_it = dirty.iterator_to(*prev);
    dirty.insert(prev_it, *next);
    dirty.erase(prev_it);
    intrusive_ptr_release(&*prev);
    intrusive_ptr_add_ref(&*next);
  } else {
    lru.remove_from_lru(*prev);
    add_to_dirty(next);
  }

  invalidate_extent(t, *prev);
}

void Cache::invalidate_extent(
    Transaction& t,
    CachedExtent& extent)
{
  LOG_PREFIX(Cache::invalidate);
  DEBUGT("conflict begin -- extent {}", t, extent);
  for (auto &&i: extent.transactions) {
    if (!i.t->conflicted) {
      assert(!i.t->is_weak());
      account_conflict(t.get_src(), i.t->get_src());
      mark_transaction_conflicted(*i.t, extent);
    }
  }
  DEBUGT("conflict end", t);
  extent.state = CachedExtent::extent_state_t::INVALID;
}

void Cache::mark_transaction_conflicted(
  Transaction& t, CachedExtent& conflicting_extent)
{
  LOG_PREFIX(Cache::mark_transaction_conflicted);
  assert(!t.conflicted);
  DEBUGT("set conflict", t);
  t.conflicted = true;

  auto& efforts = get_by_src(stats.invalidated_efforts_by_src,
                             t.get_src());

  auto& counter = get_by_ext(efforts.num_trans_invalidated,
                             conflicting_extent.get_type());
  ++counter;

  efforts.read.extents += t.read_set.size();
  for (auto &i: t.read_set) {
    efforts.read.bytes += i.ref->get_length();
  }

  if (t.get_src() != Transaction::src_t::READ) {
    efforts.retire.extents += t.retired_set.size();
    for (auto &i: t.retired_set) {
      efforts.retire.bytes += i->get_length();
    }

    auto& fresh_stats = t.get_fresh_block_stats();
    efforts.fresh.extents += fresh_stats.num;
    efforts.fresh.bytes += fresh_stats.bytes;

    for (auto &i: t.mutated_block_list) {
      if (!i->is_valid()) {
        continue;
      }
      DEBUGT("was mutating extent: {}", t, *i);
      efforts.mutate.increment(i->get_length());
      efforts.mutate_delta_bytes += i->get_delta().length();
    }

    auto& ool_stats = t.get_ool_write_stats();
    efforts.fresh_ool_written.extents += ool_stats.extents.num;
    efforts.fresh_ool_written.bytes += ool_stats.extents.bytes;
    efforts.num_ool_records += ool_stats.num_records;
    efforts.ool_record_bytes +=
      (ool_stats.header_bytes + ool_stats.data_bytes);
    // Note: we only account overhead from committed ool records

    if (t.get_src() == Transaction::src_t::CLEANER_TRIM ||
        t.get_src() == Transaction::src_t::CLEANER_RECLAIM) {
      // CLEANER transaction won't contain any onode tree operations
      assert(t.onode_tree_stats.is_clear());
    } else {
      get_by_src(stats.invalidated_onode_tree_efforts, t.get_src()
          ).increment(t.onode_tree_stats);
    }

    get_by_src(stats.invalidated_lba_tree_efforts, t.get_src()
        ).increment(t.lba_tree_stats);
  } else {
    // read transaction won't have non-read efforts
    assert(t.retired_set.empty());
    assert(t.get_fresh_block_stats().is_clear());
    assert(t.mutated_block_list.empty());
    assert(t.get_ool_write_stats().is_clear());
    assert(t.onode_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
  }
}

void Cache::on_transaction_destruct(Transaction& t)
{
  LOG_PREFIX(Cache::on_transaction_destruct);
  if (t.get_src() == Transaction::src_t::READ &&
      t.conflicted == false &&
      !t.is_weak()) {
    DEBUGT("read is successful", t);
    ++stats.success_read_efforts.num_trans;

    auto& effort = stats.success_read_efforts.read;
    effort.extents += t.read_set.size();
    for (auto &i: t.read_set) {
      effort.bytes += i.ref->get_length();
    }
    // read transaction won't have non-read efforts
    assert(t.retired_set.empty());
    assert(t.get_fresh_block_stats().num == 0);
    assert(t.mutated_block_list.empty());
    assert(t.onode_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
  }
}

CachedExtentRef Cache::alloc_new_extent_by_type(
  Transaction &t,       ///< [in, out] current transaction
  extent_types_t type,  ///< [in] type tag
  segment_off_t length, ///< [in] length
  bool delay 		///< [in] whether to delay paddr alloc
)
{
  switch (type) {
  case extent_types_t::ROOT:
    assert(0 == "ROOT is never directly alloc'd");
    return CachedExtentRef();
  case extent_types_t::LADDR_INTERNAL:
    return alloc_new_extent<lba_manager::btree::LBAInternalNode>(t, length, delay);
  case extent_types_t::LADDR_LEAF:
    return alloc_new_extent<lba_manager::btree::LBALeafNode>(t, length, delay);
  case extent_types_t::ONODE_BLOCK_STAGED:
    return alloc_new_extent<onode::SeastoreNodeExtent>(t, length, delay);
  case extent_types_t::OMAP_INNER:
    return alloc_new_extent<omap_manager::OMapInnerNode>(t, length, delay);
  case extent_types_t::OMAP_LEAF:
    return alloc_new_extent<omap_manager::OMapLeafNode>(t, length, delay);
  case extent_types_t::COLL_BLOCK:
    return alloc_new_extent<collection_manager::CollectionNode>(t, length, delay);
  case extent_types_t::OBJECT_DATA_BLOCK:
    return alloc_new_extent<ObjectDataBlock>(t, length, delay);
  case extent_types_t::RETIRED_PLACEHOLDER:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  case extent_types_t::TEST_BLOCK:
    return alloc_new_extent<TestBlock>(t, length, delay);
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return alloc_new_extent<TestBlockPhysical>(t, length, delay);
  case extent_types_t::NONE: {
    ceph_assert(0 == "NONE is an invalid extent type");
    return CachedExtentRef();
  }
  default:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  }
}

CachedExtentRef Cache::duplicate_for_write(
  Transaction &t,
  CachedExtentRef i) {
  LOG_PREFIX(Cache::duplicate_for_write);
  if (i->is_pending())
    return i;

  auto ret = i->duplicate_for_write();
  ret->prior_instance = i;
  t.add_mutated_extent(ret);
  if (ret->get_type() == extent_types_t::ROOT) {
    t.root = ret->cast<RootBlock>();
  } else {
    ret->last_committed_crc = i->last_committed_crc;
  }

  ret->version++;
  ret->state = CachedExtent::extent_state_t::MUTATION_PENDING;
  DEBUGT("{} -> {}", t, *i, *ret);
  return ret;
}

record_t Cache::prepare_record(Transaction &t)
{
  LOG_PREFIX(Cache::prepare_record);
  DEBUGT("enter", t);

  auto trans_src = t.get_src();
  assert(!t.is_weak());
  assert(trans_src != Transaction::src_t::READ);

  auto& efforts = get_by_src(stats.committed_efforts_by_src,
                             trans_src);

  // Should be valid due to interruptible future
  for (auto &i: t.read_set) {
    if (!i.ref->is_valid()) {
      DEBUGT("read_set invalid extent: {}, aborting", t, *i.ref);
      ceph_abort("no invalid extent allowed in transactions' read_set");
    }
    get_by_ext(efforts.read_by_ext,
               i.ref->get_type()).increment(i.ref->get_length());
  }
  DEBUGT("read_set validated", t);
  t.read_set.clear();

  record_t record;

  t.write_set.clear();

  // Add new copy of mutated blocks, set_io_wait to block until written
  record.deltas.reserve(t.mutated_block_list.size());
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      DEBUGT("ignoring invalid {}", t, *i);
      continue;
    }
    DEBUGT("mutating {}", t, *i);
    get_by_ext(efforts.mutate_by_ext,
               i->get_type()).increment(i->get_length());

    assert(i->prior_instance);
    commit_replace_extent(t, i, i->prior_instance);

    i->prepare_write();
    i->set_io_wait();

    assert(i->get_version() > 0);
    auto final_crc = i->get_crc32c();
    if (i->get_type() == extent_types_t::ROOT) {
      root = t.root;
      DEBUGT("writing out root delta for {}", t, *t.root);
      record.push_back(
	delta_info_t{
	  extent_types_t::ROOT,
	  paddr_t{},
	  L_ADDR_NULL,
	  0,
	  0,
	  0,
	  t.root->get_version() - 1,
	  t.root->get_delta()
	});
    } else {
      record.push_back(
	delta_info_t{
	  i->get_type(),
	  i->get_paddr(),
	  (i->is_logical()
	   ? i->cast<LogicalCachedExtent>()->get_laddr()
	   : L_ADDR_NULL),
	  i->last_committed_crc,
	  final_crc,
	  (segment_off_t)i->get_length(),
	  i->get_version() - 1,
	  i->get_delta()
	});
      i->last_committed_crc = final_crc;
    }
    auto delta_length = record.deltas.back().bl.length();
    assert(delta_length);
    get_by_ext(efforts.delta_bytes_by_ext,
               i->get_type()) += delta_length;
  }

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  for (auto &i: t.retired_set) {
    DEBUGT("retiring {}", t, *i);
    get_by_ext(efforts.retire_by_ext,
               i->get_type()).increment(i->get_length());
    commit_retire_extent(t, i);
    if (i->backend_type == device_type_t::RANDOM_BLOCK) {
      paddr_t paddr = i->get_paddr();
      rbm_alloc_delta_t delta;
      delta.op = rbm_alloc_delta_t::op_types_t::CLEAR;
      delta.alloc_blk_ranges.push_back(std::make_pair(paddr, i->get_length()));
      t.add_rbm_alloc_info_blocks(delta);
    }
  }

  record.extents.reserve(t.inline_block_list.size());
  for (auto &i: t.inline_block_list) {
    if (!i->is_valid()) {
      DEBUGT("fresh inline block (invalid) {}", t, *i);
      get_by_ext(efforts.fresh_invalid_by_ext,
                 i->get_type()).increment(i->get_length());
    } else {
      DEBUGT("fresh inline block {}", t, *i);
    }
    get_by_ext(efforts.fresh_inline_by_ext,
               i->get_type()).increment(i->get_length());
    assert(i->is_inline());

    bufferlist bl;
    i->prepare_write();
    bl.append(i->get_bptr());
    if (i->get_type() == extent_types_t::ROOT) {
      assert(0 == "ROOT never gets written as a fresh block");
    }

    assert(bl.length() == i->get_length());
    record.push_back(extent_t{
	i->get_type(),
	i->is_logical()
	? i->cast<LogicalCachedExtent>()->get_laddr()
	: (is_lba_node(i->get_type())
	  ? i->cast<lba_manager::btree::LBANode>()->get_node_meta().begin
	  : L_ADDR_NULL),
	std::move(bl)
      });
  }

  for (auto b : t.rbm_alloc_info_blocks) {
    bufferlist bl;
    encode(b, bl);
    delta_info_t delta;
    delta.type = extent_types_t::RBM_ALLOC_INFO;
    delta.bl = bl;
    record.push_back(std::move(delta));
  }

  for (auto &i: t.ool_block_list) {
    ceph_assert(i->is_valid());
    DEBUGT("fresh ool block {}", t, *i);
    get_by_ext(efforts.fresh_ool_by_ext,
               i->get_type()).increment(i->get_length());
  }

  ceph_assert(t.get_fresh_block_stats().num ==
              t.inline_block_list.size() +
              t.ool_block_list.size() +
              t.num_delayed_invalid_extents);

  auto& ool_stats = t.get_ool_write_stats();
  ceph_assert(ool_stats.extents.num == t.ool_block_list.size());

  if (record.is_empty()) {
    // XXX: improve osd logic to not submit empty transactions.
    DEBUGT("record to submit is empty, src={}!", t, trans_src);
    assert(t.onode_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
    assert(ool_stats.is_clear());
  }

  DEBUGT("record is ready to submit, src={}, mdsize={}, dsize={}; "
         "{} ool records, mdsize={}, dsize={}, fillness={}",
         t, trans_src,
         record.size.get_raw_mdlength(),
         record.size.dlength,
         ool_stats.num_records,
         ool_stats.header_raw_bytes,
         ool_stats.data_bytes,
         ((double)(ool_stats.header_raw_bytes + ool_stats.data_bytes) /
          (ool_stats.header_bytes + ool_stats.data_bytes)));
  if (trans_src == Transaction::src_t::CLEANER_TRIM ||
      trans_src == Transaction::src_t::CLEANER_RECLAIM) {
    // CLEANER transaction won't contain any onode tree operations
    assert(t.onode_tree_stats.is_clear());
  } else {
    if (t.onode_tree_stats.depth) {
      stats.onode_tree_depth = t.onode_tree_stats.depth;
    }
    get_by_src(stats.committed_onode_tree_efforts, trans_src
        ).increment(t.onode_tree_stats);
  }

  if (t.lba_tree_stats.depth) {
    stats.lba_tree_depth = t.lba_tree_stats.depth;
  }
  get_by_src(stats.committed_lba_tree_efforts, trans_src
      ).increment(t.lba_tree_stats);

  ++(efforts.num_trans);
  efforts.num_ool_records += ool_stats.num_records;
  efforts.ool_record_padding_bytes +=
    (ool_stats.header_bytes - ool_stats.header_raw_bytes);
  efforts.ool_record_metadata_bytes += ool_stats.header_raw_bytes;
  efforts.ool_record_data_bytes += ool_stats.data_bytes;
  efforts.inline_record_metadata_bytes +=
    (record.size.get_raw_mdlength() - record.get_delta_size());

  return record;
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_block_start,
  journal_seq_t seq,
  SegmentCleaner *cleaner)
{
  LOG_PREFIX(Cache::complete_commit);
  DEBUGT("enter", t);

  t.for_each_fresh_block([&](auto &i) {
    if (i->is_inline()) {
      i->set_paddr(final_block_start.add_relative(i->get_paddr()));
    }
    i->last_committed_crc = i->get_crc32c();
    i->on_initial_write();

    if (i->is_valid()) {
      i->state = CachedExtent::extent_state_t::CLEAN;
      DEBUGT("fresh {}", t, *i);
      add_extent(i);
      if (cleaner) {
	cleaner->mark_space_used(
	  i->get_paddr(),
	  i->get_length());
      }
    }
  });

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    DEBUGT("mutated {}", t, *i);
    assert(i->prior_instance);
    i->on_delta_write(final_block_start);
    i->prior_instance = CachedExtentRef();
    i->state = CachedExtent::extent_state_t::DIRTY;
    if (i->version == 1 || i->get_type() == extent_types_t::ROOT) {
      i->dirty_from_or_retired_at = seq;
    }
  }

  if (cleaner) {
    for (auto &i: t.retired_set) {
      cleaner->mark_space_free(
	i->get_paddr(),
	i->get_length());
    }
  }

  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    i->complete_io();
  }

  last_commit = seq;
  for (auto &i: t.retired_set) {
    DEBUGT("retiring {}", t, *i);
    i->dirty_from_or_retired_at = last_commit;
  }
}

void Cache::init() {
  if (root) {
    // initial creation will do mkfs followed by mount each of which calls init
    remove_extent(root);
    root = nullptr;
  }
  root = new RootBlock();
  root->state = CachedExtent::extent_state_t::CLEAN;
  extents.insert(*root);
}

Cache::mkfs_iertr::future<> Cache::mkfs(Transaction &t)
{
  return get_root(t).si_then([this, &t](auto croot) {
    duplicate_for_write(t, croot);
    return mkfs_iertr::now();
  }).handle_error_interruptible(
    mkfs_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in Cache::mkfs"
    }
  );
}

Cache::close_ertr::future<> Cache::close()
{
  root.reset();
  for (auto i = dirty.begin(); i != dirty.end(); ) {
    auto ptr = &*i;
    stats.dirty_bytes -= ptr->get_length();
    dirty.erase(i++);
    intrusive_ptr_release(ptr);
  }
  assert(stats.dirty_bytes == 0);
  clear_lru();
  return close_ertr::now();
}

Cache::replay_delta_ret
Cache::replay_delta(
  journal_seq_t journal_seq,
  paddr_t record_base,
  const delta_info_t &delta)
{
  LOG_PREFIX(Cache::replay_delta);
  if (delta.type == extent_types_t::ROOT) {
    DEBUG("found root delta");
    remove_extent(root);
    root->apply_delta_and_adjust_crc(record_base, delta.bl);
    root->dirty_from_or_retired_at = journal_seq;
    root->state = CachedExtent::extent_state_t::DIRTY;
    add_extent(root);
    return replay_delta_ertr::now();
  } else {
    auto _get_extent_if_cached = [this](paddr_t addr)
      -> get_extent_ertr::future<CachedExtentRef> {
      // replay is not included by the cache hit metrics
      auto ret = query_cache(addr, nullptr);
      if (ret) {
        // no retired-placeholder should be exist yet because no transaction
        // has been created.
        assert(ret->get_type() != extent_types_t::RETIRED_PLACEHOLDER);
        return ret->wait_io().then([ret] {
          return ret;
        });
      } else {
        return seastar::make_ready_future<CachedExtentRef>();
      }
    };
    auto extent_fut = (delta.pversion == 0 ?
      // replay is not included by the cache hit metrics
      _get_extent_by_type(
        delta.type,
        delta.paddr,
        delta.laddr,
        delta.length,
        nullptr,
        [](CachedExtent &) {}) :
      _get_extent_if_cached(
	delta.paddr)
    ).handle_error(
      replay_delta_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in Cache::replay_delta"
      }
    );
    return extent_fut.safe_then([=, &delta](auto extent) {
      if (!extent) {
	assert(delta.pversion > 0);
	DEBUG(
	  "replaying {}, extent not present so delta is obsolete",
	  delta);
	return;
      }

      DEBUG("replaying {} on {}", *extent, delta);

      assert(extent->version == delta.pversion);

      assert(extent->last_committed_crc == delta.prev_crc);
      extent->apply_delta_and_adjust_crc(record_base, delta.bl);
      assert(extent->last_committed_crc == delta.final_crc);

      if (extent->version == 0) {
	extent->dirty_from_or_retired_at = journal_seq;
      }
      extent->version++;
      mark_dirty(extent);
    });
  }
}

Cache::get_next_dirty_extents_ret Cache::get_next_dirty_extents(
  Transaction &t,
  journal_seq_t seq,
  size_t max_bytes)
{
  LOG_PREFIX(Cache::get_next_dirty_extents);
  std::vector<CachedExtentRef> cand;
  size_t bytes_so_far = 0;
  for (auto i = dirty.begin();
       i != dirty.end() && bytes_so_far < max_bytes;
       ++i) {
    if (i->get_dirty_from() != journal_seq_t() && i->get_dirty_from() < seq) {
      DEBUGT("next {}", t, *i);
      if (!(cand.empty() ||
	    cand.back()->get_dirty_from() <= i->get_dirty_from())) {
	ERRORT("last {}, next {}", t, *cand.back(), *i);
      }
      assert(cand.empty() || cand.back()->get_dirty_from() <= i->get_dirty_from());
      bytes_so_far += i->get_length();
      cand.push_back(&*i);

    } else {
      break;
    }
  }
  return seastar::do_with(
    std::move(cand),
    decltype(cand)(),
    [FNAME, this, &t](auto &cand, auto &ret) {
      return trans_intr::do_for_each(
	cand,
	[FNAME, this, &t, &ret](auto &ext) {
	  DEBUG("waiting on {}", *ext);

	  return trans_intr::make_interruptible(
	    ext->wait_io()
	  ).then_interruptible([FNAME, this, ext, &t, &ret] {
	    if (!ext->is_valid()) {
	      ++(get_by_src(stats.trans_conflicts_by_unknown, t.get_src()));
	      mark_transaction_conflicted(t, *ext);
	      return;
	    }

	    CachedExtentRef on_transaction;
	    auto result = t.get_extent(ext->get_paddr(), &on_transaction);
	    if (result == Transaction::get_extent_ret::ABSENT) {
	      DEBUGT("{} absent on t", t, *ext);
	      t.add_to_read_set(ext);
	      if (ext->get_type() == extent_types_t::ROOT) {
		if (t.root) {
		  assert(&*t.root == &*ext);
		  assert(0 == "t.root would have to already be in the read set");
		} else {
		  assert(&*ext == &*root);
		  t.root = root;
		}
	      }
	      ret.push_back(ext);
	    } else if (result == Transaction::get_extent_ret::PRESENT) {
	      DEBUGT("{} present on t as {}", t, *ext, *on_transaction);
	      ret.push_back(on_transaction);
	    } else {
	      assert(result == Transaction::get_extent_ret::RETIRED);
	      DEBUGT("{} retired on t", t, *ext);
	    }
	  });
	}).then_interruptible([&ret] {
	  return std::move(ret);
	});
    });
}

Cache::get_root_ret Cache::get_root(Transaction &t)
{
  LOG_PREFIX(Cache::get_root);
  if (t.root) {
    DEBUGT("root already on transaction {}", t, *t.root);
    return get_root_iertr::make_ready_future<RootBlockRef>(
      t.root);
  } else {
    auto ret = root;
    DEBUGT("waiting root {}", t, *ret);
    return ret->wait_io().then([this, FNAME, ret, &t] {
      DEBUGT("got root read {}", t, *ret);
      if (!ret->is_valid()) {
	DEBUGT("root became invalid: {}", t, *ret);
	++(get_by_src(stats.trans_conflicts_by_unknown, t.get_src()));
	mark_transaction_conflicted(t, *ret);
	return get_root_iertr::make_ready_future<RootBlockRef>();
      } else {
	t.root = ret;
	t.add_to_read_set(ret);
	return get_root_iertr::make_ready_future<RootBlockRef>(
	  ret);
      }
    });
  }
}

Cache::get_extent_ertr::future<CachedExtentRef> Cache::_get_extent_by_type(
  extent_types_t type,
  paddr_t offset,
  laddr_t laddr,
  segment_off_t length,
  const Transaction::src_t* p_src,
  extent_init_func_t &&extent_init_func)
{
  return [=, extent_init_func=std::move(extent_init_func)]() mutable {
    src_ext_t* p_metric_key = nullptr;
    src_ext_t metric_key;
    if (p_src) {
      metric_key = std::make_pair(*p_src, type);
      p_metric_key = &metric_key;
    }

    switch (type) {
    case extent_types_t::ROOT:
      assert(0 == "ROOT is never directly read");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::LADDR_INTERNAL:
      return get_extent<lba_manager::btree::LBAInternalNode>(
	offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::LADDR_LEAF:
      return get_extent<lba_manager::btree::LBALeafNode>(
	offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_INNER:
      return get_extent<omap_manager::OMapInnerNode>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_LEAF:
      return get_extent<omap_manager::OMapLeafNode>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::COLL_BLOCK:
      return get_extent<collection_manager::CollectionNode>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::ONODE_BLOCK_STAGED:
      return get_extent<onode::SeastoreNodeExtent>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OBJECT_DATA_BLOCK:
      return get_extent<ObjectDataBlock>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::RETIRED_PLACEHOLDER:
      ceph_assert(0 == "impossible");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::TEST_BLOCK:
      return get_extent<TestBlock>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::TEST_BLOCK_PHYSICAL:
      return get_extent<TestBlockPhysical>(
          offset, length, p_metric_key, std::move(extent_init_func)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::NONE: {
      ceph_assert(0 == "NONE is an invalid extent type");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    }
    default:
      ceph_assert(0 == "impossible");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    }
  }().safe_then([laddr](CachedExtentRef e) {
    assert(e->is_logical() == (laddr != L_ADDR_NULL));
    if (e->is_logical()) {
      e->cast<LogicalCachedExtent>()->set_laddr(laddr);
    }
    return get_extent_ertr::make_ready_future<CachedExtentRef>(e);
  });
}

}
