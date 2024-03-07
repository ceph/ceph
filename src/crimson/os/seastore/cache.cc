// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"

#include <sstream>
#include <string_view>

#include <seastar/core/metrics.hh>

#include "crimson/os/seastore/logging.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/async_cleaner.h"

// included for get_extent_by_type
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/object_data_handler.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "test/crimson/seastore/test_block.h"

using std::string_view;

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const backref_entry_t &ent) {
  return out << "backref_entry_t{"
	     << ent.paddr << "~" << ent.len << ", "
	     << "laddr: " << ent.laddr << ", "
	     << "type: " << ent.type << ", "
	     << "seq: " << ent.seq << ", "
	     << "}";
}

Cache::Cache(
  ExtentPlacementManager &epm)
  : epm(epm),
    lru(crimson::common::get_conf<Option::size_t>(
	  "seastore_cache_lru_size"))
{
  LOG_PREFIX(Cache::Cache);
  INFO("created, lru_size={}", lru.get_capacity());
  register_metrics();
  segment_providers_by_device_id.resize(DEVICE_ID_MAX, nullptr);
}

Cache::~Cache()
{
  LOG_PREFIX(Cache::~Cache);
  for (auto &i: extents) {
    ERROR("extent is still alive -- {}", i);
  }
  ceph_assert(extents.empty());
}

Cache::retire_extent_ret Cache::retire_extent_addr(
  Transaction &t, paddr_t addr, extent_len_t length)
{
  LOG_PREFIX(Cache::retire_extent_addr);
  TRACET("retire {}~{}", t, addr, length);

  assert(addr.is_real() && !addr.is_block_relative());

  CachedExtentRef ext;
  auto result = t.get_extent(addr, &ext);
  if (result == Transaction::get_extent_ret::PRESENT) {
    DEBUGT("retire {}~{} on t -- {}", t, addr, length, *ext);
    t.add_to_retired_set(CachedExtentRef(&*ext));
    return retire_extent_iertr::now();
  } else if (result == Transaction::get_extent_ret::RETIRED) {
    ERRORT("retire {}~{} failed, already retired -- {}", t, addr, length, *ext);
    ceph_abort();
  }

  // any relative addr must have been on the transaction
  assert(!addr.is_relative());

  // absent from transaction
  // retiring is not included by the cache hit metrics
  ext = query_cache(addr, nullptr);
  if (ext) {
    DEBUGT("retire {}~{} in cache -- {}", t, addr, length, *ext);
  } else {
    // add a new placeholder to Cache
    ext = CachedExtent::make_cached_extent_ref<
      RetiredExtentPlaceholder>(length);
    ext->init(CachedExtent::extent_state_t::CLEAN,
              addr,
              PLACEMENT_HINT_NULL,
              NULL_GENERATION,
	      TRANS_ID_NULL);
    DEBUGT("retire {}~{} as placeholder, add extent -- {}",
           t, addr, length, *ext);
    const auto t_src = t.get_src();
    add_extent(ext, &t_src);
  }
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
  LOG_PREFIX(Cache::register_metrics);
  DEBUG("");

  stats = {};

  namespace sm = seastar::metrics;
  using src_t = Transaction::src_t;

  std::map<src_t, sm::label_instance> labels_by_src {
    {src_t::MUTATE, sm::label_instance("src", "MUTATE")},
    {src_t::READ, sm::label_instance("src", "READ")},
    {src_t::TRIM_DIRTY, sm::label_instance("src", "TRIM_DIRTY")},
    {src_t::TRIM_ALLOC, sm::label_instance("src", "TRIM_ALLOC")},
    {src_t::CLEANER_MAIN, sm::label_instance("src", "CLEANER_MAIN")},
    {src_t::CLEANER_COLD, sm::label_instance("src", "CLEANER_COLD")},
  };
  assert(labels_by_src.size() == (std::size_t)src_t::MAX);

  std::map<extent_types_t, sm::label_instance> labels_by_ext {
    {extent_types_t::ROOT,                sm::label_instance("ext", "ROOT")},
    {extent_types_t::LADDR_INTERNAL,      sm::label_instance("ext", "LADDR_INTERNAL")},
    {extent_types_t::LADDR_LEAF,          sm::label_instance("ext", "LADDR_LEAF")},
    {extent_types_t::DINK_LADDR_LEAF,     sm::label_instance("ext", "DINK_LADDR_LEAF")},
    {extent_types_t::OMAP_INNER,          sm::label_instance("ext", "OMAP_INNER")},
    {extent_types_t::OMAP_LEAF,           sm::label_instance("ext", "OMAP_LEAF")},
    {extent_types_t::ONODE_BLOCK_STAGED,  sm::label_instance("ext", "ONODE_BLOCK_STAGED")},
    {extent_types_t::COLL_BLOCK,          sm::label_instance("ext", "COLL_BLOCK")},
    {extent_types_t::OBJECT_DATA_BLOCK,   sm::label_instance("ext", "OBJECT_DATA_BLOCK")},
    {extent_types_t::RETIRED_PLACEHOLDER, sm::label_instance("ext", "RETIRED_PLACEHOLDER")},
    {extent_types_t::ALLOC_INFO,      	  sm::label_instance("ext", "ALLOC_INFO")},
    {extent_types_t::JOURNAL_TAIL,        sm::label_instance("ext", "JOURNAL_TAIL")},
    {extent_types_t::TEST_BLOCK,          sm::label_instance("ext", "TEST_BLOCK")},
    {extent_types_t::TEST_BLOCK_PHYSICAL, sm::label_instance("ext", "TEST_BLOCK_PHYSICAL")},
    {extent_types_t::BACKREF_INTERNAL,    sm::label_instance("ext", "BACKREF_INTERNAL")},
    {extent_types_t::BACKREF_LEAF,        sm::label_instance("ext", "BACKREF_LEAF")}
  };
  assert(labels_by_ext.size() == (std::size_t)extent_types_t::NONE);

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
              "trans_invalidated_by_extent",
              counter,
              sm::description("total number of transactions invalidated by extents"),
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
              efforts.read.num,
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
        auto& effort = [&effort_name, &efforts]() -> io_stat_t& {
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
              effort.num,
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
            "trans_invalidated",
            efforts.total_trans_invalidated,
            sm::description("total number of transactions invalidated"),
            {src_label}
          ),
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
            -> counter_by_extent_t<io_stat_t>& {
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
                effort.num,
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
          stats.success_read_efforts.read.num,
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
  auto omap_label = tree_label("OMAP");
  auto lba_label = tree_label("LBA");
  auto backref_label = tree_label("BACKREF");
  auto register_tree_metrics = [&labels_by_src, &onode_label, &omap_label, this](
      const sm::label_instance& tree_label,
      uint64_t& tree_depth,
      int64_t& tree_extents_num,
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
	sm::make_counter(
	  "tree_extents_num",
	  tree_extents_num,
	  sm::description("num of extents of the tree"),
	  {tree_label}
	)
      }
    );
    for (auto& [src, src_label] : labels_by_src) {
      if (src == src_t::READ) {
        // READ transaction won't contain any tree inserts and erases
        continue;
      }
      if (is_background_transaction(src) &&
          (tree_label == onode_label ||
           tree_label == omap_label)) {
        // CLEANER transaction won't contain any onode/omap tree operations
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
            "tree_updates_committed",
            committed_efforts.num_updates,
            sm::description("total number of committed update operations"),
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
          sm::make_counter(
            "tree_updates_invalidated",
            invalidated_efforts.num_updates,
            sm::description("total number of invalidated update operations"),
            {tree_label, src_label}
          ),
        }
      );
    }
  };
  register_tree_metrics(
      onode_label,
      stats.onode_tree_depth,
      stats.onode_tree_extents_num,
      stats.committed_onode_tree_efforts,
      stats.invalidated_onode_tree_efforts);
  register_tree_metrics(
      omap_label,
      stats.omap_tree_depth,
      stats.omap_tree_extents_num,
      stats.committed_omap_tree_efforts,
      stats.invalidated_omap_tree_efforts);
  register_tree_metrics(
      lba_label,
      stats.lba_tree_depth,
      stats.lba_tree_extents_num,
      stats.committed_lba_tree_efforts,
      stats.invalidated_lba_tree_efforts);
  register_tree_metrics(
      backref_label,
      stats.backref_tree_depth,
      stats.backref_tree_extents_num,
      stats.committed_backref_tree_efforts,
      stats.invalidated_backref_tree_efforts);

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
          (src1 == Transaction::src_t::TRIM_DIRTY &&
           src2 == Transaction::src_t::TRIM_DIRTY) ||
          (src1 == Transaction::src_t::CLEANER_MAIN &&
           src2 == Transaction::src_t::CLEANER_MAIN) ||
          (src1 == Transaction::src_t::CLEANER_COLD &&
           src2 == Transaction::src_t::CLEANER_COLD) ||
          (src1 == Transaction::src_t::TRIM_ALLOC &&
           src2 == Transaction::src_t::TRIM_ALLOC)) {
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

  /**
   * rewrite version
   */
  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "version_count_dirty",
        stats.committed_dirty_version.num,
        sm::description("total number of rewrite-dirty extents")
      ),
      sm::make_counter(
        "version_sum_dirty",
        stats.committed_dirty_version.version,
        sm::description("sum of the version from rewrite-dirty extents")
      ),
      sm::make_counter(
        "version_count_reclaim",
        stats.committed_reclaim_version.num,
        sm::description("total number of rewrite-reclaim extents")
      ),
      sm::make_counter(
        "version_sum_reclaim",
        stats.committed_reclaim_version.version,
        sm::description("sum of the version from rewrite-reclaim extents")
      ),
    }
  );
}

void Cache::add_extent(
    CachedExtentRef ref,
    const Transaction::src_t* p_src=nullptr)
{
  assert(ref->is_valid());
  assert(ref->user_hint == PLACEMENT_HINT_NULL);
  assert(ref->rewrite_generation == NULL_GENERATION);
  extents.insert(*ref);
  if (ref->is_dirty()) {
    add_to_dirty(ref);
  } else {
    touch_extent(*ref, p_src);
  }
}

void Cache::mark_dirty(CachedExtentRef ref)
{
  if (ref->is_dirty()) {
    assert(ref->primary_ref_list_hook.is_linked());
    return;
  }

  lru.remove_from_lru(*ref);
  ref->state = CachedExtent::extent_state_t::DIRTY;
  add_to_dirty(ref);
}

void Cache::add_to_dirty(CachedExtentRef ref)
{
  assert(ref->is_dirty());
  assert(!ref->primary_ref_list_hook.is_linked());
  ceph_assert(ref->get_modify_time() != NULL_TIME);
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
  assert(ref->is_valid());
  if (ref->is_dirty()) {
    remove_from_dirty(ref);
  } else if (!ref->is_placeholder()) {
    lru.remove_from_lru(*ref);
  }
  extents.erase(*ref);
}

void Cache::commit_retire_extent(
    Transaction& t,
    CachedExtentRef ref)
{
  remove_extent(ref);

  ref->dirty_from_or_retired_at = JOURNAL_SEQ_NULL;
  invalidate_extent(t, *ref);
}

void Cache::commit_replace_extent(
    Transaction& t,
    CachedExtentRef next,
    CachedExtentRef prev)
{
  assert(next->is_dirty());
  assert(next->get_paddr() == prev->get_paddr());
  assert(next->version == prev->version + 1);
  extents.replace(*next, *prev);

  if (prev->get_type() == extent_types_t::ROOT) {
    assert(prev->is_stable_clean()
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

  next->on_replace_prior(t);
  invalidate_extent(t, *prev);
}

void Cache::invalidate_extent(
    Transaction& t,
    CachedExtent& extent)
{
  if (!extent.may_conflict()) {
    assert(extent.transactions.empty());
    extent.set_invalid(t);
    return;
  }

  LOG_PREFIX(Cache::invalidate_extent);
  bool do_conflict_log = true;
  for (auto &&i: extent.transactions) {
    if (!i.t->conflicted) {
      if (do_conflict_log) {
        SUBDEBUGT(seastore_t, "conflict begin -- {}", t, extent);
        do_conflict_log = false;
      }
      assert(!i.t->is_weak());
      account_conflict(t.get_src(), i.t->get_src());
      mark_transaction_conflicted(*i.t, extent);
    }
  }
  extent.set_invalid(t);
}

void Cache::mark_transaction_conflicted(
  Transaction& t, CachedExtent& conflicting_extent)
{
  LOG_PREFIX(Cache::mark_transaction_conflicted);
  SUBTRACET(seastore_t, "", t);
  assert(!t.conflicted);
  t.conflicted = true;

  auto& efforts = get_by_src(stats.invalidated_efforts_by_src,
                             t.get_src());
  ++efforts.total_trans_invalidated;

  auto& counter = get_by_ext(efforts.num_trans_invalidated,
                             conflicting_extent.get_type());
  ++counter;

  io_stat_t read_stat;
  for (auto &i: t.read_set) {
    read_stat.increment(i.ref->get_length());
  }
  efforts.read.increment_stat(read_stat);

  if (t.get_src() != Transaction::src_t::READ) {
    io_stat_t retire_stat;
    for (auto &i: t.retired_set) {
      retire_stat.increment(i->get_length());
    }
    efforts.retire.increment_stat(retire_stat);

    auto& fresh_stat = t.get_fresh_block_stats();
    efforts.fresh.increment_stat(fresh_stat);

    io_stat_t delta_stat;
    for (auto &i: t.mutated_block_list) {
      if (!i->is_valid()) {
        continue;
      }
      efforts.mutate.increment(i->get_length());
      delta_stat.increment(i->get_delta().length());
    }
    efforts.mutate_delta_bytes += delta_stat.bytes;

    for (auto &i: t.pre_alloc_list) {
      epm.mark_space_free(i->get_paddr(), i->get_length());
    }

    auto& ool_stats = t.get_ool_write_stats();
    efforts.fresh_ool_written.increment_stat(ool_stats.extents);
    efforts.num_ool_records += ool_stats.num_records;
    auto ool_record_bytes = (ool_stats.md_bytes + ool_stats.get_data_bytes());
    efforts.ool_record_bytes += ool_record_bytes;

    if (is_background_transaction(t.get_src())) {
      // CLEANER transaction won't contain any onode/omap tree operations
      assert(t.onode_tree_stats.is_clear());
      assert(t.omap_tree_stats.is_clear());
    } else {
      get_by_src(stats.invalidated_onode_tree_efforts, t.get_src()
          ).increment(t.onode_tree_stats);
      get_by_src(stats.invalidated_omap_tree_efforts, t.get_src()
          ).increment(t.omap_tree_stats);
    }

    get_by_src(stats.invalidated_lba_tree_efforts, t.get_src()
        ).increment(t.lba_tree_stats);
    get_by_src(stats.invalidated_backref_tree_efforts, t.get_src()
        ).increment(t.backref_tree_stats);

    SUBDEBUGT(seastore_t,
        "discard {} read, {} fresh, {} delta, {} retire, {}({}B) ool-records",
        t,
        read_stat,
        fresh_stat,
        delta_stat,
        retire_stat,
        ool_stats.num_records,
        ool_record_bytes);
  } else {
    // read transaction won't have non-read efforts
    assert(t.retired_set.empty());
    assert(t.get_fresh_block_stats().is_clear());
    assert(t.mutated_block_list.empty());
    assert(t.get_ool_write_stats().is_clear());
    assert(t.onode_tree_stats.is_clear());
    assert(t.omap_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
    assert(t.backref_tree_stats.is_clear());
    SUBDEBUGT(seastore_t, "discard {} read", t, read_stat);
  }
}

void Cache::on_transaction_destruct(Transaction& t)
{
  LOG_PREFIX(Cache::on_transaction_destruct);
  SUBTRACET(seastore_t, "", t);
  if (t.get_src() == Transaction::src_t::READ &&
      t.conflicted == false) {
    io_stat_t read_stat;
    for (auto &i: t.read_set) {
      read_stat.increment(i.ref->get_length());
    }
    SUBDEBUGT(seastore_t, "done {} read", t, read_stat);

    if (!t.is_weak()) {
      // exclude weak transaction as it is impossible to conflict
      ++stats.success_read_efforts.num_trans;
      stats.success_read_efforts.read.increment_stat(read_stat);
    }

    // read transaction won't have non-read efforts
    assert(t.retired_set.empty());
    assert(t.get_fresh_block_stats().is_clear());
    assert(t.mutated_block_list.empty());
    assert(t.onode_tree_stats.is_clear());
    assert(t.omap_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
    assert(t.backref_tree_stats.is_clear());
  }
}

CachedExtentRef Cache::alloc_new_extent_by_type(
  Transaction &t,        ///< [in, out] current transaction
  extent_types_t type,   ///< [in] type tag
  extent_len_t length,   ///< [in] length
  placement_hint_t hint, ///< [in] user hint
  rewrite_gen_t gen      ///< [in] rewrite generation
)
{
  LOG_PREFIX(Cache::alloc_new_extent_by_type);
  SUBDEBUGT(seastore_cache, "allocate {} {}B, hint={}, gen={}",
            t, type, length, hint, rewrite_gen_printer_t{gen});
  ceph_assert(get_extent_category(type) == data_category_t::METADATA);
  switch (type) {
  case extent_types_t::ROOT:
    ceph_assert(0 == "ROOT is never directly alloc'd");
    return CachedExtentRef();
  case extent_types_t::LADDR_INTERNAL:
    return alloc_new_non_data_extent<lba_manager::btree::LBAInternalNode>(t, length, hint, gen);
  case extent_types_t::LADDR_LEAF:
    return alloc_new_non_data_extent<lba_manager::btree::LBALeafNode>(
      t, length, hint, gen);
  case extent_types_t::ONODE_BLOCK_STAGED:
    return alloc_new_non_data_extent<onode::SeastoreNodeExtent>(
      t, length, hint, gen);
  case extent_types_t::OMAP_INNER:
    return alloc_new_non_data_extent<omap_manager::OMapInnerNode>(
      t, length, hint, gen);
  case extent_types_t::OMAP_LEAF:
    return alloc_new_non_data_extent<omap_manager::OMapLeafNode>(
      t, length, hint, gen);
  case extent_types_t::COLL_BLOCK:
    return alloc_new_non_data_extent<collection_manager::CollectionNode>(
      t, length, hint, gen);
  case extent_types_t::RETIRED_PLACEHOLDER:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return alloc_new_non_data_extent<TestBlockPhysical>(t, length, hint, gen);
  case extent_types_t::NONE: {
    ceph_assert(0 == "NONE is an invalid extent type");
    return CachedExtentRef();
  }
  default:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  }
}

std::vector<CachedExtentRef> Cache::alloc_new_data_extents_by_type(
  Transaction &t,        ///< [in, out] current transaction
  extent_types_t type,   ///< [in] type tag
  extent_len_t length,   ///< [in] length
  placement_hint_t hint, ///< [in] user hint
  rewrite_gen_t gen      ///< [in] rewrite generation
)
{
  LOG_PREFIX(Cache::alloc_new_data_extents_by_type);
  SUBDEBUGT(seastore_cache, "allocate {} {}B, hint={}, gen={}",
            t, type, length, hint, rewrite_gen_printer_t{gen});
  ceph_assert(get_extent_category(type) == data_category_t::DATA);
  std::vector<CachedExtentRef> res;
  switch (type) {
  case extent_types_t::OBJECT_DATA_BLOCK:
    {
      auto extents = alloc_new_data_extents<ObjectDataBlock>(t, length, hint, gen);
      res.insert(res.begin(), extents.begin(), extents.end());
    }
    return res;
  case extent_types_t::TEST_BLOCK:
    {
      auto extents = alloc_new_data_extents<TestBlock>(t, length, hint, gen);
      res.insert(res.begin(), extents.begin(), extents.end());
    }
    return res;
  default:
    ceph_assert(0 == "impossible");
    return res;
  }
}

CachedExtentRef Cache::duplicate_for_write(
  Transaction &t,
  CachedExtentRef i) {
  LOG_PREFIX(Cache::duplicate_for_write);
  assert(i->is_fully_loaded());

  if (i->is_mutable())
    return i;

  if (i->is_exist_clean()) {
    i->version++;
    i->state = CachedExtent::extent_state_t::EXIST_MUTATION_PENDING;
    i->last_committed_crc = i->get_crc32c();
    // deepcopy the buffer of exist clean extent beacuse it shares
    // buffer with original clean extent.
    auto bp = i->get_bptr();
    auto nbp = ceph::bufferptr(bp.c_str(), bp.length());
    i->set_bptr(std::move(nbp));

    t.add_mutated_extent(i);
    DEBUGT("duplicate existing extent {}", t, *i);
    return i;
  }

  auto ret = i->duplicate_for_write(t);
  ret->pending_for_transaction = t.get_trans_id();
  ret->prior_instance = i;
  // duplicate_for_write won't occur after ool write finished
  assert(!i->prior_poffset);
  auto [iter, inserted] = i->mutation_pendings.insert(*ret);
  ceph_assert(inserted);
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

record_t Cache::prepare_record(
  Transaction &t,
  const journal_seq_t &journal_head,
  const journal_seq_t &journal_dirty_tail)
{
  LOG_PREFIX(Cache::prepare_record);
  SUBTRACET(seastore_t, "enter", t);

  auto trans_src = t.get_src();
  assert(!t.is_weak());
  assert(trans_src != Transaction::src_t::READ);

  auto& efforts = get_by_src(stats.committed_efforts_by_src,
                             trans_src);

  // Should be valid due to interruptible future
  io_stat_t read_stat;
  for (auto &i: t.read_set) {
    if (!i.ref->is_valid()) {
      SUBERRORT(seastore_t,
          "read_set got invalid extent, aborting -- {}", t, *i.ref);
      ceph_abort("no invalid extent allowed in transactions' read_set");
    }
    get_by_ext(efforts.read_by_ext,
               i.ref->get_type()).increment(i.ref->get_length());
    read_stat.increment(i.ref->get_length());
  }
  t.read_set.clear();
  t.write_set.clear();

  record_t record(trans_src);
  auto commit_time = seastar::lowres_system_clock::now();

  // Add new copy of mutated blocks, set_io_wait to block until written
  record.deltas.reserve(t.mutated_block_list.size());
  io_stat_t delta_stat;
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      DEBUGT("invalid mutated extent -- {}", t, *i);
      continue;
    }
    assert(i->is_exist_mutation_pending() ||
	   i->prior_instance);
    get_by_ext(efforts.mutate_by_ext,
               i->get_type()).increment(i->get_length());

    auto delta_bl = i->get_delta();
    auto delta_length = delta_bl.length();
    i->set_modify_time(commit_time);
    DEBUGT("mutated extent with {}B delta -- {}",
	   t, delta_length, *i);
    if (!i->is_exist_mutation_pending()) {
      DEBUGT("commit replace extent ... -- {}, prior={}",
	     t, *i, *i->prior_instance);
      // If inplace rewrite occurs during mutation, prev->version will
      // be zero. Although this results in the version mismatch here, we can
      // correct this by changing version to 1. This is because the inplace rewrite
      // does not introduce any actual modification that could negatively
      // impact system reliability
      if (i->prior_instance->version == 0 && i->version > 1) {
	assert(can_inplace_rewrite(i->get_type()));
	assert(can_inplace_rewrite(i->prior_instance->get_type()));
	assert(i->prior_instance->dirty_from_or_retired_at == JOURNAL_SEQ_MIN);
	assert(i->prior_instance->state == CachedExtent::extent_state_t::CLEAN);
	assert(i->prior_instance->get_paddr().get_addr_type() ==
	  paddr_types_t::RANDOM_BLOCK);
	i->version = 1;
      }
      // extent with EXIST_MUTATION_PENDING doesn't have
      // prior_instance field so skip these extents.
      // the existing extents should be added into Cache
      // during complete_commit to sync with gc transaction.
      commit_replace_extent(t, i, i->prior_instance);
    }

    i->prepare_write();
    i->set_io_wait();
    i->prepare_commit();

    assert(i->get_version() > 0);
    auto final_crc = i->get_crc32c();
    if (i->get_type() == extent_types_t::ROOT) {
      SUBTRACET(seastore_t, "writing out root delta {}B -- {}",
                t, delta_length, *i);
      assert(t.root == i);
      root = t.root;
      record.push_back(
	delta_info_t{
	  extent_types_t::ROOT,
	  P_ADDR_NULL,
	  L_ADDR_NULL,
	  0,
	  0,
	  0,
	  t.root->get_version() - 1,
	  MAX_SEG_SEQ,
	  segment_type_t::NULL_SEG,
	  std::move(delta_bl)
	});
    } else {
      auto sseq = NULL_SEG_SEQ;
      auto stype = segment_type_t::NULL_SEG;

      // FIXME: This is specific to the segmented implementation
      if (i->get_paddr().get_addr_type() == paddr_types_t::SEGMENT) {
        auto sid = i->get_paddr().as_seg_paddr().get_segment_id();
        auto sinfo = get_segment_info(sid);
        if (sinfo) {
          sseq = sinfo->seq;
          stype = sinfo->type;
        }
      }

      record.push_back(
	delta_info_t{
	  i->get_type(),
	  i->get_paddr(),
	  (i->is_logical()
	   ? i->cast<LogicalCachedExtent>()->get_laddr()
	   : L_ADDR_NULL),
	  i->last_committed_crc,
	  final_crc,
	  i->get_length(),
	  i->get_version() - 1,
	  sseq,
	  stype,
	  std::move(delta_bl)
	});
      i->last_committed_crc = final_crc;
    }
    assert(delta_length);
    get_by_ext(efforts.delta_bytes_by_ext,
               i->get_type()) += delta_length;
    delta_stat.increment(delta_length);
  }

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  io_stat_t retire_stat;
  std::vector<alloc_delta_t> alloc_deltas;
  alloc_delta_t rel_delta;
  rel_delta.op = alloc_delta_t::op_types_t::CLEAR;
  for (auto &i: t.retired_set) {
    get_by_ext(efforts.retire_by_ext,
               i->get_type()).increment(i->get_length());
    retire_stat.increment(i->get_length());
    DEBUGT("retired and remove extent -- {}", t, *i);
    commit_retire_extent(t, i);
    if (is_backref_mapped_extent_node(i)
	  || is_retired_placeholder(i->get_type())) {
      rel_delta.alloc_blk_ranges.emplace_back(
	i->get_paddr(),
	L_ADDR_NULL,
	i->get_length(),
	i->get_type());
    }
  }
  alloc_deltas.emplace_back(std::move(rel_delta));

  record.extents.reserve(t.inline_block_list.size());
  io_stat_t fresh_stat;
  io_stat_t fresh_invalid_stat;
  alloc_delta_t alloc_delta;
  alloc_delta.op = alloc_delta_t::op_types_t::SET;
  for (auto &i: t.inline_block_list) {
    if (!i->is_valid()) {
      DEBUGT("invalid fresh inline extent -- {}", t, *i);
      fresh_invalid_stat.increment(i->get_length());
      get_by_ext(efforts.fresh_invalid_by_ext,
                 i->get_type()).increment(i->get_length());
    } else {
      TRACET("fresh inline extent -- {}", t, *i);
    }
    fresh_stat.increment(i->get_length());
    get_by_ext(efforts.fresh_inline_by_ext,
               i->get_type()).increment(i->get_length());
    assert(i->is_inline() || i->get_paddr().is_fake());

    bufferlist bl;
    i->prepare_write();
    i->prepare_commit();
    bl.append(i->get_bptr());
    if (i->get_type() == extent_types_t::ROOT) {
      ceph_assert(0 == "ROOT never gets written as a fresh block");
    }

    assert(bl.length() == i->get_length());
    auto modify_time = i->get_modify_time();
    if (modify_time == NULL_TIME) {
      modify_time = commit_time;
    }
    record.push_back(extent_t{
	i->get_type(),
	i->is_logical()
	? i->cast<LogicalCachedExtent>()->get_laddr()
	: (is_lba_node(i->get_type())
	  ? i->cast<lba_manager::btree::LBANode>()->get_node_meta().begin
	  : L_ADDR_NULL),
	std::move(bl)
      },
      modify_time);
    if (i->is_valid()
	&& is_backref_mapped_extent_node(i)) {
      alloc_delta.alloc_blk_ranges.emplace_back(
	i->get_paddr(),
	i->is_logical()
	? i->cast<LogicalCachedExtent>()->get_laddr()
	: (is_lba_node(i->get_type())
	  ? i->cast<lba_manager::btree::LBANode>()->get_node_meta().begin
	  : L_ADDR_NULL),
	i->get_length(),
	i->get_type());
    }
  }

  for (auto &i: t.written_ool_block_list) {
    TRACET("fresh ool extent -- {}", t, *i);
    ceph_assert(i->is_valid());
    assert(!i->is_inline());
    get_by_ext(efforts.fresh_ool_by_ext,
               i->get_type()).increment(i->get_length());
    i->prepare_commit();
    if (is_backref_mapped_extent_node(i)) {
      alloc_delta.alloc_blk_ranges.emplace_back(
	i->get_paddr(),
	i->is_logical()
	? i->cast<LogicalCachedExtent>()->get_laddr()
	: i->cast<lba_manager::btree::LBANode>()->get_node_meta().begin,
	i->get_length(),
	i->get_type());
    }
  }

  for (auto &i: t.written_inplace_ool_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    assert(i->state == CachedExtent::extent_state_t::DIRTY);
    assert(i->version > 0);
    remove_from_dirty(i);
    // set the version to zero because the extent state is now clean
    // in order to handle this transparently
    i->version = 0;
    i->dirty_from_or_retired_at = JOURNAL_SEQ_MIN;
    i->state = CachedExtent::extent_state_t::CLEAN;
    assert(i->is_logical());
    i->clear_modified_region();
    touch_extent(*i);
    DEBUGT("inplace rewrite ool block is commmitted -- {}", t, *i);
  }

  for (auto &i: t.existing_block_list) {
    if (i->is_valid()) {
      alloc_delta.alloc_blk_ranges.emplace_back(
        i->get_paddr(),
	i->cast<LogicalCachedExtent>()->get_laddr(),
	i->get_length(),
	i->get_type());
    }
  }
  alloc_deltas.emplace_back(std::move(alloc_delta));

  for (auto b : alloc_deltas) {
    bufferlist bl;
    encode(b, bl);
    delta_info_t delta;
    delta.type = extent_types_t::ALLOC_INFO;
    delta.bl = bl;
    record.push_back(std::move(delta));
  }

  if (is_background_transaction(trans_src)) {
    assert(journal_head != JOURNAL_SEQ_NULL);
    assert(journal_dirty_tail != JOURNAL_SEQ_NULL);
    journal_seq_t dirty_tail;
    auto maybe_dirty_tail = get_oldest_dirty_from();
    if (!maybe_dirty_tail.has_value()) {
      dirty_tail = journal_head;
      SUBINFOT(seastore_t, "dirty_tail all trimmed, set to head {}, src={}",
               t, dirty_tail, trans_src);
    } else if (*maybe_dirty_tail == JOURNAL_SEQ_NULL) {
      dirty_tail = journal_dirty_tail;
      SUBINFOT(seastore_t, "dirty_tail is pending, set to {}, src={}",
               t, dirty_tail, trans_src);
    } else {
      dirty_tail = *maybe_dirty_tail;
    }
    ceph_assert(dirty_tail != JOURNAL_SEQ_NULL);
    journal_seq_t alloc_tail;
    auto maybe_alloc_tail = get_oldest_backref_dirty_from();
    if (!maybe_alloc_tail.has_value()) {
      // FIXME: the replay point of the allocations requires to be accurate.
      // Setting the alloc_tail to get_journal_head() cannot skip replaying the
      // last unnecessary record.
      alloc_tail = journal_head;
      SUBINFOT(seastore_t, "alloc_tail all trimmed, set to head {}, src={}",
               t, alloc_tail, trans_src);
    } else if (*maybe_alloc_tail == JOURNAL_SEQ_NULL) {
      ceph_abort("impossible");
    } else {
      alloc_tail = *maybe_alloc_tail;
    }
    ceph_assert(alloc_tail != JOURNAL_SEQ_NULL);
    auto tails = journal_tail_delta_t{alloc_tail, dirty_tail};
    SUBDEBUGT(seastore_t, "update tails as delta {}", t, tails);
    bufferlist bl;
    encode(tails, bl);
    delta_info_t delta;
    delta.type = extent_types_t::JOURNAL_TAIL;
    delta.bl = bl;
    record.push_back(std::move(delta));
  }

  ceph_assert(t.get_fresh_block_stats().num ==
              t.inline_block_list.size() +
              t.written_ool_block_list.size() +
              t.num_delayed_invalid_extents +
	      t.num_allocated_invalid_extents);

  auto& ool_stats = t.get_ool_write_stats();
  ceph_assert(ool_stats.extents.num == t.written_ool_block_list.size() +
    t.written_inplace_ool_block_list.size());

  if (record.is_empty()) {
    SUBINFOT(seastore_t,
        "record to submit is empty, src={}", t, trans_src);
    assert(t.onode_tree_stats.is_clear());
    assert(t.omap_tree_stats.is_clear());
    assert(t.lba_tree_stats.is_clear());
    assert(t.backref_tree_stats.is_clear());
    assert(ool_stats.is_clear());
  }

  if (record.modify_time == NULL_TIME) {
    record.modify_time = commit_time;
  }

  SUBDEBUGT(seastore_t,
      "commit H{} dirty_from={}, alloc_from={}, "
      "{} read, {} fresh with {} invalid, "
      "{} delta, {} retire, {}(md={}B, data={}B) ool-records, "
      "{}B md, {}B data, modify_time={}",
      t, (void*)&t.get_handle(),
      get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL),
      get_oldest_backref_dirty_from().value_or(JOURNAL_SEQ_NULL),
      read_stat,
      fresh_stat,
      fresh_invalid_stat,
      delta_stat,
      retire_stat,
      ool_stats.num_records,
      ool_stats.md_bytes,
      ool_stats.get_data_bytes(),
      record.size.get_raw_mdlength(),
      record.size.dlength,
      sea_time_point_printer_t{record.modify_time});
  if (is_background_transaction(trans_src)) {
    // background transaction won't contain any onode tree operations
    assert(t.onode_tree_stats.is_clear());
    assert(t.omap_tree_stats.is_clear());
  } else {
    if (t.onode_tree_stats.depth) {
      stats.onode_tree_depth = t.onode_tree_stats.depth;
    }
    if (t.omap_tree_stats.depth) {
      stats.omap_tree_depth = t.omap_tree_stats.depth;
    }
    stats.onode_tree_extents_num += t.onode_tree_stats.extents_num_delta;
    ceph_assert(stats.onode_tree_extents_num >= 0);
    get_by_src(stats.committed_onode_tree_efforts, trans_src
        ).increment(t.onode_tree_stats);
    stats.omap_tree_extents_num += t.omap_tree_stats.extents_num_delta;
    ceph_assert(stats.omap_tree_extents_num >= 0);
    get_by_src(stats.committed_omap_tree_efforts, trans_src
        ).increment(t.omap_tree_stats);
  }

  if (t.lba_tree_stats.depth) {
    stats.lba_tree_depth = t.lba_tree_stats.depth;
  }
  stats.lba_tree_extents_num += t.lba_tree_stats.extents_num_delta;
  ceph_assert(stats.lba_tree_extents_num >= 0);
  get_by_src(stats.committed_lba_tree_efforts, trans_src
      ).increment(t.lba_tree_stats);
  if (t.backref_tree_stats.depth) {
    stats.backref_tree_depth = t.backref_tree_stats.depth;
  }
  stats.backref_tree_extents_num += t.backref_tree_stats.extents_num_delta;
  ceph_assert(stats.backref_tree_extents_num >= 0);
  get_by_src(stats.committed_backref_tree_efforts, trans_src
      ).increment(t.backref_tree_stats);

  ++(efforts.num_trans);
  efforts.num_ool_records += ool_stats.num_records;
  efforts.ool_record_metadata_bytes += ool_stats.md_bytes;
  efforts.ool_record_data_bytes += ool_stats.get_data_bytes();
  efforts.inline_record_metadata_bytes +=
    (record.size.get_raw_mdlength() - record.get_delta_size());

  auto &rewrite_version_stats = t.get_rewrite_version_stats();
  if (trans_src == Transaction::src_t::TRIM_DIRTY) {
    stats.committed_dirty_version.increment_stat(rewrite_version_stats);
  } else if (trans_src == Transaction::src_t::CLEANER_MAIN ||
             trans_src == Transaction::src_t::CLEANER_COLD) {
    stats.committed_reclaim_version.increment_stat(rewrite_version_stats);
  } else {
    assert(rewrite_version_stats.is_clear());
  }

  return record;
}

void Cache::backref_batch_update(
  std::vector<backref_entry_ref> &&list,
  const journal_seq_t &seq)
{
  LOG_PREFIX(Cache::backref_batch_update);
  DEBUG("inserting {} entries at {}", list.size(), seq);
  ceph_assert(seq != JOURNAL_SEQ_NULL);

  for (auto &ent : list) {
    backref_entry_mset.insert(*ent);
  }

  auto iter = backref_entryrefs_by_seq.find(seq);
  if (iter == backref_entryrefs_by_seq.end()) {
    backref_entryrefs_by_seq.emplace(seq, std::move(list));
  } else {
    iter->second.insert(
      iter->second.end(),
      std::make_move_iterator(list.begin()),
      std::make_move_iterator(list.end()));
  }
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_block_start,
  journal_seq_t start_seq)
{
  LOG_PREFIX(Cache::complete_commit);
  SUBTRACET(seastore_t, "final_block_start={}, start_seq={}",
            t, final_block_start, start_seq);

  std::vector<backref_entry_ref> backref_list;
  t.for_each_fresh_block([&](const CachedExtentRef &i) {
    if (!i->is_valid()) {
      return;
    }

    bool is_inline = false;
    if (i->is_inline()) {
      is_inline = true;
      i->set_paddr(final_block_start.add_relative(i->get_paddr()));
    }
    i->last_committed_crc = i->get_crc32c();
    i->pending_for_transaction = TRANS_ID_NULL;
    i->on_initial_write();

    i->state = CachedExtent::extent_state_t::CLEAN;
    DEBUGT("add extent as fresh, inline={} -- {}",
	   t, is_inline, *i);
    const auto t_src = t.get_src();
    i->invalidate_hints();
    add_extent(i, &t_src);
    epm.commit_space_used(i->get_paddr(), i->get_length());
    if (is_backref_mapped_extent_node(i)) {
      DEBUGT("backref_list new {} len {}",
	     t,
	     i->get_paddr(),
	     i->get_length());
      backref_list.emplace_back(
	std::make_unique<backref_entry_t>(
	  i->get_paddr(),
	  i->is_logical()
	  ? i->cast<LogicalCachedExtent>()->get_laddr()
	  : (is_lba_node(i->get_type())
	    ? i->cast<lba_manager::btree::LBANode>()->get_node_meta().begin
	    : L_ADDR_NULL),
	  i->get_length(),
	  i->get_type(),
	  start_seq));
    } else if (is_backref_node(i->get_type())) {
	add_backref_extent(
	  i->get_paddr(),
	  i->cast<backref::BackrefNode>()->get_node_meta().begin,
	  i->get_type());
    } else {
      ERRORT("{}", t, *i);
      ceph_abort("not possible");
    }
  });

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    assert(i->is_exist_mutation_pending() ||
	   i->prior_instance);
    i->on_delta_write(final_block_start);
    i->pending_for_transaction = TRANS_ID_NULL;
    i->prior_instance = CachedExtentRef();
    i->state = CachedExtent::extent_state_t::DIRTY;
    assert(i->version > 0);
    if (i->version == 1 || i->get_type() == extent_types_t::ROOT) {
      i->dirty_from_or_retired_at = start_seq;
      DEBUGT("commit extent done, become dirty -- {}", t, *i);
    } else {
      DEBUGT("commit extent done -- {}", t, *i);
    }
  }

  for (auto &i: t.retired_set) {
    epm.mark_space_free(i->get_paddr(), i->get_length());
  }
  for (auto &i: t.existing_block_list) {
    if (i->is_valid()) {
      epm.mark_space_used(i->get_paddr(), i->get_length());
    }
  }

  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    i->complete_io();
  }

  last_commit = start_seq;
  for (auto &i: t.retired_set) {
    i->dirty_from_or_retired_at = start_seq;
    if (is_backref_mapped_extent_node(i)
	  || is_retired_placeholder(i->get_type())) {
      DEBUGT("backref_list free {} len {}",
	     t,
	     i->get_paddr(),
	     i->get_length());
      backref_list.emplace_back(
	std::make_unique<backref_entry_t>(
	  i->get_paddr(),
	  L_ADDR_NULL,
	  i->get_length(),
	  i->get_type(),
	  start_seq));
    } else if (is_backref_node(i->get_type())) {
      remove_backref_extent(i->get_paddr());
    } else {
      ERRORT("{}", t, *i);
      ceph_abort("not possible");
    }
  }

  auto existing_stats = t.get_existing_block_stats();
  DEBUGT("total existing blocks num: {}, exist clean num: {}, "
	 "exist mutation pending num: {}",
	 t,
	 existing_stats.valid_num,
	 existing_stats.clean_num,
	 existing_stats.mutated_num);
  for (auto &i: t.existing_block_list) {
    if (i->is_valid()) {
      if (i->is_exist_clean()) {
	i->state = CachedExtent::extent_state_t::CLEAN;
      } else {
	assert(i->state == CachedExtent::extent_state_t::DIRTY);
      }
      DEBUGT("backref_list new existing {} len {}",
	     t,
	     i->get_paddr(),
	     i->get_length());
      backref_list.emplace_back(
        std::make_unique<backref_entry_t>(
	  i->get_paddr(),
	  i->cast<LogicalCachedExtent>()->get_laddr(),
	  i->get_length(),
	  i->get_type(),
	  start_seq));
      const auto t_src = t.get_src();
      add_extent(i, &t_src);
    }
  }
  if (!backref_list.empty()) {
    backref_batch_update(std::move(backref_list), start_seq);
  }

  for (auto &i: t.pre_alloc_list) {
    if (!i->is_valid()) {
      epm.mark_space_free(i->get_paddr(), i->get_length());
    }
  }
}

void Cache::init()
{
  LOG_PREFIX(Cache::init);
  if (root) {
    // initial creation will do mkfs followed by mount each of which calls init
    DEBUG("remove extent -- prv_root={}", *root);
    remove_extent(root);
    root = nullptr;
  }
  root = new RootBlock();
  root->init(CachedExtent::extent_state_t::CLEAN,
             P_ADDR_ROOT,
             PLACEMENT_HINT_NULL,
             NULL_GENERATION,
	     TRANS_ID_NULL);
  INFO("init root -- {}", *root);
  extents.insert(*root);
}

Cache::mkfs_iertr::future<> Cache::mkfs(Transaction &t)
{
  LOG_PREFIX(Cache::mkfs);
  INFOT("create root", t);
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
  LOG_PREFIX(Cache::close);
  INFO("close with {}({}B) dirty, dirty_from={}, alloc_from={}, "
       "{}({}B) lru, totally {}({}B) indexed extents",
       dirty.size(),
       stats.dirty_bytes,
       get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL),
       get_oldest_backref_dirty_from().value_or(JOURNAL_SEQ_NULL),
       lru.get_current_contents_extents(),
       lru.get_current_contents_bytes(),
       extents.size(),
       extents.get_bytes());
  root.reset();
  for (auto i = dirty.begin(); i != dirty.end(); ) {
    auto ptr = &*i;
    stats.dirty_bytes -= ptr->get_length();
    dirty.erase(i++);
    intrusive_ptr_release(ptr);
  }
  backref_extents.clear();
  backref_entryrefs_by_seq.clear();
  assert(stats.dirty_bytes == 0);
  lru.clear();
  return close_ertr::now();
}

Cache::replay_delta_ret
Cache::replay_delta(
  journal_seq_t journal_seq,
  paddr_t record_base,
  const delta_info_t &delta,
  const journal_seq_t &dirty_tail,
  const journal_seq_t &alloc_tail,
  sea_time_point modify_time)
{
  LOG_PREFIX(Cache::replay_delta);
  assert(dirty_tail != JOURNAL_SEQ_NULL);
  assert(alloc_tail != JOURNAL_SEQ_NULL);
  ceph_assert(modify_time != NULL_TIME);

  // FIXME: This is specific to the segmented implementation
  /* The journal may validly contain deltas for extents in
   * since released segments.  We can detect those cases by
   * checking whether the segment in question currently has a
   * sequence number > the current journal segment seq. We can
   * safetly skip these deltas because the extent must already
   * have been rewritten.
   */
  if (delta.paddr != P_ADDR_NULL &&
      delta.paddr.get_addr_type() == paddr_types_t::SEGMENT) {
    auto& seg_addr = delta.paddr.as_seg_paddr();
    auto seg_info = get_segment_info(seg_addr.get_segment_id());
    if (seg_info) {
      auto delta_paddr_segment_seq = seg_info->seq;
      auto delta_paddr_segment_type = seg_info->type;
      if (delta_paddr_segment_seq != delta.ext_seq ||
          delta_paddr_segment_type != delta.seg_type) {
        DEBUG("delta is obsolete, delta_paddr_segment_seq={},"
              " delta_paddr_segment_type={} -- {}",
              segment_seq_printer_t{delta_paddr_segment_seq},
              delta_paddr_segment_type,
              delta);
        return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
	  std::make_pair(false, nullptr));
      }
    }
  }

  if (delta.type == extent_types_t::JOURNAL_TAIL) {
    // this delta should have been dealt with during segment cleaner mounting
    return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
      std::make_pair(false, nullptr));
  }

  // replay alloc
  if (delta.type == extent_types_t::ALLOC_INFO) {
    if (journal_seq < alloc_tail) {
      DEBUG("journal_seq {} < alloc_tail {}, don't replay {}",
	journal_seq, alloc_tail, delta);
      return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
	std::make_pair(false, nullptr));
    }

    alloc_delta_t alloc_delta;
    decode(alloc_delta, delta.bl);
    std::vector<backref_entry_ref> backref_list;
    for (auto &alloc_blk : alloc_delta.alloc_blk_ranges) {
      if (alloc_blk.paddr.is_relative()) {
	assert(alloc_blk.paddr.is_record_relative());
	alloc_blk.paddr = record_base.add_relative(alloc_blk.paddr);
      }
      DEBUG("replay alloc_blk {}~{} {}, journal_seq: {}",
	alloc_blk.paddr, alloc_blk.len, alloc_blk.laddr, journal_seq);
      backref_list.emplace_back(
	std::make_unique<backref_entry_t>(
	  alloc_blk.paddr,
	  alloc_blk.laddr,
	  alloc_blk.len,
	  alloc_blk.type,
	  journal_seq));
    }
    if (!backref_list.empty()) {
      backref_batch_update(std::move(backref_list), journal_seq);
    }
    return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
      std::make_pair(true, nullptr));
  }

  // replay dirty
  if (journal_seq < dirty_tail) {
    DEBUG("journal_seq {} < dirty_tail {}, don't replay {}",
      journal_seq, dirty_tail, delta);
    return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
      std::make_pair(false, nullptr));
  }

  if (delta.type == extent_types_t::ROOT) {
    TRACE("replay root delta at {} {}, remove extent ... -- {}, prv_root={}",
          journal_seq, record_base, delta, *root);
    remove_extent(root);
    root->apply_delta_and_adjust_crc(record_base, delta.bl);
    root->dirty_from_or_retired_at = journal_seq;
    root->state = CachedExtent::extent_state_t::DIRTY;
    DEBUG("replayed root delta at {} {}, add extent -- {}, root={}",
          journal_seq, record_base, delta, *root);
    root->set_modify_time(modify_time);
    add_extent(root);
    return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
      std::make_pair(true, root));
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
      do_get_caching_extent_by_type(
        delta.type,
        delta.paddr,
        delta.laddr,
        delta.length,
        nullptr,
        [](CachedExtent &) {},
        [](CachedExtent &) {}) :
      _get_extent_if_cached(
	delta.paddr)
    ).handle_error(
      replay_delta_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in Cache::replay_delta"
      }
    );
    return extent_fut.safe_then([=, this, &delta](auto extent) {
      if (!extent) {
	DEBUG("replay extent is not present, so delta is obsolete at {} {} -- {}",
	      journal_seq, record_base, delta);
	assert(delta.pversion > 0);
	return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
	  std::make_pair(false, nullptr));
      }

      DEBUG("replay extent delta at {} {} ... -- {}, prv_extent={}",
            journal_seq, record_base, delta, *extent);

      if (delta.paddr.get_addr_type() == paddr_types_t::SEGMENT ||
	  !can_inplace_rewrite(delta.type)) {
	ceph_assert_always(extent->last_committed_crc == delta.prev_crc);
	assert(extent->version == delta.pversion);
	extent->apply_delta_and_adjust_crc(record_base, delta.bl);
	extent->set_modify_time(modify_time);
	ceph_assert_always(extent->last_committed_crc == delta.final_crc);
      } else {
	assert(delta.paddr.get_addr_type() == paddr_types_t::RANDOM_BLOCK);
	extent->apply_delta_and_adjust_crc(record_base, delta.bl);
	extent->set_modify_time(modify_time);
	// crc will be checked after journal replay is done
      }

      extent->version++;
      if (extent->version == 1) {
	extent->dirty_from_or_retired_at = journal_seq;
        DEBUG("replayed extent delta at {} {}, become dirty -- {}, extent={}" ,
              journal_seq, record_base, delta, *extent);
      } else {
        DEBUG("replayed extent delta at {} {} -- {}, extent={}" ,
              journal_seq, record_base, delta, *extent);
      }
      mark_dirty(extent);
      return replay_delta_ertr::make_ready_future<std::pair<bool, CachedExtentRef>>(
	std::make_pair(true, extent));
    });
  }
}

Cache::get_next_dirty_extents_ret Cache::get_next_dirty_extents(
  Transaction &t,
  journal_seq_t seq,
  size_t max_bytes)
{
  LOG_PREFIX(Cache::get_next_dirty_extents);
  if (dirty.empty()) {
    DEBUGT("max_bytes={}B, seq={}, dirty is empty",
           t, max_bytes, seq);
  } else {
    DEBUGT("max_bytes={}B, seq={}, dirty_from={}",
           t, max_bytes, seq, dirty.begin()->get_dirty_from());
  }
  std::vector<CachedExtentRef> cand;
  size_t bytes_so_far = 0;
  for (auto i = dirty.begin();
       i != dirty.end() && bytes_so_far < max_bytes;
       ++i) {
    auto dirty_from = i->get_dirty_from();
    //dirty extents must be fully loaded
    assert(i->is_fully_loaded());
    if (unlikely(dirty_from == JOURNAL_SEQ_NULL)) {
      ERRORT("got dirty extent with JOURNAL_SEQ_NULL -- {}", t, *i);
      ceph_abort();
    }
    if (dirty_from < seq) {
      TRACET("next extent -- {}", t, *i);
      if (!cand.empty() && cand.back()->get_dirty_from() > dirty_from) {
	ERRORT("dirty extents are not ordered by dirty_from -- last={}, next={}",
               t, *cand.back(), *i);
        ceph_abort();
      }
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
	  TRACET("waiting on extent -- {}", t, *ext);
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
	      DEBUGT("extent is absent on t -- {}", t, *ext);
	      t.add_to_read_set(ext);
	      if (ext->get_type() == extent_types_t::ROOT) {
		if (t.root) {
		  assert(&*t.root == &*ext);
		  ceph_assert(0 == "t.root would have to already be in the read set");
		} else {
		  assert(&*ext == &*root);
		  t.root = root;
		}
	      }
	      ret.push_back(ext);
	    } else if (result == Transaction::get_extent_ret::PRESENT) {
	      DEBUGT("extent is present on t -- {}, on t {}", t, *ext, *on_transaction);
	      ret.push_back(on_transaction);
	    } else {
	      assert(result == Transaction::get_extent_ret::RETIRED);
	      DEBUGT("extent is retired on t -- {}", t, *ext);
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
    TRACET("root already on t -- {}", t, *t.root);
    return t.root->wait_io().then([&t] {
      return get_root_iertr::make_ready_future<RootBlockRef>(
	t.root);
    });
  } else {
    DEBUGT("root not on t -- {}", t, *root);
    t.root = root;
    t.add_to_read_set(root);
    return root->wait_io().then([root=root] {
      return get_root_iertr::make_ready_future<RootBlockRef>(
	root);
    });
  }
}

Cache::get_extent_ertr::future<CachedExtentRef>
Cache::do_get_caching_extent_by_type(
  extent_types_t type,
  paddr_t offset,
  laddr_t laddr,
  extent_len_t length,
  const Transaction::src_t* p_src,
  extent_init_func_t &&extent_init_func,
  extent_init_func_t &&on_cache)
{
  return [=, this, extent_init_func=std::move(extent_init_func)]() mutable {
    src_ext_t* p_metric_key = nullptr;
    src_ext_t metric_key;
    if (p_src) {
      metric_key = std::make_pair(*p_src, type);
      p_metric_key = &metric_key;
    }

    switch (type) {
    case extent_types_t::ROOT:
      ceph_assert(0 == "ROOT is never directly read");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::BACKREF_INTERNAL:
      return do_get_caching_extent<backref::BackrefInternalNode>(
	offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::BACKREF_LEAF:
      return do_get_caching_extent<backref::BackrefLeafNode>(
	offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::LADDR_INTERNAL:
      return do_get_caching_extent<lba_manager::btree::LBAInternalNode>(
	offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::LADDR_LEAF:
      return do_get_caching_extent<lba_manager::btree::LBALeafNode>(
	offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_INNER:
      return do_get_caching_extent<omap_manager::OMapInnerNode>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_LEAF:
      return do_get_caching_extent<omap_manager::OMapLeafNode>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::COLL_BLOCK:
      return do_get_caching_extent<collection_manager::CollectionNode>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::ONODE_BLOCK_STAGED:
      return do_get_caching_extent<onode::SeastoreNodeExtent>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OBJECT_DATA_BLOCK:
      return do_get_caching_extent<ObjectDataBlock>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::RETIRED_PLACEHOLDER:
      ceph_assert(0 == "impossible");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::TEST_BLOCK:
      return do_get_caching_extent<TestBlock>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::TEST_BLOCK_PHYSICAL:
      return do_get_caching_extent<TestBlockPhysical>(
          offset, length, p_metric_key, std::move(extent_init_func), std::move(on_cache)
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
