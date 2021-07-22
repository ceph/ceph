// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/cache.h"

// included for get_extent_by_type
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/object_data_handler.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "test/crimson/seastore/test_block.h"

namespace crimson::os::seastore {

Cache::Cache(SegmentManager &segment_manager) :
  segment_manager(segment_manager)
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
  namespace sm = seastar::metrics;
  using src_t = Transaction::src_t;

  auto src_label = sm::label("src");
  std::map<src_t, sm::label_instance> labels_by_src {
    {src_t::MUTATE,  src_label("MUTATE")},
    {src_t::READ,    src_label("READ")},
    {src_t::INIT,    src_label("INIT")},
    {src_t::CLEANER, src_label("CLEANER")},
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

  auto counter_label = sm::label("counter");
  std::map<std::string, sm::label_instance> labels_by_counter {
    {"EXTENTS",     counter_label("EXTENTS")},
    {"BYTES",       counter_label("BYTES")},
    {"DELTA_BYTES", counter_label("DELTA_BYTES")},
  };

  /*
   * trans_created
   */
  stats.trans_created_by_src.fill(0);
  auto register_trans_created = [this, &labels_by_src](src_t src) {
    std::ostringstream oss_desc;
    oss_desc << "total number of transaction created (src="
             << src << ")";
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "trans_created",
          get_counter(stats.trans_created_by_src, src),
          sm::description(oss_desc.str()),
          {labels_by_src.find(src)->second}
        ),
      }
    );
  };
  for (auto& [src, label] : labels_by_src) {
    register_trans_created(src);
  }

  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "trans_created",
        [this] {
          uint64_t total = 0;
          for (auto& v : stats.trans_created_by_src) {
            total += v;
          }
          return total;
        },
        sm::description("total number of transaction created"),
        {src_label("ALL")}
      ),
    }
  );

  /*
   * trans_committed
   */
  stats.trans_committed_by_src.fill(0);
  auto register_trans_committed = [this, &labels_by_src](src_t src) {
    std::ostringstream oss_desc;
    oss_desc << "total number of transaction committed (src="
             << src << ")";
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "trans_committed",
          get_counter(stats.trans_committed_by_src, src),
          sm::description(oss_desc.str()),
          {labels_by_src.find(src)->second}
        ),
      }
    );
  };
  for (auto& [src, label] : labels_by_src) {
    if (src == src_t::READ) {
      // READ transaction won't commit
      continue;
    }
    register_trans_committed(src);
  }

  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "trans_committed",
        [this] {
          uint64_t total = 0;
          for (auto& v : stats.trans_committed_by_src) {
            total += v;
          }
          return total;
        },
        sm::description("total number of transaction committed"),
        {src_label("ALL")}
      ),
    }
  );

  /*
   * trans_invalidated
   */
  auto register_trans_invalidated =
    [this, &labels_by_src, &labels_by_ext](src_t src, extent_types_t ext) {
      auto m_key = std::make_pair(src, ext);
      stats.trans_invalidated[m_key] = 0;
      std::ostringstream oss_desc;
      oss_desc << "total number of transaction invalidated (src="
               << src << ", ext="
               << ext << ")";
      metrics.add_group(
        "cache",
        {
          sm::make_counter(
            "trans_invalidated",
            stats.trans_invalidated.find(m_key)->second,
            sm::description(oss_desc.str()),
            {labels_by_src.find(src)->second,
             labels_by_ext.find(ext)->second}
          ),
        }
      );
    };
  for (auto& [src, label] : labels_by_src) {
    for (auto& [ext, _label] : labels_by_ext) {
      register_trans_invalidated(src, ext);
    }
  }

  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "trans_invalidated",
        [this] {
          uint64_t total = 0;
          for (auto& [k, v] : stats.trans_invalidated) {
            total += v;
          }
          return total;
        },
        sm::description("total number of transaction invalidated"),
        {src_label("ALL")}
      ),
    }
  );

  /**
   * read_transactions_successful
   */
  stats.read_transactions_successful = 0;
  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "read_trans_successful",
        stats.read_transactions_successful,
        sm::description("total number of successful read transactions"),
        {}
      ),
    }
  );

  /*
   * cache_query: cache_access and cache_hit
   */
  auto register_cache_access =
    [this, &labels_by_src, &labels_by_ext]
    (src_t src, const sm::label_instance& src_label,
     extent_types_t ext, const sm::label_instance& ext_label) {
      auto m_key = std::make_pair(src, ext);
      stats.cache_query[m_key] = {0, 0};
      {
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "cache_access",
              stats.cache_query.find(m_key)->second.access,
              sm::description("total number of cache accesses labeled by "
                              "transaction source and extent type"),
              {src_label, ext_label}
            ),
            sm::make_counter(
              "cache_hit",
              stats.cache_query.find(m_key)->second.hit,
              sm::description("total number of cache hits labeled by "
                              "transaction source and extent type"),
              {src_label, ext_label}
            ),
          }
        );
      }
    };

  for (auto& [src, src_label] : labels_by_src) {
    for (auto& [ext, ext_label] : labels_by_ext) {
      if (ext != extent_types_t::RETIRED_PLACEHOLDER) {
        register_cache_access(src, src_label, ext, ext_label);
      }
    }
  }

  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "cache_access",
        [this] {
          uint64_t total = 0;
          for (auto& [k, v] : stats.cache_query) {
            total += v.access;
          }
          return total;
        },
        sm::description("total number of cache accesses"),
        {src_label("ALL")}
      ),
      sm::make_counter(
        "cache_hit",
        [this] {
          uint64_t total = 0;
          for (auto& [k, v] : stats.cache_query) {
            total += v.hit;
          }
          return total;
        },
        sm::description("total number of cache hits"),
        {src_label("ALL")}
      ),
    }
  );

  {
    /*
     * efforts discarded/committed
     *
     * XXX: include ext_label if want to measure efforts at the granularity of
     * sub-components.
     */
    auto effort_label = sm::label("effort");
    std::map<std::string, sm::label_instance> labels_by_effort {
      {"READ",   effort_label("READ")},
      {"MUTATE", effort_label("MUTATE")},
      {"RETIRE", effort_label("RETIRE")},
      {"FRESH",  effort_label("FRESH")},
    };

    auto register_effort =
      [this, &labels_by_src, &labels_by_effort, &labels_by_counter]
      (const char* category,
       src_t src,
       const std::string& effort_name,
       const char* counter_name,
       uint64_t& value) {
        std::ostringstream oss_desc;
        oss_desc << "total number of " << category
                 << " transactional efforts labeled by source, effort and counter";
        std::ostringstream oss_metric;
        oss_metric << category << "_efforts";
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              oss_metric.str(),
              value,
              sm::description(oss_desc.str()),
              {labels_by_src.find(src)->second,
               labels_by_effort.find(effort_name)->second,
               labels_by_counter.find(counter_name)->second}
            ),
          }
        );
      };

    auto get_efforts_by_category =
      [this](const char* category) -> auto& {
        if (strcmp(category, "committed") == 0) {
          return stats.committed_efforts_by_src;
        } else {
          assert(strcmp(category, "invalidated") == 0);
          return stats.invalidated_efforts_by_src;
        }
      };

    for (auto& category : {"committed", "invalidated"}) {
      auto& efforts_by_src = get_efforts_by_category(category);
      for (auto& [src, label] : labels_by_src) {
        if (std::strcmp(category, "committed") == 0 && src == src_t::READ) {
          // READ transaction won't commit
          continue;
        }

        auto& efforts = get_counter(efforts_by_src, src);
        for (auto& [effort_name, _label] : labels_by_effort) {
          auto& effort = efforts.get_by_name(effort_name);
          for (auto& counter_name : {"EXTENTS", "BYTES"}) {
            auto& value = effort.get_by_name(counter_name);
            register_effort(category, src, effort_name, counter_name, value);
          }
          if (effort_name == "MUTATE") {
            register_effort(category, src, effort_name, "DELTA_BYTES",
                            efforts.mutate_delta_bytes);
          }
        } // effort_name
      } // src
    } // category

    /**
     * read_effort_successful
     */
    stats.read_effort_successful = {};
    auto register_read_effort_successful =
      [this, &labels_by_counter]
      (const char* counter_name, uint64_t& value) {
        std::ostringstream oss_desc;
        oss_desc << "total successful read transactional effort labeled by counter";
        metrics.add_group(
          "cache",
          {
            sm::make_counter(
              "read_effort_successful",
              value,
              sm::description(oss_desc.str()),
              {labels_by_counter.find(counter_name)->second}
            ),
          }
        );
      };
    for (auto& counter_name : {"EXTENTS", "BYTES"}) {
      auto& value = stats.read_effort_successful.get_by_name(counter_name);
      register_read_effort_successful(counter_name, value);
    }
  }

  /**
   * Cached extents (including placeholders)
   *
   * Dirty extents
   */
  stats.dirty_bytes = 0;
  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "cached_extents",
        [this] {
          return extents.size();
        },
        sm::description("total number of cached extents"),
        {labels_by_counter.find("EXTENTS")->second}
      ),
      sm::make_counter(
        "cached_extents",
        [this] {
          return extents.get_bytes();
        },
        sm::description("total bytes of cached extents"),
        {labels_by_counter.find("BYTES")->second}
      ),
      sm::make_counter(
        "dirty_extents",
        [this] {
          return dirty.size();
        },
        sm::description("total number of dirty extents"),
        {labels_by_counter.find("EXTENTS")->second}
      ),
      sm::make_counter(
        "dirty_extents",
        stats.dirty_bytes,
        sm::description("total bytes of dirty extents"),
        {labels_by_counter.find("BYTES")->second}
      ),
    }
  );
}

void Cache::add_extent(CachedExtentRef ref)
{
  LOG_PREFIX(Cache::add_extent);
  assert(ref->is_valid());
  extents.insert(*ref);

  if (ref->is_dirty()) {
    add_to_dirty(ref);
  } else {
    ceph_assert(!ref->primary_ref_list_hook.is_linked());
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
  remove_from_dirty(ref);
  extents.erase(*ref);
}

void Cache::retire_extent(CachedExtentRef ref)
{
  LOG_PREFIX(Cache::retire_extent);
  DEBUG("extent {}", *ref);
  assert(ref->is_valid());

  remove_from_dirty(ref);
  ref->dirty_from_or_retired_at = JOURNAL_SEQ_MAX;
  retired_extent_gate.add_extent(*ref);

  invalidate(*ref);
  extents.erase(*ref);
}

void Cache::replace_extent(CachedExtentRef next, CachedExtentRef prev)
{
  LOG_PREFIX(Cache::replace_extent);
  DEBUG("prev {}, next {}", *prev, *next);
  assert(next->get_paddr() == prev->get_paddr());
  assert(next->version == prev->version + 1);
  extents.replace(*next, *prev);

  if (prev->get_type() == extent_types_t::ROOT) {
    assert(prev->primary_ref_list_hook.is_linked());
    assert(prev->is_dirty());
    stats.dirty_bytes -= prev->get_length();
    dirty.erase(dirty.s_iterator_to(*prev));
    intrusive_ptr_release(&*prev);
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
    add_to_dirty(next);
  }

  invalidate(*prev);
}

void Cache::invalidate(CachedExtent &extent)
{
  LOG_PREFIX(Cache::invalidate);
  DEBUG("invalidate begin -- extent {}", extent);
  for (auto &&i: extent.transactions) {
    if (!i.t->conflicted) {
      assert(!i.t->is_weak());
      invalidate(*i.t, extent);
    }
  }
  DEBUG("invalidate end");
  extent.state = CachedExtent::extent_state_t::INVALID;
}

void Cache::invalidate(Transaction& t, CachedExtent& conflicting_extent)
{
  LOG_PREFIX(Cache::invalidate);
  assert(!t.conflicted);
  DEBUGT("set conflict", t);
  t.conflicted = true;
  auto m_key = std::make_pair(
      t.get_src(), conflicting_extent.get_type());
  assert(stats.trans_invalidated.count(m_key));
  ++(stats.trans_invalidated[m_key]);
  auto& efforts = get_counter(stats.invalidated_efforts_by_src,
                              t.get_src());
  measure_efforts(t, efforts);
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      continue;
    }
    ++efforts.mutate.extents;
    efforts.mutate.bytes += i->get_length();
    efforts.mutate_delta_bytes += i->get_delta().length();
  }
}

void Cache::measure_efforts(Transaction& t, trans_efforts_t& efforts)
{
  efforts.read.extents += t.read_set.size();
  for (auto &i: t.read_set) {
    efforts.read.bytes += i.ref->get_length();
  }

  efforts.retire.extents += t.retired_set.size();
  for (auto &i: t.retired_set) {
    efforts.retire.bytes += i->get_length();
  }

  efforts.fresh.extents += t.fresh_block_list.size();
  for (auto &i: t.fresh_block_list) {
    efforts.fresh.bytes += i->get_length();
  }

  /**
   * Mutated blocks are special because CachedExtent::get_delta() is not
   * idempotent, so they need to be dealt later.
   */
}

void Cache::on_transaction_destruct(Transaction& t)
{
  LOG_PREFIX(Cache::on_transaction_destruct);
  if (t.get_src() == Transaction::src_t::READ &&
      t.conflicted == false &&
      !t.is_weak()) {
    DEBUGT("read is successful", t);
    ++stats.read_transactions_successful;

    assert(t.retired_set.empty());
    assert(t.fresh_block_list.empty());
    assert(t.mutated_block_list.empty());
    stats.read_effort_successful.extents += t.read_set.size();
    for (auto &i: t.read_set) {
      stats.read_effort_successful.bytes += i.ref->get_length();
    }
  }
}

CachedExtentRef Cache::alloc_new_extent_by_type(
  Transaction &t,       ///< [in, out] current transaction
  extent_types_t type,  ///< [in] type tag
  segment_off_t length  ///< [in] length
)
{
  switch (type) {
  case extent_types_t::ROOT:
    assert(0 == "ROOT is never directly alloc'd");
    return CachedExtentRef();
  case extent_types_t::LADDR_INTERNAL:
    return alloc_new_extent<lba_manager::btree::LBAInternalNode>(t, length);
  case extent_types_t::LADDR_LEAF:
    return alloc_new_extent<lba_manager::btree::LBALeafNode>(t, length);
  case extent_types_t::ONODE_BLOCK_STAGED:
    return alloc_new_extent<onode::SeastoreNodeExtent>(t, length);
  case extent_types_t::OMAP_INNER:
    return alloc_new_extent<omap_manager::OMapInnerNode>(t, length);
  case extent_types_t::OMAP_LEAF:
    return alloc_new_extent<omap_manager::OMapLeafNode>(t, length);
  case extent_types_t::COLL_BLOCK:
    return alloc_new_extent<collection_manager::CollectionNode>(t, length);
  case extent_types_t::OBJECT_DATA_BLOCK:
    return alloc_new_extent<ObjectDataBlock>(t, length);
  case extent_types_t::RETIRED_PLACEHOLDER:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  case extent_types_t::TEST_BLOCK:
    return alloc_new_extent<TestBlock>(t, length);
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return alloc_new_extent<TestBlockPhysical>(t, length);
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

  assert(!t.is_weak());
  assert(t.get_src() != Transaction::src_t::READ);
  ++(get_counter(stats.trans_committed_by_src, t.get_src()));
  auto& efforts = get_counter(stats.committed_efforts_by_src,
                              t.get_src());
  measure_efforts(t, efforts);

  // Should be valid due to interruptible future
  for (auto &i: t.read_set) {
    assert(i.ref->is_valid());
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
    ++efforts.mutate.extents;
    efforts.mutate.bytes += i->get_length();

    assert(i->prior_instance);
    replace_extent(i, i->prior_instance);

    i->prepare_write();
    i->set_io_wait();

    assert(i->get_version() > 0);
    auto final_crc = i->get_crc32c();
    if (i->get_type() == extent_types_t::ROOT) {
      root = t.root;
      DEBUGT("writing out root delta for {}", t, *t.root);
      record.deltas.push_back(
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
      record.deltas.push_back(
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
    efforts.mutate_delta_bytes += delta_length;
  }

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  for (auto &i: t.retired_set) {
    DEBUGT("retiring {}", t, *i);
    retire_extent(i);
  }

  record.extents.reserve(t.fresh_block_list.size());
  for (auto &i: t.fresh_block_list) {
    DEBUGT("fresh block {}", t, *i);
    bufferlist bl;
    i->prepare_write();
    bl.append(i->get_bptr());
    if (i->get_type() == extent_types_t::ROOT) {
      assert(0 == "ROOT never gets written as a fresh block");
    }

    assert(bl.length() == i->get_length());
    record.extents.push_back(extent_t{
	i->get_type(),
	i->is_logical()
	? i->cast<LogicalCachedExtent>()->get_laddr()
	: L_ADDR_NULL,
	std::move(bl)
      });
  }

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

  for (auto &i: t.fresh_block_list) {
    i->set_paddr(final_block_start.add_relative(i->get_paddr()));
    i->last_committed_crc = i->get_crc32c();
    i->on_initial_write();

    if (!i->is_valid()) {
      DEBUGT("invalid {}", t, *i);
      continue;
    }

    i->state = CachedExtent::extent_state_t::CLEAN;
    DEBUGT("fresh {}", t, *i);
    add_extent(i);
    if (cleaner) {
      cleaner->mark_space_used(
	i->get_paddr(),
	i->get_length());
    }
  }

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

  retired_extent_gate.prune();
}

void Cache::init() {
  if (root) {
    // initial creation will do mkfs followed by mount each of which calls init
    remove_extent(root);
    root = nullptr;
  }
  root = new RootBlock();
  root->state = CachedExtent::extent_state_t::DIRTY;
  add_extent(root);
}

Cache::mkfs_ertr::future<> Cache::mkfs(Transaction &t)
{
  return with_trans_intr(
    t,
    [this](auto &t) {
      return get_root(t).si_then([this, &t](auto croot) {
	duplicate_for_write(t, croot);
	return base_ertr::now();
      });
    }).handle_error(
      mkfs_ertr::pass_further{},
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
      get_extent_by_type(
        delta.type,
        delta.paddr,
        delta.laddr,
        delta.length,
        nullptr) :
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
  journal_seq_t seq,
  size_t max_bytes)
{
  LOG_PREFIX(Cache::get_next_dirty_extents);
  std::vector<CachedExtentRef> ret;
  size_t bytes_so_far = 0;
  for (auto i = dirty.begin();
       i != dirty.end() && bytes_so_far < max_bytes;
       ++i) {
    CachedExtentRef cand;
    if (i->get_dirty_from() != journal_seq_t() && i->get_dirty_from() < seq) {
      DEBUG("next {}", *i);
      if (!(ret.empty() ||
	    ret.back()->get_dirty_from() <= i->get_dirty_from())) {
	DEBUG("last {}, next {}", *ret.back(), *i);
      }
      assert(ret.empty() || ret.back()->get_dirty_from() <= i->get_dirty_from());
      bytes_so_far += i->get_length();
      ret.push_back(&*i);
    } else {
      break;
    }
  }
  return seastar::do_with(
    std::move(ret),
    [FNAME](auto &ret) {
      return seastar::do_for_each(
	ret,
	[FNAME](auto &ext) {
	  DEBUG("waiting on {}", *ext);
	  return ext->wait_io();
	}).then([&ret]() mutable {
	  return seastar::make_ready_future<std::vector<CachedExtentRef>>(
	    std::move(ret));
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
    return ret->wait_io().then([FNAME, ret, &t] {
      DEBUGT("got root read {}", t, *ret);
      t.root = ret;
      t.add_to_read_set(ret);
      return get_root_iertr::make_ready_future<RootBlockRef>(
	ret);
    });
  }
}

Cache::get_extent_ertr::future<CachedExtentRef> Cache::get_extent_by_type(
  extent_types_t type,
  paddr_t offset,
  laddr_t laddr,
  segment_off_t length,
  const Transaction::src_t* p_src)
{
  return [=] {
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
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::LADDR_LEAF:
      return get_extent<lba_manager::btree::LBALeafNode>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_INNER:
      return get_extent<omap_manager::OMapInnerNode>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_LEAF:
      return get_extent<omap_manager::OMapLeafNode>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::COLL_BLOCK:
      return get_extent<collection_manager::CollectionNode>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::ONODE_BLOCK_STAGED:
      return get_extent<onode::SeastoreNodeExtent>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OBJECT_DATA_BLOCK:
      return get_extent<ObjectDataBlock>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::RETIRED_PLACEHOLDER:
      ceph_assert(0 == "impossible");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::TEST_BLOCK:
      return get_extent<TestBlock>(
          offset, length, p_metric_key
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::TEST_BLOCK_PHYSICAL:
      return get_extent<TestBlockPhysical>(
          offset, length, p_metric_key
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
