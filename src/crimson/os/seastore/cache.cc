// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"
#include "crimson/common/log.h"

// included for get_extent_by_type
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node_impl.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "test/crimson/seastore/test_block.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

Cache::Cache(SegmentManager &segment_manager) :
  segment_manager(segment_manager) {}

Cache::~Cache()
{
  for (auto &i: extents) {
    logger().error("~Cache: extent {} still alive", i);
  }
  ceph_assert(extents.empty());
}

Cache::retire_extent_ret Cache::retire_extent_if_cached(
  Transaction &t, paddr_t addr, extent_len_t length)
{
  if (auto ext = t.write_set.find_offset(addr); ext != t.write_set.end()) {
    logger().debug("{}: found {} in t.write_set", __func__, addr);
    t.add_to_retired_set(CachedExtentRef(&*ext));
    return retire_extent_ertr::now();
  } else if (auto iter = extents.find_offset(addr);
      iter != extents.end()) {
    auto ret = CachedExtentRef(&*iter);
    return ret->wait_io().then([&t, ret=std::move(ret)]() mutable {
      t.add_to_retired_set(ret);
      return retire_extent_ertr::now();
    });
  } else {
    t.add_to_retired_uncached(addr, length);
    return retire_extent_ertr::now();
  }
}

void Cache::add_extent(CachedExtentRef ref)
{
  assert(ref->is_valid());
  extents.insert(*ref);

  if (ref->is_dirty()) {
    add_to_dirty(ref);
  } else {
    ceph_assert(!ref->primary_ref_list_hook.is_linked());
  }
  logger().debug("add_extent: {}", *ref);
}

void Cache::mark_dirty(CachedExtentRef ref)
{
  if (ref->is_dirty()) {
    assert(ref->primary_ref_list_hook.is_linked());
    return;
  }

  add_to_dirty(ref);
  ref->state = CachedExtent::extent_state_t::DIRTY;

  logger().debug("mark_dirty: {}", *ref);
}

void Cache::add_to_dirty(CachedExtentRef ref)
{
  assert(ref->is_valid());
  assert(!ref->primary_ref_list_hook.is_linked());
  intrusive_ptr_add_ref(&*ref);
  dirty.push_back(*ref);
}

void Cache::remove_extent(CachedExtentRef ref)
{
  logger().debug("remove_extent: {}", *ref);
  assert(ref->is_valid());
  extents.erase(*ref);

  if (ref->is_dirty()) {
    ceph_assert(ref->primary_ref_list_hook.is_linked());
    dirty.erase(dirty.s_iterator_to(*ref));
    intrusive_ptr_release(&*ref);
  } else {
    ceph_assert(!ref->primary_ref_list_hook.is_linked());
  }
}

void Cache::replace_extent(CachedExtentRef next, CachedExtentRef prev)
{
  assert(next->get_paddr() == prev->get_paddr());
  assert(next->version == prev->version + 1);
  extents.replace(*next, *prev);

  if (prev->get_type() == extent_types_t::ROOT) {
    assert(prev->primary_ref_list_hook.is_linked());
    assert(prev->is_dirty());
    dirty.erase(dirty.s_iterator_to(*prev));
    intrusive_ptr_release(&*prev);
    add_to_dirty(next);
  } else if (prev->is_dirty()) {
    assert(prev->dirty_from == next->dirty_from);
    assert(prev->primary_ref_list_hook.is_linked());
    auto prev_it = dirty.iterator_to(*prev);
    dirty.insert(prev_it, *next);
    dirty.erase(prev_it);
    intrusive_ptr_release(&*prev);
    intrusive_ptr_add_ref(&*next);
  } else {
    add_to_dirty(next);
  }

  prev->state = CachedExtent::extent_state_t::INVALID;
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
  case extent_types_t::EXTMAP_INNER:
    return alloc_new_extent<extentmap_manager::ExtMapInnerNode>(t, length);
  case extent_types_t::EXTMAP_LEAF:
    return alloc_new_extent<extentmap_manager::ExtMapLeafNode>(t, length);
  case extent_types_t::OMAP_INNER:
    return alloc_new_extent<omap_manager::OMapInnerNode>(t, length);
  case extent_types_t::OMAP_LEAF:
    return alloc_new_extent<omap_manager::OMapLeafNode>(t, length);
  case extent_types_t::COLL_BLOCK:
    return alloc_new_extent<collection_manager::CollectionNode>(t, length);
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
  logger().debug("Cache::duplicate_for_write: {} -> {}", *i, *ret);
  return ret;
}

std::optional<record_t> Cache::try_construct_record(Transaction &t)
{
  // First, validate read set
  for (auto &i: t.read_set) {
    if (i->state == CachedExtent::extent_state_t::INVALID)
      return std::nullopt;
  }

  record_t record;

  t.write_set.clear();

  // Add new copy of mutated blocks, set_io_wait to block until written
  record.deltas.reserve(t.mutated_block_list.size());
  for (auto &i: t.mutated_block_list) {
    if (!i->is_valid()) {
      logger().debug("try_construct_record: ignoring invalid {}", *i);
      continue;
    }
    logger().debug("try_construct_record: mutating {}", *i);

    assert(i->prior_instance);
    replace_extent(i, i->prior_instance);

    i->prepare_write();
    i->set_io_wait();

    assert(i->get_version() > 0);
    auto final_crc = i->get_crc32c();
    if (i->get_type() == extent_types_t::ROOT) {
      root = t.root;
      logger().debug(
	"{}: writing out root delta for {}",
	__func__,
	*t.root);
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
  }

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  for (auto &i: t.retired_set) {
    logger().debug("try_construct_record: retiring {}", *i);
    ceph_assert(i->is_valid());
    remove_extent(i);
    i->state = CachedExtent::extent_state_t::INVALID;
  }

  record.extents.reserve(t.fresh_block_list.size());
  for (auto &i: t.fresh_block_list) {
    logger().debug("try_construct_record: fresh block {}", *i);
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

  return std::make_optional<record_t>(std::move(record));
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_block_start,
  journal_seq_t seq,
  SegmentCleaner *cleaner)
{
  for (auto &i: t.fresh_block_list) {
    i->set_paddr(final_block_start.add_relative(i->get_paddr()));
    i->last_committed_crc = i->get_crc32c();
    i->on_initial_write();

    if (!i->is_valid()) {
      logger().debug("complete_commit: invalid {}", *i);
      continue;
    }

    i->state = CachedExtent::extent_state_t::CLEAN;
    logger().debug("complete_commit: fresh {}", *i);
    add_extent(i);
    if (cleaner) {
      cleaner->mark_space_used(
	i->get_paddr(),
	i->get_length());
    }
  }

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    logger().debug("complete_commit: mutated {}", *i);
    assert(i->prior_instance);
    i->on_delta_write(final_block_start);
    i->prior_instance = CachedExtentRef();
    if (!i->is_valid()) {
      logger().debug("complete_commit: not dirtying invalid {}", *i);
      continue;
    }
    i->state = CachedExtent::extent_state_t::DIRTY;
    if (i->version == 1 || i->get_type() == extent_types_t::ROOT) {
      i->dirty_from = seq;
    }
  }

  if (cleaner) {
    for (auto &i: t.retired_set) {
      cleaner->mark_space_free(
	i->get_paddr(),
	i->get_length());
    }
    for (auto &i: t.retired_uncached) {
      cleaner->mark_space_free(
	i.first,
	i.second);
    }
  }

  for (auto &i: t.mutated_block_list) {
    i->complete_io();
  }
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
  return get_root(t).safe_then([this, &t](auto croot) {
    duplicate_for_write(t, croot);
    return mkfs_ertr::now();
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
    dirty.erase(i++);
    intrusive_ptr_release(ptr);
  }
  return close_ertr::now();
}

Cache::replay_delta_ret
Cache::replay_delta(
  journal_seq_t journal_seq,
  paddr_t record_base,
  const delta_info_t &delta)
{
  if (delta.type == extent_types_t::ROOT) {
    logger().debug("replay_delta: found root delta");
    remove_extent(root);
    root->apply_delta_and_adjust_crc(record_base, delta.bl);
    root->dirty_from = journal_seq;
    add_extent(root);
    return replay_delta_ertr::now();
  } else {
    auto get_extent_if_cached = [this](paddr_t addr)
      -> get_extent_ertr::future<CachedExtentRef> {
      auto retiter = extents.find_offset(addr);
      if (retiter != extents.end()) {
	return seastar::make_ready_future<CachedExtentRef>(&*retiter);
      } else {
	return seastar::make_ready_future<CachedExtentRef>();
      }
    };
    auto extent_fut = (delta.pversion == 0 ?
      get_extent_by_type(
	delta.type,
	delta.paddr,
	delta.laddr,
	delta.length) :
      get_extent_if_cached(
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
	logger().debug(
	  "replay_delta: replaying {}, extent not present so delta is obsolete",
	  delta);
	return;
      }

      logger().debug(
	"replay_delta: replaying {} on {}",
	  *extent,
	delta);

      assert(extent->version == delta.pversion);

      assert(extent->last_committed_crc == delta.prev_crc);
      extent->apply_delta_and_adjust_crc(record_base, delta.bl);
      assert(extent->last_committed_crc == delta.final_crc);

      if (extent->version == 0) {
	extent->dirty_from = journal_seq;
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
  std::vector<CachedExtentRef> ret;
  size_t bytes_so_far = 0;
  for (auto i = dirty.begin();
       i != dirty.end() && bytes_so_far < max_bytes;
       ++i) {
    CachedExtentRef cand;
    if (i->dirty_from != journal_seq_t() && i->dirty_from < seq) {
      logger().debug(
	"Cache::get_next_dirty_extents: next {}",
	*i);
      if (!(ret.empty() || ret.back()->dirty_from <= i->dirty_from)) {
	logger().debug(
	  "Cache::get_next_dirty_extents: last {}, next {}",
	  *ret.back(),
	  *i);
      }
      assert(ret.empty() || ret.back()->dirty_from <= i->dirty_from);
      bytes_so_far += i->get_length();
      ret.push_back(&*i);
    } else {
      break;
    }
  }
  return seastar::do_with(
    std::move(ret),
    [](auto &ret) {
      return seastar::do_for_each(
	ret,
	[](auto &ext) {
	  logger().debug(
	    "get_next_dirty_extents: waiting on {}",
	    *ext);
	  return ext->wait_io();
	}).then([&ret]() mutable {
	  return seastar::make_ready_future<std::vector<CachedExtentRef>>(
	    std::move(ret));
	});
    });
}

Cache::get_root_ret Cache::get_root(Transaction &t)
{
  if (t.root) {
    return get_root_ret(
      get_root_ertr::ready_future_marker{},
      t.root);
  } else {
    auto ret = root;
    return ret->wait_io().then([ret, &t] {
      t.root = ret;
      t.add_to_read_set(ret);
      return get_root_ret(
	get_root_ertr::ready_future_marker{},
	ret);
    });
  }
}

Cache::get_extent_ertr::future<CachedExtentRef> Cache::get_extent_by_type(
  extent_types_t type,
  paddr_t offset,
  laddr_t laddr,
  segment_off_t length)
{
  return [=] {
    switch (type) {
    case extent_types_t::ROOT:
      assert(0 == "ROOT is never directly read");
      return get_extent_ertr::make_ready_future<CachedExtentRef>();
    case extent_types_t::LADDR_INTERNAL:
      return get_extent<lba_manager::btree::LBAInternalNode>(offset, length
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::LADDR_LEAF:
      return get_extent<lba_manager::btree::LBALeafNode>(offset, length
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::EXTMAP_INNER:
      return get_extent<extentmap_manager::ExtMapInnerNode>(offset, length
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::EXTMAP_LEAF:
      return get_extent<extentmap_manager::ExtMapLeafNode>(offset, length
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_INNER:
      return get_extent<omap_manager::OMapInnerNode>(offset, length
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::OMAP_LEAF:
      return get_extent<omap_manager::OMapLeafNode>(offset, length
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::COLL_BLOCK:
      return get_extent<collection_manager::CollectionNode>(offset, length
      ).safe_then([](auto extent) {
        return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::ONODE_BLOCK_STAGED:
      return get_extent<onode::SeastoreNodeExtent>(offset, length
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::TEST_BLOCK:
      return get_extent<TestBlock>(offset, length
      ).safe_then([](auto extent) {
	return CachedExtentRef(extent.detach(), false /* add_ref */);
      });
    case extent_types_t::TEST_BLOCK_PHYSICAL:
      return get_extent<TestBlockPhysical>(offset, length
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
