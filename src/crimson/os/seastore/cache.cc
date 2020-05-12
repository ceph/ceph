// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"
#include "crimson/common/log.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

Cache::~Cache()
{
  retire_extent(root);
  root.reset();
  for (auto i = dirty.begin(); i != dirty.end(); ) {
    auto ptr = &*i;
    dirty.erase(i++);
    intrusive_ptr_release(ptr);
  }
  for (auto &i: extents) {
    logger().error("~Cache: extent {} still alive", i);
  }
  ceph_assert(extents.empty());
}

void Cache::add_extent(CachedExtentRef ref)
{
  assert(ref->is_valid());
  extents.insert(*ref);

  ceph_assert(!ref->primary_ref_list_hook.is_linked());
  if (ref->is_dirty()) {
    intrusive_ptr_add_ref(&*ref);
    dirty.push_back(*ref);
  }
  logger().debug("add_extent: {}", *ref);
}

void Cache::retire_extent(CachedExtentRef ref)
{
  logger().debug("retire_extent: {}", *ref);
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

CachedExtentRef Cache::duplicate_for_write(
  Transaction &t,
  CachedExtentRef i) {
  if (i->is_pending())
    return i;

  auto ret = i->duplicate_for_write();
  ret->version++;
  ret->state = CachedExtent::extent_state_t::MUTATION_PENDING;

  if (ret->get_type() == extent_types_t::ROOT) {
    t.root = ret->cast<RootBlock>();
  }

  t.add_to_retired_set(i);
  t.add_mutated_extent(ret);

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

  // Transaction is now a go, set up in-memory cache state
  // invalidate now invalid blocks
  for (auto &i: t.retired_set) {
    logger().debug("try_construct_record: retiring {}", *i);
    ceph_assert(!i->is_pending());
    ceph_assert(i->is_valid());
    retire_extent(i);
    i->state = CachedExtent::extent_state_t::INVALID;
  }

  t.write_set.clear();

  // Add new copy of mutated blocks, set_io_wait to block until written
  record.deltas.reserve(t.mutated_block_list.size());
  for (auto &i: t.mutated_block_list) {
    logger().debug("try_construct_record: mutating {}", *i);
    add_extent(i);
    i->prepare_write();
    i->set_io_wait();
    record.deltas.push_back(
      delta_info_t{
	i->get_type(),
	i->get_paddr(),
	(segment_off_t)i->get_length(),
	i->get_version(),
	i->get_delta()
      });
  }

  record.extents.reserve(t.fresh_block_list.size());
  for (auto &i: t.fresh_block_list) {
    logger().debug("try_construct_record: fresh block {}", *i);
    bufferlist bl;
    i->prepare_write();
    bl.append(i->get_bptr());
    if (i->get_type() == extent_types_t::ROOT) {
      record.deltas.push_back(
	delta_info_t{
	  extent_types_t::ROOT_LOCATION,
	  i->get_paddr(),
	  0,
	  0,
	  bufferlist()
	});
    }
    record.extents.push_back(extent_t{std::move(bl)});
  }

  t.read_set.clear();
  return std::make_optional<record_t>(std::move(record));
}

void Cache::complete_commit(
  Transaction &t,
  paddr_t final_block_start)
{
  if (t.root)
    root = t.root;

  paddr_t cur = final_block_start;
  for (auto &i: t.fresh_block_list) {
    i->set_paddr(cur);
    cur.offset += i->get_length();
    i->state = CachedExtent::extent_state_t::CLEAN;
    logger().debug("complete_commit: fresh {}", *i);
    i->on_initial_write();
    add_extent(i);
  }

  // Add new copy of mutated blocks, set_io_wait to block until written
  for (auto &i: t.mutated_block_list) {
    i->state = CachedExtent::extent_state_t::DIRTY;
    logger().debug("complete_commit: mutated {}", *i);
    i->on_delta_write(final_block_start);
  }

  for (auto &i: t.mutated_block_list) {
    i->complete_io();
  }
}

Cache::mkfs_ertr::future<> Cache::mkfs(Transaction &t)
{
  t.root = alloc_new_extent<RootBlock>(t, RootBlock::SIZE);
  return mkfs_ertr::now();
}

Cache::close_ertr::future<> Cache::close()
{
  return close_ertr::now();
}

Cache::replay_delta_ret
Cache::replay_delta(paddr_t record_base, const delta_info_t &delta)
{
  if (delta.type == extent_types_t::ROOT_LOCATION) {
    auto root_location = delta.paddr.is_relative()
      ? record_base.add_record_relative(delta.paddr)
      : delta.paddr;
    logger().debug("replay_delta: found root addr {}", root_location);
    return get_extent<RootBlock>(
      root_location,
      RootBlock::SIZE
    ).safe_then([this, root_location](auto ref) {
      logger().debug("replay_delta: finished reading root at {}", root_location);
      root = ref;
      return root->complete_load();
    }).safe_then([root_location] {
      logger().debug("replay_delta: finished loading root at {}", root_location);
      return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
    });
  }
  // TODO
  return replay_delta_ret(replay_delta_ertr::ready_future_marker{});
}

Cache::get_root_ret Cache::get_root(Transaction &t)
{
  if (t.root) {
    return get_root_ret(
      get_root_ertr::ready_future_marker{},
      t.root);
  } else {
    auto ret = root;
    return ret->wait_io().then([ret] {
      return get_root_ret(
	get_root_ertr::ready_future_marker{},
	ret);
    });
  }
}

}
