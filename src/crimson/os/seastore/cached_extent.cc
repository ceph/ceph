// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/btree/fixed_kv_node.h"
#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore {

#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *ptr)
{
  intrusive_ptr_add_ref(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
    logger().debug("intrusive_ptr_add_ref: {}", *ptr);
}

void intrusive_ptr_release(CachedExtent *ptr)
{
  logger().debug("intrusive_ptr_release: {}", *ptr);
  intrusive_ptr_release(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
}

#endif

std::ostream &operator<<(std::ostream &out, CachedExtent::extent_state_t state)
{
  switch (state) {
  case CachedExtent::extent_state_t::INITIAL_WRITE_PENDING:
    return out << "INITIAL_WRITE_PENDING";
  case CachedExtent::extent_state_t::MUTATION_PENDING:
    return out << "MUTATION_PENDING";
  case CachedExtent::extent_state_t::CLEAN_PENDING:
    return out << "CLEAN_PENDING";
  case CachedExtent::extent_state_t::CLEAN:
    return out << "CLEAN";
  case CachedExtent::extent_state_t::DIRTY:
    return out << "DIRTY";
  case CachedExtent::extent_state_t::EXIST_CLEAN:
    return out << "EXIST_CLEAN";
  case CachedExtent::extent_state_t::EXIST_MUTATION_PENDING:
    return out << "EXIST_MUTATION_PENDING";
  case CachedExtent::extent_state_t::INVALID:
    return out << "INVALID";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext)
{
  return ext.print(out);
}

CachedExtent::~CachedExtent()
{
  if (parent_index) {
    assert(is_linked());
    parent_index->erase(*this);
  }
}
CachedExtent* CachedExtent::get_transactional_view(Transaction &t) {
  return get_transactional_view(t.get_trans_id());
}

CachedExtent* CachedExtent::get_transactional_view(transaction_id_t tid) {
  auto it = mutation_pending_extents.find(tid, trans_spec_view_t::cmp_t());
  if (it != mutation_pending_extents.end()) {
    return (CachedExtent*)&(*it);
  } else {
    return this;
  }
}

std::ostream &LogicalCachedExtent::print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr
      << ", seen=" << seen_by_users;
  return print_detail_l(out);
}

void CachedExtent::set_invalid(Transaction &t) {
  state = extent_state_t::INVALID;
  if (trans_view_hook.is_linked()) {
    trans_view_hook.unlink();
  }
  on_invalidated(t);
}

std::pair<bool, CachedExtent::viewable_state_t>
CachedExtent::is_viewable_by_trans(Transaction &t) {
  if (!is_valid()) {
    return std::make_pair(false, viewable_state_t::invalid);
  }

  auto trans_id = t.get_trans_id();
  if (is_pending()) {
    ceph_assert(is_pending_in_trans(trans_id));
    return std::make_pair(true, viewable_state_t::pending);
  }

  // shared by multiple transactions
  assert(t.is_in_read_set(this));
  assert(is_stable_written());

  auto cmp = trans_spec_view_t::cmp_t();
  if (mutation_pending_extents.find(trans_id, cmp) !=
      mutation_pending_extents.end()) {
    return std::make_pair(false, viewable_state_t::stable_become_pending);
  }

  if (retired_transactions.find(trans_id, cmp) !=
      retired_transactions.end()) {
    assert(t.is_stable_extent_retired(get_paddr(), get_length()));
    return std::make_pair(false, viewable_state_t::stable_become_retired);
  }

  return std::make_pair(true, viewable_state_t::stable);
}

std::ostream &operator<<(
  std::ostream &out,
  CachedExtent::viewable_state_t state)
{
  switch(state) {
  case CachedExtent::viewable_state_t::stable:
    return out << "stable";
  case CachedExtent::viewable_state_t::pending:
    return out << "pending";
  case CachedExtent::viewable_state_t::invalid:
    return out << "invalid";
  case CachedExtent::viewable_state_t::stable_become_retired:
    return out << "stable_become_retired";
  case CachedExtent::viewable_state_t::stable_become_pending:
    return out << "stable_become_pending";
  default:
    __builtin_unreachable();
  }
}

bool BufferSpace::is_range_loaded(extent_len_t offset, extent_len_t length) const
{
  assert(length > 0);
  auto i = buffer_map.upper_bound(offset);
  if (i == buffer_map.begin()) {
    return false;
  }
  --i;
  auto& [i_offset, i_bl] = *i;
  assert(offset >= i_offset);
  assert(i_bl.length() > 0);
  if (offset + length > i_offset + i_bl.length()) {
    return false;
  } else {
    return true;
  }
}

ceph::bufferlist BufferSpace::get_buffer(extent_len_t offset, extent_len_t length) const
{
  assert(length > 0);
  auto i = buffer_map.upper_bound(offset);
  assert(i != buffer_map.begin());
  --i;
  auto& [i_offset, i_bl] = *i;
  assert(offset >= i_offset);
  assert(i_bl.length() > 0);
  assert(offset + length <= i_offset + i_bl.length());
  ceph::bufferlist res;
  res.substr_of(i_bl, offset - i_offset, length);
  return res;
}

load_ranges_t BufferSpace::load_ranges(extent_len_t offset, extent_len_t length)
{
  assert(length > 0);
  load_ranges_t ret;
  auto next = buffer_map.upper_bound(offset);

  // must be assigned for the main-loop
  map_t::iterator previous;
  extent_len_t range_offset;
  extent_len_t range_length;

  // returns whether to proceed main-loop or not
  auto f_merge_next_check_hole = [this, &next, &range_offset, &range_length](
      ceph::bufferlist& previous_bl,
      extent_len_t hole_length,
      extent_len_t next_offset,
      const ceph::bufferlist& next_bl) {
    range_length -= hole_length;
    previous_bl.append(next_bl);
    if (range_length <= next_bl.length()) {
      // "next" end includes or beyonds the range
      buffer_map.erase(next);
      return false;
    } else {
      range_offset = next_offset + next_bl.length();
      range_length -= next_bl.length();
      // erase next should destruct next_bl
      next = buffer_map.erase(next);
      return true;
    }
  };

  // returns whether to proceed main-loop or not
  auto f_prepare_without_merge_previous = [
      this, offset, length,
      &ret, &previous, &next, &range_length,
      &f_merge_next_check_hole]() {
    if (next == buffer_map.end()) {
      // "next" reaches end,
      // range has no "next" to merge
      create_hole_insert_map(ret, offset, length, next);
      return false;
    }
    // "next" is valid
    auto& [n_offset, n_bl] = *next;
    // next is from upper_bound()
    assert(offset < n_offset);
    extent_len_t hole_length = n_offset - offset;
    if (length < hole_length) {
      // "next" is beyond the range end,
      // range has no "next" to merge
      create_hole_insert_map(ret, offset, length, next);
      return false;
    }
    // length >= hole_length
    // insert hole as "previous"
    previous = create_hole_insert_map(ret, offset, hole_length, next);
    auto& p_bl = previous->second;
    range_length = length;
    return f_merge_next_check_hole(p_bl, hole_length, n_offset, n_bl);
  };

  /*
   * prepare main-loop
   */
  if (next == buffer_map.begin()) {
    // "previous" is invalid
    if (!f_prepare_without_merge_previous()) {
      return ret;
    }
  } else {
    // "previous" is valid
    previous = std::prev(next);
    auto& [p_offset, p_bl] = *previous;
    assert(offset >= p_offset);
    extent_len_t p_end = p_offset + p_bl.length();
    if (offset <= p_end) {
      // "previous" is adjacent or overlaps the range
      range_offset = p_end;
      assert(offset + length > p_end);
      range_length = offset + length - p_end;
      // start the main-loop (merge "previous")
    } else {
      // "previous" is not adjacent to the range
      // range and buffer_map should not overlap
      assert(offset > p_end);
      if (!f_prepare_without_merge_previous()) {
        return ret;
      }
    }
  }

  /*
   * main-loop: merge the range with "previous" and look at "next"
   *
   * "previous": the previous buffer_map entry, must be valid, must be mergable
   * "next": the next buffer_map entry, maybe end, maybe mergable
   * range_offset/length: the current range right after "previous"
   */
  assert(std::next(previous) == next);
  auto& [p_offset, p_bl] = *previous;
  assert(range_offset == p_offset + p_bl.length());
  assert(range_length > 0);
  while (next != buffer_map.end()) {
    auto& [n_offset, n_bl] = *next;
    assert(range_offset < n_offset);
    extent_len_t hole_length = n_offset - range_offset;
    if (range_length < hole_length) {
      // "next" offset is beyond the range end
      break;
    }
    // range_length >= hole_length
    create_hole_append_bl(ret, p_bl, range_offset, hole_length);
    if (!f_merge_next_check_hole(p_bl, hole_length, n_offset, n_bl)) {
      return ret;
    }
    assert(std::next(previous) == next);
    assert(range_offset == p_offset + p_bl.length());
    assert(range_length > 0);
  }
  // range has no "next" to merge:
  // 1. "next" reaches end
  // 2. "next" offset is beyond the range end
  create_hole_append_bl(ret, p_bl, range_offset, range_length);
  return ret;
}

ceph::bufferptr BufferSpace::to_full_ptr(extent_len_t length)
{
  assert(length > 0);
  assert(buffer_map.size() == 1);
  auto it = buffer_map.begin();
  auto& [i_off, i_buf] = *it;
  assert(i_off == 0);
  if (!i_buf.is_contiguous()) {
    // Allocate page aligned ptr, also see create_extent_ptr_*()
    i_buf.rebuild();
  }
  assert(i_buf.get_num_buffers() == 1);
  ceph::bufferptr ptr(i_buf.front());
  assert(ptr.is_page_aligned());
  assert(ptr.length() == length);
  buffer_map.clear();
  return ptr;
}

}
