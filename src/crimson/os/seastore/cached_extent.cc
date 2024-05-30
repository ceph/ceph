// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/btree/fixed_kv_node.h"

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

bool is_backref_mapped_extent_node(const CachedExtentRef &extent) {
  return extent->is_logical()
    || is_lba_node(extent->get_type())
    || extent->get_type() == extent_types_t::TEST_BLOCK_PHYSICAL;
}

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
  auto it = mutation_pendings.find(tid, trans_spec_view_t::cmp_t());
  if (it != mutation_pendings.end()) {
    return (CachedExtent*)&(*it);
  } else {
    return this;
  }
}

std::ostream &operator<<(std::ostream &out, const parent_tracker_t &tracker) {
  return out << "parent_tracker=" << (void*)&tracker
	     << ", parent=" << (void*)tracker.get_parent().get();
}

std::ostream &ChildableCachedExtent::print_detail(std::ostream &out) const {
  if (parent_tracker) {
    out << *parent_tracker;
  } else {
    out << ", parent_tracker=" << (void*)nullptr;
  }
  _print_detail(out);
  return out;
}

std::ostream &LogicalCachedExtent::_print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr;
  return print_detail_l(out);
}

void child_pos_t::link_child(ChildableCachedExtent *c) {
  get_parent<FixedKVNode<laddr_t>>()->link_child(c, pos);
}

void CachedExtent::set_invalid(Transaction &t) {
  state = extent_state_t::INVALID;
  if (trans_view_hook.is_linked()) {
    trans_view_hook.unlink();
  }
  on_invalidated(t);
}

LogicalCachedExtent::~LogicalCachedExtent() {
  if (has_parent_tracker() && is_valid() && !is_pending()) {
    assert(get_parent_node());
    auto parent = get_parent_node<FixedKVNode<laddr_t>>();
    auto off = parent->lower_bound_offset(laddr);
    assert(parent->get_key_from_idx(off) == laddr);
    assert(parent->children[off] == this);
    parent->children[off] = nullptr;
  }
}

void LogicalCachedExtent::on_replace_prior() {
  assert(is_mutation_pending());
  take_prior_parent_tracker();
  assert(get_parent_node());
  auto parent = get_parent_node<FixedKVNode<laddr_t>>();
  //TODO: can this search be avoided?
  auto off = parent->lower_bound_offset(laddr);
  assert(parent->get_key_from_idx(off) == laddr);
  parent->children[off] = this;
}

parent_tracker_t::~parent_tracker_t() {
  // this is parent's tracker, reset it
  auto &p = (FixedKVNode<laddr_t>&)*parent;
  if (p.my_tracker == this) {
    p.my_tracker = nullptr;
  }
}

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs)
{
  out << "LBAMapping(" << rhs.get_key() << "~" << rhs.get_length()
      << "->" << rhs.get_val();
  if (rhs.is_indirect()) {
    out << " indirect(" << rhs.get_intermediate_base() << "~"
	<< rhs.get_intermediate_key() << "~"
	<< rhs.get_intermediate_length() << ")";
  }
  out << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

void BufferSpace::add_buffer(extent_len_t offset, ceph::bufferptr&& ptr)
{
  ceph::bufferlist buffer;
  buffer.append(std::move(ptr));
  const extent_len_t tail = offset + buffer.length();
  auto i = buffer_map.find(tail);
  if (i != buffer_map.end()) {
    buffer.append(i->second);
    buffer_map.erase(i);
  }
  i = buffer_map.lower_bound(offset);
  ceph_assert(i == buffer_map.end() || i->first > tail);
  if (i != buffer_map.begin()) {
    --i;
    assert(i->first + i->second.length() <= offset);
  }
  if (i != buffer_map.end() &&
    i->first + i->second.length() == offset) {
    i->second.append(buffer);
  } else {
    buffer_map[offset] = buffer;
  }
}

ceph::bufferlist BufferSpace::get_data(extent_len_t offset, extent_len_t length)
{
  auto i = buffer_map.upper_bound(offset);
  assert(i != buffer_map.begin());
  --i;
  ceph::bufferlist res;
  assert(i->second.length() >= length + offset - i->first);
  res.substr_of(i->second, offset - i->first, length);
  return res;
}

ceph::bufferptr BufferSpace::build_ptr(extent_len_t length)
{
  assert(buffer_map.size() == 1);
  auto& it = buffer_map[0];
  if (!it.is_contiguous()) {
    it.rebuild();
  }
  ceph::bufferptr ptr(it.buffers().front());
  assert(ptr.is_page_aligned());
  assert(ptr.length() == length);
  buffer_map.clear();
  return ptr;
}

bool BufferSpace::is_range_loaded(extent_len_t offset, extent_len_t length)
{
  auto i = buffer_map.upper_bound(offset);
  if (i == buffer_map.begin()) {
    return false;
  }
  --i;
  if (i->first + i->second.length() < offset + length) {
    return false;
  }
  return true;
}

region_list_t BufferSpace::get_unloaded_ranges(extent_len_t offset, extent_len_t length)
{
  auto tail = offset + length;
  region_list_t r2r;
  auto i = buffer_map.lower_bound(offset);
  if (i != buffer_map.begin()) {
    --i;
    if (i->first + i->second.length() <= offset) {
      ++i;
    }
  }
  while(offset < tail){
    if (i == buffer_map.end()) {
      r2r.emplace_back(offset, length);
      break;
    }
    auto i_off = i->first;
    auto i_len = i->second.length();
    if (i_off <= offset) {
      extent_len_t skip = i_off + i_len - offset;
      extent_len_t l = std::min(length, skip);
      offset += l;
      length -= l;
    } else {
      extent_len_t hole = std::min(i_off - offset, length);
      r2r.emplace_back(offset, hole);
      extent_len_t l = std::min(length, i_len + hole);
      offset += l;
      length -= l;
    }
    ++i;
  }
  return r2r;
}

}
