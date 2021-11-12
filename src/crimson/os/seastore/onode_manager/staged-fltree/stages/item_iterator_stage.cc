// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "item_iterator_stage.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

#define ITER_T item_iterator_t<NODE_TYPE>
#define ITER_INST(NT) item_iterator_t<NT>

template <node_type_t NODE_TYPE>
template <KeyT KT>
memory_range_t ITER_T::insert_prefix(
    NodeExtentMutable& mut, const ITER_T& iter, const full_key_t<KT>& key,
    bool is_end, node_offset_t size, const char* p_left_bound)
{
  // 1. insert range
  char* p_insert;
  if (is_end) {
    assert(!iter.has_next());
    p_insert = const_cast<char*>(iter.p_start());
  } else {
    p_insert = const_cast<char*>(iter.p_end());
  }
  char* p_insert_front = p_insert - size;

  // 2. shift memory
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = p_insert;
  mut.shift_absolute(p_shift_start,
                     p_shift_end - p_shift_start,
                     -(int)size);

  // 3. append header
  p_insert -= sizeof(node_offset_t);
  node_offset_t back_offset = (p_insert - p_insert_front);
  mut.copy_in_absolute(p_insert, back_offset);
  ns_oid_view_t::append<KT>(mut, key, p_insert);

  return {p_insert_front, p_insert};
}
#define IP_TEMPLATE(NT, KT)                                              \
  template memory_range_t ITER_INST(NT)::insert_prefix<KT>(              \
      NodeExtentMutable&, const ITER_INST(NT)&, const full_key_t<KT>&, \
      bool, node_offset_t, const char*)
IP_TEMPLATE(node_type_t::LEAF, KeyT::VIEW);
IP_TEMPLATE(node_type_t::INTERNAL, KeyT::VIEW);
IP_TEMPLATE(node_type_t::LEAF, KeyT::HOBJ);
IP_TEMPLATE(node_type_t::INTERNAL, KeyT::HOBJ);

template <node_type_t NODE_TYPE>
void ITER_T::update_size(
    NodeExtentMutable& mut, const ITER_T& iter, int change)
{
  node_offset_t offset = iter.get_back_offset();
  int new_size = change + offset;
  assert(new_size > 0 && new_size < (int)mut.get_length());
  mut.copy_in_absolute(
      (void*)iter.get_item_range().p_end, node_offset_t(new_size));
}

template <node_type_t NODE_TYPE>
node_offset_t ITER_T::trim_until(NodeExtentMutable& mut, const ITER_T& iter)
{
  assert(iter.index() != 0);
  size_t ret = iter.p_end() - iter.p_items_start;
  assert(ret < mut.get_length());
  return ret;
}

template <node_type_t NODE_TYPE>
node_offset_t ITER_T::trim_at(
    NodeExtentMutable& mut, const ITER_T& iter, node_offset_t trimmed)
{
  size_t trim_size = iter.p_start() - iter.p_items_start + trimmed;
  assert(trim_size < mut.get_length());
  assert(iter.get_back_offset() > trimmed);
  node_offset_t new_offset = iter.get_back_offset() - trimmed;
  mut.copy_in_absolute((void*)iter.item_range.p_end, new_offset);
  return trim_size;
}

template <node_type_t NODE_TYPE>
node_offset_t ITER_T::erase(
    NodeExtentMutable& mut, const ITER_T& iter, const char* p_left_bound)
{
  node_offset_t erase_size = iter.p_end() - iter.p_start();
  const char* p_shift_start = p_left_bound;
  assert(p_left_bound <= iter.p_start());
  extent_len_t shift_len = iter.p_start() - p_left_bound;
  int shift_off = erase_size;
  mut.shift_absolute(p_shift_start, shift_len, shift_off);
  return erase_size;
}

#define ITER_TEMPLATE(NT) template class ITER_INST(NT)
ITER_TEMPLATE(node_type_t::LEAF);
ITER_TEMPLATE(node_type_t::INTERNAL);

#define APPEND_T ITER_T::Appender<KT>

template <node_type_t NODE_TYPE>
template <KeyT KT>
APPEND_T::Appender(NodeExtentMutable* p_mut,
                   const item_iterator_t& iter,
                   bool open) : p_mut{p_mut}
{
  assert(!iter.has_next());
  if (open) {
    p_append = const_cast<char*>(iter.get_key().p_start());
    p_offset_while_open = const_cast<char*>(iter.item_range.p_end);
  } else {
    // XXX: this doesn't need to advance the iter to last
    p_append = const_cast<char*>(iter.p_items_start);
  }
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
bool APPEND_T::append(const ITER_T& src, index_t& items)
{
  auto p_end = src.p_end();
  bool append_till_end = false;
  if (is_valid_index(items)) {
    for (auto i = 1u; i <= items; ++i) {
      if (!src.has_next()) {
        assert(i == items);
        append_till_end = true;
        break;
      }
      ++src;
    }
  } else {
    if (items == INDEX_END) {
      append_till_end = true;
    } else {
      assert(items == INDEX_LAST);
    }
    items = 0;
    while (src.has_next()) {
      ++src;
      ++items;
    }
    if (append_till_end) {
      ++items;
    }
  }

  const char* p_start;
  if (append_till_end) {
    p_start = src.p_start();
  } else {
    p_start = src.p_end();
  }
  assert(p_end >= p_start);
  size_t append_size = p_end - p_start;
  p_append -= append_size;
  p_mut->copy_in_absolute(p_append, p_start, append_size);
  return append_till_end;
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const key_get_type& partial_key)
{
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append(*p_mut, partial_key, p_append);
  return {p_mut, p_append};
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const full_key_t<KT>& key)
{
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append<KT>(*p_mut, key, p_append);
  return {p_mut, p_append};
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
void APPEND_T::wrap_nxt(char* _p_append)
{
  assert(_p_append < p_append);
  p_mut->copy_in_absolute(
      p_offset_while_open, node_offset_t(p_offset_while_open - _p_append));
  p_append = _p_append;
}

#define APPEND_TEMPLATE(NT, KT) template class ITER_INST(NT)::Appender<KT>
APPEND_TEMPLATE(node_type_t::LEAF, KeyT::VIEW);
APPEND_TEMPLATE(node_type_t::INTERNAL, KeyT::VIEW);
APPEND_TEMPLATE(node_type_t::LEAF, KeyT::HOBJ);
APPEND_TEMPLATE(node_type_t::INTERNAL, KeyT::HOBJ);

}
