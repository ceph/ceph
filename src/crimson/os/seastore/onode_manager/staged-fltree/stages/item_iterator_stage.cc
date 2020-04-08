// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "item_iterator_stage.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

#define ITER_T item_iterator_t<NODE_TYPE>
#define ITER_INST(NT) item_iterator_t<NT>
#define ITER_TEMPLATE(NT) template class ITER_INST(NT)
ITER_TEMPLATE(node_type_t::LEAF);
ITER_TEMPLATE(node_type_t::INTERNAL);

template <node_type_t NODE_TYPE>
template <KeyT KT>
memory_range_t ITER_T::insert_prefix(
    NodeExtentMutable& mut, const ITER_T& iter, const full_key_t<KT>& key,
    bool is_end, node_offset_t size, const char* p_left_bound) {
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
    NodeExtentMutable& mut, const ITER_T& iter, int change) {
  node_offset_t offset = iter.get_back_offset();
  assert(change + offset > 0);
  assert(change + offset < NODE_BLOCK_SIZE);
  mut.copy_in_absolute(
      (void*)iter.get_item_range().p_end, node_offset_t(offset + change));
}

template <node_type_t NODE_TYPE>
size_t ITER_T::trim_until(NodeExtentMutable&, const ITER_T& iter) {
  assert(iter.index() != 0);
  return iter.p_end() - iter.p_items_start;
}

template <node_type_t NODE_TYPE>
size_t ITER_T::trim_at(
    NodeExtentMutable& mut, const ITER_T& iter, size_t trimmed) {
  size_t trim_size = iter.p_start() - iter.p_items_start + trimmed;
  assert(iter.get_back_offset() > trimmed);
  node_offset_t new_offset = iter.get_back_offset() - trimmed;
  mut.copy_in_absolute((void*)iter.item_range.p_end, new_offset);
  return trim_size;
}

#define APPEND_T ITER_T::Appender<KT>
template class ITER_INST(node_type_t::LEAF)::Appender<KeyT::VIEW>;
template class ITER_INST(node_type_t::INTERNAL)::Appender<KeyT::VIEW>;
template class ITER_INST(node_type_t::LEAF)::Appender<KeyT::HOBJ>;
template class ITER_INST(node_type_t::INTERNAL)::Appender<KeyT::HOBJ>;

template <node_type_t NODE_TYPE>
template <KeyT KT>
bool APPEND_T::append(const ITER_T& src, size_t& items, index_t type) {
  auto p_end = src.p_end();
  if (items != INDEX_END) {
    for (auto i = 1u; i <= items; ++i) {
      if (!src.has_next()) {
        assert(i == items);
        type = index_t::end;
        break;
      }
      ++src;
    }
  } else if (type != index_t::none) {
    items = 0;
    while (src.has_next()) {
      ++src;
      ++items;
    }
    if (type == index_t::end) {
      ++items;
    }
  } else {
    assert(false);
  }
  const char* p_start;
  if (type == index_t::end) {
    // include last
    p_start = src.p_start();
  } else {
    // exclude last
    p_start = src.p_end();
  }
  assert(p_end >= p_start);
  size_t append_size = p_end - p_start;
  p_append -= append_size;
  p_mut->copy_in_absolute(p_append, p_start, append_size);
  return type == index_t::end;
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const key_get_type& partial_key) {
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append(*p_mut, partial_key, p_append);
  return {p_mut, p_append};
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const full_key_t<KT>& key) {
  p_append -= sizeof(node_offset_t);
  p_offset_while_open = p_append;
  ns_oid_view_t::append<KT>(*p_mut, key, p_append);
  return {p_mut, p_append};
}

template <node_type_t NODE_TYPE>
template <KeyT KT>
void APPEND_T::wrap_nxt(char* _p_append) {
  assert(_p_append < p_append);
  p_mut->copy_in_absolute(
      p_offset_while_open, node_offset_t(p_offset_while_open - _p_append));
  p_append = _p_append;
}

}
