// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node_stage.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"
#include "node_stage_layout.h"

namespace crimson::os::seastore::onode {

#define NODE_T node_extent_t<FieldType, NODE_TYPE>
#define NODE_INST(FT, NT) node_extent_t<FT, NT>

template <typename FieldType, node_type_t NODE_TYPE>
const char* NODE_T::p_left_bound() const
{
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    // N3 internal node doesn't have the right part
    return nullptr;
  } else {
    auto ret = p_start() + fields().get_item_end_offset(keys());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (is_level_tail()) {
        ret -= sizeof(laddr_t);
      }
    }
    return ret;
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
node_offset_t NODE_T::size_to_nxt_at(index_t index) const
{
  assert(index < keys());
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    return FieldType::estimate_insert_one();
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    auto p_end = p_start() + p_fields->get_item_end_offset(index);
    return FieldType::estimate_insert_one() + ns_oid_view_t(p_end).size();
  } else {
    ceph_abort("N3 node is not nested");
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
memory_range_t NODE_T::get_nxt_container(index_t index) const
{
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    ceph_abort("N3 internal node doesn't have the right part");
  } else {
    node_offset_t item_start_offset = p_fields->get_item_start_offset(index);
    node_offset_t item_end_offset = p_fields->get_item_end_offset(index);
    assert(item_start_offset < item_end_offset);
    auto item_p_start = p_start() + item_start_offset;
    auto item_p_end = p_start() + item_end_offset;
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      // range for sub_items_t<NODE_TYPE>
      item_p_end = ns_oid_view_t(item_p_end).p_start();
      assert(item_p_start < item_p_end);
    } else {
      // range for item_iterator_t<NODE_TYPE>
    }
    return {item_p_start, item_p_end};
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
void NODE_T::bootstrap_extent(
    NodeExtentMutable& mut,
    field_type_t field_type, node_type_t node_type,
    bool is_level_tail, level_t level)
{
  node_header_t::bootstrap_extent(
      mut, field_type, node_type, is_level_tail, level);
  mut.copy_in_relative(
      sizeof(node_header_t), typename FieldType::num_keys_t(0u));
}

template <typename FieldType, node_type_t NODE_TYPE>
void NODE_T::update_is_level_tail(
    NodeExtentMutable& mut, const node_extent_t& extent, bool value)
{
  node_header_t::update_is_level_tail(mut, extent.p_fields->header, value);
}

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
memory_range_t NODE_T::insert_prefix_at(
    NodeExtentMutable& mut, const node_extent_t& node, const full_key_t<KT>& key,
    index_t index, node_offset_t size, const char* p_left_bound)
{
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    assert(index <= node.keys());
    assert(p_left_bound == node.p_left_bound());
    assert(size > FieldType::estimate_insert_one());
    auto size_right = size - FieldType::estimate_insert_one();
    const char* p_insert = node.p_start() + node.fields().get_item_end_offset(index);
    const char* p_insert_front = p_insert - size_right;
    FieldType::template insert_at<KT>(mut, key, node.fields(), index, size_right);
    mut.shift_absolute(p_left_bound,
                       p_insert - p_left_bound,
                       -(int)size_right);
    return {p_insert_front, p_insert};
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    ceph_abort("not implemented");
  } else {
    ceph_abort("impossible");
  }
}
#define IPA_TEMPLATE(FT, NT, KT)                                         \
  template memory_range_t NODE_INST(FT, NT)::insert_prefix_at<KT>(       \
      NodeExtentMutable&, const node_extent_t&, const full_key_t<KT>&, \
      index_t, node_offset_t, const char*)
IPA_TEMPLATE(node_fields_0_t, node_type_t::INTERNAL, KeyT::VIEW);
IPA_TEMPLATE(node_fields_1_t, node_type_t::INTERNAL, KeyT::VIEW);
IPA_TEMPLATE(node_fields_2_t, node_type_t::INTERNAL, KeyT::VIEW);
IPA_TEMPLATE(node_fields_0_t, node_type_t::LEAF, KeyT::VIEW);
IPA_TEMPLATE(node_fields_1_t, node_type_t::LEAF, KeyT::VIEW);
IPA_TEMPLATE(node_fields_2_t, node_type_t::LEAF, KeyT::VIEW);
IPA_TEMPLATE(node_fields_0_t, node_type_t::INTERNAL, KeyT::HOBJ);
IPA_TEMPLATE(node_fields_1_t, node_type_t::INTERNAL, KeyT::HOBJ);
IPA_TEMPLATE(node_fields_2_t, node_type_t::INTERNAL, KeyT::HOBJ);
IPA_TEMPLATE(node_fields_0_t, node_type_t::LEAF, KeyT::HOBJ);
IPA_TEMPLATE(node_fields_1_t, node_type_t::LEAF, KeyT::HOBJ);
IPA_TEMPLATE(node_fields_2_t, node_type_t::LEAF, KeyT::HOBJ);

template <typename FieldType, node_type_t NODE_TYPE>
void NODE_T::update_size_at(
    NodeExtentMutable& mut, const node_extent_t& node, index_t index, int change)
{
  assert(index < node.keys());
  FieldType::update_size_at(mut, node.fields(), index, change);
}

template <typename FieldType, node_type_t NODE_TYPE>
node_offset_t NODE_T::trim_until(
    NodeExtentMutable& mut, const node_extent_t& node, index_t index)
{
  assert(!node.is_level_tail());
  auto keys = node.keys();
  assert(index <= keys);
  if (index == keys) {
    return 0;
  }
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    ceph_abort("not implemented");
  } else {
    mut.copy_in_absolute(
        (void*)&node.p_fields->num_keys, num_keys_t(index));
  }
  // no need to calculate trim size for node
  return 0;
}

template <typename FieldType, node_type_t NODE_TYPE>
node_offset_t NODE_T::trim_at(
    NodeExtentMutable& mut, const node_extent_t& node,
    index_t index, node_offset_t trimmed)
{
  assert(!node.is_level_tail());
  assert(index < node.keys());
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    ceph_abort("not implemented");
  } else {
    node_offset_t offset = node.p_fields->get_item_start_offset(index);
    size_t new_offset = offset + trimmed;
    assert(new_offset < node.p_fields->get_item_end_offset(index));
    mut.copy_in_absolute(const_cast<void*>(node.p_fields->p_offset(index)),
                         node_offset_t(new_offset));
    mut.copy_in_absolute(
        (void*)&node.p_fields->num_keys, num_keys_t(index + 1));
  }
  // no need to calculate trim size for node
  return 0;
}

#define NODE_TEMPLATE(FT, NT) template class NODE_INST(FT, NT)
NODE_TEMPLATE(node_fields_0_t, node_type_t::INTERNAL);
NODE_TEMPLATE(node_fields_1_t, node_type_t::INTERNAL);
NODE_TEMPLATE(node_fields_2_t, node_type_t::INTERNAL);
NODE_TEMPLATE(internal_fields_3_t, node_type_t::INTERNAL);
NODE_TEMPLATE(node_fields_0_t, node_type_t::LEAF);
NODE_TEMPLATE(node_fields_1_t, node_type_t::LEAF);
NODE_TEMPLATE(node_fields_2_t, node_type_t::LEAF);
NODE_TEMPLATE(leaf_fields_3_t, node_type_t::LEAF);

#define APPEND_T node_extent_t<FieldType, NODE_TYPE>::Appender<KT>

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
void APPEND_T::append(const node_extent_t& src, index_t from, index_t items)
{
  assert(from <= src.keys());
  if (p_src == nullptr) {
    p_src = &src;
  } else {
    assert(p_src == &src);
  }
  if (items == 0) {
    return;
  }
  assert(from < src.keys());
  assert(from + items <= src.keys());
  num_keys += items;
  if constexpr (std::is_same_v<FieldType, internal_fields_3_t>) {
    ceph_abort("impossible path");
  } else {
    // append left part forwards
    node_offset_t offset_left_start = src.fields().get_key_start_offset(from);
    node_offset_t offset_left_end = src.fields().get_key_start_offset(from + items);
    node_offset_t left_size = offset_left_end - offset_left_start;
    if (num_keys == 0) {
      // no need to adjust offset
      assert(from == 0);
      assert(p_start + offset_left_start == p_append_left);
      p_mut->copy_in_absolute(p_append_left,
          src.p_start() + offset_left_start, left_size);
    } else {
      node_offset_t step_size = FieldType::estimate_insert_one();
      node_offset_t offset_base = src.fields().get_item_end_offset(from);
      int offset_change = p_append_right - p_start - offset_base;
      auto p_offset_dst = p_append_left;
      if constexpr (FIELD_TYPE != field_type_t::N2) {
        // copy keys
        p_mut->copy_in_absolute(p_append_left,
            src.p_start() + offset_left_start, left_size);
        // point to offset for update
        p_offset_dst += sizeof(typename FieldType::key_t);
      }
      for (auto i = from; i < from + items; ++i) {
        p_mut->copy_in_absolute(p_offset_dst,
            node_offset_t(src.fields().get_item_start_offset(i) + offset_change));
        p_offset_dst += step_size;
      }
      assert(p_append_left + left_size + sizeof(typename FieldType::key_t) ==
             p_offset_dst);
    }
    p_append_left += left_size;

    // append right part backwards
    node_offset_t offset_right_start = src.fields().get_item_end_offset(from + items);
    node_offset_t offset_right_end = src.fields().get_item_end_offset(from);
    node_offset_t right_size = offset_right_end - offset_right_start;
    p_append_right -= right_size;
    p_mut->copy_in_absolute(p_append_right,
        src.p_start() + offset_right_start, right_size);
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
void APPEND_T::append(
    const full_key_t<KT>& key, const value_input_t& value, const value_t*& p_value)
{
  if constexpr (FIELD_TYPE == field_type_t::N3) {
    ceph_abort("not implemented");
  } else {
    ceph_abort("should not happen");
  }
}

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const key_get_type& partial_key)
{
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    FieldType::append_key(*p_mut, partial_key, p_append_left);
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    FieldType::append_key(*p_mut, partial_key, p_append_right);
  } else {
    ceph_abort("impossible path");
  }
  return {p_mut, p_append_right};
}

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
std::tuple<NodeExtentMutable*, char*>
APPEND_T::open_nxt(const full_key_t<KT>& key)
{
  if constexpr (FIELD_TYPE == field_type_t::N0 ||
                FIELD_TYPE == field_type_t::N1) {
    FieldType::template append_key<KT>(*p_mut, key, p_append_left);
  } else if constexpr (FIELD_TYPE == field_type_t::N2) {
    FieldType::template append_key<KT>(*p_mut, key, p_append_right);
  } else {
    ceph_abort("impossible path");
  }
  return {p_mut, p_append_right};
}

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
char* APPEND_T::wrap()
{
  assert(p_append_left <= p_append_right);
  assert(p_src);
  if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
    if (p_src->is_level_tail()) {
      laddr_t tail_value = p_src->get_end_p_laddr()->value;
      p_append_right -= sizeof(laddr_t);
      assert(p_append_left <= p_append_right);
      p_mut->copy_in_absolute(p_append_right, tail_value);
    }
  }
  p_mut->copy_in_absolute(p_start + offsetof(FieldType, num_keys), num_keys);
  return p_append_left;
}

#define APPEND_TEMPLATE(FT, NT, KT) template class node_extent_t<FT, NT>::Appender<KT>
APPEND_TEMPLATE(node_fields_0_t, node_type_t::INTERNAL, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_1_t, node_type_t::INTERNAL, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_2_t, node_type_t::INTERNAL, KeyT::VIEW);
APPEND_TEMPLATE(internal_fields_3_t, node_type_t::INTERNAL, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_0_t, node_type_t::LEAF, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_1_t, node_type_t::LEAF, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_2_t, node_type_t::LEAF, KeyT::VIEW);
APPEND_TEMPLATE(leaf_fields_3_t, node_type_t::LEAF, KeyT::VIEW);
APPEND_TEMPLATE(node_fields_0_t, node_type_t::INTERNAL, KeyT::HOBJ);
APPEND_TEMPLATE(node_fields_1_t, node_type_t::INTERNAL, KeyT::HOBJ);
APPEND_TEMPLATE(node_fields_2_t, node_type_t::INTERNAL, KeyT::HOBJ);
APPEND_TEMPLATE(internal_fields_3_t, node_type_t::INTERNAL, KeyT::HOBJ);
APPEND_TEMPLATE(node_fields_0_t, node_type_t::LEAF, KeyT::HOBJ);
APPEND_TEMPLATE(node_fields_1_t, node_type_t::LEAF, KeyT::HOBJ);
APPEND_TEMPLATE(node_fields_2_t, node_type_t::LEAF, KeyT::HOBJ);
APPEND_TEMPLATE(leaf_fields_3_t, node_type_t::LEAF, KeyT::HOBJ);

}
