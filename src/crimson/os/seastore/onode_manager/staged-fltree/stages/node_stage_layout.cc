// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node_stage_layout.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

void node_header_t::bootstrap_extent(
    NodeExtentMutable& mut,
    field_type_t field_type, node_type_t node_type,
    bool is_level_tail, level_t level)
{
  node_header_t header;
  header.set_field_type(field_type);
  header.set_node_type(node_type);
  header.set_is_level_tail(is_level_tail);
  header.level = level;
  mut.copy_in_relative(0, header);
}

void node_header_t::update_is_level_tail(
    NodeExtentMutable& mut, const node_header_t& header, bool value)
{
  auto& _header = const_cast<node_header_t&>(header);
  _header.set_is_level_tail(value);
  mut.validate_inplace_update(_header);
}

#define F013_T _node_fields_013_t<SlotType>
#define F013_INST(ST) _node_fields_013_t<ST>

template <typename SlotType>
void F013_T::update_size_at(
    NodeExtentMutable& mut, const me_t& node, index_t index, int change)
{
  assert(index <= node.num_keys);
  [[maybe_unused]] extent_len_t node_size = mut.get_length();
#ifndef NDEBUG
  // check underflow
  if (change < 0 && index != node.num_keys) {
    assert(node.get_item_start_offset(index, node_size) <
           node.get_item_end_offset(index, node_size));
  }
#endif
  for (const auto* p_slot = &node.slots[index];
       p_slot < &node.slots[node.num_keys];
       ++p_slot) {
    node_offset_t offset = p_slot->right_offset;
    int new_offset = offset - change;
    assert(new_offset > 0);
    assert(new_offset < (int)node_size);
    mut.copy_in_absolute(
        (void*)&(p_slot->right_offset),
        node_offset_t(new_offset));
  }
#ifndef NDEBUG
  // check overflow
  if (change > 0 && index != node.num_keys) {
    assert(node.num_keys > 0);
    assert(node.get_key_start_offset(node.num_keys, node_size) <=
           node.slots[node.num_keys - 1].right_offset);
  }
#endif
}

template <typename SlotType>
void F013_T::append_key(
    NodeExtentMutable& mut, const key_t& key, char*& p_append)
{
  mut.copy_in_absolute(p_append, key);
  p_append += sizeof(key_t);
}

template <typename SlotType>
void F013_T::append_offset(
    NodeExtentMutable& mut, node_offset_t offset_to_right, char*& p_append)
{
  mut.copy_in_absolute(p_append, offset_to_right);
  p_append += sizeof(node_offset_t);
}

template <typename SlotType>
template <IsFullKey Key>
void F013_T::insert_at(
    NodeExtentMutable& mut, const Key& key,
    const me_t& node, index_t index, node_offset_t size_right)
{
  assert(index <= node.num_keys);
  extent_len_t node_size = mut.get_length();
  update_size_at(mut, node, index, size_right);
  auto p_insert = const_cast<char*>(fields_start(node)) +
                  node.get_key_start_offset(index, node_size);
  auto p_shift_end = fields_start(node) +
                     node.get_key_start_offset(node.num_keys, node_size);
  mut.shift_absolute(p_insert, p_shift_end - p_insert, estimate_insert_one());
  mut.copy_in_absolute((void*)&node.num_keys, num_keys_t(node.num_keys + 1));
  append_key(mut, key_t::from_key(key), p_insert);
  int new_offset = node.get_item_end_offset(index, node_size) - size_right;
  assert(new_offset > 0);
  assert(new_offset < (int)node_size);
  append_offset(mut, new_offset, p_insert);
}
#define IA_TEMPLATE(ST, KT) template void F013_INST(ST)::      \
    insert_at<KT>(NodeExtentMutable&, const KT&, \
                  const F013_INST(ST)&, index_t, node_offset_t)
IA_TEMPLATE(slot_0_t, key_view_t);
IA_TEMPLATE(slot_1_t, key_view_t);
IA_TEMPLATE(slot_3_t, key_view_t);
IA_TEMPLATE(slot_0_t, key_hobj_t);
IA_TEMPLATE(slot_1_t, key_hobj_t);
IA_TEMPLATE(slot_3_t, key_hobj_t);

template <typename SlotType>
node_offset_t F013_T::erase_at(
    NodeExtentMutable& mut, const me_t& node, index_t index, const char* p_left_bound)
{
  extent_len_t node_size = mut.get_length();
  auto offset_item_start = node.get_item_start_offset(index, node_size);
  auto offset_item_end = node.get_item_end_offset(index, node_size);
  assert(offset_item_start < offset_item_end);
  auto erase_size = offset_item_end - offset_item_start;
  // fix and shift the left part
  update_size_at(mut, node, index + 1, -erase_size);
  const char* p_shift_start = fields_start(node) +
                              node.get_key_start_offset(index + 1, node_size);
  extent_len_t shift_len = sizeof(SlotType) * (node.num_keys - index - 1);
  int shift_off = -(int)sizeof(SlotType);
  mut.shift_absolute(p_shift_start, shift_len, shift_off);
  // shift the right part
  p_shift_start = p_left_bound;
  shift_len = fields_start(node) + offset_item_start - p_left_bound;
  shift_off = erase_size;
  mut.shift_absolute(p_shift_start, shift_len, shift_off);
  // fix num_keys
  mut.copy_in_absolute((void*)&node.num_keys, num_keys_t(node.num_keys - 1));
  return erase_size;
}

#define F013_TEMPLATE(ST) template struct F013_INST(ST)
F013_TEMPLATE(slot_0_t);
F013_TEMPLATE(slot_1_t);
F013_TEMPLATE(slot_3_t);

void node_fields_2_t::append_offset(
    NodeExtentMutable& mut, node_offset_t offset_to_right, char*& p_append)
{
  mut.copy_in_absolute(p_append, offset_to_right);
  p_append += sizeof(node_offset_t);
}

}
