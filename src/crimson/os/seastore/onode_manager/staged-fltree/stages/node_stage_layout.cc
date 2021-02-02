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
  for (const auto* p_slot = &node.slots[index];
       p_slot < &node.slots[node.num_keys];
       ++p_slot) {
    node_offset_t offset = p_slot->right_offset;
    mut.copy_in_absolute(
        (void*)&(p_slot->right_offset),
        node_offset_t(offset - change));
  }
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
template <KeyT KT>
void F013_T::insert_at(
    NodeExtentMutable& mut, const full_key_t<KT>& key,
    const me_t& node, index_t index, node_offset_t size_right)
{
  assert(index <= node.num_keys);
  update_size_at(mut, node, index, size_right);
  auto p_insert = const_cast<char*>(fields_start(node)) +
                  node.get_key_start_offset(index);
  auto p_shift_end = fields_start(node) + node.get_key_start_offset(node.num_keys);
  mut.shift_absolute(p_insert, p_shift_end - p_insert, estimate_insert_one());
  mut.copy_in_absolute((void*)&node.num_keys, num_keys_t(node.num_keys + 1));
  append_key(mut, key_t::template from_key<KT>(key), p_insert);
  append_offset(mut, node.get_item_end_offset(index) - size_right, p_insert);
}
#define IA_TEMPLATE(ST, KT) template void F013_INST(ST)::      \
    insert_at<KT>(NodeExtentMutable&, const full_key_t<KT>&, \
                  const F013_INST(ST)&, index_t, node_offset_t)
IA_TEMPLATE(slot_0_t, KeyT::VIEW);
IA_TEMPLATE(slot_1_t, KeyT::VIEW);
IA_TEMPLATE(slot_3_t, KeyT::VIEW);
IA_TEMPLATE(slot_0_t, KeyT::HOBJ);
IA_TEMPLATE(slot_1_t, KeyT::HOBJ);
IA_TEMPLATE(slot_3_t, KeyT::HOBJ);

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
