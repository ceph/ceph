// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sub_items_stage.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

template <KeyT KT>
const laddr_t* internal_sub_items_t::insert_at(
    NodeExtentMutable& mut, const internal_sub_items_t& sub_items,
    const full_key_t<KT>& key, const laddr_t& value,
    size_t index, node_offset_t size, const char* p_left_bound) {
  assert(index <= sub_items.keys());
  assert(size == estimate_insert<KT>(key, value));
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = reinterpret_cast<const char*>(
      sub_items.p_first_item + 1 - index);
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, -(int)size);

  auto p_insert = const_cast<char*>(p_shift_end) - size;
  auto item = internal_sub_item_t{snap_gen_t::from_key<KT>(key), value};
  mut.copy_in_absolute(p_insert, item);
  return &reinterpret_cast<internal_sub_item_t*>(p_insert)->value;
}
template const laddr_t* internal_sub_items_t::insert_at<KeyT::VIEW>(
    NodeExtentMutable&, const internal_sub_items_t&, const full_key_t<KeyT::VIEW>&,
    const laddr_t&, size_t, node_offset_t, const char*);

size_t internal_sub_items_t::trim_until(
    NodeExtentMutable&, internal_sub_items_t& items, size_t index) {
  assert(index != 0);
  auto keys = items.keys();
  assert(index <= keys);
  return sizeof(internal_sub_item_t) * (keys - index);
}

template class internal_sub_items_t::Appender<KeyT::VIEW>;
template class internal_sub_items_t::Appender<KeyT::HOBJ>;

template <KeyT KT>
void internal_sub_items_t::Appender<KT>::append(
    const internal_sub_items_t& src, size_t from, size_t items) {
  assert(from <= src.keys());
  if (items == 0) {
    return;
  }
  assert(from < src.keys());
  assert(from + items <= src.keys());
  node_offset_t size = sizeof(internal_sub_item_t) * items;
  p_append -= size;
  p_mut->copy_in_absolute(p_append, src.p_first_item + 1 - from - items, size);
}

template <KeyT KT>
void internal_sub_items_t::Appender<KT>::append(
    const full_key_t<KT>& key, const laddr_t& value, const laddr_t*& p_value) {
  assert(pp_value == nullptr);
  p_append -= sizeof(internal_sub_item_t);
  auto item = internal_sub_item_t{snap_gen_t::from_key<KT>(key), value};
  p_mut->copy_in_absolute(p_append, item);
  p_value = &reinterpret_cast<internal_sub_item_t*>(p_append)->value;
}

template <KeyT KT>
const onode_t* leaf_sub_items_t::insert_at(
    NodeExtentMutable& mut, const leaf_sub_items_t& sub_items,
    const full_key_t<KT>& key, const onode_t& value,
    size_t index, node_offset_t size, const char* p_left_bound) {
  assert(index <= sub_items.keys());
  assert(size == estimate_insert<KT>(key, value));
  // a. [... item(index)] << size
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = sub_items.get_item_end(index);
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, -(int)size);

  // b. insert item
  auto p_insert = const_cast<char*>(p_shift_end - size);
  auto p_value = reinterpret_cast<const onode_t*>(p_insert);
  mut.copy_in_absolute(p_insert, &value, value.size);
  p_insert += value.size;
  mut.copy_in_absolute(p_insert, snap_gen_t::template from_key<KT>(key));
  assert(p_insert + sizeof(snap_gen_t) + sizeof(node_offset_t) == p_shift_end);

  // c. compensate affected offsets
  auto item_size = value.size + sizeof(snap_gen_t);
  for (auto i = index; i < sub_items.keys(); ++i) {
    const node_offset_t& offset_i = sub_items.get_offset(i);
    mut.copy_in_absolute((void*)&offset_i, node_offset_t(offset_i + item_size));
  }

  // d. [item(index-1) ... item(0) ... offset(index)] <<< sizeof(node_offset_t)
  const char* p_offset = (index == 0 ?
                          (const char*)&sub_items.get_offset(0) + sizeof(node_offset_t) :
                          (const char*)&sub_items.get_offset(index - 1));
  p_shift_start = p_shift_end;
  p_shift_end = p_offset;
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, -(int)sizeof(node_offset_t));

  // e. insert offset
  node_offset_t offset_to_item_start = item_size + sub_items.get_offset_to_end(index);
  mut.copy_in_absolute(
      const_cast<char*>(p_shift_end) - sizeof(node_offset_t), offset_to_item_start);

  // f. update num_sub_keys
  mut.copy_in_absolute((void*)sub_items.p_num_keys, num_keys_t(sub_items.keys() + 1));

  return p_value;
}
template const onode_t* leaf_sub_items_t::insert_at<KeyT::HOBJ>(
    NodeExtentMutable&, const leaf_sub_items_t&, const full_key_t<KeyT::HOBJ>&,
    const onode_t&, size_t, node_offset_t, const char*);

size_t leaf_sub_items_t::trim_until(
    NodeExtentMutable& mut, leaf_sub_items_t& items, size_t index) {
  assert(index != 0);
  auto keys = items.keys();
  assert(index <= keys);
  if (index == keys) {
    return 0;
  }
  size_t trim_items = keys - index;
  const char* p_items_start = items.p_start();
  const char* p_shift_start = items.get_item_end(index);
  const char* p_shift_end = items.get_item_end(0);
  size_t size_trim_offsets = sizeof(node_offset_t) * trim_items;
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start,
                     size_trim_offsets);
  mut.copy_in_absolute((void*)items.p_num_keys, num_keys_t(index));
  return size_trim_offsets + (p_shift_start - p_items_start);
}

// helper type for the visitor
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template class leaf_sub_items_t::Appender<KeyT::VIEW>;
template class leaf_sub_items_t::Appender<KeyT::HOBJ>;

template <KeyT KT>
char* leaf_sub_items_t::Appender<KT>::wrap() {
  auto p_cur = p_append;
  num_keys_t num_keys = 0;
  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) { num_keys += arg.items; },
      [&] (const kv_item_t& arg) { ++num_keys; }
    }, a);
  }
  assert(num_keys);
  p_cur -= sizeof(num_keys_t);
  p_mut->copy_in_absolute(p_cur, num_keys);

  node_offset_t last_offset = 0;
  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) {
        int compensate = (last_offset - op_src->get_offset_to_end(arg.from));
        node_offset_t offset;
        for (auto i = arg.from; i < arg.from + arg.items; ++i) {
          offset = op_src->get_offset(i) + compensate;
          p_cur -= sizeof(node_offset_t);
          p_mut->copy_in_absolute(p_cur, offset);
        }
        last_offset = offset;
      },
      [&] (const kv_item_t& arg) {
        last_offset += sizeof(snap_gen_t) + arg.p_value->size;
        p_cur -= sizeof(node_offset_t);
        p_mut->copy_in_absolute(p_cur, last_offset);
      }
    }, a);
  }

  for (auto i = 0u; i < cnt; ++i) {
    auto& a = appends[i];
    std::visit(overloaded {
      [&] (const range_items_t& arg) {
        auto _p_start = op_src->get_item_end(arg.from + arg.items);
        size_t _len = op_src->get_item_end(arg.from) - _p_start;
        p_cur -= _len;
        p_mut->copy_in_absolute(p_cur, _p_start, _len);
      },
      [&] (const kv_item_t& arg) {
        assert(pp_value);
        p_cur -= sizeof(snap_gen_t);
        p_mut->copy_in_absolute(p_cur, snap_gen_t::template from_key<KT>(*arg.p_key));
        p_cur -= arg.p_value->size;
        p_mut->copy_in_absolute(p_cur, arg.p_value, arg.p_value->size);
        *pp_value = reinterpret_cast<const onode_t*>(p_cur);
      }
    }, a);
  }
  return p_cur;
}

}
