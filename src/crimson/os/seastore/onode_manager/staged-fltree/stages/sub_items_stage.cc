// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "sub_items_stage.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

template <KeyT KT>
const laddr_packed_t* internal_sub_items_t::insert_at(
    NodeExtentMutable& mut, const internal_sub_items_t& sub_items,
    const full_key_t<KT>& key, const laddr_t& value,
    index_t index, node_offset_t size, const char* p_left_bound)
{
  assert(index <= sub_items.keys());
  assert(size == estimate_insert<KT>(key, value));
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = reinterpret_cast<const char*>(
      sub_items.p_first_item + 1 - index);
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, -(int)size);

  auto p_insert = const_cast<char*>(p_shift_end) - size;
  auto item = internal_sub_item_t{
    snap_gen_t::from_key(key), laddr_packed_t{value}};
  mut.copy_in_absolute(p_insert, item);
  return &reinterpret_cast<internal_sub_item_t*>(p_insert)->value;
}
#define IA_TEMPLATE(KT)                                                     \
  template const laddr_packed_t* internal_sub_items_t::insert_at<KT>(       \
    NodeExtentMutable&, const internal_sub_items_t&, const full_key_t<KT>&, \
    const laddr_t&, index_t, node_offset_t, const char*)
IA_TEMPLATE(KeyT::VIEW);
IA_TEMPLATE(KeyT::HOBJ);

node_offset_t internal_sub_items_t::trim_until(
    NodeExtentMutable& mut, internal_sub_items_t& items, index_t index)
{
  assert(index != 0);
  auto keys = items.keys();
  assert(index <= keys);
  size_t ret = sizeof(internal_sub_item_t) * (keys - index);
  assert(ret < mut.get_length());
  return ret;
}

node_offset_t internal_sub_items_t::erase_at(
    NodeExtentMutable& mut, const internal_sub_items_t& sub_items,
    index_t index, const char* p_left_bound)
{
  assert(index < sub_items.keys());
  node_offset_t erase_size = sizeof(internal_sub_item_t);
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = reinterpret_cast<const char*>(
      sub_items.p_first_item - index);
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, erase_size);
  return erase_size;
}

template <KeyT KT>
void internal_sub_items_t::Appender<KT>::append(
    const internal_sub_items_t& src, index_t from, index_t items)
{
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
    const full_key_t<KT>& key, const laddr_t& value,
    const laddr_packed_t*& p_value)
{
  p_append -= sizeof(internal_sub_item_t);
  auto item = internal_sub_item_t{
    snap_gen_t::from_key(key), laddr_packed_t{value}};
  p_mut->copy_in_absolute(p_append, item);
  p_value = &reinterpret_cast<internal_sub_item_t*>(p_append)->value;
}

template <KeyT KT>
const value_header_t* leaf_sub_items_t::insert_at(
    NodeExtentMutable& mut, const leaf_sub_items_t& sub_items,
    const full_key_t<KT>& key, const value_config_t& value,
    index_t index, node_offset_t size, const char* p_left_bound)
{
  assert(index <= sub_items.keys());
  assert(size == estimate_insert<KT>(key, value));
  // a. [... item(index)] << size
  const char* p_shift_start = p_left_bound;
  const char* p_shift_end = sub_items.get_item_end(index);
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start, -(int)size);

  // b. insert item
  auto p_insert = const_cast<char*>(p_shift_end - size);
  auto p_value = reinterpret_cast<value_header_t*>(p_insert);
  p_value->initiate(mut, value);
  p_insert += value.allocation_size();
  mut.copy_in_absolute(p_insert, snap_gen_t::from_key(key));
  assert(p_insert + sizeof(snap_gen_t) + sizeof(node_offset_t) == p_shift_end);

  // c. compensate affected offsets
  auto item_size = value.allocation_size() + sizeof(snap_gen_t);
  for (auto i = index; i < sub_items.keys(); ++i) {
    const node_offset_packed_t& offset_i = sub_items.get_offset(i);
    mut.copy_in_absolute((void*)&offset_i, node_offset_t(offset_i.value + item_size));
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
template const value_header_t* leaf_sub_items_t::insert_at<KeyT::HOBJ>(
    NodeExtentMutable&, const leaf_sub_items_t&, const full_key_t<KeyT::HOBJ>&,
    const value_config_t&, index_t, node_offset_t, const char*);

node_offset_t leaf_sub_items_t::trim_until(
    NodeExtentMutable& mut, leaf_sub_items_t& items, index_t index)
{
  assert(index != 0);
  auto keys = items.keys();
  assert(index <= keys);
  if (index == keys) {
    return 0;
  }
  index_t trim_items = keys - index;
  const char* p_items_start = items.p_start();
  const char* p_shift_start = items.get_item_end(index);
  const char* p_shift_end = items.get_item_end(0);
  size_t size_trim_offsets = sizeof(node_offset_t) * trim_items;
  mut.shift_absolute(p_shift_start, p_shift_end - p_shift_start,
                     size_trim_offsets);
  mut.copy_in_absolute((void*)items.p_num_keys, num_keys_t(index));
  size_t ret = size_trim_offsets + (p_shift_start - p_items_start);
  assert(ret < mut.get_length());
  return ret;
}

node_offset_t leaf_sub_items_t::erase_at(
    NodeExtentMutable& mut, const leaf_sub_items_t& sub_items,
    index_t index, const char* p_left_bound)
{
  assert(sub_items.keys() > 0);
  assert(index < sub_items.keys());
  auto p_item_start = sub_items.get_item_start(index);
  auto p_item_end = sub_items.get_item_end(index);
  assert(p_item_start < p_item_end);
  node_offset_t item_erase_size = p_item_end - p_item_start;
  node_offset_t erase_size = item_erase_size + sizeof(node_offset_t);
  auto p_offset_end = (const char*)&sub_items.get_offset(index);

  // a. compensate affected offset[n] ... offset[index+1]
  for (auto i = index + 1; i < sub_items.keys(); ++i) {
    const node_offset_packed_t& offset_i = sub_items.get_offset(i);
    mut.copy_in_absolute((void*)&offset_i, node_offset_t(offset_i.value - item_erase_size));
  }

  // b. kv[index-1] ... kv[0] ... offset[index+1] >> sizeof(node_offset_t)
  mut.shift_absolute(p_item_end, p_offset_end - p_item_end, sizeof(node_offset_t));

  // c. ... kv[n] ... kv[index+1] >> item_erase_size
  mut.shift_absolute(p_left_bound, p_item_start - p_left_bound, erase_size);

  // d. update num_keys
  mut.copy_in_absolute((void*)sub_items.p_num_keys, num_keys_t(sub_items.keys() - 1));

  return erase_size;
}

template class internal_sub_items_t::Appender<KeyT::VIEW>;
template class internal_sub_items_t::Appender<KeyT::HOBJ>;

// helper type for the visitor
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <KeyT KT>
void leaf_sub_items_t::Appender<KT>::append(
    const leaf_sub_items_t& src, index_t from, index_t items)
{
  if (p_append) {
    // append from empty
    assert(cnt <= APPENDER_LIMIT);
    assert(from <= src.keys());
    if (items == 0) {
      return;
    }
    if (op_src) {
      assert(*op_src == src);
    } else {
      op_src = src;
    }
    assert(from < src.keys());
    assert(from + items <= src.keys());
    appends[cnt] = range_items_t{from, items};
    ++cnt;
  } else {
    // append from existing
    assert(op_dst.has_value());
    assert(!p_appended);
    assert(from == 0);
    assert(items);
    assert(items == src.keys());

    num_keys_t num_keys = op_dst->keys();
    node_offset_t compensate = op_dst->get_offset(num_keys - 1).value;
    const char* p_items_start = op_dst->p_start();
    const char* p_items_end = op_dst->p_items_end;

    // update dst num_keys
    num_keys += items;
    p_mut->copy_in_absolute((char*)op_dst->p_num_keys, num_keys);

    // shift dst items
    std::size_t src_offsets_size = sizeof(node_offset_t) * items;
    p_mut->shift_absolute(p_items_start,
                          p_items_end - p_items_start,
                          -(int)src_offsets_size);

    // fill offsets from src
    node_offset_t offset;
    char* p_cur_offset = const_cast<char*>(p_items_end);
    for (auto i = from; i < from + items; ++i) {
      offset = src.get_offset(i).value + compensate;
      p_cur_offset -= sizeof(node_offset_t);
      p_mut->copy_in_absolute(p_cur_offset, offset);
    }

    // fill items from src
    auto p_src_items_start = src.get_item_end(from + items);
    std::size_t src_items_size = src.get_item_end(from) - p_src_items_start;
    p_appended = const_cast<char*>(p_items_start) - src_offsets_size - src_items_size;
    p_mut->copy_in_absolute(p_appended, p_src_items_start, src_items_size);
  }
}

template <KeyT KT>
char* leaf_sub_items_t::Appender<KT>::wrap()
{
  if (op_dst.has_value()) {
    // append from existing
    assert(p_appended);
    return p_appended;
  }
  // append from empty
  assert(p_append);
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
          offset = op_src->get_offset(i).value + compensate;
          p_cur -= sizeof(node_offset_t);
          p_mut->copy_in_absolute(p_cur, offset);
        }
        last_offset = offset;
      },
      [&] (const kv_item_t& arg) {
        last_offset += sizeof(snap_gen_t) + arg.value_config.allocation_size();
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
        p_mut->copy_in_absolute(p_cur, snap_gen_t::from_key(*arg.p_key));
        p_cur -= arg.value_config.allocation_size();
        auto p_value = reinterpret_cast<value_header_t*>(p_cur);
        p_value->initiate(*p_mut, arg.value_config);
        *pp_value = p_value;
      }
    }, a);
  }
  return p_cur;
}

template class leaf_sub_items_t::Appender<KeyT::VIEW>;
template class leaf_sub_items_t::Appender<KeyT::HOBJ>;

}
