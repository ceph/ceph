// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <variant>

#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

struct internal_sub_item_t {
  const snap_gen_t& get_key() const { return key; }
  #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
  const laddr_t* get_p_value() const { return &value; }

  snap_gen_t key;
  laddr_t value;
} __attribute__((packed));

/*
 * internal node N0, N1, N2
 *
 * p_first_item +
 * (num_items)  |
 *              V
 * |   fix|sub |fix|sub |
 * |...key|addr|key|addr|
 * |   1  |1   |0  |0   |
 */
class internal_sub_items_t {
 public:
  using num_keys_t = size_t;

  internal_sub_items_t(const memory_range_t& range) {
    assert(range.p_start < range.p_end);
    assert((range.p_end - range.p_start) % sizeof(internal_sub_item_t) == 0);
    num_items = (range.p_end - range.p_start) / sizeof(internal_sub_item_t);
    assert(num_items > 0);
    auto _p_first_item = range.p_end - sizeof(internal_sub_item_t);
    p_first_item = reinterpret_cast<const internal_sub_item_t*>(_p_first_item);
  }

  // container type system
  using key_get_type = const snap_gen_t&;
  static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
  num_keys_t keys() const { return num_items; }
  key_get_type operator[](size_t index) const {
    assert(index < num_items);
    return (p_first_item - index)->get_key();
  }
  size_t size_before(size_t index) const {
    return index * sizeof(internal_sub_item_t);
  }
  const laddr_t* get_p_value(size_t index) const {
    assert(index < num_items);
    return (p_first_item - index)->get_p_value();
  }

  static node_offset_t header_size() { return 0u; }

  template <KeyT KT>
  static node_offset_t estimate_insert(const full_key_t<KT>&, const laddr_t&) {
    return sizeof(internal_sub_item_t);
  }

  template <KeyT KT>
  static const laddr_t* insert_at(
      NodeExtentMutable&, const internal_sub_items_t&,
      const full_key_t<KT>&, const laddr_t&,
      size_t index, node_offset_t size, const char* p_left_bound);

  static size_t trim_until(NodeExtentMutable&, internal_sub_items_t&, size_t);

  template <KeyT KT>
  class Appender;

 private:
  size_t num_items;
  const internal_sub_item_t* p_first_item;
};

template <KeyT KT>
class internal_sub_items_t::Appender {
 public:
  Appender(NodeExtentMutable* p_mut, char* p_append)
    : p_mut{p_mut}, p_append{p_append} {}
  void append(const internal_sub_items_t& src, size_t from, size_t items);
  void append(const full_key_t<KT>&, const laddr_t&, const laddr_t*&);
  char* wrap() { return p_append; }
 private:
  const laddr_t** pp_value = nullptr;
  NodeExtentMutable* p_mut;
  char* p_append;
};

/*
 * leaf node N0, N1, N2
 *
 * p_num_keys -----------------+
 * p_offsets --------------+   |
 * p_items_end -----+      |   |
 *                  |      |   |
 *                  V      V   V
 * |   fix|o-  |fix|   off|off|num |
 * |...key|node|key|...set|set|sub |
 * |   1  |0   |0  |   1  |0  |keys|
 *         ^        |       |
 *         |        |       |
 *         +--------+ <=====+
 */
class leaf_sub_items_t {
 public:
  // TODO: decide by NODE_BLOCK_SIZE, sizeof(snap_gen_t),
  //       and the minimal size of onode_t
  using num_keys_t = uint8_t;

  leaf_sub_items_t(const memory_range_t& range) {
    assert(range.p_start < range.p_end);
    auto _p_num_keys = range.p_end - sizeof(num_keys_t);
    assert(range.p_start < _p_num_keys);
    p_num_keys = reinterpret_cast<const num_keys_t*>(_p_num_keys);
    assert(keys());
    auto _p_offsets = _p_num_keys - sizeof(node_offset_t);
    assert(range.p_start < _p_offsets);
    p_offsets = reinterpret_cast<const node_offset_t*>(_p_offsets);
    p_items_end = reinterpret_cast<const char*>(&get_offset(keys() - 1));
    assert(range.p_start < p_items_end);
    assert(range.p_start == p_start());
  }

  bool operator==(const leaf_sub_items_t& x) {
    return (p_num_keys == x.p_num_keys &&
            p_offsets == x.p_offsets &&
            p_items_end == x.p_items_end);
  }

  const char* p_start() const { return get_item_end(keys()); }

  const node_offset_t& get_offset(size_t index) const {
    assert(index < keys());
    return *(p_offsets - index);
  }

  const node_offset_t get_offset_to_end(size_t index) const {
    assert(index <= keys());
    return index == 0 ? 0 : get_offset(index - 1);
  }

  const char* get_item_start(size_t index) const {
    return p_items_end - get_offset(index);
  }

  const char* get_item_end(size_t index) const {
    return p_items_end - get_offset_to_end(index);
  }

  // container type system
  using key_get_type = const snap_gen_t&;
  static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
  num_keys_t keys() const { return *p_num_keys; }
  key_get_type operator[](size_t index) const {
    assert(index < keys());
    auto pointer = get_item_end(index);
    assert(get_item_start(index) < pointer);
    pointer -= sizeof(snap_gen_t);
    assert(get_item_start(index) < pointer);
    return *reinterpret_cast<const snap_gen_t*>(pointer);
  }
  size_t size_before(size_t index) const {
    assert(index <= keys());
    if (index == 0) {
      return sizeof(num_keys_t);
    }
    --index;
    auto ret = sizeof(num_keys_t) +
               (index + 1) * sizeof(node_offset_t) +
               get_offset(index);
    return ret;
  }
  const onode_t* get_p_value(size_t index) const {
    assert(index < keys());
    auto pointer = get_item_start(index);
    auto value = reinterpret_cast<const onode_t*>(pointer);
    assert(pointer + value->size + sizeof(snap_gen_t) == get_item_end(index));
    return value;
  }

  static node_offset_t header_size() { return sizeof(num_keys_t); }

  template <KeyT KT>
  static node_offset_t estimate_insert(const full_key_t<KT>&, const onode_t& value) {
    return value.size + sizeof(snap_gen_t) + sizeof(node_offset_t);
  }

  template <KeyT KT>
  static const onode_t* insert_at(
      NodeExtentMutable&, const leaf_sub_items_t&,
      const full_key_t<KT>&, const onode_t&,
      size_t index, node_offset_t size, const char* p_left_bound);

  static size_t trim_until(NodeExtentMutable&, leaf_sub_items_t&, size_t index);

  template <KeyT KT>
  class Appender;

 private:
  // TODO: support unaligned access
  const num_keys_t* p_num_keys;
  const node_offset_t* p_offsets;
  const char* p_items_end;
};

auto constexpr APPENDER_LIMIT = 3u;

template <KeyT KT>
class leaf_sub_items_t::Appender {
  struct range_items_t {
    size_t from;
    size_t items;
  };
  struct kv_item_t {
    const full_key_t<KT>* p_key;
    const onode_t* p_value;
  };
  using var_t = std::variant<range_items_t, kv_item_t>;

 public:
  Appender(NodeExtentMutable* p_mut, char* p_append)
    : p_mut{p_mut}, p_append{p_append} {
  }

  void append(const leaf_sub_items_t& src, size_t from, size_t items) {
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
  }
  void append(const full_key_t<KT>& key,
              const onode_t& value, const onode_t*& p_value) {
    assert(pp_value == nullptr);
    assert(cnt <= APPENDER_LIMIT);
    appends[cnt] = kv_item_t{&key, &value};
    ++cnt;
    pp_value = &p_value;
  }
  char* wrap();

 private:
  std::optional<leaf_sub_items_t> op_src;
  const onode_t** pp_value = nullptr;
  NodeExtentMutable* p_mut;
  char* p_append;
  var_t appends[APPENDER_LIMIT];
  size_t cnt = 0;
};

template <node_type_t> struct _sub_items_t;
template<> struct _sub_items_t<node_type_t::INTERNAL> { using type = internal_sub_items_t; };
template<> struct _sub_items_t<node_type_t::LEAF> { using type = leaf_sub_items_t; };
template <node_type_t NODE_TYPE>
using sub_items_t = typename _sub_items_t<NODE_TYPE>::type;

}
