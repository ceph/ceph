// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

/*
 * internal/leaf node N0, N1
 *
 * (_index)
 * p_items_start
 *  |   item_range ------------+
 *  |   |     +----key---------+
 *  |   |     |                |
 *  V   V     V                V
 * |   |sub  |oid char|ns char|colli-|   |
 * |...|items|array & |array &|-sion |...|
 * |   |...  |len     |len    |offset|   |
 *      ^                      |
 *      |                      |
 *      +---- back_offset -----+
 */
template <node_type_t NODE_TYPE>
class item_iterator_t {
  using value_t = value_type_t<NODE_TYPE>;
 public:
  item_iterator_t(const memory_range_t& range)
    : p_items_start(range.p_start) { next_item_range(range.p_end); }

  const char* p_start() const { return item_range.p_start; }
  const char* p_end() const { return item_range.p_end + sizeof(node_offset_t); }
  const memory_range_t& get_item_range() const { return item_range; }
  node_offset_t get_back_offset() const { return back_offset; }

  // container type system
  using key_get_type = const ns_oid_view_t&;
  static constexpr auto CONTAINER_TYPE = ContainerType::ITERATIVE;
  size_t index() const { return _index; }
  key_get_type get_key() const {
    if (!key.has_value()) {
      key = ns_oid_view_t(item_range.p_end);
      assert(item_range.p_start < (*key).p_start());
    }
    return *key;
  }
  size_t size() const {
    return item_range.p_end - item_range.p_start + sizeof(node_offset_t);
  };
  size_t size_to_nxt() const {
    return get_key().size() + sizeof(node_offset_t);
  }
  memory_range_t get_nxt_container() const {
    return {item_range.p_start, get_key().p_start()};
  }
  bool has_next() const {
    assert(p_items_start <= item_range.p_start);
    return p_items_start < item_range.p_start;
  }
  const item_iterator_t<NODE_TYPE>& operator++() const {
    assert(has_next());
    next_item_range(item_range.p_start);
    key.reset();
    ++_index;
    return *this;
  }

  static node_offset_t header_size() { return 0u; }

  template <KeyT KT>
  static node_offset_t estimate_insert(
      const full_key_t<KT>& key, const value_t&) {
    return ns_oid_view_t::estimate_size<KT>(key) + sizeof(node_offset_t);
  }

  template <KeyT KT>
  static memory_range_t insert_prefix(
      NodeExtentMutable& mut, const item_iterator_t<NODE_TYPE>& iter,
      const full_key_t<KT>& key, bool is_end,
      node_offset_t size, const char* p_left_bound);

  static void update_size(
      NodeExtentMutable& mut, const item_iterator_t<NODE_TYPE>& iter, int change);

  static size_t trim_until(NodeExtentMutable&, const item_iterator_t<NODE_TYPE>&);
  static size_t trim_at(
      NodeExtentMutable&, const item_iterator_t<NODE_TYPE>&, size_t trimmed);

  enum class index_t { none, last, end };
  template <KeyT KT>
  class Appender;

 private:
  void next_item_range(const char* p_end) const {
    auto p_item_end = p_end - sizeof(node_offset_t);
    assert(p_items_start < p_item_end);
    back_offset = *reinterpret_cast<const node_offset_t*>(p_item_end);
    assert(back_offset);
    const char* p_item_start = p_item_end - back_offset;
    assert(p_items_start <= p_item_start);
    item_range = {p_item_start, p_item_end};
  }

  const char* p_items_start;
  mutable memory_range_t item_range;
  mutable node_offset_t back_offset;
  mutable std::optional<ns_oid_view_t> key;
  mutable size_t _index = 0u;
};

template <node_type_t NODE_TYPE>
template <KeyT KT>
class item_iterator_t<NODE_TYPE>::Appender {
 public:
  Appender(NodeExtentMutable* p_mut, char* p_append)
    : p_mut{p_mut}, p_append{p_append} {}
  bool append(const item_iterator_t<NODE_TYPE>& src, size_t& items, index_t type);
  char* wrap() { return p_append; }
  std::tuple<NodeExtentMutable*, char*> open_nxt(const key_get_type&);
  std::tuple<NodeExtentMutable*, char*> open_nxt(const full_key_t<KT>&);
  void wrap_nxt(char* _p_append);

 private:
  NodeExtentMutable* p_mut;
  char* p_append;
  char* p_offset_while_open;
};

}
