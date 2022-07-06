// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

/**
 * item_iterator_t
 *
 * The STAGE_STRING implementation for node N0/N1, implements staged contract
 * as an iterative container to resolve crush hash conflicts.
 *
 * The layout of the contaner to index ns, oid strings storing n items:
 *
 * # <--------- container range ---------> #
 * #<~># items [i+1, n)                    #
 * #   #                  items [0, i) #<~>#
 * #   # <------ item i -------------> #   #
 * #   # <--- item_range ---> |        #   #
 * #   #                      |        #   #
 * #   # next-stage | ns-oid  | back_  #   #
 * #   #  contaner  | strings | offset #   #
 * #...#   range    |         |        #...#
 * ^   ^                           |       ^
 * |   |                           |       |
 * |   +---------------------------+       |
 * + p_items_start             p_items_end +
 */
template <node_type_t NODE_TYPE>
class item_iterator_t {
  using value_input_t = value_input_type_t<NODE_TYPE>;
  using value_t = value_type_t<NODE_TYPE>;
 public:
  item_iterator_t(const container_range_t& range)
      : node_size{range.node_size},
        p_items_start(range.range.p_start),
        p_items_end(range.range.p_end) {
    assert(is_valid_node_size(node_size));
    assert(p_items_start < p_items_end);
    next_item_range(p_items_end);
  }

  const char* p_start() const { return item_range.p_start; }
  const char* p_end() const { return item_range.p_end + sizeof(node_offset_t); }
  const memory_range_t& get_item_range() const { return item_range; }
  node_offset_t get_back_offset() const { return back_offset; }

  // container type system
  using key_get_type = const ns_oid_view_t&;
  static constexpr auto CONTAINER_TYPE = ContainerType::ITERATIVE;
  index_t index() const { return _index; }
  key_get_type get_key() const {
    if (!key.has_value()) {
      key = ns_oid_view_t(item_range.p_end);
      assert(item_range.p_start < (*key).p_start());
    }
    return *key;
  }
  node_offset_t size() const {
    size_t ret = item_range.p_end - item_range.p_start + sizeof(node_offset_t);
    assert(ret < node_size);
    return ret;
  };
  node_offset_t size_to_nxt() const {
    size_t ret = get_key().size() + sizeof(node_offset_t);
    assert(ret < node_size);
    return ret;
  }
  node_offset_t size_overhead() const {
    return sizeof(node_offset_t) + get_key().size_overhead();
  }
  container_range_t get_nxt_container() const {
    return {{item_range.p_start, get_key().p_start()}, node_size};
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
  void encode(const char* p_node_start, ceph::bufferlist& encoded) const {
    int start_offset = p_items_start - p_node_start;
    int stage_size = p_items_end - p_items_start;
    assert(start_offset > 0);
    assert(stage_size > 0);
    assert(start_offset + stage_size <= (int)node_size);
    ceph::encode(static_cast<node_offset_t>(start_offset), encoded);
    ceph::encode(static_cast<node_offset_t>(stage_size), encoded);
    ceph::encode(_index, encoded);
  }

  static item_iterator_t decode(const char* p_node_start,
                                extent_len_t node_size,
                                ceph::bufferlist::const_iterator& delta) {
    node_offset_t start_offset;
    ceph::decode(start_offset, delta);
    node_offset_t stage_size;
    ceph::decode(stage_size, delta);
    assert(start_offset > 0);
    assert(stage_size > 0);
    assert((unsigned)start_offset + stage_size <= node_size);
    index_t index;
    ceph::decode(index, delta);

    item_iterator_t ret({{p_node_start + start_offset,
                          p_node_start + start_offset + stage_size},
                         node_size});
    while (index > 0) {
      ++ret;
      --index;
    }
    return ret;
  }

  static node_offset_t header_size() { return 0u; }

  template <KeyT KT>
  static node_offset_t estimate_insert(
      const full_key_t<KT>& key, const value_input_t&) {
    return ns_oid_view_t::estimate_size<KT>(key) + sizeof(node_offset_t);
  }

  template <KeyT KT>
  static memory_range_t insert_prefix(
      NodeExtentMutable& mut, const item_iterator_t<NODE_TYPE>& iter,
      const full_key_t<KT>& key, bool is_end,
      node_offset_t size, const char* p_left_bound);

  static void update_size(
      NodeExtentMutable& mut, const item_iterator_t<NODE_TYPE>& iter, int change);

  static node_offset_t trim_until(NodeExtentMutable&, const item_iterator_t<NODE_TYPE>&);
  static node_offset_t trim_at(
      NodeExtentMutable&, const item_iterator_t<NODE_TYPE>&, node_offset_t trimmed);

  static node_offset_t erase(
      NodeExtentMutable&, const item_iterator_t<NODE_TYPE>&, const char*);

  template <KeyT KT>
  class Appender;

 private:
  void next_item_range(const char* p_end) const {
    auto p_item_end = p_end - sizeof(node_offset_t);
    assert(p_items_start < p_item_end);
    back_offset = reinterpret_cast<const node_offset_packed_t*>(p_item_end)->value;
    assert(back_offset);
    const char* p_item_start = p_item_end - back_offset;
    assert(p_items_start <= p_item_start);
    item_range = {p_item_start, p_item_end};
  }

  extent_len_t node_size;
  const char* p_items_start;
  const char* p_items_end;
  mutable memory_range_t item_range;
  mutable node_offset_t back_offset;
  mutable std::optional<ns_oid_view_t> key;
  mutable index_t _index = 0u;
};

template <node_type_t NODE_TYPE>
template <KeyT KT>
class item_iterator_t<NODE_TYPE>::Appender {
 public:
  Appender(NodeExtentMutable* p_mut, char* p_append)
    : p_mut{p_mut}, p_append{p_append} {}
  Appender(NodeExtentMutable*, const item_iterator_t&, bool open);
  bool append(const item_iterator_t<NODE_TYPE>& src, index_t& items);
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
