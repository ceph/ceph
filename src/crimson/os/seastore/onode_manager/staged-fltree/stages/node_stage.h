// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

/**
 * node_extent_t
 *
 * The top indexing stage implementation for node N0/N1/N2/N3, implements
 * staged contract as an indexable container, and provides access to node
 * header.
 *
 * The specific field layout are defined by FieldType which are
 * node_fields_0_t, node_fields_1_t, node_fields_2_t, internal_fields_3_t and
 * leaf_fields_3_t. Diagrams see node_stage_layout.h.
 */
template <typename FieldType, node_type_t _NODE_TYPE>
class node_extent_t {
 public:
  using value_input_t = value_input_type_t<_NODE_TYPE>;
  using value_t = value_type_t<_NODE_TYPE>;
  using num_keys_t = typename FieldType::num_keys_t;
  static constexpr node_type_t NODE_TYPE = _NODE_TYPE;
  static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;

  // TODO: remove
  node_extent_t() = default;

  node_extent_t(const FieldType* p_fields, extent_len_t node_size)
      : p_fields{p_fields}, node_size{node_size} {
    assert(is_valid_node_size(node_size));
    validate(*p_fields);
  }

  const char* p_start() const { return fields_start(*p_fields); }

  bool is_level_tail() const { return p_fields->is_level_tail(); }
  level_t level() const { return p_fields->header.level; }
  node_offset_t free_size() const {
    return p_fields->template free_size_before<NODE_TYPE>(
        keys(), node_size);
  }
  extent_len_t total_size() const {
    return p_fields->total_size(node_size);
  }
  const char* p_left_bound() const;
  template <node_type_t T = NODE_TYPE>
  std::enable_if_t<T == node_type_t::INTERNAL, const laddr_packed_t*>
  get_end_p_laddr() const {
    assert(is_level_tail());
    if constexpr (FIELD_TYPE == field_type_t::N3) {
      return p_fields->get_p_child_addr(keys(), node_size);
    } else {
      auto offset_start = p_fields->get_item_end_offset(
          keys(), node_size);
      assert(offset_start <= node_size);
      offset_start -= sizeof(laddr_packed_t);
      auto p_addr = p_start() + offset_start;
      return reinterpret_cast<const laddr_packed_t*>(p_addr);
    }
  }

  // container type system
  using key_get_type = typename FieldType::key_get_type;
  static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
  index_t keys() const { return p_fields->num_keys; }
  key_get_type operator[] (index_t index) const {
    return p_fields->get_key(index, node_size);
  }
  extent_len_t size_before(index_t index) const {
    auto free_size = p_fields->template free_size_before<NODE_TYPE>(
        index, node_size);
    assert(total_size() >= free_size);
    return total_size() - free_size;
  }
  node_offset_t size_to_nxt_at(index_t index) const;
  node_offset_t size_overhead_at(index_t index) const {
    return FieldType::ITEM_OVERHEAD; }
  container_range_t get_nxt_container(index_t index) const;

  template <typename T = FieldType>
  std::enable_if_t<T::FIELD_TYPE == field_type_t::N3, const value_t*>
  get_p_value(index_t index) const {
    assert(index < keys());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      return p_fields->get_p_child_addr(index, node_size);
    } else {
      auto range = get_nxt_container(index).range;
      auto ret = reinterpret_cast<const value_header_t*>(range.p_start);
      assert(range.p_start + ret->allocation_size() == range.p_end);
      return ret;
    }
  }

  void encode(const char* p_node_start, ceph::bufferlist& encoded) const {
    assert(p_node_start == p_start());
    // nothing to encode as the container range is the entire extent
  }

  static node_extent_t decode(const char* p_node_start,
                              extent_len_t node_size,
                              ceph::bufferlist::const_iterator& delta) {
    // nothing to decode
    return node_extent_t(
        reinterpret_cast<const FieldType*>(p_node_start),
        node_size);
  }

  static void validate(const FieldType& fields) {
#ifndef NDEBUG
    assert(fields.header.get_node_type() == NODE_TYPE);
    assert(fields.header.get_field_type() == FieldType::FIELD_TYPE);
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(fields.header.level > 0u);
    } else {
      assert(fields.header.level == 0u);
    }
#endif
  }

  static void bootstrap_extent(
      NodeExtentMutable&, field_type_t, node_type_t, bool, level_t);

  static void update_is_level_tail(NodeExtentMutable&, const node_extent_t&, bool);

  static node_offset_t header_size() { return FieldType::HEADER_SIZE; }

  template <KeyT KT>
  static node_offset_t estimate_insert(
      const full_key_t<KT>& key, const value_input_t& value) {
    auto size = FieldType::estimate_insert_one();
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      size += ns_oid_view_t::estimate_size<KT>(key);
    } else if constexpr (FIELD_TYPE == field_type_t::N3 &&
                         NODE_TYPE == node_type_t::LEAF) {
      size += value.allocation_size();
    }
    return size;
  }

  template <KeyT KT>
  static const value_t* insert_at(
      NodeExtentMutable& mut, const node_extent_t&,
      const full_key_t<KT>& key, const value_input_t& value,
      index_t index, node_offset_t size, const char* p_left_bound) {
    if constexpr (FIELD_TYPE == field_type_t::N3) {
      ceph_abort("not implemented");
    } else {
      ceph_abort("impossible");
    }
  }

  template <KeyT KT>
  static memory_range_t insert_prefix_at(
      NodeExtentMutable&, const node_extent_t&,
      const full_key_t<KT>& key,
      index_t index, node_offset_t size, const char* p_left_bound);

  static void update_size_at(
      NodeExtentMutable&, const node_extent_t&, index_t index, int change);

  static node_offset_t trim_until(
      NodeExtentMutable&, const node_extent_t&, index_t index);
  static node_offset_t trim_at(NodeExtentMutable&, const node_extent_t&,
                        index_t index, node_offset_t trimmed);

  static node_offset_t erase_at(NodeExtentMutable&, const node_extent_t&,
                                index_t index, const char* p_left_bound);

  template <KeyT KT>
  class Appender;

 private:
  const FieldType& fields() const { return *p_fields; }
  const FieldType* p_fields;
  extent_len_t node_size;
};

template <typename FieldType, node_type_t NODE_TYPE>
template <KeyT KT>
class node_extent_t<FieldType, NODE_TYPE>::Appender {
 public:
  Appender(NodeExtentMutable* p_mut, char* p_append)
    : p_mut{p_mut}, p_start{p_append} {
#ifndef NDEBUG
    auto p_fields = reinterpret_cast<const FieldType*>(p_append);
    assert(*(p_fields->header.get_field_type()) == FIELD_TYPE);
    assert(p_fields->header.get_node_type() == NODE_TYPE);
    assert(p_fields->num_keys == 0);
#endif
    p_append_left = p_start + FieldType::HEADER_SIZE;
    p_append_right = p_start + p_mut->get_length();
  }
  Appender(NodeExtentMutable*, const node_extent_t&, bool open = false);
  void append(const node_extent_t& src, index_t from, index_t items);
  void append(const full_key_t<KT>&, const value_input_t&, const value_t*&);
  char* wrap();
  std::tuple<NodeExtentMutable*, char*> open_nxt(const key_get_type&);
  std::tuple<NodeExtentMutable*, char*> open_nxt(const full_key_t<KT>&);
  void wrap_nxt(char* p_append) {
    if constexpr (FIELD_TYPE != field_type_t::N3) {
      assert(p_append < p_append_right);
      assert(p_append_left < p_append);
      p_append_right = p_append;
      auto new_offset = p_append - p_start;
      assert(new_offset > 0);
      assert(new_offset < p_mut->get_length());
      FieldType::append_offset(*p_mut, new_offset, p_append_left);
      ++num_keys;
    } else {
      ceph_abort("not implemented");
    }
  }

 private:
  const node_extent_t* p_src = nullptr;
  NodeExtentMutable* p_mut;
  char* p_start;
  char* p_append_left;
  char* p_append_right;
  num_keys_t num_keys = 0;
};

}
