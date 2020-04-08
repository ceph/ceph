// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "key_layout.h"
#include "stage_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

template <typename FieldType, node_type_t _NODE_TYPE>
class node_extent_t {
 public:
  using value_t = value_type_t<_NODE_TYPE>;
  using num_keys_t = typename FieldType::num_keys_t;
  static constexpr node_type_t NODE_TYPE = _NODE_TYPE;
  static constexpr field_type_t FIELD_TYPE = FieldType::FIELD_TYPE;
  static constexpr node_offset_t EXTENT_SIZE =
    (FieldType::SIZE + DISK_BLOCK_SIZE - 1u) / DISK_BLOCK_SIZE * DISK_BLOCK_SIZE;

  // TODO: remove
  node_extent_t() = default;

  node_extent_t(const FieldType* p_fields) : p_fields{p_fields} {
    validate(*p_fields);
  }

  const char* p_start() const { return fields_start(*p_fields); }

  const char* off_to_ptr(node_offset_t off) const {
    assert(off <= FieldType::SIZE);
    return p_start() + off;
  }

  node_offset_t ptr_to_off(const void* ptr) const {
    auto _ptr = static_cast<const char*>(ptr);
    assert(_ptr >= p_start());
    auto off = _ptr - p_start();
    assert(off <= FieldType::SIZE);
    return off;
  }

  bool is_level_tail() const { return p_fields->is_level_tail(); }
  level_t level() const { return p_fields->header.level; }
  size_t free_size() const {
    return p_fields->template free_size_before<NODE_TYPE>(keys());
  }
  size_t total_size() const { return p_fields->total_size(); }
  const char* p_left_bound() const;
  template <node_type_t T = NODE_TYPE>
  std::enable_if_t<T == node_type_t::INTERNAL, const laddr_t*>
  get_end_p_laddr() const {
    assert(is_level_tail());
    if constexpr (FIELD_TYPE == field_type_t::N3) {
      #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
      return &p_fields->child_addrs[keys()];
    } else {
      auto offset_start = p_fields->get_item_end_offset(keys());
      assert(offset_start <= FieldType::SIZE);
      offset_start -= sizeof(laddr_t);
      auto p_addr = p_start() + offset_start;
      return reinterpret_cast<const laddr_t*>(p_addr);
    }
  }

  // container type system
  using key_get_type = typename FieldType::key_get_type;
  static constexpr auto CONTAINER_TYPE = ContainerType::INDEXABLE;
  size_t keys() const { return p_fields->num_keys; }
  key_get_type operator[] (size_t index) const { return p_fields->get_key(index); }
  size_t size_before(size_t index) const {
    auto free_size = p_fields->template free_size_before<NODE_TYPE>(index);
    assert(total_size() >= free_size);
    return total_size() - free_size;
  }
  size_t size_to_nxt_at(size_t index) const;
  memory_range_t get_nxt_container(size_t index) const;

  template <typename T = FieldType>
  std::enable_if_t<T::FIELD_TYPE == field_type_t::N3, const value_t*>
  get_p_value(size_t index) const {
    assert(index < keys());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      #pragma GCC diagnostic ignored "-Waddress-of-packed-member"
      return &p_fields->child_addrs[index];
    } else {
      auto range = get_nxt_container(index);
      auto ret = reinterpret_cast<const onode_t*>(range.p_start);
      assert(range.p_start + ret->size == range.p_end);
      return ret;
    }
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
      const full_key_t<KT>& key, const value_t& value) {
    auto size = FieldType::estimate_insert_one();
    if constexpr (FIELD_TYPE == field_type_t::N2) {
      size += ns_oid_view_t::estimate_size<KT>(key);
    } else if constexpr (FIELD_TYPE == field_type_t::N3 &&
                         NODE_TYPE == node_type_t::LEAF) {
      size += value.size;
    }
    return size;
  }

  template <KeyT KT>
  static const value_t* insert_at(
      NodeExtentMutable& mut, const node_extent_t&,
      const full_key_t<KT>& key, const value_t& value,
      size_t index, node_offset_t size, const char* p_left_bound) {
    if constexpr (FIELD_TYPE == field_type_t::N3) {
      assert(false && "not implemented");
    } else {
      assert(false && "impossible");
    }
  }

  template <KeyT KT>
  static memory_range_t insert_prefix_at(
      NodeExtentMutable&, const node_extent_t&,
      const full_key_t<KT>& key,
      size_t index, node_offset_t size, const char* p_left_bound);

  static void update_size_at(
      NodeExtentMutable&, const node_extent_t&, size_t index, int change);

  static size_t trim_until(NodeExtentMutable&, const node_extent_t&, size_t index);
  static size_t trim_at(NodeExtentMutable&, const node_extent_t&,
                        size_t index, size_t trimmed);

  template <KeyT KT>
  class Appender;

 private:
  const FieldType& fields() const { return *p_fields; }
  const FieldType* p_fields;
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
    p_append_right = p_start + FieldType::SIZE;
  }
  void append(const node_extent_t& src, size_t from, size_t items);
  void append(const full_key_t<KT>&, const value_t&, const value_t*&);
  char* wrap();
  std::tuple<NodeExtentMutable*, char*> open_nxt(const key_get_type&);
  std::tuple<NodeExtentMutable*, char*> open_nxt(const full_key_t<KT>&);
  void wrap_nxt(char* p_append) {
    if constexpr (FIELD_TYPE != field_type_t::N3) {
      assert(p_append < p_append_right);
      assert(p_append_left < p_append);
      p_append_right = p_append;
      FieldType::append_offset(*p_mut, p_append - p_start, p_append_left);
      ++num_keys;
    } else {
      assert(false);
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
