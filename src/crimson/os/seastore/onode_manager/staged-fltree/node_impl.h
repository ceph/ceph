// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "node_extent_mutable.h"
#include "node_types.h"
#include "stages/stage_types.h"

namespace crimson::os::seastore::onode {

struct key_hobj_t;
struct key_view_t;
class NodeExtentMutable;

class NodeImpl {
 public:
  using alloc_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual ~NodeImpl() = default;

  virtual field_type_t field_type() const = 0;
  virtual laddr_t laddr() const = 0;
  virtual void prepare_mutate(context_t) = 0;
  virtual bool is_level_tail() const = 0;
  virtual bool is_empty() const = 0;
  virtual level_t level() const = 0;
  virtual node_offset_t free_size() const = 0;
  virtual key_view_t get_key_view(const search_position_t&) const = 0;
  virtual key_view_t get_largest_key_view() const = 0;

  virtual std::ostream& dump(std::ostream&) const = 0;
  virtual std::ostream& dump_brief(std::ostream&) const = 0;
  virtual void validate_layout() const = 0;

  virtual void test_copy_to(NodeExtentMutable&) const = 0;
  virtual void test_set_tail(NodeExtentMutable&) = 0;

 protected:
  NodeImpl() = default;
};

class InternalNodeImpl : public NodeImpl {
 public:
  struct internal_marker_t {};
  virtual ~InternalNodeImpl() = default;

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const laddr_packed_t* get_p_value(
      const search_position_t&,
      key_view_t* = nullptr, internal_marker_t = {}) const {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual lookup_result_t<node_type_t::INTERNAL> lower_bound(
      const key_hobj_t&, MatchHistory&,
      key_view_t* = nullptr, internal_marker_t = {}) const {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const laddr_packed_t* insert(
      const key_view_t&, const laddr_packed_t&, search_position_t&, match_stage_t&, node_offset_t&) {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual std::tuple<search_position_t, bool, const laddr_packed_t*> split_insert(
      NodeExtentMutable&, NodeImpl&, const key_view_t&, const laddr_packed_t&,
      search_position_t&, match_stage_t&, node_offset_t&) {
    assert(false && "impossible path");
  }

  virtual void replace_child_addr(const search_position_t&, laddr_t dst, laddr_t src) = 0;
  virtual std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_view_t&, const laddr_t&, search_position_t&) const = 0;

  struct fresh_impl_t {
    InternalNodeImplURef impl;
    NodeExtentMutable mut;
    std::pair<NodeImplURef, NodeExtentMutable> make_pair() {
      return {std::move(impl), mut};
    }
  };
  static alloc_ertr::future<fresh_impl_t> allocate(context_t, field_type_t, bool, level_t);
  static InternalNodeImplURef load(NodeExtentRef, field_type_t, bool);

 protected:
  InternalNodeImpl() = default;
};

class LeafNodeImpl : public NodeImpl {
 public:
  struct leaf_marker_t {};
  virtual ~LeafNodeImpl() = default;

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const onode_t* get_p_value(
      const search_position_t&,
      key_view_t* = nullptr, leaf_marker_t={}) const {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual lookup_result_t<node_type_t::LEAF> lower_bound(
      const key_hobj_t&, MatchHistory&,
      key_view_t* = nullptr, leaf_marker_t = {}) const {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const onode_t* insert(
      const key_hobj_t&, const onode_t&, search_position_t&, match_stage_t&, node_offset_t&) {
    assert(false && "impossible path");
  }
  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual std::tuple<search_position_t, bool, const onode_t*> split_insert(
      NodeExtentMutable&, NodeImpl&, const key_hobj_t&, const onode_t&,
      search_position_t&, match_stage_t&, node_offset_t&) {
    assert(false && "impossible path");
  }

  virtual void get_largest_slot(
      search_position_t&, key_view_t&, const onode_t**) const = 0;
  virtual std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_hobj_t&, const onode_t&,
      const MatchHistory&, match_stat_t, search_position_t&) const = 0;

  struct fresh_impl_t {
    LeafNodeImplURef impl;
    NodeExtentMutable mut;
    std::pair<NodeImplURef, NodeExtentMutable> make_pair() {
      return {std::move(impl), mut};
    }
  };
  static alloc_ertr::future<fresh_impl_t> allocate(context_t, field_type_t, bool);
  static LeafNodeImplURef load(NodeExtentRef, field_type_t, bool);

 protected:
  LeafNodeImpl() = default;
};

}
