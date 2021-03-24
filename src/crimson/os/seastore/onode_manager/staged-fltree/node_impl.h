// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "node_extent_mutable.h"
#include "node_types.h"
#include "stages/stage_types.h"

namespace crimson::os::seastore::onode {

#ifdef UNIT_TESTS_BUILT
enum class InsertType { BEGIN, LAST, MID };
struct split_expectation_t {
  match_stage_t split_stage;
  match_stage_t insert_stage;
  bool is_insert_left;
  InsertType insert_type;
};
struct last_split_info_t {
  search_position_t split_pos;
  match_stage_t insert_stage;
  bool is_insert_left;
  InsertType insert_type;
  bool match(const split_expectation_t& e) const {
    match_stage_t split_stage;
    if (split_pos.nxt.nxt.index == 0) {
      if (split_pos.nxt.index == 0) {
        split_stage = 2;
      } else {
        split_stage = 1;
      }
    } else {
      split_stage = 0;
    }
    return split_stage == e.split_stage &&
           insert_stage == e.insert_stage &&
           is_insert_left == e.is_insert_left &&
           insert_type == e.insert_type;
  }
  bool match_split_pos(const search_position_t& pos) const {
    return split_pos == pos;
  }
};
extern last_split_info_t last_split;
#endif

struct key_hobj_t;
struct key_view_t;
class NodeExtentMutable;

/**
 * NodeImpl
 *
 * Hides type specific node layout implementations for Node.
 */
class NodeImpl {
 public:
  using alloc_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange,
    crimson::ct_error::eagain
   >;
  virtual ~NodeImpl() = default;

  virtual node_type_t node_type() const = 0;
  virtual field_type_t field_type() const = 0;
  virtual laddr_t laddr() const = 0;
  virtual const char* read() const = 0;
  virtual bool is_duplicate() const = 0;
  virtual void prepare_mutate(context_t) = 0;
  virtual bool is_level_tail() const = 0;
  virtual bool is_empty() const = 0;
  virtual level_t level() const = 0;
  virtual node_offset_t free_size() const = 0;
  virtual std::optional<key_view_t> get_pivot_index() const = 0;

  virtual node_stats_t get_stats() const = 0;
  virtual std::ostream& dump(std::ostream&) const = 0;
  virtual std::ostream& dump_brief(std::ostream&) const = 0;
  virtual void validate_layout() const = 0;

  virtual void test_copy_to(NodeExtentMutable&) const = 0;
  virtual void test_set_tail(NodeExtentMutable&) = 0;

 protected:
  NodeImpl() = default;
};

/**
 * InternalNodeImpl
 *
 * Hides type specific node layout implementations for InternalNode.
 */
class InternalNodeImpl : public NodeImpl {
 public:
  struct internal_marker_t {};
  virtual ~InternalNodeImpl() = default;

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_slot(const search_position_t&,                 // IN
                        key_view_t* = nullptr,                    // OUT
                        const laddr_packed_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_next_slot(search_position_t&,                       // IN&OUT
                             key_view_t* = nullptr,                    // OUT
                             const laddr_packed_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_largest_slot(search_position_t* = nullptr,             // OUT
                                key_view_t* = nullptr,                    // OUT
                                const laddr_packed_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual lookup_result_t<node_type_t::INTERNAL> lower_bound(
      const key_hobj_t&, MatchHistory&,
      key_view_t* = nullptr, internal_marker_t = {}) const {
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const laddr_packed_t* insert(
      const key_view_t&, const laddr_t&, search_position_t&, match_stage_t&, node_offset_t&) {
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual std::tuple<search_position_t, bool, const laddr_packed_t*> split_insert(
      NodeExtentMutable&, NodeImpl&, const key_view_t&, const laddr_t&,
      search_position_t&, match_stage_t&, node_offset_t&) {
    ceph_abort("impossible path");
  }

  virtual const laddr_packed_t* get_tail_value() const = 0;

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

/**
 * LeafNodeImpl
 *
 * Hides type specific node layout implementations for LeafNode.
 */
class LeafNodeImpl : public NodeImpl {
 public:
  struct leaf_marker_t {};
  virtual ~LeafNodeImpl() = default;

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_slot(const search_position_t&,                 // IN
                        key_view_t* = nullptr,                    // OUT
                        const value_header_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_next_slot(search_position_t&,                       // IN&OUT
                             key_view_t* = nullptr,                    // OUT
                             const value_header_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual void get_largest_slot(search_position_t* = nullptr,             // OUT
                                key_view_t* = nullptr,                    // OUT
                                const value_header_t** = nullptr) const { // OUT
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual lookup_result_t<node_type_t::LEAF> lower_bound(
      const key_hobj_t&, MatchHistory&,
      key_view_t* = nullptr, leaf_marker_t = {}) const {
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual const value_header_t* insert(
      const key_hobj_t&, const value_config_t&, search_position_t&, match_stage_t&, node_offset_t&) {
    ceph_abort("impossible path");
  }

  #pragma GCC diagnostic ignored "-Woverloaded-virtual"
  virtual std::tuple<search_position_t, bool, const value_header_t*> split_insert(
      NodeExtentMutable&, NodeImpl&, const key_hobj_t&, const value_config_t&,
      search_position_t&, match_stage_t&, node_offset_t&) {
    ceph_abort("impossible path");
  }

  virtual std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_hobj_t&, const value_config_t&,
      const MatchHistory&, match_stat_t, search_position_t&) const = 0;

  virtual std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  prepare_mutate_value_payload(context_t) = 0;

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
