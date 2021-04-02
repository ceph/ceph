// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "value.h"

#include "node.h"
#include "node_delta_recorder.h"

// value implementations
#include "test/crimson/seastore/onode_tree/test_value.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

namespace crimson::os::seastore::onode {

using ertr = Value::ertr;
template <class ValueT=void>
using future = Value::future<ValueT>;

ceph::bufferlist&
ValueDeltaRecorder::get_encoded(NodeExtentMutable& payload_mut)
{
  ceph::encode(node_delta_op_t::SUBOP_UPDATE_VALUE, encoded);
  node_offset_t offset = payload_mut.get_node_offset();
  assert(offset > sizeof(value_header_t));
  offset -= sizeof(value_header_t);
  ceph::encode(offset, encoded);
  return encoded;
}

Value::Value(NodeExtentManager& nm,
             const ValueBuilder& vb,
             Ref<tree_cursor_t>& p_cursor)
  : nm{nm}, vb{vb}, p_cursor{p_cursor} {}

Value::~Value() {}

future<> Value::extend(Transaction& t, value_size_t extend_size)
{
  [[maybe_unused]] auto target_size = get_payload_size() + extend_size;
  return p_cursor->extend_value(get_context(t), extend_size)
#ifndef NDEBUG
  .safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  })
#endif
  ;
}

future<> Value::trim(Transaction& t, value_size_t trim_size)
{
  assert(get_payload_size() > trim_size);
  [[maybe_unused]] auto target_size = get_payload_size() - trim_size;
  return p_cursor->trim_value(get_context(t), trim_size)
#ifndef NDEBUG
  .safe_then([this, target_size] {
    assert(target_size == get_payload_size());
  })
#endif
  ;
}

const value_header_t* Value::read_value_header() const
{
  return p_cursor->read_value_header(vb.get_header_magic());
}

std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
Value::do_prepare_mutate_payload(Transaction& t)
{
   return p_cursor->prepare_mutate_value_payload(get_context(t));
}

std::unique_ptr<ValueDeltaRecorder>
build_value_recorder_by_type(ceph::bufferlist& encoded,
                             const value_magic_t& magic)
{
  std::unique_ptr<ValueDeltaRecorder> ret;
  switch (magic) {
  case value_magic_t::TEST:
    ret = std::make_unique<TestValue::Recorder>(encoded);
    break;
  case value_magic_t::ONODE:
    ret = std::make_unique<FLTreeOnode::Recorder>(encoded);
    break;
  default:
    ret = nullptr;
    break;
  }
  assert(!ret || ret->get_header_magic() == magic);
  return ret;
}

}
