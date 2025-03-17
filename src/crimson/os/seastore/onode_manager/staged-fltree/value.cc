// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "value.h"

#include "node.h"
#include "node_delta_recorder.h"
#include "node_layout.h"

// value implementations
#include "test/crimson/seastore/onode_tree/test_value.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

namespace crimson::os::seastore::onode {

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

bool Value::is_tracked() const
{
  assert(!p_cursor->is_end());
  return p_cursor->is_tracked();
}

void Value::invalidate()
{
  p_cursor.reset();
}

eagain_ifuture<> Value::extend(Transaction& t, value_size_t extend_size)
{
  assert(is_tracked());
  [[maybe_unused]] auto target_size = get_payload_size() + extend_size;
  return p_cursor->extend_value(get_context(t), extend_size)
#ifndef NDEBUG
  .si_then([this, target_size] {
    assert(target_size == get_payload_size());
  })
#endif
  ;
}

eagain_ifuture<> Value::trim(Transaction& t, value_size_t trim_size)
{
  assert(is_tracked());
  assert(get_payload_size() > trim_size);
  [[maybe_unused]] auto target_size = get_payload_size() - trim_size;
  return p_cursor->trim_value(get_context(t), trim_size)
#ifndef NDEBUG
  .si_then([this, target_size] {
    assert(target_size == get_payload_size());
  })
#endif
  ;
}

const value_header_t* Value::read_value_header() const
{
  auto ret = p_cursor->read_value_header(vb.get_header_magic());
  assert(ret->payload_size <= vb.get_max_value_payload_size());
  return ret;
}

std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
Value::do_prepare_mutate_payload(Transaction& t)
{
   return p_cursor->prepare_mutate_value_payload(get_context(t));
}

laddr_t Value::get_hint() const
{
  return p_cursor->get_key_view(vb.get_header_magic()).get_hint();
}

std::unique_ptr<ValueDeltaRecorder>
build_value_recorder_by_type(ceph::bufferlist& encoded,
                             const value_magic_t& magic)
{
  std::unique_ptr<ValueDeltaRecorder> ret;
  switch (magic) {
  case value_magic_t::ONODE:
    ret = std::make_unique<FLTreeOnode::Recorder>(encoded);
    break;
  case value_magic_t::TEST_UNBOUND:
    ret = std::make_unique<UnboundedValue::Recorder>(encoded);
    break;
  case value_magic_t::TEST_BOUNDED:
    ret = std::make_unique<BoundedValue::Recorder>(encoded);
    break;
  case value_magic_t::TEST_EXTENDED:
    ret = std::make_unique<ExtendedValue::Recorder>(encoded);
    break;
  default:
    ret = nullptr;
    break;
  }
  assert(!ret || ret->get_header_magic() == magic);
  return ret;
}

void validate_tree_config(const tree_conf_t& conf)
{
  ceph_assert(conf.max_ns_size <
              string_key_view_t::VALID_UPPER_BOUND);
  ceph_assert(conf.max_oid_size <
              string_key_view_t::VALID_UPPER_BOUND);
  ceph_assert(is_valid_node_size(conf.internal_node_size));
  ceph_assert(is_valid_node_size(conf.leaf_node_size));

  if (conf.do_split_check) {
    // In hope to comply with 3 * (oid + ns) + 2 * value < node
    //
    // see node_layout.h for NODE_BLOCK_SIZE considerations
    //
    // The below calculations also consider the internal indexing overhead in
    // order to be accurate, so the equation has become:
    //   node-header-size + 2 * max-full-insert-size +
    //   max-ns/oid-split-overhead <= node-size

    auto obj = ghobject_t{shard_id_t{0}, 0, 0, "", "", 0, 0};
    key_hobj_t key(obj);
    auto max_str_size = conf.max_ns_size + conf.max_oid_size;
#define _STAGE_T(NodeType) node_to_stage_t<typename NodeType::node_stage_t>
#define NXT_T(StageType)  staged<typename StageType::next_param_t>

    laddr_t i_value = L_ADDR_MIN;
    auto insert_size_2 =
      _STAGE_T(InternalNode0)::insert_size(key, i_value);
    auto insert_size_0 =
      NXT_T(NXT_T(_STAGE_T(InternalNode0)))::insert_size(key, i_value);
    unsigned internal_size_bound = sizeof(node_header_t) +
                                   (insert_size_2 + max_str_size) * 2 +
                                   (insert_size_2 - insert_size_0 + max_str_size);
    ceph_assert(internal_size_bound <= conf.internal_node_size);

    value_config_t l_value;
    l_value.payload_size = conf.max_value_payload_size;
    insert_size_2 =
      _STAGE_T(LeafNode0)::insert_size(key, l_value);
    insert_size_0 =
      NXT_T(NXT_T(_STAGE_T(LeafNode0)))::insert_size(key, l_value);
    unsigned leaf_size_bound = sizeof(node_header_t) +
                               (insert_size_2 + max_str_size) * 2 +
                               (insert_size_2 - insert_size_0 + max_str_size);
    ceph_assert(leaf_size_bound <= conf.leaf_node_size);
  }
}

}
