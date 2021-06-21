// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node_impl.h"
#include "node_layout.h"

namespace crimson::os::seastore::onode {

#ifdef UNIT_TESTS_BUILT
last_split_info_t last_split = {};
#endif

// XXX: branchless allocation
eagain_future<InternalNodeImpl::fresh_impl_t>
InternalNodeImpl::allocate(
    context_t c, field_type_t type, bool is_level_tail, level_t level)
{
  if (type == field_type_t::N0) {
    return InternalNode0::allocate(c, is_level_tail, level);
  } else if (type == field_type_t::N1) {
    return InternalNode1::allocate(c, is_level_tail, level);
  } else if (type == field_type_t::N2) {
    return InternalNode2::allocate(c, is_level_tail, level);
  } else if (type == field_type_t::N3) {
    return InternalNode3::allocate(c, is_level_tail, level);
  } else {
    ceph_abort("impossible path");
  }
}

eagain_future<LeafNodeImpl::fresh_impl_t>
LeafNodeImpl::allocate(
    context_t c, field_type_t type, bool is_level_tail)
{
  if (type == field_type_t::N0) {
    return LeafNode0::allocate(c, is_level_tail, 0);
  } else if (type == field_type_t::N1) {
    return LeafNode1::allocate(c, is_level_tail, 0);
  } else if (type == field_type_t::N2) {
    return LeafNode2::allocate(c, is_level_tail, 0);
  } else if (type == field_type_t::N3) {
    return LeafNode3::allocate(c, is_level_tail, 0);
  } else {
    ceph_abort("impossible path");
  }
}

InternalNodeImplURef InternalNodeImpl::load(
    NodeExtentRef extent, field_type_t type)
{
  if (type == field_type_t::N0) {
    return InternalNode0::load(extent);
  } else if (type == field_type_t::N1) {
    return InternalNode1::load(extent);
  } else if (type == field_type_t::N2) {
    return InternalNode2::load(extent);
  } else if (type == field_type_t::N3) {
    return InternalNode3::load(extent);
  } else {
    ceph_abort("impossible path");
  }
}

LeafNodeImplURef LeafNodeImpl::load(
    NodeExtentRef extent, field_type_t type)
{
  if (type == field_type_t::N0) {
    return LeafNode0::load(extent);
  } else if (type == field_type_t::N1) {
    return LeafNode1::load(extent);
  } else if (type == field_type_t::N2) {
    return LeafNode2::load(extent);
  } else if (type == field_type_t::N3) {
    return LeafNode3::load(extent);
  } else {
    ceph_abort("impossible path");
  }
}

}
