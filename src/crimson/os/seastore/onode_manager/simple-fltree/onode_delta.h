// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>

#include "common/hobject.h"
#include "include/buffer_fwd.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/seastore_types.h"

using crimson::os::seastore::OnodeRef;

struct delta_t {
  enum class op_t : uint8_t {
    nop,
    insert_onode,
    update_onode,
    insert_child,
    update_key,
    shift_left,
    trim_right,
    insert_front,
    insert_back,
    remove_from,
    // finer grained op?
    //  - changing the embedded extent map of given oid
    //  - mutating the embedded xattrs of given oid
  } op = op_t::nop;

  unsigned n = 0;
  ghobject_t oid;
  crimson::os::seastore::laddr_t addr = 0;
  OnodeRef onode;
  ceph::bufferptr keys;
  ceph::bufferptr cells;

  delta_t() = default;
  delta_t(op_t op)
    : op{op}
  {}
  delta_t(delta_t&& delta);
  delta_t& operator=(delta_t&& delta);

  static delta_t nop();
  static delta_t insert_onode(unsigned slot, const ghobject_t& oid, OnodeRef onode);
  static delta_t update_onode(unsigned slot, const ghobject_t& oid, OnodeRef onode);
  static delta_t insert_child(unsigned slot, const ghobject_t& oid, crimson::os::seastore::laddr_t addr);
  static delta_t update_key(unsigned slot, const ghobject_t& oid);
  static delta_t shift_left(unsigned n);
  static delta_t trim_right(unsigned n);
  static delta_t insert_front(ceph::buffer::ptr keys,
                              ceph::buffer::ptr cells);
  static delta_t insert_back(ceph::buffer::ptr keys,
                             ceph::buffer::ptr cells);
  static delta_t remove_from(unsigned slot);

  // shortcuts
  static delta_t insert_item(unsigned slot, const ghobject_t& oid, OnodeRef onode) {
    return insert_onode(slot, oid, onode);
  }
  static delta_t insert_item(unsigned slot, const ghobject_t& oid, crimson::os::seastore::laddr_t addr) {
    return insert_child(slot, oid, addr);
  }

  void encode(ceph::bufferlist& bl);
  void decode(ceph::bufferlist::const_iterator& p);
};
