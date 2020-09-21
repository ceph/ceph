// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "onode_delta.h"

delta_t::delta_t(delta_t&& delta)
{
  assert(op == op_t::nop);
  op = delta.op;
  n = delta.n;
  oid = std::move(delta.oid);
  onode = std::move(delta.onode);
  keys = std::move(delta.keys);
  cells = std::move(delta.cells);
  delta.op = op_t::nop;
}

delta_t& delta_t::operator=(delta_t&& delta)
{
  assert(op == op_t::nop);
  op = delta.op;
  n = delta.n;
  oid = std::move(delta.oid);
  onode = std::move(delta.onode);
  keys = std::move(delta.keys);
  cells = std::move(delta.cells);
  delta.op = op_t::nop;
  return *this;
}

delta_t delta_t::nop()
{
  return delta_t{op_t::nop};
}

delta_t delta_t::insert_onode(unsigned slot, const ghobject_t& oid, OnodeRef onode)
{
  delta_t delta{op_t::insert_onode};
  delta.n = slot;
  delta.oid = oid;
  delta.onode = onode;
  return delta;
}

delta_t delta_t::update_onode(unsigned slot, const ghobject_t& oid, OnodeRef onode)
{
  delta_t delta{op_t::update_onode};
  delta.n = slot;
  delta.oid = oid;
  delta.onode = onode;
  return delta;
}

delta_t delta_t::insert_child(unsigned slot,
                              const ghobject_t& oid,
                              crimson::os::seastore::laddr_t addr)
{
  delta_t delta{op_t::insert_child};
  delta.n = slot;
  delta.oid = oid;
  delta.addr = addr;
  return delta;
}

delta_t delta_t::update_key(unsigned slot, const ghobject_t& oid)
{
  delta_t delta{op_t::update_key};
  delta.n = slot;
  delta.oid = oid;
  return delta;
}

delta_t delta_t::shift_left(unsigned n)
{
  delta_t delta{op_t::shift_left};
  delta.n = n;
  return delta;
}

delta_t delta_t::trim_right(unsigned n)
{
  delta_t delta{op_t::trim_right};
  delta.n = n;
  return delta;
}

delta_t delta_t::insert_front(ceph::buffer::ptr keys,
                              ceph::buffer::ptr cells)
{
  delta_t delta{op_t::insert_front};
  delta.keys = std::move(keys);
  delta.cells = std::move(cells);
  return delta;
}

delta_t delta_t::insert_back(ceph::buffer::ptr keys,
                             ceph::buffer::ptr cells)
{
  delta_t delta{op_t::insert_back};
  delta.keys = std::move(keys);
  delta.cells = std::move(cells);
  return delta;
}

delta_t delta_t::remove_from(unsigned slot)
{
  delta_t delta{op_t::remove_from};
  delta.n = slot;
  return delta;
}

void delta_t::encode(ceph::bufferlist& bl)
{
  using ceph::encode;
  switch (op) {
  case op_t::insert_onode:
    [[fallthrough]];
  case op_t::update_onode:
    // the slot # is not encoded, because we can alway figure it out
    // when we have to replay the delta by looking the oid up in the
    // node block
    encode(oid, bl);
    encode(*onode, bl);
    break;
  case op_t::insert_child:
    encode(oid, bl);
    encode(addr, bl);
  case op_t::update_key:
    encode(n, bl);
    encode(oid, bl);
    break;
  case op_t::shift_left:
    encode(n, bl);
    break;
  case op_t::trim_right:
    encode(n, bl);
    break;
  case op_t::insert_front:
    [[fallthrough]];
  case op_t::insert_back:
    encode(n, bl);
    encode(keys, bl);
    encode(cells, bl);
    break;
  case op_t::remove_from:
    encode(n, bl);
    break;
  default:
    assert(0 == "unknown onode op");
  }
}

void delta_t::decode(ceph::bufferlist::const_iterator& p) {
  using ceph::decode;
  decode(op, p);
  switch (op) {
  case op_t::insert_onode:
    [[fallthrough]];
  case op_t::update_onode:
    decode(oid, p);
    decode(*onode, p);
    break;
  case op_t::insert_child:
    [[fallthrough]];
  case op_t::update_key:
    decode(n, p);
    decode(oid, p);
    break;
  case op_t::shift_left:
    decode(n, p);
    break;
  case op_t::trim_right:
    decode(n, p);
    break;
  case op_t::insert_front:
    [[fallthrough]];
  case op_t::insert_back:
    decode(n, p);
    decode(keys, p);
    decode(cells, p);
    break;
  case op_t::remove_from:
    decode(n, p);
    break;
  default:
    assert(0 == "unknown onode op");
  }
}
