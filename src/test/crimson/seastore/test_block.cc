// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/seastore/test_block.h"

namespace crimson::os::seastore {


ceph::bufferlist TestBlock::get_delta() {
  ceph::bufferlist bl;
  encode(delta, bl);
  return bl;
}


void TestBlock::apply_delta(const ceph::bufferlist &bl) {
  auto biter = bl.begin();
  decltype(delta) deltas;
  decode(deltas, biter);
  for (auto &&d : deltas) {
    set_contents(d.val, d.offset, d.len);
    modified_region.union_insert(d.offset, d.len);
  }
}

ceph::bufferlist TestBlockPhysical::get_delta() {
  ceph::bufferlist bl;
  encode(delta, bl);
  return bl;
}

void TestBlockPhysical::apply_delta_and_adjust_crc(
    paddr_t, const ceph::bufferlist &bl) {
  auto biter = bl.begin();
  decltype(delta) deltas;
  decode(deltas, biter);
  for (auto &&d : deltas) {
    set_contents(d.val, d.offset, d.len);
  }
}

}
