// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "onode_block.h"

namespace crimson::os::seastore {

ceph::bufferlist OnodeBlock::get_delta()
{
  bufferlist bl;
  assert(deltas.size() <= std::numeric_limits<uint8_t>::max());
  uint8_t n_deltas = deltas.size();
  ceph::encode(n_deltas, bl);
  for (auto& delta : deltas) {
    delta->encode(bl);
  }
  return bl;
}

void OnodeBlock::on_initial_write()
{}

void OnodeBlock::on_delta_write(paddr_t)
{
  // journal submitted to disk, now update the memory
  apply_pending_changes(true);
}

void OnodeBlock::apply_delta(const ceph::bufferlist &bl)
{
  assert(deltas.empty());

  auto p = bl.cbegin();
  uint8_t n_deltas = 0;
  ceph::decode(n_deltas, p);
  for (uint8_t i = 0; i < n_deltas; i++) {
    delta_t delta;
    delta.decode(p);
    mutate(std::move(delta));
  }
  apply_pending_changes(true);
}

void OnodeBlock::mutate(delta_t&& d)
{
  if (is_initial_pending()) {
    char* const p = get_bptr().c_str();
    mutate_func(p, d);
  }
  deltas.push_back(std::make_unique<delta_t>(std::move(d)));
}

void OnodeBlock::apply_pending_changes(bool do_cleanup)
{
  if (!is_mutation_pending()) {
    return;
  }
  if (share_buffer) {
    // do a deep copy so i can change my own copy
    get_bptr() = ceph::bufferptr{get_bptr().c_str(),
				 get_bptr().length()};
    share_buffer = false;
  }
  assert(mutate_func);
  char* const p = get_bptr().c_str();
  for (auto& delta : deltas) {
    mutate_func(p, *delta);
    if (do_cleanup) {
      delta.reset();
    }
  }
}

}
