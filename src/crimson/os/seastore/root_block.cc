// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

void RootBlock::prepare_write()
{
  bufferlist tmp;
  ::encode(root, tmp);
  auto bpiter = tmp.begin();
  bpiter.copy(tmp.length(), get_bptr().c_str());
}

RootBlock::complete_load_ertr::future<> RootBlock::complete_load()
{
  auto biter = get_bptr().cbegin();
  root.decode(biter);
  if (root.lba_root.lba_root_addr.is_relative()) {
    root.lba_root.lba_root_addr = get_paddr().add_block_relative(
      root.lba_root.lba_root_addr);
  }
  return complete_load_ertr::now();
}

void RootBlock::on_delta_write(paddr_t record_block_offset)
{
  if (root.lba_root.lba_root_addr.is_relative()) {
    root.lba_root.lba_root_addr = record_block_offset.add_record_relative(
      root.lba_root.lba_root_addr);
  }
}

void RootBlock::on_initial_write()
{
  if (root.lba_root.lba_root_addr.is_relative()) {
    root.lba_root.lba_root_addr = get_paddr().add_block_relative(
      root.lba_root.lba_root_addr);
  }
}

btree_lba_root_t &RootBlock::get_lba_root()
{
  return root.lba_root;
}

}
