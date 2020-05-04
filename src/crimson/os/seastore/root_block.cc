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

CachedExtent::complete_load_ertr::future<> RootBlock::complete_load()
{
  auto biter = get_bptr().cbegin();
  root.decode(biter);
  return complete_load_ertr::now();
}

void RootBlock::set_lba_root(btree_lba_root_t lba_root)
{
  root.lba_root = lba_root;
}

}
