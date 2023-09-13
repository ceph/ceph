// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

void RootBlock::on_replace_prior(Transaction &t) {
  if (!lba_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    lba_root_node = prior.lba_root_node;
    if (lba_root_node) {
      ((lba_manager::btree::LBANode*)lba_root_node)->root_block = this;
    }
  }
  if (!backref_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    backref_root_node = prior.backref_root_node;
    if (backref_root_node) {
      ((backref::BackrefNode*)backref_root_node)->root_block = this;
    }
  }
}

} // namespace crimson::os::seastore
