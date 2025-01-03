// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

void RootBlock::on_replace_prior() {
  if (!lba_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.lba_root_node) {
      RootBlockRef this_ref = this;
      link_phy_tree_root_node(
        this_ref,
        static_cast<lba_manager::btree::LBANode*>(prior.lba_root_node)
      );
    }
  }
  if (!backref_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.backref_root_node) {
      RootBlockRef this_ref = this;
      link_phy_tree_root_node(
        this_ref,
        static_cast<backref::BackrefNode*>(prior.backref_root_node)
      );
    }
  }
}

} // namespace crimson::os::seastore
