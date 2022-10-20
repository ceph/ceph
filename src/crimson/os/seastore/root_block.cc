// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

void RootBlock::on_replace_prior(Transaction &t) {
  if (lba_root_node) {
    auto lba_root =
      (crimson::os::seastore::lba_manager::btree::LBANode*
      )lba_root_node->get_transactional_view(t);
    lba_root->root_block = this;
    lba_root_node = lba_root;
  }

  if (backref_root_node) {
    auto backref_root =
      (crimson::os::seastore::backref::BackrefNode*
      )backref_root_node->get_transactional_view(t);
    backref_root->root_block = this;
    backref_root_node = backref_root;
  }
}

} // namespace crimson::os::seastore
