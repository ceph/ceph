// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "crimson/os/seastore/linked_tree_node.h"

namespace crimson::os::seastore {

void RootBlock::on_replace_prior() {
  if (!lba_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.lba_root_node) {
      RootBlockRef this_ref = this;
      auto lba_root = static_cast<
	lba::LBANode*>(prior.lba_root_node);
      if (likely(lba_root->range.depth > 1)) {
	TreeRootLinker<RootBlock, lba::LBAInternalNode>::link_root(
	  this_ref,
	  static_cast<lba::LBAInternalNode*>(prior.lba_root_node)
	);
      } else {
	assert(lba_root->range.depth == 1);
	TreeRootLinker<RootBlock, lba::LBALeafNode>::link_root(
	  this_ref,
	  static_cast<lba::LBALeafNode*>(prior.lba_root_node)
	);
      }
    }
  }
  if (!backref_root_node) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.backref_root_node) {
      RootBlockRef this_ref = this;
      auto backref_root = static_cast<
	backref::BackrefNode*>(prior.backref_root_node);
      if (likely(backref_root->range.depth > 1)) {
	TreeRootLinker<RootBlock, backref::BackrefInternalNode>::link_root(
	  this_ref,
	  static_cast<backref::BackrefInternalNode*>(prior.backref_root_node)
	);
      } else {
	assert(backref_root->range.depth == 1);
	TreeRootLinker<RootBlock, backref::BackrefLeafNode>::link_root(
	  this_ref,
	  static_cast<backref::BackrefLeafNode*>(prior.backref_root_node)
	);
      }
    }
  }
}

} // namespace crimson::os::seastore
