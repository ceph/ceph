// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <experimental/iterator>
#include <iostream>

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/btree_extentmap_manager.h"
namespace crimson::os::seastore::extentmap_manager {

ExtentMapManagerRef create_extentmap_manager(TransactionManager &trans_manager) {
  return ExtentMapManagerRef(new BtreeExtentMapManager(trans_manager));
}

}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const extent_mapping_t &rhs)
{
  return out << "extent_mapping_t (" << rhs.logical_offset << "~" << rhs.length
	        << "->" << rhs.laddr << ")";
}

std::ostream &operator<<(std::ostream &out, const extent_map_list_t &rhs)
{
  out << '[';
  std::copy(std::begin(rhs), std::end(rhs), std::experimental::make_ostream_joiner(out, ", "));
  return out << ']';
}

}
