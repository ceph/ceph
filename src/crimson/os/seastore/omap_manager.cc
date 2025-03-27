// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <experimental/iterator>
#include <iostream>

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"

namespace crimson::os::seastore::omap_manager {

OMapManagerRef create_omap_manager(TransactionManager &trans_manager) {
  return OMapManagerRef(new BtreeOMapManager(trans_manager));
}

}

namespace std {
std::ostream &operator<<(std::ostream &out, const std::pair<std::string, std::string> &rhs)
{
  return out << "key_value_map (" << rhs.first<< "->" << rhs.second << ")";
}
}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const std::list<std::string> &rhs)
{
  out << '[';
  std::copy(std::begin(rhs), std::end(rhs), std::experimental::make_ostream_joiner(out, ", "));
  return out << ']';
}

std::ostream &operator<<(std::ostream &out, const std::vector<std::pair<std::string, std::string>> &rhs)
{
  out << '[';
  std::ostream_iterator<std::pair<std::string, std::string>> out_it(out, ", ");
  std::copy(rhs.begin(), rhs.end(), out_it);
  return out << ']';
}

}
