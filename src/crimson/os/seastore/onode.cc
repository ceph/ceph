// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "onode.h"
#include <iostream>

namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream &out, const Onode &rhs)
{
  auto &layout = rhs.get_layout();
  return out << "Onode("
	     << "size=" << static_cast<uint32_t>(layout.size)
	     << ")";
}

}

