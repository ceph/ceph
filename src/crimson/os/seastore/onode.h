// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/byteorder.h"
#include "seastore_types.h"

namespace crimson::os::seastore {

struct onode_layout_t {
  ceph_le32 size{0};
  omap_root_le_t omap_root;
} __attribute__((packed));

class Transaction;

/**
 * Onode
 *
 * Interface manipulated by seastore.  OnodeManager implementations should
 * return objects derived from this interface with layout referencing
 * internal representation of onode_layout_t.
 */
class Onode : public boost::intrusive_ref_counter<
  Onode,
  boost::thread_unsafe_counter>
{
public:

  virtual const onode_layout_t &get_layout() const = 0;
  virtual onode_layout_t &get_mutable_layout(Transaction &t) = 0;
  virtual ~Onode() = default;
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}
