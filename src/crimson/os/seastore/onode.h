// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/byteorder.h"
#include "seastore_types.h"

namespace crimson::os::seastore {

struct onode_layout_t {
  // around 350 bytes for fixed fields in object_info_t,
  // the left are for the variable-sized fields like oid
  // FIXME: object_info_t may need to shrinked, at least
  // 	    oid doesn't need to be held in it.
  static constexpr int MAX_OI_LENGTH = 1024;
  // We might want to move the ss field out of onode_layout_t.
  // The reason is that ss_attr may grow to relative large, as
  // its clone_overlap may grow to a large size, if applications
  // set objects to a relative large size(for the purpose of reducing
  // the number of objects per OSD, so that all objects' metadata
  // can be cached in memory) and do many modifications between
  // snapshots.
  static constexpr int MAX_SS_LENGTH = 128;

  ceph_le32 size{0};
  ceph_le32 oi_size{0};
  ceph_le32 ss_size{0};
  omap_root_le_t omap_root;
  omap_root_le_t xattr_root;

  object_data_le_t object_data;

  char oi[MAX_OI_LENGTH];
  char ss[MAX_SS_LENGTH];
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
