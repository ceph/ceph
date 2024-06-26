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
  // The expected decode size of object_info_t without oid.
  static constexpr int MAX_OI_LENGTH = 232;
  // We might want to move the ss field out of onode_layout_t.
  // The reason is that ss_attr may grow to relative large, as
  // its clone_overlap may grow to a large size, if applications
  // set objects to a relative large size(for the purpose of reducing
  // the number of objects per OSD, so that all objects' metadata
  // can be cached in memory) and do many modifications between
  // snapshots.
  // TODO: implement flexible-sized onode value to store inline ss_attr
  // effectively.
  static constexpr int MAX_SS_LENGTH = 1;

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
protected:
  virtual laddr_t get_hint() const = 0;
  const uint32_t default_metadata_offset = 0;
  const uint32_t default_metadata_range = 0;
public:
  Onode(uint32_t ddr, uint32_t dmr)
    : default_metadata_offset(ddr),
      default_metadata_range(dmr)
  {}

  virtual bool is_alive() const = 0;
  virtual const onode_layout_t &get_layout() const = 0;
  virtual ~Onode() = default;

  virtual void update_onode_size(Transaction&, uint32_t) = 0;
  virtual void update_omap_root(Transaction&, omap_root_t&) = 0;
  virtual void update_xattr_root(Transaction&, omap_root_t&) = 0;
  virtual void update_object_data(Transaction&, object_data_t&) = 0;
  virtual void update_object_info(Transaction&, ceph::bufferlist&) = 0;
  virtual void update_snapset(Transaction&, ceph::bufferlist&) = 0;
  virtual void clear_object_info(Transaction&) = 0;
  virtual void clear_snapset(Transaction&) = 0;

  laddr_t get_metadata_hint(uint64_t block_size) const {
    assert(default_metadata_offset);
    assert(default_metadata_range);
    uint64_t range_blocks = default_metadata_range / block_size;
    return get_hint() + default_metadata_offset +
      (((uint32_t)std::rand() % range_blocks) * block_size);
  }
  laddr_t get_data_hint() const {
    return get_hint();
  }
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::Onode> : fmt::ostream_formatter {};
#endif
