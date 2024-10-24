// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/hobject.h"
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
  local_object_id_le_t local_object_id{LOCAL_OBJECT_ID_NULL};
  local_clone_id_le_t local_clone_id{LOCAL_CLONE_ID_NULL};

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
  virtual laddr_hint_t generate_data_hint(
    std::optional<local_object_id_t> object_id,
    std::optional<local_clone_id_t> clone_id) const = 0;

  virtual laddr_hint_t generate_data_clone_hint(
    local_object_id_t object_id) const = 0;

  virtual laddr_hint_t generate_metadata_hint(
    std::optional<local_object_id_t> object_id,
    std::optional<local_clone_id_t> clone_id,
    extent_len_t block_size) const = 0;

  const hobject_t hobj;
public:
  explicit Onode(const hobject_t &hobj) : hobj(hobj) {}

  virtual bool is_alive() const = 0;
  virtual const onode_layout_t &get_layout() const = 0;
  virtual ~Onode() = default;

  virtual void update_local_object_id(Transaction&, local_object_id_t) = 0;
  virtual void update_local_clone_id(Transaction&, local_clone_id_t) = 0;
  virtual void update_onode_size(Transaction&, uint32_t) = 0;
  virtual void update_omap_root(Transaction&, omap_root_t&) = 0;
  virtual void update_xattr_root(Transaction&, omap_root_t&) = 0;
  virtual void update_object_data(Transaction&, object_data_t&) = 0;
  virtual void update_object_info(Transaction&, ceph::bufferlist&) = 0;
  virtual void update_snapset(Transaction&, ceph::bufferlist&) = 0;
  virtual void clear_object_info(Transaction&) = 0;
  virtual void clear_snapset(Transaction&) = 0;

  // local object id doesn't use all of 32 bits internally,
  // LOCAL_OBJECT_ID_NULL means this onode doesn't have
  // local object id.
  std::optional<local_object_id_t> get_local_object_id() const {
    std::optional<local_object_id_t> id = std::nullopt;
    if (auto loid = local_object_id_t(get_layout().local_object_id);
        loid != LOCAL_OBJECT_ID_NULL) {
      // valid local object id should not be zero.
      ceph_assert(loid != LOCAL_OBJECT_ID_ZERO);
      id = loid;
    }
    return id;
  }

  std::optional<local_clone_id_t> get_local_clone_id() const {
    std::optional<local_clone_id_t> id = std::nullopt;
    if (auto lcid = local_clone_id_t(get_layout().local_clone_id);
        lcid != LOCAL_CLONE_ID_NULL) {
      id = lcid;
    }
    return id;
  }

  laddr_hint_t get_data_hint() const {
    return generate_data_hint(
      get_local_object_id(), get_local_clone_id());
  }
  laddr_hint_t get_data_clone_hint() const {
    auto object_id = get_local_object_id();
    ceph_assert(object_id);
    return generate_data_clone_hint(*object_id);
  }
  laddr_hint_t get_metadata_hint(extent_len_t block_size) const {
    return generate_metadata_hint(
      get_local_object_id(), get_local_clone_id(), block_size);
  }
  friend std::ostream& operator<<(std::ostream &out, const Onode &rhs);
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::Onode> : fmt::ostream_formatter {};
#endif
