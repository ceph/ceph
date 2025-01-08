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

  char oi[MAX_OI_LENGTH] = {0};
  char ss[MAX_SS_LENGTH] = {0};
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
    std::optional<local_clone_id_t> clone_id,
    extent_len_t block_size) const = 0;

  virtual laddr_hint_t generate_data_clone_hint(
    local_object_id_t object_id,
    extent_len_t block_size) const = 0;

  virtual laddr_hint_t generate_metadata_hint(
    std::optional<local_object_id_t> object_id,
    std::optional<local_clone_id_t> clone_id,
    extent_len_t block_size) const = 0;

  virtual laddr_hint_t generate_metadata_clone_hint(
    local_object_id_t object_id,
    extent_len_t block_size) const = 0;

  const hobject_t hobj;

  void validate_root_laddr(laddr_t laddr) const {
    auto prefix = get_clone_prefix();
    if (prefix) {
      ceph_assert(laddr.get_clone_prefix() == prefix->get_clone_prefix());
    }
  }

public:
  explicit Onode(const hobject_t &hobj) : hobj(hobj) {}

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

  laddr_hint_t get_data_hint(
    extent_len_t block_size = laddr_t::UNIT_SIZE) const {
    auto prefix = get_clone_prefix();
    if (prefix) {
      return generate_data_hint(
        prefix->get_local_object_id(),
        prefix->get_local_clone_id(),
        block_size);
    } else {
      return generate_data_hint(std::nullopt, std::nullopt, block_size);
    }
  }
  laddr_hint_t get_data_clone_hint(
    extent_len_t block_size = laddr_t::UNIT_SIZE) const {
    auto prefix = get_object_prefix();
    assert(prefix);
    return generate_data_clone_hint(prefix->get_local_object_id(), block_size);
  }
  laddr_hint_t get_metadata_hint(extent_len_t block_size) const {
    auto prefix = get_clone_prefix();
    if (prefix) {
      return generate_metadata_hint(
        prefix->get_local_object_id(),
        prefix->get_local_clone_id(),
        block_size);
    } else {
      return generate_metadata_hint(std::nullopt, std::nullopt, block_size);
    }
  }
  laddr_hint_t get_metadata_clone_hint(extent_len_t block_size) const {
    auto prefix = get_object_prefix();
    assert(prefix);
    return generate_metadata_clone_hint(prefix->get_local_object_id(), block_size);
  }

  std::optional<laddr_t> get_clone_prefix() const {
    std::optional<laddr_t> prefix = std::nullopt;

    const auto &layout = get_layout();
    auto omap_root = layout.omap_root.get(LADDR_HINT_NULL);
    if (!omap_root.is_null()) {
      prefix.emplace(omap_root.addr.get_clone_prefix());
    }

    auto xattr_root = layout.xattr_root.get(LADDR_HINT_NULL);
    if (!xattr_root.is_null()) {
      auto laddr = xattr_root.addr.get_clone_prefix();
      if (prefix) {
        ceph_assert(*prefix == laddr);
      } else {
        prefix.emplace(laddr);
      }
    }

    auto obj_data = layout.object_data.get();
    if (!obj_data.is_null()) {
      auto laddr = obj_data.get_reserved_data_base().get_clone_prefix();
      if (prefix) {
        ceph_assert(*prefix == laddr);
      } else {
        prefix.emplace(laddr);
      }
    }

    return prefix;
  }

  std::optional<laddr_t> get_object_prefix() const {
    auto prefix = get_clone_prefix();
    if (prefix) {
      return prefix->get_object_prefix();
    }
    return prefix;
  }

  void validate_prefix(shard_t shard, pool_t pool, crush_hash_t crush) const {
    auto prefix = get_object_prefix();
    if (prefix) {
      ceph_assert(prefix->match_shard_bits(shard));
      ceph_assert(prefix->match_pool_bits(pool));
      ceph_assert(prefix->get_reversed_hash() == crush);
    }
  }

  friend std::ostream& operator<<(std::ostream &out, const Onode &rhs);
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::Onode> : fmt::ostream_formatter {};
#endif
