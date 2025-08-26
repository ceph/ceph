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
  static constexpr int MAX_OI_LENGTH = 236;
  // We might want to move the ss field out of onode_layout_t.
  // The reason is that ss_attr may grow to relative large, as
  // its clone_overlap may grow to a large size, if applications
  // set objects to a relative large size(for the purpose of reducing
  // the number of objects per OSD, so that all objects' metadata
  // can be cached in memory) and do many modifications between
  // snapshots.
  // TODO: implement flexible-sized onode value to store inline ss_attr
  // effectively.
  // The expected decode size of SnapSet when there's no snapshot
  static constexpr int MAX_SS_LENGTH = 35;

  ceph_le32 size{0};
  ceph_le32 oi_size{0};
  ceph_le32 ss_size{0};
  omap_root_le_t omap_root;
  omap_root_le_t log_root;
  omap_root_le_t xattr_root;

  object_data_le_t object_data;

  char oi[MAX_OI_LENGTH] = {0};
  char ss[MAX_SS_LENGTH] = {0};
  /**
    * needs_cow
    *
    * If true, all lba mappings for onode must be cloned
    * to a new range prior to mutation. See ObjectDataHandler::copy_on_write,
    * do_clone, do_clonerange
    */
  bool need_cow = false;

  onode_layout_t() : omap_root(omap_type_t::OMAP), log_root(omap_type_t::LOG),
    xattr_root(omap_type_t::XATTR) {}

  const omap_root_le_t& get_root(omap_type_t type) const {
    if (type == omap_type_t::XATTR) {
      return xattr_root;
    } else if (type == omap_type_t::OMAP) {
      return omap_root;
    } else {
      assert(type == omap_type_t::LOG);
      return log_root;
    }
  }
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
  virtual laddr_hint_t init_hint(
    extent_len_t block_size,
    bool is_metadata) const = 0;
  virtual laddr_hint_t generate_clone_hint(
    local_object_id_t object_id,
    extent_len_t block_size,
    bool is_metadata) const = 0;
  laddr_hint_t get_hint(extent_len_t block_size, bool is_metadata) const {
    assert(block_size >= laddr_t::UNIT_SIZE);
    auto prefix = get_clone_prefix();
    if (prefix) {
      if (is_metadata) {
        return laddr_hint_t::create_object_md_hint(*prefix, block_size);
      } else {
        return laddr_hint_t::create_object_data_hint(*prefix, block_size);
      }
    } else if (sibling_object_id) {
      return generate_clone_hint(
        *sibling_object_id, block_size, is_metadata);
    } else {
      return init_hint(block_size, is_metadata);
    }
  }
  laddr_hint_t get_clone_hint(extent_len_t block_size, bool is_metadata) const {
    return generate_clone_hint(
      get_clone_prefix()->get_local_object_id(), block_size, is_metadata);
  }
  const hobject_t hobj;
  std::optional<local_object_id_t> sibling_object_id;

public:
  explicit Onode(const hobject_t &hobj) : hobj(hobj) {}

  virtual bool is_alive() const = 0;
  virtual const onode_layout_t &get_layout() const = 0;
  virtual ~Onode() = default;

  const hobject_t &get_hobj() const {
    return hobj;
  }
  bool is_head() const {
    return hobj.is_head();
  }
  bool is_snap() const {
    return hobj.is_snap();
  }
  bool need_cow() const {
    return get_layout().need_cow;
  }
  virtual void update_onode_size(Transaction&, uint32_t) = 0;
  virtual void update_omap_root(Transaction&, omap_root_t&) = 0;
  virtual void update_log_root(Transaction&, omap_root_t&) = 0;
  virtual void update_xattr_root(Transaction&, omap_root_t&) = 0;
  virtual void update_object_data(Transaction&, object_data_t&) = 0;
  virtual void update_object_info(Transaction&, ceph::bufferlist&) = 0;
  virtual void update_snapset(Transaction&, ceph::bufferlist&) = 0;
  virtual void clear_object_info(Transaction&) = 0;
  virtual void clear_snapset(Transaction&) = 0;
  virtual void set_need_cow(Transaction&) = 0;
  virtual void unset_need_cow(Transaction&) = 0;
  virtual void swap_layout(Transaction&, Onode&) = 0;

  laddr_hint_t get_metadata_hint(uint64_t block_size = laddr_t::UNIT_SIZE) const {
    return get_hint(block_size, /*is_metadata*/true);
  }
  laddr_hint_t get_data_hint(uint64_t block_size = laddr_t::UNIT_SIZE) const {
    return get_hint(block_size, /*is_metadata*/false);
  }
  laddr_hint_t get_metadata_clone_hint(uint64_t block_size = laddr_t::UNIT_SIZE) const {
    return get_clone_hint(block_size, /*is_metadata*/true);
  }
  laddr_hint_t get_data_clone_hint(uint64_t block_size = laddr_t::UNIT_SIZE) const {
    return get_clone_hint(block_size, /*is_metadata*/false);
  }
  const omap_root_le_t& get_root(omap_type_t type) const {
    return get_layout().get_root(type);
  }
  std::optional<laddr_t> get_clone_prefix() const {
    std::optional<laddr_t> prefix = std::nullopt;

    const auto &layout = get_layout();
    auto omap_root = layout.omap_root.get(L_ADDR_NULL);
    if (!omap_root.is_null()) {
      prefix.emplace(omap_root.addr.get_clone_prefix());
    }

    auto log_root = layout.log_root.get(L_ADDR_NULL);
    if (!log_root.is_null()) {
      auto laddr = log_root.addr.get_clone_prefix();
      if (prefix) {
        ceph_assert(*prefix == laddr);
      } else {
        prefix.emplace(laddr);
      }
    }

    auto xattr_root = layout.xattr_root.get(L_ADDR_NULL);
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
    return std::nullopt;
  }
  void set_sibling_object_id(local_object_id_t id) {
    assert(!sibling_object_id);
    sibling_object_id = id;
  }
  friend std::ostream& operator<<(std::ostream &out, const Onode &rhs);
};


std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::Onode> : fmt::ostream_formatter {};
#endif
