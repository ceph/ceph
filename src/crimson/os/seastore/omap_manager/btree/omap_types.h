// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::omap_manager {

struct omap_node_meta_t {
  depth_t depth = 0;

  std::pair<omap_node_meta_t, omap_node_meta_t> split_into() const {
    return std::make_pair(
      omap_node_meta_t{depth},
      omap_node_meta_t{depth});
  }

  static omap_node_meta_t merge_from(
    const omap_node_meta_t &lhs, const omap_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return omap_node_meta_t{lhs.depth};
  }

  static std::pair<omap_node_meta_t, omap_node_meta_t>
  rebalance(const omap_node_meta_t &lhs, const omap_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return std::make_pair(
      omap_node_meta_t{lhs.depth},
      omap_node_meta_t{lhs.depth});
  }
};

struct omap_node_meta_le_t {
  depth_le_t depth = init_depth_le(0);

  omap_node_meta_le_t() = default;
  omap_node_meta_le_t(const omap_node_meta_le_t &) = default;
  explicit omap_node_meta_le_t(const omap_node_meta_t &val)
    : depth(init_depth_le(val.depth)) {}

  operator omap_node_meta_t() const {
    return omap_node_meta_t{ depth };
  }
};

struct omap_inner_key_t {
  uint16_t key_off = 0;
  uint16_t key_len = 0;
  laddr_t laddr = L_ADDR_NULL;

  omap_inner_key_t() = default;
  omap_inner_key_t(uint16_t off, uint16_t len, laddr_t addr)
  : key_off(off), key_len(len), laddr(addr) {}

  inline bool operator==(const omap_inner_key_t b) const {
    return key_off == b.key_off && key_len == b.key_len && laddr == b.laddr;
  }
  inline bool operator!=(const omap_inner_key_t b) const {
    return key_off != b.key_off || key_len != b.key_len || laddr != b.laddr;
  }
  DENC(omap_inner_key_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_off, p);
    denc(v.key_len, p);
    denc(v.laddr, p);
    DENC_FINISH(p);
  }
};

struct omap_inner_key_le_t {
  ceph_le16 key_off{0};
  ceph_le16 key_len{0};
  laddr_le_t laddr{L_ADDR_NULL};

  omap_inner_key_le_t() = default;
  omap_inner_key_le_t(const omap_inner_key_le_t &) = default;
  explicit omap_inner_key_le_t(const omap_inner_key_t &key)
    : key_off(key.key_off),
      key_len(key.key_len),
      laddr(key.laddr) {}

  operator omap_inner_key_t() const {
    return omap_inner_key_t{uint16_t(key_off), uint16_t(key_len), laddr_t(laddr)};
  }

  omap_inner_key_le_t& operator=(omap_inner_key_t key) {
    key_off = key.key_off;
    key_len = key.key_len;
    laddr = laddr_le_t(key.laddr);
    return *this;
  }

  inline bool operator==(const omap_inner_key_le_t b) const {
    return key_off == b.key_off && key_len == b.key_len && laddr_t(laddr) == laddr_t(b.laddr);
  }
};

struct omap_leaf_key_t {
  uint16_t key_off = 0;
  uint16_t key_len = 0;
  uint16_t val_len = 0;

  omap_leaf_key_t() = default;
  omap_leaf_key_t(uint16_t k_off, uint16_t k_len, uint16_t v_len)
  : key_off(k_off), key_len(k_len), val_len(v_len) {}

  inline bool operator==(const omap_leaf_key_t b) const {
    return key_off == b.key_off && key_len == b.key_len &&
           val_len == b.val_len;
  }
  inline bool operator!=(const omap_leaf_key_t b) const {
    return key_off != b.key_off || key_len != b.key_len ||
           val_len != b.val_len;
  }

  DENC(omap_leaf_key_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_off, p);
    denc(v.key_len, p);
    denc(v.val_len, p);
    DENC_FINISH(p);
  }
};

struct omap_leaf_key_le_t {
  ceph_le16 key_off{0};
  ceph_le16 key_len{0};
  ceph_le16 val_len{0};

  omap_leaf_key_le_t() = default;
  omap_leaf_key_le_t(const omap_leaf_key_le_t &) = default;
  explicit omap_leaf_key_le_t(const omap_leaf_key_t &key)
    : key_off(key.key_off),
      key_len(key.key_len),
      val_len(key.val_len) {}

  operator omap_leaf_key_t() const {
    return omap_leaf_key_t{uint16_t(key_off), uint16_t(key_len),
                           uint16_t(val_len)};
  }

  omap_leaf_key_le_t& operator=(omap_leaf_key_t key) {
    key_off = key.key_off;
    key_len = key.key_len;
    val_len = key.val_len;
    return *this;
  }

  inline bool operator==(const omap_leaf_key_le_t b) const {
    return key_off == b.key_off && key_len == b.key_len &&
           val_len == b.val_len;
  }
};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::omap_manager::omap_inner_key_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::omap_manager::omap_leaf_key_t)
