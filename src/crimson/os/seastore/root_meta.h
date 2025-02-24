// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

struct RootMetaBlock : LogicalChildNode {
  using meta_t = std::map<std::string, std::string>;
  using Ref = TCachedExtentRef<RootMetaBlock>;
  static constexpr size_t SIZE = 4096;
  static constexpr int MAX_META_LENGTH = 1024;

  explicit RootMetaBlock(ceph::bufferptr &&ptr)
    : LogicalChildNode(std::move(ptr)) {}
  explicit RootMetaBlock(extent_len_t length)
    : LogicalChildNode(length) {}
  RootMetaBlock(const RootMetaBlock &rhs)
    : LogicalChildNode(rhs) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new RootMetaBlock(*this));
  }

  static constexpr extent_types_t TYPE = extent_types_t::ROOT_META;
  extent_types_t get_type() const final {
    return extent_types_t::ROOT_META;
  }

  /// dumps root meta as delta
  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    ceph::buffer::ptr bptr(get_bptr(), 0, MAX_META_LENGTH);
    bl.append(bptr);
    return bl;
  }

  /// overwrites root
  void apply_delta(const ceph::bufferlist &_bl) final
  {
    assert(_bl.length() == MAX_META_LENGTH);
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    get_bptr().copy_in(0, MAX_META_LENGTH, bl.front().c_str());
  }

  meta_t get_meta() const {
    bufferlist bl;
    bl.append(get_bptr());
    meta_t ret;
    auto iter = bl.cbegin();
    decode(ret, iter);
    return ret;
  }

  void set_meta(const meta_t &m) {
    ceph::bufferlist bl;
    encode(m, bl);
    ceph_assert(bl.length() <= MAX_META_LENGTH);
    bl.rebuild();
    get_bptr().zero(0, MAX_META_LENGTH);
    get_bptr().copy_in(0, bl.length(), bl.front().c_str());
  }

};
using RootMetaBlockRef = RootMetaBlock::Ref;

} // crimson::os::seastore


#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::RootMetaBlock>
  : fmt::ostream_formatter {};
#endif
