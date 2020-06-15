// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

struct test_extent_desc_t {
  size_t len = 0;
  unsigned checksum = 0;

  bool operator==(const test_extent_desc_t &rhs) const {
    return (len == rhs.len &&
	    checksum == rhs.checksum);
  }
  bool operator!=(const test_extent_desc_t &rhs) const {
    return !(*this == rhs);
  }
};

inline std::ostream &operator<<(
  std::ostream &lhs, const test_extent_desc_t &rhs) {
  return lhs << "test_extent_desc_t(len=" << rhs.len
	     << ", checksum=" << rhs.checksum << ")";
}

struct TestBlock : crimson::os::seastore::LogicalCachedExtent {
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlock>;

  TestBlock(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  TestBlock(const TestBlock &other) : LogicalCachedExtent(other) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new TestBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    return ceph::bufferlist();
  }

  void set_contents(char c) {
    ::memset(get_bptr().c_str(), c, get_length());
  }

  int checksum() {
    return ceph_crc32c(
      1,
      (const unsigned char *)get_bptr().c_str(),
      get_length());
  }

  test_extent_desc_t get_desc() {
    return { get_length(), get_crc32c(1) };
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }
};
using TestBlockRef = TCachedExtentRef<TestBlock>;

}
