// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <random>

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

struct test_block_delta_t {
  int8_t val = 0;
  extent_len_t offset = 0;
  extent_len_t len = 0;


  DENC(test_block_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.val, p);
    denc(v.offset, p);
    denc(v.len, p);
    DENC_FINISH(p);
  }
};

inline std::ostream &operator<<(
  std::ostream &lhs, const test_extent_desc_t &rhs) {
  return lhs << "test_extent_desc_t(len=" << rhs.len
	     << ", checksum=" << rhs.checksum << ")";
}

struct TestBlock : crimson::os::seastore::LogicalCachedExtent {
  constexpr static extent_len_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlock>;

  std::vector<test_block_delta_t> delta = {};

  interval_set<extent_len_t> modified_region;

  TestBlock(ceph::bufferptr &&ptr)
    : LogicalCachedExtent(std::move(ptr)) {}
  TestBlock(const TestBlock &other)
    : LogicalCachedExtent(other), modified_region(other.modified_region) {}
  TestBlock(extent_len_t length)
    : LogicalCachedExtent(length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new TestBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final;

  void set_contents(char c, extent_len_t offset, extent_len_t len) {
    assert(offset + len <= get_length());
    assert(len > 0);
    ::memset(get_bptr().c_str() + offset, c, len);
    delta.push_back({c, offset, len});
    modified_region.union_insert(offset, len);
  }

  void set_contents(char c) {
    set_contents(c, 0, get_length());
  }

  test_extent_desc_t get_desc() {
    return { get_length(), calc_crc32c() };
  }

  void apply_delta(const ceph::bufferlist &bl) final;

  std::optional<modified_region_t> get_modified_region() final {
    if (modified_region.empty()) {
      return std::nullopt;
    }
    return modified_region_t{modified_region.range_start(),
      modified_region.range_end() - modified_region.range_start()};
  }

  void clear_modified_region() final {
    modified_region.clear();
  }

  void logical_on_delta_write() final {
    delta.clear();
  }
};
using TestBlockRef = TCachedExtentRef<TestBlock>;

struct TestBlockPhysical : crimson::os::seastore::CachedExtent{
  constexpr static extent_len_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlockPhysical>;

  std::vector<test_block_delta_t> delta = {};

  void on_rewrite(CachedExtent&, extent_len_t) final {}

  TestBlockPhysical(ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)) {}
  TestBlockPhysical(const TestBlockPhysical &other)
    : CachedExtent(other) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new TestBlockPhysical(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK_PHYSICAL;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void set_contents(char c, extent_len_t offset, extent_len_t len) {
    ::memset(get_bptr().c_str() + offset, c, len);
    delta.push_back({c, offset, len});
  }

  void set_contents(char c) {
    set_contents(c, 0, get_length());
  }

  ceph::bufferlist get_delta() final;

  void apply_delta_and_adjust_crc(paddr_t, const ceph::bufferlist &bl) final;
};
using TestBlockPhysicalRef = TCachedExtentRef<TestBlockPhysical>;

struct test_block_mutator_t {
  std::uniform_int_distribution<int8_t>
  contents_distribution = std::uniform_int_distribution<int8_t>(
    std::numeric_limits<int8_t>::min(),
    std::numeric_limits<int8_t>::max());

  std::uniform_int_distribution<extent_len_t>
  offset_distribution = std::uniform_int_distribution<extent_len_t>(
    0, TestBlock::SIZE - 1);

  std::uniform_int_distribution<extent_len_t> length_distribution(extent_len_t offset) {
    return std::uniform_int_distribution<extent_len_t>(
      1, TestBlock::SIZE - offset);
  }


  template <typename generator_t>
  void mutate(TestBlock &block, generator_t &gen) {
    auto offset = offset_distribution(gen);
    block.set_contents(
      contents_distribution(gen),
      offset,
      length_distribution(offset)(gen));
  }
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::test_block_delta_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::test_extent_desc_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::TestBlock> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::TestBlockPhysical> : fmt::ostream_formatter {};
#endif
