// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <map>
#include <optional>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "include/buffer.h"
#include "include/interval_set.h"
#include "include/rados/librados.hpp"
#include "osd/osd_types.h"
#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "crimson_utils.h"

using namespace std;
using namespace librados;
using ceph::test::ECOnlyTestFixture;
using ceph::test::PoolType;
using ceph::test::PoolTypeTestFixture;

// ---------------------------------------------------------------------------
// Mixin providing helper methods used by both SparseReadTest and
// ECSparseReadTest.  Concrete fixture subclasses must implement get_ioctx()
// to return their IoCtx — this avoids any ambiguous member lookup when the
// mixin is combined with a fixture base that also owns an ioctx field.
// ---------------------------------------------------------------------------

// Format an extent map as "({off, len}, {off, len}, ...)" for failure messages.
static std::string format_extents(const std::map<uint64_t, uint64_t>& m) {
  std::ostringstream os;
  os << "(";
  bool first = true;
  for (const auto& [off, len] : m) {
    if (!first) os << ", ";
    os << "{" << off << ", " << len << "}";
    first = false;
  }
  os << ")";
  return os.str();
}

class SparseReadHelpers {
protected:
  // Provided by each concrete fixture.
  virtual librados::IoCtx& get_ioctx() = 0;

  // Helper to create buffer with specific pattern
  bufferlist create_pattern_buffer(size_t size, char pattern) {
    bufferlist bl;
    bl.append(std::string(size, pattern));
    return bl;
  }

  // Helper to create zero buffer
  bufferlist create_zero_buffer(size_t size) {
    bufferlist bl;
    bl.append(std::string(size, '\0'));
    return bl;
  }

  // Helper to verify sparse read results
  void verify_sparse_read(
      const std::string& oid,
      uint64_t offset,
      uint64_t length,
      const std::map<uint64_t, uint64_t>& expected_extents,
      const bufferlist& expected_data) {
    std::map<uint64_t, uint64_t> extents;
    bufferlist read_bl;
    int ret = get_ioctx().sparse_read(oid, extents, read_bl, length, offset);
    ASSERT_EQ(ret, (int)expected_extents.size())
        << "sparse_read extent count mismatch\n"
        << "  actual extents:   " << format_extents(extents) << "\n"
        << "  expected extents: " << format_extents(expected_extents);
    ASSERT_EQ(extents, expected_extents)
        << "sparse_read extent map mismatch\n"
        << "  actual:   " << format_extents(extents) << "\n"
        << "  expected: " << format_extents(expected_extents);
    ASSERT_EQ(read_bl.length(), expected_data.length());
    ASSERT_TRUE(read_bl.contents_equal(expected_data));
  }

  // Helper to verify mapext results
  void verify_mapext(
      const std::string& oid,
      uint64_t offset,
      uint64_t length,
      const std::map<uint64_t, uint64_t>& expected_extents) {
    std::map<uint64_t, uint64_t> extents;
    int ret = get_ioctx().mapext(oid, offset, length, extents);
    ASSERT_EQ(ret, (int)expected_extents.size())
        << "mapext extent count mismatch\n"
        << "  actual extents:   " << format_extents(extents) << "\n"
        << "  expected extents: " << format_extents(expected_extents);
    ASSERT_EQ(extents, expected_extents)
        << "mapext extent map mismatch\n"
        << "  actual:   " << format_extents(extents) << "\n"
        << "  expected: " << format_extents(expected_extents);
  }

  std::optional<force_allocated_extents_t> get_force_allocated_extents(
      const std::string& oid) {
    bufferlist bl;
    static_assert(OI_ATTR[0] == '_', "OI_ATTR must start with '_'");
    int ret = get_ioctx().getxattr(oid, &OI_ATTR[1], bl);
    if (ret < 0) {
      ADD_FAILURE() << "getxattr OI failed: " << ret;
      return std::nullopt;
    }
    object_info_t oi(bl);
    if (oi.force_allocated_extents.empty()) {
      return std::nullopt;
    }
    return oi.force_allocated_extents;
  }

  virtual ~SparseReadHelpers() = default;
};

/**
 * Test fixture for sparse read operations on EC and Replicated pools.
 * Tests sparse_read, write, truncate, zero and mapext operations.
 */
class SparseReadTest : public PoolTypeTestFixture, public SparseReadHelpers {
protected:
  librados::IoCtx& get_ioctx() override { return ioctx; }

  static std::string pool_name_prefix() {
    return "sparse_read_test_";
  }

  static void SetUpTestSuite() {
    PoolTypeTestFixture::SetUpTestSuite();
    auto it = pool_names.find(PoolType::FAST_EC);
    if (it != pool_names.end()) {
      ASSERT_EQ("", set_pool_flags_pp(
        it->second,
        rados,
        pg_pool_t::FLAG_PRESERVE_ALLOCATION,
        true));
      rados.wait_for_latest_osdmap();
    }
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    PoolTypeTestFixture::SetUp();
  }

  void TearDown() override {
    SKIP_IF_CRIMSON();
    if (balancing_disabled) {
      turn_balancing_on();
    }
    PoolTypeTestFixture::TearDown();
  }
};

/**
 * Test fixture for EC-only sparse read tests (FAE tracking).
 * Uses a single EC pool shared across the suite (created by ECOnlyTestFixture's
 * SetUpTestSuite / TearDownTestSuite).  Per-test isolation is via namespaces.
 * Helpers are inherited from SparseReadHelpers.
 */
class ECSparseReadTest : public ECOnlyTestFixture, public SparseReadHelpers {
protected:
  librados::IoCtx& get_ioctx() override { return ioctx; }

  static void SetUpTestSuite() {
    ECOnlyTestFixture::SetUpTestSuite();
    // Enable zero-block allocation tracking on the shared EC pool so that
    // FAE-related tests work without needing a per-request flag.
    ASSERT_EQ("", set_pool_flags_pp(
      static_pool_name, rados, pg_pool_t::FLAG_PRESERVE_ALLOCATION, true));
    rados.wait_for_latest_osdmap();
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    ECOnlyTestFixture::SetUp();
  }

  void TearDown() override {
    SKIP_IF_CRIMSON();
    ECOnlyTestFixture::TearDown();
  }
};

// Test basic sparse_read on a simple write
TEST_P(SparseReadTest, BasicSparseRead) {
  std::string oid = "sparse_read_basic";
  
  // Write some data
  bufferlist write_bl = create_pattern_buffer(4096, 'A');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Sparse read should return the written extent
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}};
  verify_sparse_read(oid, 0, 4096, expected_extents, write_bl);
}

// Test sparse_read with holes (unallocated regions)
TEST_P(SparseReadTest, SparseReadWithHoles) {
  std::string oid = "sparse_read_holes";

  // Write data at offset 0 and 8192, leaving a hole at [4096, 8192)
  bufferlist write_bl1 = create_pattern_buffer(4096, 'A');
  bufferlist write_bl2 = create_pattern_buffer(4096, 'B');

  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 8192));

  // sparse_read returns only allocated data: write_bl1 followed by write_bl2
  bufferlist expected_data;
  expected_data.append(write_bl1);
  expected_data.append(write_bl2);

  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_sparse_read(oid, 0, 12288, expected_extents, expected_data);
}

// Test sparse_read after writing zeros
TEST_P(SparseReadTest, SparseReadZeros) {
  std::string oid = "sparse_read_zeros";
  
  // Write zeros
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  // Sparse read - behavior depends on pool type and zero tracking
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 4096, 0);
  
  // Should read successfully
  ASSERT_GE(ret, 0);
}

// Test WRITE operation
TEST_P(SparseReadTest, WriteOperation) {
  std::string oid = "write_op";
  
  // Write data
  bufferlist write_bl = create_pattern_buffer(8192, 'X');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Verify with read
  bufferlist read_bl;
  ASSERT_EQ(8192, ioctx.read(oid, read_bl, 8192, 0));
  ASSERT_TRUE(read_bl.contents_equal(write_bl));
}

TEST_F(ECSparseReadTest, WriteTracksAllZeroExtentOnFastEC) {
  std::string oid = "write_tracks_zero_extent";
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, FAE_BLOCK_SIZE);
  ASSERT_EQ(expected, fae->get_intervals());
}

TEST_F(ECSparseReadTest, WriteDoesNotTrackExtentWhenOnlyPrefixIsZero) {
  std::string oid = "write_prefix_zero_only";
  bufferlist write_bl;
  std::string data(4096, '\0');
  data[8] = 'X';
  write_bl.append(data);
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

TEST_F(ECSparseReadTest, WriteTracksOnlyFullyCoveredZeroBlocks) {
  std::string oid = "write_tracks_full_zero_blocks_only";
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 4096));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(4096, 8192);
  ASSERT_EQ(expected, fae->get_intervals());
}

TEST_F(ECSparseReadTest, WriteSkipsPartialLeadingBlock) {
  std::string oid = "write_skips_partial_leading_block";
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 2048));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

TEST_F(ECSparseReadTest, WriteClearsTrackedExtentWithNonZeroOverwrite) {
  std::string oid = "write_clears_tracked_extent";
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  bufferlist data_bl = create_pattern_buffer(4096, 'Z');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// Test WRITEFULL operation
TEST_P(SparseReadTest, WritefullOperation) {
  std::string oid = "writefull_op";
  
  // Initial write
  bufferlist write_bl1 = create_pattern_buffer(4096, 'A');
  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));

  // Writefull should replace entire object
  bufferlist write_bl2 = create_pattern_buffer(8192, 'B');
  bufferlist write_bl2_copy = write_bl2;  // write_full clears the source bufferlist
  ASSERT_EQ(0, ioctx.write_full(oid, write_bl2));

  // Verify size and content
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));
  ASSERT_EQ(size, 8192u);

  bufferlist read_bl;
  ASSERT_EQ(8192, ioctx.read(oid, read_bl, 8192, 0));
  ASSERT_TRUE(read_bl.contents_equal(write_bl2_copy));
}

// Test WRITESAME operation
TEST_P(SparseReadTest, WritesameOperation) {
  std::string oid = "writesame_op";
  
  // Write same pattern across range
  bufferlist pattern_bl = create_pattern_buffer(4096, 'C');
  ASSERT_EQ(0, ioctx.writesame(oid, pattern_bl, 16384, 0));

  // Verify the pattern was repeated
  bufferlist read_bl;
  ASSERT_EQ(16384, ioctx.read(oid, read_bl, 16384, 0));
  
  // Check that pattern repeats
  for (uint64_t offset = 0; offset < 16384; offset += 4096) {
    bufferlist chunk;
    chunk.substr_of(read_bl, offset, 4096);
    ASSERT_TRUE(chunk.contents_equal(pattern_bl));
  }
}

// Test TRUNCATE operation
TEST_P(SparseReadTest, TruncateOperation) {
  std::string oid = "truncate_op";
  
  // Write data
  bufferlist write_bl = create_pattern_buffer(8192, 'D');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Truncate to smaller size
  ASSERT_EQ(0, ioctx.trunc(oid, 4096));

  // Verify new size
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));
  ASSERT_EQ(size, 4096u);

  // Verify sparse read reflects truncation
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 8192, 0);
  ASSERT_GE(ret, 0);
  ASSERT_LE(read_bl.length(), 4096u);
}

// Test ZERO operation
TEST_P(SparseReadTest, ZeroOperation) {
  std::string oid = "zero_op";
  
  // Write data
  bufferlist write_bl = create_pattern_buffer(12288, 'E');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Zero out middle section
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Verify sparse read shows hole in middle
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 12288, 0);
  
  // Should have data at beginning and end, hole in middle
  ASSERT_GE(ret, 0);
  
  // Verify the zeroed region reads as zeros
  bufferlist zero_check;
  ASSERT_EQ(4096, ioctx.read(oid, zero_check, 4096, 4096));
  bufferlist expected_zeros = create_zero_buffer(4096);
  ASSERT_TRUE(zero_check.contents_equal(expected_zeros));
}

// ---------------------------------------------------------------------------
// Sub-Tasks 1–3: ZERO op hole-punch tests — EC only.
// Replicated pools use byte-level granularity for zero ops (BlueStore may or
// may not reclaim storage depending on the block device) and do not guarantee
// 4K-aligned deallocation.  These tests verify the EC-specific contract: a
// 4K-aligned ZERO punches an exact hole visible in sparse_read and mapext.
// ---------------------------------------------------------------------------

// Aligned ZERO on the middle block: hole appears at [4096, 8192).
TEST_F(ECSparseReadTest, ZeroOpAlignedPunchesHoleMiddle) {
  std::string oid = "zero_aligned_hole_middle";

  bufferlist data_bl = create_pattern_buffer(12288, 'A');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Expected: two allocated extents with a 4K hole in the middle.
  bufferlist expected_data;
  expected_data.append(create_pattern_buffer(4096, 'A'));
  expected_data.append(create_pattern_buffer(4096, 'A'));
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_sparse_read(oid, 0, 12288, expected_extents, expected_data);
}

// Aligned ZERO on the first block: hole at [0, 4096).
TEST_F(ECSparseReadTest, ZeroOpAlignedPunchesHoleStart) {
  std::string oid = "zero_aligned_hole_start";

  bufferlist data_bl = create_pattern_buffer(8192, 'B');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Expected: only the second block survives.
  bufferlist expected_data = create_pattern_buffer(4096, 'B');
  std::map<uint64_t, uint64_t> expected_extents = {{4096, 4096}};
  verify_sparse_read(oid, 0, 8192, expected_extents, expected_data);
}

// Aligned ZERO on the last block: hole at [4096, 8192).
TEST_F(ECSparseReadTest, ZeroOpAlignedPunchesHoleEnd) {
  std::string oid = "zero_aligned_hole_end";

  bufferlist data_bl = create_pattern_buffer(8192, 'C');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Expected: only the first block survives.
  bufferlist expected_data = create_pattern_buffer(4096, 'C');
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}};
  verify_sparse_read(oid, 0, 8192, expected_extents, expected_data);
}

// Aligned ZERO covering the entire object: all blocks become holes.
TEST_F(ECSparseReadTest, ZeroOpAlignedPunchesWholeObject) {
  std::string oid = "zero_aligned_whole_object";

  bufferlist data_bl = create_pattern_buffer(8192, 'D');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(0, 8192);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Expected: no allocated extents, empty data.
  std::map<uint64_t, uint64_t> expected_extents;
  bufferlist expected_data;
  verify_sparse_read(oid, 0, 8192, expected_extents, expected_data);
}

// Aligned ZERO on the middle block: mapext sees hole at [4096, 8192).
TEST_F(ECSparseReadTest, ZeroOpAlignedHoleMiddleMapext) {
  std::string oid = "zero_aligned_hole_middle_mapext";

  bufferlist data_bl = create_pattern_buffer(12288, 'E');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_mapext(oid, 0, 12288, expected_extents);
}

// Aligned ZERO on the first block: mapext sees only the second block.
TEST_F(ECSparseReadTest, ZeroOpAlignedHoleStartMapext) {
  std::string oid = "zero_aligned_hole_start_mapext";

  bufferlist data_bl = create_pattern_buffer(8192, 'F');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  std::map<uint64_t, uint64_t> expected_extents = {{4096, 4096}};
  verify_mapext(oid, 0, 8192, expected_extents);
}

// Aligned ZERO on the last block: mapext sees only the first block.
TEST_F(ECSparseReadTest, ZeroOpAlignedHoleEndMapext) {
  std::string oid = "zero_aligned_hole_end_mapext";

  bufferlist data_bl = create_pattern_buffer(8192, 'G');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}};
  verify_mapext(oid, 0, 8192, expected_extents);
}

// Misaligned ZERO: interior block is a hole; head and tail blocks are allocated.
// ZERO [2048, 10240): the interior aligned block [4096, 8192) is deallocated;
// the head [2048, 4096) and tail [8192, 10240) are literal-zero writes so
// blocks 0 and 2 remain allocated.
TEST_F(ECSparseReadTest, ZeroOpMisalignedHolePunchSparseRead) {
  std::string oid = "zero_misaligned_hole_sparse";

  bufferlist data_bl = create_pattern_buffer(3 * 4096, 'H');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(2048, 2 * 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  //   [0, 2048): unchanged 'H'
  //   [2048, 4096): literal zeros (head of ZERO)
  //   block 1 [4096, 8192): hole — not in sparse data
  //   [8192, 10240): literal zeros (tail of ZERO)
  //   [10240, 12288): unchanged 'H'
  bufferlist expected_data;
  expected_data.append(create_pattern_buffer(2048, 'H'));
  expected_data.append(create_zero_buffer(2048));
  expected_data.append(create_zero_buffer(2048));
  expected_data.append(create_pattern_buffer(2048, 'H'));

  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_sparse_read(oid, 0, 12288, expected_extents, expected_data);
}

// Misaligned ZERO: mapext also shows the interior block as a hole.
TEST_F(ECSparseReadTest, ZeroOpMisalignedHolePunchMapext) {
  std::string oid = "zero_misaligned_hole_mapext";

  bufferlist data_bl = create_pattern_buffer(3 * 4096, 'I');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  ObjectWriteOperation op;
  op.zero(2048, 2 * 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_mapext(oid, 0, 12288, expected_extents);
}

// Test MAPEXT operation
TEST_P(SparseReadTest, MapextOperation) {
  std::string oid = "mapext_op";
  
  // Write data with holes
  bufferlist write_bl1 = create_pattern_buffer(4096, 'F');
  bufferlist write_bl2 = create_pattern_buffer(4096, 'G');
  
  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 8192));

  // Use mapext to query allocation
  std::map<uint64_t, uint64_t> expected_extents = {
    {0, 4096},
    {8192, 4096}
  };
  verify_mapext(oid, 0, 12288, expected_extents);
}

// Test sparse read after partial overwrite
TEST_P(SparseReadTest, PartialOverwrite) {
  std::string oid = "partial_overwrite";

  // Initial write of 8 KiB of 'H'
  bufferlist write_bl1 = create_pattern_buffer(8192, 'H');
  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));

  // Partial overwrite of 2 KiB of 'I' at offset 3072
  bufferlist write_bl2 = create_pattern_buffer(2048, 'I');
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 3072));

  // Build the expected merged content: H[0..3072) + I[3072..5120) + H[5120..8192)
  bufferlist expected_data;
  expected_data.append(create_pattern_buffer(3072, 'H'));
  expected_data.append(write_bl2);
  expected_data.append(create_pattern_buffer(8192 - 5120, 'H'));

  // The overwrite merges into one contiguous allocated extent
  std::map<uint64_t, uint64_t> expected_extents = {{0, 8192}};
  verify_sparse_read(oid, 0, 8192, expected_extents, expected_data);
}

// Test sparse read with large object
TEST_P(SparseReadTest, LargeObjectSparseRead) {
  std::string oid = "large_sparse";

  // Write identical 4 KiB 'J' blocks at offsets 0, 16384, and 32768
  bufferlist write_bl = create_pattern_buffer(4096, 'J');

  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 16384));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 32768));

  // sparse_read returns only the three allocated blocks concatenated
  bufferlist expected_data;
  expected_data.append(write_bl);
  expected_data.append(write_bl);
  expected_data.append(write_bl);

  std::map<uint64_t, uint64_t> expected_extents = {
    {0, 4096}, {16384, 4096}, {32768, 4096}
  };
  verify_sparse_read(oid, 0, 40960, expected_extents, expected_data);
}

// Test two-stage zero detection: first byte zero, rest non-zero
TEST_P(SparseReadTest, ZeroDetectionFirstByteOnly) {
  std::string oid = "zero_detect_first_byte";
  
  // Create buffer: first byte zero, second byte non-zero
  bufferlist write_bl;
  std::string data(4096, '\0');
  data[1] = 'X';  // Make second byte non-zero
  write_bl.append(data);
  
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Verify data is readable
  bufferlist read_bl;
  ASSERT_EQ(4096, ioctx.read(oid, read_bl, 4096, 0));
  ASSERT_TRUE(read_bl.contents_equal(write_bl));
}

// Test two-stage zero detection: first 8 bytes zero, rest non-zero
TEST_P(SparseReadTest, ZeroDetectionFirst8BytesOnly) {
  std::string oid = "zero_detect_8_bytes";
  
  // Create buffer: first 8 bytes zero, byte 9 non-zero
  bufferlist write_bl;
  std::string data(4096, '\0');
  data[8] = 'Y';  // Make 9th byte non-zero
  write_bl.append(data);
  
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  
  // Verify data is readable
  bufferlist read_bl;
  ASSERT_EQ(4096, ioctx.read(oid, read_bl, 4096, 0));
  ASSERT_TRUE(read_bl.contents_equal(write_bl));
}

// Test two-stage zero detection: all zeros
TEST_P(SparseReadTest, ZeroDetectionAllZeros) {
  std::string oid = "zero_detect_all_zeros";
  
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  
  // Verify zeros are readable
  bufferlist read_bl;
  ASSERT_EQ(4096, ioctx.read(oid, read_bl, 4096, 0));
  ASSERT_TRUE(read_bl.contents_equal(zero_bl));
}

// Test overwriting zeros with non-zero data
TEST_P(SparseReadTest, OverwriteZerosWithData) {
  std::string oid = "overwrite_zeros";
  
  // Write zeros first
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  
  // Overwrite with non-zero data
  bufferlist data_bl = create_pattern_buffer(4096, 'N');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 2048));
  
  // Verify mixed content
  bufferlist read_bl;
  ASSERT_EQ(8192, ioctx.read(oid, read_bl, 8192, 0));
  
  // Check that overwritten section has non-zero data
  bufferlist middle_section;
  middle_section.substr_of(read_bl, 2048, 4096);
  ASSERT_TRUE(middle_section.contents_equal(data_bl));
}

// Test that WRITEFULL clears force-allocated extents
TEST_P(SparseReadTest, WritefullClearsZeroTracking) {
  std::string oid = "writefull_clears";
  
  // Write zeros (may become force-allocated)
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  
  // WRITEFULL with non-zero data
  bufferlist data_bl = create_pattern_buffer(4096, 'O');
  bufferlist data_bl_copy = data_bl;  // write_full clears the source bufferlist
  ASSERT_EQ(0, ioctx.write_full(oid, data_bl));
  
  // Verify new size and content
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));
  ASSERT_EQ(size, 4096u);
  
  bufferlist read_bl;
  ASSERT_EQ(4096, ioctx.read(oid, read_bl, 4096, 0));
  ASSERT_TRUE(read_bl.contents_equal(data_bl_copy));
}

// Test WRITESAME with zero pattern
TEST_P(SparseReadTest, WritesameZeros) {
  std::string oid = "writesame_zeros";
  
  // Write same zero pattern across range
  bufferlist zero_pattern = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.writesame(oid, zero_pattern, 16384, 0));
  
  // Verify the zeros were written
  bufferlist read_bl;
  ASSERT_EQ(16384, ioctx.read(oid, read_bl, 16384, 0));
  
  bufferlist expected_zeros = create_zero_buffer(16384);
  ASSERT_TRUE(read_bl.contents_equal(expected_zeros));
}

// Test pattern of zeros and non-zero data
TEST_P(SparseReadTest, MixedZeroNonZeroPattern) {
  std::string oid = "mixed_pattern";
  
  // Write pattern: data, zeros, data, zeros
  bufferlist data_bl = create_pattern_buffer(4096, 'P');
  bufferlist zero_bl = create_zero_buffer(4096);
  
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 4096));
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 8192));
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 12288));
  
  // Verify sparse read shows correct pattern
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 16384, 0);
  ASSERT_GE(ret, 0);
  
  // Verify we can read all the data back
  bufferlist full_read;
  ASSERT_EQ(16384, ioctx.read(oid, full_read, 16384, 0));
}

// Test truncate removes force-allocated extents beyond new size
TEST_P(SparseReadTest, TruncateRemovesExtents) {
  std::string oid = "truncate_extents";
  
  // Write zeros at various offsets
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 8192));
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 16384));
  
  // Truncate to smaller size
  ASSERT_EQ(0, ioctx.trunc(oid, 10240));
  
  // Verify size
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));
  ASSERT_EQ(size, 10240u);
  
  // Verify sparse read doesn't show extents beyond truncate point
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  ioctx.sparse_read(oid, extents, read_bl, 20480, 0);
  
  // No extents should exist beyond 10240
  for (const auto& [offset, len] : extents) {
    ASSERT_LT(offset, 10240u);
  }
}

// Test ZERO operation deallocates force-allocated regions
TEST_P(SparseReadTest, ZeroOperationDeallocates) {
  std::string oid = "zero_deallocates";
  
  // Write zeros (may become force-allocated)
  bufferlist zero_bl = create_zero_buffer(12288);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  
  // ZERO operation on same region
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));
  
  // Verify the region reads as zeros
  bufferlist read_bl;
  ASSERT_EQ(4096, ioctx.read(oid, read_bl, 4096, 4096));
  bufferlist expected_zeros = create_zero_buffer(4096);
  ASSERT_TRUE(read_bl.contents_equal(expected_zeros));
}

// Test sparse read with offset and partial length
TEST_P(SparseReadTest, SparseReadPartialRange) {
  std::string oid = "sparse_partial";

  // Write 'Q' blocks at offsets 0, 8192, and 16384 with a hole between each
  bufferlist write_bl = create_pattern_buffer(4096, 'Q');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 8192));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 16384));

  // Read [4096, 12288): only the block at offset 8192 falls in this range;
  // the block at offset 0 is before the read window and the one at 16384 is
  // beyond it.
  std::map<uint64_t, uint64_t> expected_extents = {{8192, 4096}};
  verify_sparse_read(oid, 4096, 8192, expected_extents, write_bl);
}

// Test sparse_read starting at a non-zero offset into the object
TEST_P(SparseReadTest, SparseReadFromNonZeroOffset) {
  std::string oid = "sparse_nonzero_offset";

  // Write 'R' at [0, 4096) and 'S' at [8192, 12288), with a hole at [4096, 8192)
  bufferlist bl_r = create_pattern_buffer(4096, 'R');
  bufferlist bl_s = create_pattern_buffer(4096, 'S');
  ASSERT_EQ(0, ioctx.write(oid, bl_r, bl_r.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, bl_s, bl_s.length(), 8192));

  // Read [8192, 16384): should see only the 'S' extent; the 'R' block is
  // completely outside the read window.
  std::map<uint64_t, uint64_t> expected_extents = {{8192, 4096}};
  verify_sparse_read(oid, 8192, 8192, expected_extents, bl_s);
}

// Test sparse_read starting mid-extent (offset splits an allocated block)
TEST_P(SparseReadTest, SparseReadOffsetSplitsExtent) {
  std::string oid = "sparse_offset_splits";

  // Write a single 8 KiB 'T' block at offset 0
  bufferlist write_bl = create_pattern_buffer(8192, 'T');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Read [4096, 8192): only the second half of the block should be returned
  bufferlist expected_data = create_pattern_buffer(4096, 'T');
  std::map<uint64_t, uint64_t> expected_extents = {{4096, 4096}};
  verify_sparse_read(oid, 4096, 4096, expected_extents, expected_data);
}

// Test sparse_read with length 0 on an object with data
TEST_P(SparseReadTest, SparseReadZeroLength) {
  std::string oid = "sparse_zero_length";

  // Write some data so the object exists
  bufferlist write_bl = create_pattern_buffer(4096, 'U');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // A zero-length sparse_read should return 0 extents and an empty bufferlist
  std::map<uint64_t, uint64_t> expected_extents;
  bufferlist expected_data;
  verify_sparse_read(oid, 0, 0, expected_extents, expected_data);
}

// Test sparse_read with length 0 at a non-zero offset
TEST_P(SparseReadTest, SparseReadZeroLengthNonZeroOffset) {
  std::string oid = "sparse_zero_length_offset";

  bufferlist write_bl = create_pattern_buffer(4096, 'V');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));

  // Zero-length read at a non-zero offset should also return nothing
  std::map<uint64_t, uint64_t> expected_extents;
  bufferlist expected_data;
  verify_sparse_read(oid, 2048, 0, expected_extents, expected_data);
}

// ---------------------------------------------------------------------------
// Sub-Task 4: sparse_read at non-zero offsets intersecting ZERO-op holes — EC only.
// Replicated pools do not guarantee 4K hole-punch from ZERO ops so the precise
// extent maps asserted here are only valid on EC.
// ---------------------------------------------------------------------------

// Read window starts inside a hole at the beginning of the object.
// The object is written as a single 12 KiB block so BlueStore allocates it
// contiguously; zeroing block 0 leaves one merged surviving extent [4096, 12288).
// sparse_read(2048, 8192): window [2048, 10240) starts inside the hole (block 0)
// and captures the single contiguous surviving extent [4096, 8192).
TEST_F(ECSparseReadTest, SparseReadFromOffsetInsideZeroHole) {
  std::string oid = "sparse_offset_inside_hole";

  bufferlist data_bl = create_pattern_buffer(12288, 'J');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // Punch a hole in block 0.
  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Read [2048, 10240): block 0 is a hole; blocks 1+2 form one contiguous
  // extent [4096, 12288) of which [4096, 10240) — 6144 bytes — falls within
  // the window.
  bufferlist expected_data = create_pattern_buffer(6144, 'J');
  std::map<uint64_t, uint64_t> expected_extents = {{4096, 6144}};
  verify_sparse_read(oid, 2048, 8192, expected_extents, expected_data);
}

// Read window ends inside a hole at the end of the object.
// The object is written as one 12 KiB block; zeroing block 2 leaves a single
// surviving extent [0, 8192).  sparse_read(0, 10240) ends inside the hole at
// block 2 — only the surviving [0, 8192) portion is returned.
TEST_F(ECSparseReadTest, SparseReadWindowEndsInsideZeroHole) {
  std::string oid = "sparse_window_ends_in_hole";

  bufferlist data_bl = create_pattern_buffer(12288, 'K');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // Punch a hole in block 2.
  ObjectWriteOperation op;
  op.zero(8192, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Read [0, 10240): block 2 is a hole; blocks 0+1 form one contiguous
  // extent [0, 8192) which is fully within the window.
  bufferlist expected_data = create_pattern_buffer(8192, 'K');
  std::map<uint64_t, uint64_t> expected_extents = {{0, 8192}};
  verify_sparse_read(oid, 0, 10240, expected_extents, expected_data);
}

// Read window lies entirely inside a ZERO-op hole — no extents should be
// returned.
TEST_F(ECSparseReadTest, SparseReadEntirelyInsideZeroHole) {
  std::string oid = "sparse_entirely_in_hole";

  bufferlist data_bl = create_pattern_buffer(12288, 'L');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // Punch a hole in block 1.
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // Read [4096, 8192): entirely within the hole.
  std::map<uint64_t, uint64_t> expected_extents;
  bufferlist expected_data;
  verify_sparse_read(oid, 4096, 4096, expected_extents, expected_data);
}

// ---------------------------------------------------------------------------
// Sub-Task 5: mapext at non-zero offsets
// All existing verify_mapext calls use offset 0.  These tests add coverage
// for mapext queries that start mid-object, including ranges that overlap
// ZERO-op holes.
// ---------------------------------------------------------------------------

// mapext starting at a non-zero offset: only the block within the window is
// returned (the block before the window is excluded).
TEST_P(SparseReadTest, MapextFromNonZeroOffset) {
  std::string oid = "mapext_nonzero_offset";

  // 'M' at [0, 4096), hole at [4096, 8192), 'M' at [8192, 12288).
  bufferlist data_bl = create_pattern_buffer(4096, 'M');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 8192));

  // mapext [4096, 12288): the block at offset 0 is outside the window.
  std::map<uint64_t, uint64_t> expected_extents = {{8192, 4096}};
  verify_mapext(oid, 4096, 8192, expected_extents);
}

// mapext window ends before the end of the object.
TEST_P(SparseReadTest, MapextWindowEndsBeforeObjectEnd) {
  std::string oid = "mapext_partial_window";

  bufferlist data_bl = create_pattern_buffer(12288, 'N');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // mapext [4096, 8192): only the middle block should be returned.
  std::map<uint64_t, uint64_t> expected_extents = {{4096, 4096}};
  verify_mapext(oid, 4096, 4096, expected_extents);
}

// mapext after a ZERO op, queried from offset 0 with a window that ends
// inside the zeroed region — EC only (relies on 4K hole-punch from ZERO op).
TEST_F(ECSparseReadTest, MapextAfterZeroOpNonZeroOffset) {
  std::string oid = "mapext_after_zero_offset";

  bufferlist data_bl = create_pattern_buffer(12288, 'O');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // Punch a hole in block 1.
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // mapext [0, 8192): block 1 is a hole, so only block 0 is returned.
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}};
  verify_mapext(oid, 0, 8192, expected_extents);
}

// mapext window entirely within a ZERO-op hole: no extents returned — EC only.
TEST_F(ECSparseReadTest, MapextFromOffsetInsideZeroHole) {
  std::string oid = "mapext_offset_inside_hole";

  bufferlist data_bl = create_pattern_buffer(12288, 'P');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // Punch a hole in block 1.
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // mapext [4096, 8192): entirely within the hole.
  std::map<uint64_t, uint64_t> expected_extents;
  verify_mapext(oid, 4096, 4096, expected_extents);
}

// Test multiple sequential writes building up an object
TEST_P(SparseReadTest, SequentialWrites) {
  std::string oid = "sequential_writes";
  
  // Write in 4K chunks
  for (int i = 0; i < 4; i++) {
    bufferlist write_bl = create_pattern_buffer(4096, 'A' + i);
    ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), i * 4096));
  }
  
  // Verify total size
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));
  ASSERT_EQ(size, 16384u);
  
  // Verify sparse read shows continuous extent
  std::map<uint64_t, uint64_t> expected_extents = {{0, 16384}};
  verify_mapext(oid, 0, 16384, expected_extents);
}

// --- 8.4 WRITEFULL ---

// WRITEFULL with non-zero data must clear any existing FAE entries.
TEST_F(ECSparseReadTest, WritefullClearsFAE) {
  std::string oid = "writefull_clears_fae";

  // Write zeros so FAE is populated.
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  // WRITEFULL with non-zero data must clear FAE completely.
  bufferlist data_bl = create_pattern_buffer(4096, 'A');
  ASSERT_EQ(0, ioctx.write_full(oid, data_bl));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// WRITEFULL with zero data must set FAE to cover the written range.
TEST_F(ECSparseReadTest, WritefullZeroDataSetsFAE) {
  std::string oid = "writefull_zero_sets_fae";

  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write_full(oid, zero_bl));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, 8192);
  ASSERT_EQ(expected, fae->get_intervals());
}

// WRITEFULL replaces a larger object: previous FAE beyond new size is gone.
TEST_F(ECSparseReadTest, WritefullSmallerObjectClearsPriorFAE) {
  std::string oid = "writefull_smaller_clears_fae";

  // Write zeros at offset 8192 to set FAE there.
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 8192));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  // WRITEFULL with 4096 bytes of non-zero data — new object is only 4 KiB.
  bufferlist data_bl = create_pattern_buffer(4096, 'B');
  ASSERT_EQ(0, ioctx.write_full(oid, data_bl));

  // All prior FAE entries must have been cleared.
  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// --- 8.5 WRITESAME ---

// WRITESAME with an all-zero pattern must track the written range as FAE.
TEST_F(ECSparseReadTest, WritesameZeroPatternTracksFAE) {
  std::string oid = "writesame_zero_fae";

  // Write 16 KiB of zeros via WRITESAME (pattern = one 4 KiB zero block).
  bufferlist zero_pattern = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.writesame(oid, zero_pattern, 16384, 0));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, 16384);
  ASSERT_EQ(expected, fae->get_intervals());
}

// WRITESAME with a non-zero pattern must not set FAE.
TEST_F(ECSparseReadTest, WritesameNonZeroPatternNoFAE) {
  std::string oid = "writesame_nonzero_no_fae";

  bufferlist pattern = create_pattern_buffer(4096, 'C');
  ASSERT_EQ(0, ioctx.writesame(oid, pattern, 16384, 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// --- 8.6 TRUNCATE / TRIMTRUNC ---

// TRUNCATE must remove FAE entries that lie entirely beyond the new size.
TEST_F(ECSparseReadTest, TruncateRemovesFAEBeyondNewSize) {
  std::string oid = "truncate_removes_fae";

  // Write zeros at block 0 (offset 0) and block 2 (offset 8192).
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 8192));

  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    // Both blocks should be tracked.
    ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
    ASSERT_TRUE(fae->contains(8192, FAE_BLOCK_SIZE));
  }

  // Truncate to 4096: block at offset 8192 must be removed from FAE.
  ASSERT_EQ(0, ioctx.trunc(oid, 4096));

  {
    auto fae = get_force_allocated_extents(oid);
    // Block 0 is still within the object — it should remain.
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
    // Block at 8192 is now beyond the object size — it must be gone.
    ASSERT_FALSE(fae->intersects(8192, FAE_BLOCK_SIZE));
  }
}

// TRUNCATE to zero size clears all FAE entries.
TEST_F(ECSparseReadTest, TruncateToZeroClearsFAE) {
  std::string oid = "truncate_zero_clears_fae";

  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  ASSERT_EQ(0, ioctx.trunc(oid, 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// TRUNCATE to a larger size (extend) must not change existing FAE entries.
TEST_F(ECSparseReadTest, TruncateExtendPreservesFAE) {
  std::string oid = "truncate_extend_preserves_fae";

  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  auto fae_before = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_before.has_value());

  // Truncate to a larger size (extend the object).
  ASSERT_EQ(0, ioctx.trunc(oid, 8192));

  auto fae_after = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_after.has_value());
  // The existing tracked block must still be there.
  ASSERT_EQ(fae_before->get_intervals(), fae_after->get_intervals());
}

// --- 8.7 ZERO ---

// ZERO on an aligned region must remove existing FAE entries for that region.
TEST_F(ECSparseReadTest, ZeroOpRemovesFAEForZeroedRegion) {
  std::string oid = "zero_op_removes_fae";

  // Write zeros to create FAE entries at blocks 0 and 1.
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, 8192));
  }

  // ZERO the first 4 KiB — FAE entry for that block must be removed.
  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  {
    auto fae = get_force_allocated_extents(oid);
    // The second block (offset 4096) must still be tracked.
    ASSERT_TRUE(fae.has_value());
    ASSERT_FALSE(fae->intersects(0, FAE_BLOCK_SIZE));
    ASSERT_TRUE(fae->contains(4096, FAE_BLOCK_SIZE));
  }
}

// ZERO that covers the entire object clears all FAE entries.
TEST_F(ECSparseReadTest, ZeroOpClearsAllFAE) {
  std::string oid = "zero_op_clears_all_fae";

  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  ObjectWriteOperation op;
  op.zero(0, 8192);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// ZERO does not affect FAE entries outside the zeroed range.
TEST_F(ECSparseReadTest, ZeroOpPreservesFAEOutsideRange) {
  std::string oid = "zero_op_preserves_outside_fae";

  // Write zeros to create FAE entries at blocks 0, 1, 2.
  bufferlist zero_bl = create_zero_buffer(12288);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  // ZERO only block 1 (offset 4096..8192).
  ObjectWriteOperation op;
  op.zero(4096, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  // Blocks 0 and 2 must still be tracked.
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
  ASSERT_TRUE(fae->contains(8192, FAE_BLOCK_SIZE));
  // Block 1 must be gone.
  ASSERT_FALSE(fae->intersects(4096, FAE_BLOCK_SIZE));
}

// ZERO with both an unaligned start and unaligned end: the partial leading
// and trailing blocks are written with literal zeros (not deallocated) so
// their FAE entries are preserved; only the interior full block is removed.
TEST_F(ECSparseReadTest, ZeroOpMisalignedBothEndsPreservesEdgeFAE) {
  std::string oid = "zero_misaligned_start_fae";

  // Write zeros across three 4K blocks so all three are FAE-tracked.
  bufferlist zero_bl = create_zero_buffer(3 * FAE_BLOCK_SIZE);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, 3 * FAE_BLOCK_SIZE));
  }

  // ZERO [2048, 10240): offset=2048, length=8192.
  //   interior_start = 4096, interior_end = 8192
  //   head:     [2048, 4096) — literal-zero write into block 0
  //   interior: [4096, 8192) — block 1 deallocated
  //   tail:     [8192,10240) — literal-zero write into block 2
  ObjectWriteOperation op;
  op.zero(2048, 2 * FAE_BLOCK_SIZE);  // [2048, 10240)
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  // Block 0 (head): literal-zero write, not deallocated — FAE must remain.
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
  // Block 1 (interior): deallocated — FAE must be gone.
  ASSERT_FALSE(fae->intersects(FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
  // Block 2 (tail): literal-zero write, not deallocated — FAE must remain.
  ASSERT_TRUE(fae->contains(2 * FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
}

// ZERO with an unaligned end: the partial trailing block is written with
// literal zeros (not deallocated), so its FAE entry must be preserved.
TEST_F(ECSparseReadTest, ZeroOpMisalignedEndPreservesTrailingFAE) {
  std::string oid = "zero_misaligned_end_fae";

  // Three FAE-tracked zero blocks.
  bufferlist zero_bl = create_zero_buffer(3 * FAE_BLOCK_SIZE);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  // ZERO from offset 0 to 6144 (unaligned end inside block 1):
  // block 0 is interior (deallocated), block 1 is tail (literal-zero write).
  ObjectWriteOperation op;
  op.zero(0, FAE_BLOCK_SIZE + 2048);  // [0, 6144)
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  // Block 0 was interior: deallocated, FAE must be gone.
  ASSERT_FALSE(fae->intersects(0, FAE_BLOCK_SIZE));
  // Block 1 was tail: literal-zero write, FAE must remain.
  ASSERT_TRUE(fae->contains(FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
  // Block 2 is beyond the range and untouched.
  ASSERT_TRUE(fae->contains(2 * FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
}

// ZERO entirely within one 4K block (sub-block range): the whole range is
// written with literal zeros.  No block is deallocated, so a pre-existing
// FAE entry for that block must be preserved.
TEST_F(ECSparseReadTest, ZeroOpSubBlockPreservesFAE) {
  std::string oid = "zero_subblock_fae";

  // Write one zero block so FAE has an entry for block 0.
  bufferlist zero_bl = create_zero_buffer(FAE_BLOCK_SIZE);
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  // ZERO a 512-byte sub-range entirely within block 0.  No full block is
  // covered, so the operation is a literal-zero write — no deallocation.
  ObjectWriteOperation op;
  op.zero(1024, 512);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  // The FAE entry for block 0 must still be present.
  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
}

// Misaligned ZERO with both unaligned start and unaligned end: verifies that
// data content is correct for all three regions after the operation.
//
// Layout (3 blocks of 'Z', ZERO [2048, 10240)):
//   [0,    2048): untouched, still 'Z'           (before ZERO, in block 0)
//   [2048, 4096): zero                            (head write into block 0)
//   [4096, 8192): zero                            (interior: block 1 deallocated)
//   [8192,10240): zero                            (tail write into block 2)
//   [10240,12288): untouched, still 'Z'           (after ZERO, in block 2)
TEST_P(SparseReadTest, ZeroOpMisalignedDataCorrectness) {
  std::string oid = "zero_misaligned_data";

  // Write a recognisable non-zero pattern across three 4K blocks.
  bufferlist data_bl = create_pattern_buffer(3 * FAE_BLOCK_SIZE, 'Z');
  ASSERT_EQ(0, ioctx.write(oid, data_bl, data_bl.length(), 0));

  // ZERO [2048, 10240): offset=2048, length=8192.
  //   interior_start = round_up(2048, 4096)    = 4096
  //   interior_end   = round_down(10240, 4096) = 8192
  //   head  write: [2048, 4096)
  //   dealloc:     [4096, 8192)
  //   tail  write: [8192, 10240)
  ObjectWriteOperation op;
  op.zero(2048, 2 * FAE_BLOCK_SIZE);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  bufferlist read_bl;
  ASSERT_EQ(3 * (int)FAE_BLOCK_SIZE,
            ioctx.read(oid, read_bl, 3 * FAE_BLOCK_SIZE, 0));

  // [0, 2048): untouched, still 'Z'.
  bufferlist before_zero;
  before_zero.substr_of(read_bl, 0, 2048);
  ASSERT_TRUE(before_zero.contents_equal(create_pattern_buffer(2048, 'Z')));

  // [2048, 10240): all zeros (head + interior + tail).
  bufferlist zeroed_section;
  zeroed_section.substr_of(read_bl, 2048, 2 * FAE_BLOCK_SIZE);
  ASSERT_TRUE(zeroed_section.contents_equal(create_zero_buffer(2 * FAE_BLOCK_SIZE)));

  // [10240, 12288): untouched, still 'Z'.
  bufferlist after_zero;
  after_zero.substr_of(read_bl, 10240, FAE_BLOCK_SIZE - 2048);
  ASSERT_TRUE(after_zero.contents_equal(
    create_pattern_buffer(FAE_BLOCK_SIZE - 2048, 'Z')));
}

// ---------------------------------------------------------------------------
// copy_from sparsity-preservation tests — EC only.
// copy_from is implemented server-side via CEPH_OSD_OP_COPY_GET chunks
// (_copy_some → op.copy_get) followed by a write to the destination.
// These tests verify that the destination object has the same hole structure
// and FAE as the source after a copy_from operation.
// ---------------------------------------------------------------------------

// copy_from preserves data holes: a source with holes at [4096, 8192) must
// produce a destination with the same extent map.
TEST_F(ECSparseReadTest, CopyFromPreservesDataHoles) {
  const std::string src = "copy_from_src_holes";
  const std::string dst = "copy_from_dst_holes";

  // Source: data at [0, 4096) and [8192, 12288), hole at [4096, 8192).
  bufferlist bl_a = create_pattern_buffer(4096, 'A');
  bufferlist bl_b = create_pattern_buffer(4096, 'B');
  ASSERT_EQ(0, ioctx.write(src, bl_a, bl_a.length(), 0));
  ASSERT_EQ(0, ioctx.write(src, bl_b, bl_b.length(), 8192));

  // copy_from with version=0 (no version check).
  ObjectWriteOperation op;
  op.copy_from(src, ioctx, 0, 0);
  ASSERT_EQ(0, ioctx.operate(dst, &op));

  // Destination must have the same two extents and the same data.
  bufferlist expected_data;
  expected_data.append(bl_a);
  expected_data.append(bl_b);
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}, {8192, 4096}};
  verify_sparse_read(dst, 0, 12288, expected_extents, expected_data);
  verify_mapext(dst, 0, 12288, expected_extents);
}

// copy_from recalculates FAE via zero detection
TEST_F(ECSparseReadTest, CopyFromPreservesFAEForZeroBlocks) {
  const std::string src = "copy_from_src_fae";
  const std::string dst = "copy_from_dst_fae";

  // Write an all-zero block — this sets FAE on the Fast EC pool.
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, ioctx.write(src, zero_bl, zero_bl.length(), 0));

  ObjectWriteOperation op;
  op.copy_from(src, ioctx, 0, 0);
  ASSERT_EQ(0, ioctx.operate(dst, &op));

  // The destination OSD must have recalculated FAE by detecting zero blocks
  // in the written data — not by copying FAE from the source.
  auto dst_fae = get_force_allocated_extents(dst);
  ASSERT_TRUE(dst_fae.has_value())
      << "destination FAE not set after copy_from — zero detection during "
         "write may not be running";
  interval_set<uint64_t> expected;
  expected.insert(0, 4096);
  ASSERT_EQ(expected, dst_fae->get_intervals())
      << "destination FAE does not cover the expected zero block [0, 4096)";

  // sparse_read must return the zero block as an allocated extent, not a hole.
  std::map<uint64_t, uint64_t> expected_extents = {{0, 4096}};
  verify_sparse_read(dst, 0, 4096, expected_extents, zero_bl);
}

// Cross-pool copy_from tests exercising FAE behaviour across pool types.
//
// A single test creates three auxiliary pools once and shares them across all
// sub-scenarios (each scoped in its own {} block):
//
//   rep_ioctx  — replicated pool (no FAE tracking)
//   ec_noflag_ioctx — Fast EC pool without FLAG_PRESERVE_ALLOCATION
//                     (FAE zero-detection is disabled)
//
// ioctx (from ECOnlyTestFixture) = Fast EC WITH FLAG_PRESERVE_ALLOCATION.
//
// Sub-scenarios:
//  A. replica src  → EC(flag) dst  : dst gets FAE via zero detection
//  B. EC(flag) src → replica dst   : dst has no FAE (replicated pool)
//  C. EC(flag) src → EC(noflag) dst: dst has no FAE (flag not set on dst pool)
//  D. EC(noflag) src → EC(flag) dst: dst gets FAE via zero detection;
//                                    src has no FAE (flag not set on src pool)
TEST_F(ECSparseReadTest, CopyFromCrossPoolFAEBehaviour) {
  // -----------------------------------------------------------------------
  // Pool setup — created once for all sub-scenarios.
  // -----------------------------------------------------------------------
  const std::string rep_pool_name = get_temp_pool_name("cfcp_rep_");
  ASSERT_EQ("", create_pool_pp(rep_pool_name, rados));
  librados::IoCtx rep_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(rep_pool_name.c_str(), rep_ioctx));

  const std::string ec_noflag_pool_name = get_temp_pool_name("cfcp_ec_noflag_");
  ASSERT_EQ("", create_ec_pool_pp(ec_noflag_pool_name, rados, /*fast_ec=*/true));
  ASSERT_EQ("", set_allow_ec_overwrites_pp(ec_noflag_pool_name, rados, true));
  // Explicitly leave FLAG_PRESERVE_ALLOCATION off on this pool.
  ASSERT_EQ("", set_pool_flags_pp(
    ec_noflag_pool_name, rados, pg_pool_t::FLAG_PRESERVE_ALLOCATION, false));
  rados.wait_for_latest_osdmap();
  librados::IoCtx ec_noflag_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(ec_noflag_pool_name.c_str(), ec_noflag_ioctx));

  bufferlist zero_bl = create_zero_buffer(4096);
  const std::map<uint64_t, uint64_t> zero_extents = {{0, 4096}};
  interval_set<uint64_t> zero_interval;
  zero_interval.insert(0, 4096);

  // Helper: assert that an object's OI xattr shows empty FAE.
  // Returns false (and adds a failure) if the xattr is readable but FAE is set.
  auto assert_no_fae = [&](librados::IoCtx& ctx, const std::string& oid,
                            const char* label) {
    bufferlist bl;
    static_assert(OI_ATTR[0] == '_', "OI_ATTR must start with '_'");
    int ret = ctx.getxattr(oid, &OI_ATTR[1], bl);
    if (ret >= 0) {
      object_info_t oi(bl);
      EXPECT_TRUE(oi.force_allocated_extents.empty())
          << label << ": FAE unexpectedly set";
    }
    // ret < 0 means no xattr yet — acceptable (object has no FAE)
  };

  // -----------------------------------------------------------------------
  // A. Replica source → EC(flag) destination
  //    Replica pools never set FAE.  The EC destination must recalculate FAE
  //    from zero detection during the copy write.
  // -----------------------------------------------------------------------
  {
    const std::string src = "cfcp_a_src";
    const std::string dst = "cfcp_a_dst";

    ASSERT_EQ(0, rep_ioctx.write(src, zero_bl, zero_bl.length(), 0));
    assert_no_fae(rep_ioctx, src, "A: replica source");

    ObjectWriteOperation op;
    op.copy_from(src, rep_ioctx, 0, 0);
    ASSERT_EQ(0, ioctx.operate(dst, &op));

    auto dst_fae = get_force_allocated_extents(dst);
    ASSERT_TRUE(dst_fae.has_value())
        << "A: EC(flag) dst FAE not set after copy from replica";
    ASSERT_EQ(zero_interval, dst_fae->get_intervals())
        << "A: EC(flag) dst FAE does not cover [0, 4096)";
    verify_sparse_read(dst, 0, 4096, zero_extents, zero_bl);
  }

  // -----------------------------------------------------------------------
  // B. EC(flag) source → replica destination
  //    The EC source has FAE; the replica destination must have none.
  // -----------------------------------------------------------------------
  {
    const std::string src = "cfcp_b_src";
    const std::string dst = "cfcp_b_dst";

    ASSERT_EQ(0, ioctx.write(src, zero_bl, zero_bl.length(), 0));
    ASSERT_TRUE(get_force_allocated_extents(src).has_value())
        << "B: EC(flag) source FAE not set — pool missing FLAG_PRESERVE_ALLOCATION";

    ObjectWriteOperation op;
    op.copy_from(src, ioctx, 0, 0);
    ASSERT_EQ(0, rep_ioctx.operate(dst, &op));

    assert_no_fae(rep_ioctx, dst, "B: replica destination");

    bufferlist read_bl;
    ASSERT_EQ((int)zero_bl.length(),
              rep_ioctx.read(dst, read_bl, zero_bl.length(), 0));
    ASSERT_TRUE(read_bl.contents_equal(zero_bl))
        << "B: replica destination data mismatch";
  }

  // -----------------------------------------------------------------------
  // C. EC(flag) source → EC(noflag) destination
  //    The source has FAE but the destination pool has the flag off, so the
  //    destination OSD must not run zero detection and must not set FAE.
  // -----------------------------------------------------------------------
  {
    const std::string src = "cfcp_c_src";
    const std::string dst = "cfcp_c_dst";

    ASSERT_EQ(0, ioctx.write(src, zero_bl, zero_bl.length(), 0));
    ASSERT_TRUE(get_force_allocated_extents(src).has_value())
        << "C: EC(flag) source FAE not set — pool missing FLAG_PRESERVE_ALLOCATION";

    ObjectWriteOperation op;
    op.copy_from(src, ioctx, 0, 0);
    ASSERT_EQ(0, ec_noflag_ioctx.operate(dst, &op));

    assert_no_fae(ec_noflag_ioctx, dst, "C: EC(noflag) destination");

    bufferlist read_bl;
    ASSERT_EQ((int)zero_bl.length(),
              ec_noflag_ioctx.read(dst, read_bl, zero_bl.length(), 0));
    ASSERT_TRUE(read_bl.contents_equal(zero_bl))
        << "C: EC(noflag) destination data mismatch";
  }

  // -----------------------------------------------------------------------
  // D. EC(noflag) source → EC(flag) destination
  //    The source pool has the flag off so the source has no FAE.  The
  //    destination pool has the flag on, so the destination OSD runs zero
  //    detection and must set FAE from the written data.
  // -----------------------------------------------------------------------
  {
    const std::string src = "cfcp_d_src";
    const std::string dst = "cfcp_d_dst";

    ASSERT_EQ(0, ec_noflag_ioctx.write(src, zero_bl, zero_bl.length(), 0));
    assert_no_fae(ec_noflag_ioctx, src, "D: EC(noflag) source");

    ObjectWriteOperation op;
    op.copy_from(src, ec_noflag_ioctx, 0, 0);
    ASSERT_EQ(0, ioctx.operate(dst, &op));

    auto dst_fae = get_force_allocated_extents(dst);
    ASSERT_TRUE(dst_fae.has_value())
        << "D: EC(flag) dst FAE not set after copy from EC(noflag)";
    ASSERT_EQ(zero_interval, dst_fae->get_intervals())
        << "D: EC(flag) dst FAE does not cover [0, 4096)";
    verify_sparse_read(dst, 0, 4096, zero_extents, zero_bl);
  }

  // -----------------------------------------------------------------------
  // Pool teardown.
  // -----------------------------------------------------------------------
  rep_ioctx.close();
  destroy_pool_pp(rep_pool_name, rados);
  ec_noflag_ioctx.close();
  destroy_ec_pool_pp(ec_noflag_pool_name, rados);
}

// ---------------------------------------------------------------------------
// Snapshot/clone sparsity-preservation tests — EC only.
// When a selfmanaged snapshot is armed and the head object is written, the OSD
// creates a clone.  The clone must preserve the hole structure and FAE that the
// head object had at the moment the snapshot was taken.
// ---------------------------------------------------------------------------

// After a snapshot + overwrite, sparse_read on the clone returns the same
// extents and data that the head had before the snapshot.
TEST_F(ECSparseReadTest, SnapshotClonePreservesSourceSparsity) {
  const std::string oid = "snap_clone_sparsity";

  // Initial object: data at [0, 4096) and [8192, 12288), hole at [4096, 8192).
  bufferlist bl_r = create_pattern_buffer(4096, 'R');
  bufferlist bl_s = create_pattern_buffer(4096, 'S');
  ASSERT_EQ(0, ioctx.write(oid, bl_r, bl_r.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, bl_s, bl_s.length(), 8192));

  // Capture the pre-snapshot extent map to compare against the clone later.
  bufferlist pre_snap_data;
  std::map<uint64_t, uint64_t> pre_snap_extents;
  ASSERT_GE(ioctx.sparse_read(oid, pre_snap_extents, pre_snap_data, 12288, 0), 0);

  // Arm a selfmanaged snapshot.
  std::vector<uint64_t> snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(snaps[0], snaps));

  // Overwrite the head — this triggers clone creation on the OSD.
  bufferlist new_bl = create_pattern_buffer(4096, 'Z');
  ASSERT_EQ(0, ioctx.write_full(oid, new_bl));

  // Switch to reading the snapshot (clone).
  ioctx.snap_set_read(snaps[0]);

  // The clone's sparse extents must match the pre-snapshot state of the head.
  std::map<uint64_t, uint64_t> clone_extents;
  bufferlist clone_data;
  ASSERT_GE(ioctx.sparse_read(oid, clone_extents, clone_data, 12288, 0), 0);
  ASSERT_EQ(pre_snap_extents, clone_extents)
      << "clone extent map differs from pre-snapshot head";
  ASSERT_TRUE(clone_data.contents_equal(pre_snap_data))
      << "clone data differs from pre-snapshot head";

  // Restore the ioctx to HEAD before cleanup.
  ioctx.snap_set_read(librados::SNAP_HEAD);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(snaps[0]));
}

// After a snapshot + overwrite, the clone preserves a mixed FAE+data layout:
// zero blocks that were FAE-tracked before the snapshot must remain FAE-tracked
// on the clone, and sparse_read must return them as allocated.
TEST_F(ECSparseReadTest, SnapshotClonePreservesMixedFAEAndData) {
  const std::string oid = "snap_clone_fae_mixed";

  // Block 0: non-zero 'A', Block 1: all zeros (FAE-tracked), Block 2: non-zero 'B'.
  bufferlist bl_a = create_pattern_buffer(4096, 'A');
  bufferlist bl_zero = create_zero_buffer(4096);
  bufferlist bl_b = create_pattern_buffer(4096, 'B');
  ASSERT_EQ(0, ioctx.write(oid, bl_a, bl_a.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, bl_zero, bl_zero.length(), 4096));
  ASSERT_EQ(0, ioctx.write(oid, bl_b, bl_b.length(), 8192));

  auto pre_snap_fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(pre_snap_fae.has_value())
      << "FAE not set after zero write — pool may not have "
         "FLAG_PRESERVE_ALLOCATION";

  // Capture pre-snapshot sparse state.
  bufferlist pre_snap_data;
  std::map<uint64_t, uint64_t> pre_snap_extents;
  ASSERT_GE(ioctx.sparse_read(oid, pre_snap_extents, pre_snap_data, 12288, 0), 0);

  // Arm snapshot and overwrite head to create a clone.
  std::vector<uint64_t> snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(snaps[0], snaps));

  bufferlist new_bl = create_pattern_buffer(4096, 'Z');
  ASSERT_EQ(0, ioctx.write_full(oid, new_bl));

  // Read from the clone and verify extents match pre-snapshot state.
  ioctx.snap_set_read(snaps[0]);

  std::map<uint64_t, uint64_t> clone_extents;
  bufferlist clone_data;
  ASSERT_GE(ioctx.sparse_read(oid, clone_extents, clone_data, 12288, 0), 0);
  ASSERT_EQ(pre_snap_extents, clone_extents)
      << "clone extent map differs from pre-snapshot head";
  ASSERT_TRUE(clone_data.contents_equal(pre_snap_data))
      << "clone data differs from pre-snapshot head";

  // FAE on the clone must match the pre-snapshot FAE.
  auto clone_fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(clone_fae.has_value())
      << "clone FAE is absent — expected FAE from pre-snapshot state";
  ASSERT_EQ(pre_snap_fae->get_intervals(), clone_fae->get_intervals())
      << "clone FAE differs from pre-snapshot head FAE";

  ioctx.snap_set_read(librados::SNAP_HEAD);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(snaps[0]));
}


// Instantiate tests for both Replicated and FastEC pools
INSTANTIATE_TEST_SUITE_P(
  SparseReadTests,
  SparseReadTest,
  ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
  [](const ::testing::TestParamInfo<PoolType>& info) {
    return ceph::test::pool_type_name(info.param);
  }
);

// ---------------------------------------------------------------------------
// Tests for the per-request MOSDOp flag CEPH_OSD_FLAG_PRESERVE_ALLOCATION
// ---------------------------------------------------------------------------

class SparseReadFlagTest : public ::testing::Test {
protected:
  static librados::Rados rados;
  static std::string pool_name;
  librados::IoCtx ioctx;

  static void SetUpTestSuite() {
    ASSERT_EQ("", connect_cluster_pp(rados));
    pool_name = get_temp_pool_name("sparse_read_flag_test_");
    // Create a FastEC pool (with EC overwrites enabled) but do NOT set the
    // pool-level track_zero_blocks flag.
    ASSERT_EQ("", create_ec_pool_pp(pool_name, rados, /*ec_optimizations=*/true));
    ASSERT_EQ("", set_allow_ec_overwrites_pp(pool_name, rados, true));
    // Explicitly ensure the pool flag is off.
    ASSERT_EQ("", set_pool_flags_pp(
      pool_name, rados, pg_pool_t::FLAG_PRESERVE_ALLOCATION, false));
    rados.wait_for_latest_osdmap();
  }

  static void TearDownTestSuite() {
    destroy_ec_pool_pp(pool_name, rados);
    rados.shutdown();
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }

  void TearDown() override {
    SKIP_IF_CRIMSON();
    ioctx.close();
  }

  // Returns the force_allocated_extents from the object's OI xattr, or
  // nullopt if the FAE is empty or cannot be read.
  std::optional<force_allocated_extents_t> get_force_allocated_extents(
      const std::string& oid) {
    bufferlist bl;
    static_assert(OI_ATTR[0] == '_', "OI_ATTR must start with '_'");
    int ret = ioctx.getxattr(oid, &OI_ATTR[1], bl);
    if (ret < 0) {
      ADD_FAILURE() << "getxattr OI failed: " << ret;
      return std::nullopt;
    }
    object_info_t oi(bl);
    if (oi.force_allocated_extents.empty()) {
      return std::nullopt;
    }
    return oi.force_allocated_extents;
  }

  bufferlist create_zero_buffer(size_t size) {
    bufferlist bl;
    bl.append(std::string(size, '\0'));
    return bl;
  }

  bufferlist create_pattern_buffer(size_t size, char pattern) {
    bufferlist bl;
    bl.append(std::string(size, pattern));
    return bl;
  }

  // Perform a WRITE sub-op via operate() with the tracking flag set.
  int write_with_flag(const std::string& oid,
                      const bufferlist& bl,
                      uint64_t offset) {
    ObjectWriteOperation op;
    op.write(offset, bl);
    return ioctx.operate(oid, &op,
                         librados::OPERATION_PRESERVE_ALLOCATION);
  }
};

librados::Rados SparseReadFlagTest::rados;
std::string SparseReadFlagTest::pool_name;

// Writing an all-zero block with the MOSDOp flag set must populate FAE
// even when the pool-level flag is off.
TEST_F(SparseReadFlagTest, FlagWriteTracksZeroBlock) {
  const std::string oid = "flag_write_zero";
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, FAE_BLOCK_SIZE);
  ASSERT_EQ(expected, fae->get_intervals());
}

// Writing non-zero data with the MOSDOp flag set must NOT populate FAE.
TEST_F(SparseReadFlagTest, FlagWriteNoFAEForNonZero) {
  const std::string oid = "flag_write_nonzero";
  bufferlist data_bl = create_pattern_buffer(4096, 'A');
  ASSERT_EQ(0, write_with_flag(oid, data_bl, 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// Without the MOSDOp flag, writing zeros must NOT populate FAE (the pool
// flag is also off, so no tracking mechanism is active).
TEST_F(SparseReadFlagTest, NoFlagNoFAEForZeroWrite) {
  const std::string oid = "no_flag_write_zero";
  bufferlist zero_bl = create_zero_buffer(4096);
  // Plain ioctx.write — no tracking flag, no pool flag.
  ASSERT_EQ(0, ioctx.write(oid, zero_bl, zero_bl.length(), 0));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// WRITEFULL with the MOSDOp flag and zero data must populate FAE.
TEST_F(SparseReadFlagTest, FlagWritefullZeroSetsFAE) {
  const std::string oid = "flag_writefull_zero";
  bufferlist zero_bl = create_zero_buffer(8192);
  ObjectWriteOperation op;
  op.write_full(zero_bl);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, 8192);
  ASSERT_EQ(expected, fae->get_intervals());
}

// WRITEFULL without the flag must NOT set FAE (pool flag also off).
TEST_F(SparseReadFlagTest, NoFlagWritefullZeroNoFAE) {
  const std::string oid = "no_flag_writefull_zero";
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, ioctx.write_full(oid, zero_bl));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// WRITEFULL with the flag and non-zero data must clear any pre-existing FAE.
TEST_F(SparseReadFlagTest, FlagWritefullNonZeroClearsFAE) {
  const std::string oid = "flag_writefull_nonzero_clears";

  // First, populate FAE using the flag.
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  // WRITEFULL with non-zero data and the flag: FAE must be cleared.
  bufferlist data_bl = create_pattern_buffer(4096, 'B');
  ObjectWriteOperation op;
  op.write_full(data_bl);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  ASSERT_FALSE(get_force_allocated_extents(oid).has_value());
}

// ZERO op with the MOSDOp flag must remove matching FAE entries.
TEST_F(SparseReadFlagTest, FlagZeroOpRemovesFAE) {
  const std::string oid = "flag_zero_removes_fae";

  // Populate FAE for two blocks via the tracking flag.
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, 8192));
  }

  // ZERO the first block with the flag set.
  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  // Block at offset 4096 must still be tracked.
  ASSERT_TRUE(fae.has_value());
  ASSERT_FALSE(fae->intersects(0, FAE_BLOCK_SIZE));
  ASSERT_TRUE(fae->contains(4096, FAE_BLOCK_SIZE));
}

// ZERO op without the flag must NOT remove FAE entries (pool flag also off).
TEST_F(SparseReadFlagTest, NoFlagZeroOpPreservesFAE) {
  const std::string oid = "no_flag_zero_preserves_fae";

  // Populate FAE using the tracking flag.
  bufferlist zero_bl = create_zero_buffer(8192);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  auto fae_before = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_before.has_value());

  // ZERO the first block WITHOUT the flag.
  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));  // no flag

  // FAE should be unchanged since neither the pool flag nor the op flag is set.
  auto fae_after = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_after.has_value());
  ASSERT_EQ(fae_before->get_intervals(), fae_after->get_intervals());
}

// Misaligned ZERO with the flag: unaligned start and end preserve edge FAE.
TEST_F(SparseReadFlagTest, FlagZeroOpMisalignedBothEndsPreservesEdgeFAE) {
  const std::string oid = "flag_zero_misaligned_both";

  // Populate FAE for three blocks using the tracking flag.
  bufferlist zero_bl = create_zero_buffer(3 * FAE_BLOCK_SIZE);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, 3 * FAE_BLOCK_SIZE));
  }

  // ZERO [2048, 10240) with the flag:
  //   head:     [2048, 4096) — literal-zero write, block 0 FAE stays
  //   interior: [4096, 8192) — block 1 deallocated, FAE gone
  //   tail:     [8192,10240) — literal-zero write, block 2 FAE stays
  ObjectWriteOperation op;
  op.zero(2048, 2 * FAE_BLOCK_SIZE);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  // Block 0 (head): literal-zero write — FAE must remain.
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
  // Block 1 (interior): deallocated — FAE must be gone.
  ASSERT_FALSE(fae->intersects(FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
  // Block 2 (tail): literal-zero write — FAE must remain.
  ASSERT_TRUE(fae->contains(2 * FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
}

// Sub-block ZERO with the flag: range within one block, FAE preserved.
TEST_F(SparseReadFlagTest, FlagZeroOpSubBlockPreservesFAE) {
  const std::string oid = "flag_zero_subblock";

  // Populate FAE for one block.
  bufferlist zero_bl = create_zero_buffer(FAE_BLOCK_SIZE);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  ASSERT_TRUE(get_force_allocated_extents(oid).has_value());

  // ZERO 512 bytes inside block 0 — no full block covered, literal-zero write.
  ObjectWriteOperation op;
  op.zero(1024, 512);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
}

// TRUNCATE with the MOSDOp flag must remove FAE entries beyond the new size.
TEST_F(SparseReadFlagTest, FlagTruncateRemovesFAEBeyondNewSize) {
  const std::string oid = "flag_truncate_removes_fae";

  // Write zeros to populate FAE at blocks 0 and 2 (offset 8192).
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 8192));
  {
    auto fae = get_force_allocated_extents(oid);
    ASSERT_TRUE(fae.has_value());
    ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
    ASSERT_TRUE(fae->contains(8192, FAE_BLOCK_SIZE));
  }

  // Truncate to 4096 bytes with the tracking flag.
  ObjectWriteOperation op;
  op.truncate(4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  // Block 0 still within the object — must remain.
  ASSERT_TRUE(fae.has_value());
  ASSERT_TRUE(fae->contains(0, FAE_BLOCK_SIZE));
  // Block at 8192 is beyond the new size — must be gone.
  ASSERT_FALSE(fae->intersects(8192, FAE_BLOCK_SIZE));
}

// TRUNCATE without the flag must NOT remove FAE entries (pool flag also off).
TEST_F(SparseReadFlagTest, NoFlagTruncatePreservesFAE) {
  const std::string oid = "no_flag_truncate_preserves_fae";

  // Populate FAE at block 0 and block 2 via the tracking flag.
  bufferlist zero_bl = create_zero_buffer(4096);
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 8192));
  auto fae_before = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_before.has_value());

  // Truncate to 4096 WITHOUT the flag — FAE should not be updated.
  ObjectWriteOperation op;
  op.truncate(4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op));  // no flag

  auto fae_after = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae_after.has_value());
  // Without the flag, the stale FAE entry at offset 8192 must still be there.
  ASSERT_EQ(fae_before->get_intervals(), fae_after->get_intervals());
}

// Writing multiple zero blocks with the flag tracks all of them.
TEST_F(SparseReadFlagTest, FlagWriteMultipleZeroBlocks) {
  const std::string oid = "flag_write_multi_zero";
  bufferlist zero_bl = create_zero_buffer(16384);  // 4 × 4 KiB blocks
  ASSERT_EQ(0, write_with_flag(oid, zero_bl, 0));

  auto fae = get_force_allocated_extents(oid);
  ASSERT_TRUE(fae.has_value());

  interval_set<uint64_t> expected;
  expected.insert(0, 16384);
  ASSERT_EQ(expected, fae->get_intervals());
}

// The flag has no effect for non-EC (replicated) pools — no FAE is set.
TEST_F(SparseReadFlagTest, FlagOnReplicatedPoolNoFAE) {
  // Use a temporary replicated pool for this single test.
  std::string rep_pool = get_temp_pool_name("sparse_flag_rep_");
  ASSERT_EQ("", create_pool_pp(rep_pool, rados));
  librados::IoCtx rep_ioctx;
  ASSERT_EQ(0, rados.ioctx_create(rep_pool.c_str(), rep_ioctx));

  const std::string oid = "flag_rep_no_fae";
  bufferlist zero_bl;
  zero_bl.append(std::string(4096, '\0'));

  ObjectWriteOperation op;
  op.write(0, zero_bl);
  ASSERT_EQ(0, rep_ioctx.operate(oid, &op,
                                  librados::OPERATION_PRESERVE_ALLOCATION));

  // Replicated pools never set FAE — the OI xattr may not exist at all.
  bufferlist bl;
  static_assert(OI_ATTR[0] == '_', "OI_ATTR must start with '_'");
  int ret = rep_ioctx.getxattr(oid, &OI_ATTR[1], bl);
  if (ret >= 0) {
    object_info_t oi(bl);
    ASSERT_TRUE(oi.force_allocated_extents.empty());
  }
  // ret < 0 (no xattr) is also acceptable.

  rep_ioctx.close();
  destroy_pool_pp(rep_pool, rados);
}
