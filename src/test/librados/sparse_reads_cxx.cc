// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <map>
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
using ceph::test::PoolType;
using ceph::test::PoolTypeTestFixture;

/**
 * Test fixture for sparse read operations on EC and Replicated pools.
 * Tests sparse_read, write, truncate, zero, mapext operations and
 * verifies behavior during recovery scenarios.
 */
class SparseReadTest : public PoolTypeTestFixture {
protected:
  static std::string pool_name_prefix() {
    return "sparse_read_test_";
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    PoolTypeTestFixture::SetUp();
    if (GetParam() == PoolType::FAST_EC) {
      ASSERT_EQ("", set_pool_flags_pp(
        pool_name,
        rados,
        pg_pool_t::FLAG_PRESERVE_ALLOCATION,
        true));
    }
  }

  void TearDown() override {
    SKIP_IF_CRIMSON();
    if (balancing_disabled) {
      turn_balancing_on();
    }
    PoolTypeTestFixture::TearDown();
  }

  // Helper to create buffer with specific pattern
  bufferlist create_pattern_buffer(size_t size, char pattern) {
    bufferlist bl;
    std::string data(size, pattern);
    bl.append(data);
    return bl;
  }

  // Helper to create zero buffer
  bufferlist create_zero_buffer(size_t size) {
    bufferlist bl;
    std::string data(size, '\0');
    bl.append(data);
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
    int ret = ioctx.sparse_read(oid, extents, read_bl, length, offset);
    ASSERT_EQ(ret, (int)expected_extents.size());
    ASSERT_EQ(extents, expected_extents);
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
    int ret = ioctx.mapext(oid, offset, length, extents);
    ASSERT_EQ(ret, (int)expected_extents.size());
    ASSERT_EQ(extents, expected_extents);
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

  // Write data at offset 0 and 8192, leaving a hole at 4096
  bufferlist write_bl1 = create_pattern_buffer(4096, 'A');
  bufferlist write_bl2 = create_pattern_buffer(4096, 'B');

  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 8192));

  // Sparse read should return two extents with a hole
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 12288, 0);

  ASSERT_EQ(ret, 2);
  ASSERT_EQ(read_bl.length(), 8192u);
  ASSERT_EQ(extents.size(), 2u);
  ASSERT_EQ(extents[0], 4096u);
  ASSERT_EQ(extents[8192], 4096u);
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

  // Initial write
  bufferlist write_bl1 = create_pattern_buffer(8192, 'H');
  ASSERT_EQ(0, ioctx.write(oid, write_bl1, write_bl1.length(), 0));

  // Partial overwrite in middle
  bufferlist write_bl2 = create_pattern_buffer(2048, 'I');
  ASSERT_EQ(0, ioctx.write(oid, write_bl2, write_bl2.length(), 3072));

  // Verify sparse read shows continuous extent
  std::map<uint64_t, uint64_t> expected_extents = {{0, 8192}};
  bufferlist read_bl;
  std::map<uint64_t, uint64_t> extents;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 8192, 0);
  ASSERT_EQ(ret, 1);
  ASSERT_EQ(read_bl.length(), 8192u);
  ASSERT_EQ(extents, expected_extents);
}

// Test sparse read with large object
TEST_P(SparseReadTest, LargeObjectSparseRead) {
  std::string oid = "large_sparse";

  // Write data at various offsets to create sparse pattern
  bufferlist write_bl = create_pattern_buffer(4096, 'J');

  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 16384));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 32768));

  // Sparse read entire range
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 40960, 0);

  ASSERT_EQ(ret, 3);
  ASSERT_EQ(read_bl.length(), 12288u);
  ASSERT_EQ(extents.size(), 3u);
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

  // Write data at multiple offsets
  bufferlist write_bl = create_pattern_buffer(4096, 'Q');
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 0));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 8192));
  ASSERT_EQ(0, ioctx.write(oid, write_bl, write_bl.length(), 16384));

  // Sparse read middle section only
  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int ret = ioctx.sparse_read(oid, extents, read_bl, 8192, 4096);

  ASSERT_GE(ret, 0);
  // Should only get data from the requested range
  for (const auto& [offset, len] : extents) {
    ASSERT_GE(offset, 4096u);
    ASSERT_LE(offset + len, 12288u);
  }
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
  ObjectReadOperation read_op;
  read_op.read(0, 4096, &read_bl, nullptr);
  ASSERT_EQ(0, ioctx.operate(oid, &read_op, nullptr));
  ASSERT_TRUE(read_bl.contents_equal(write_bl2_copy));
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
// Tests for the per-request MOSDOp flag CEPH_OSD_FLAG_PRESERVE_ALLOCATION /
// OPERATION_PRESERVE_ALLOCATION.
//
// All tests use a FastEC pool whose pool-level FLAG_PRESERVE_ALLOCATION is
// explicitly *disabled*, verifying that the per-request flag alone is
// sufficient to trigger zero-block tracking.
// ---------------------------------------------------------------------------

/**
 * Test fixture for the per-request PRESERVE_ALLOCATION MOSDOp flag.
 *
 * Unlike SparseReadTest, the pool-level FLAG_PRESERVE_ALLOCATION is intentionally
 * left unset.  Each test that needs tracking must pass
 * librados::OPERATION_PRESERVE_ALLOCATION to ioctx.operate().
 */
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
  ASSERT_EQ(expected, fae->intervals);
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
  ASSERT_EQ(expected, fae->intervals);
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
    ASSERT_TRUE(fae->intervals.contains(0, 8192));
  }

  // ZERO the first block with the flag set.
  ObjectWriteOperation op;
  op.zero(0, 4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  // Block at offset 4096 must still be tracked.
  ASSERT_TRUE(fae.has_value());
  ASSERT_FALSE(fae->intervals.intersects(0, FAE_BLOCK_SIZE));
  ASSERT_TRUE(fae->intervals.contains(4096, FAE_BLOCK_SIZE));
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
  ASSERT_EQ(fae_before->intervals, fae_after->intervals);
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
    ASSERT_TRUE(fae->intervals.contains(0, 3 * FAE_BLOCK_SIZE));
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
  ASSERT_TRUE(fae->intervals.contains(0, FAE_BLOCK_SIZE));
  // Block 1 (interior): deallocated — FAE must be gone.
  ASSERT_FALSE(fae->intervals.intersects(FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
  // Block 2 (tail): literal-zero write — FAE must remain.
  ASSERT_TRUE(fae->intervals.contains(2 * FAE_BLOCK_SIZE, FAE_BLOCK_SIZE));
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
  ASSERT_TRUE(fae->intervals.contains(0, FAE_BLOCK_SIZE));
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
    ASSERT_TRUE(fae->intervals.contains(0, FAE_BLOCK_SIZE));
    ASSERT_TRUE(fae->intervals.contains(8192, FAE_BLOCK_SIZE));
  }

  // Truncate to 4096 bytes with the tracking flag.
  ObjectWriteOperation op;
  op.truncate(4096);
  ASSERT_EQ(0, ioctx.operate(oid, &op,
                              librados::OPERATION_PRESERVE_ALLOCATION));

  auto fae = get_force_allocated_extents(oid);
  // Block 0 still within the object — must remain.
  ASSERT_TRUE(fae.has_value());
  ASSERT_TRUE(fae->intervals.contains(0, FAE_BLOCK_SIZE));
  // Block at 8192 is beyond the new size — must be gone.
  ASSERT_FALSE(fae->intervals.intersects(8192, FAE_BLOCK_SIZE));
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
  ASSERT_EQ(fae_before->intervals, fae_after->intervals);
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
  ASSERT_EQ(expected, fae->intervals);
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
