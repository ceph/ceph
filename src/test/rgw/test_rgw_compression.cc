// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "rgw/rgw_compression.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"


struct MockGetDataCB : public RGWGetDataCB {
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    return 0;
  }
} cb;

using range_t = std::pair<off_t, off_t>;

// call filter->fixup_range() and return the range as a pair. this makes it easy
// to fit on a single line for ASSERT_EQ()
range_t fixup_range(RGWGetObj_Decompress *filter, off_t ofs, off_t end)
{
  filter->fixup_range(ofs, end);
  return {ofs, end};
}


TEST(Decompress, FixupRangePartial)
{
  RGWCompressionInfo cs_info;

  // array of blocks with original len=8, compressed to len=6
  auto& blocks = cs_info.blocks;
  blocks.emplace_back(compression_block{0, 0, 6});
  blocks.emplace_back(compression_block{8, 6, 6});
  blocks.emplace_back(compression_block{16, 12, 6});
  blocks.emplace_back(compression_block{24, 18, 6});

  const bool partial = true;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, partial, &cb);

  // test translation from logical ranges to compressed ranges
  ASSERT_EQ(range_t(0, 6), fixup_range(&decompress, 0, 1));
  ASSERT_EQ(range_t(0, 6), fixup_range(&decompress, 1, 7));
  ASSERT_EQ(range_t(0, 6), fixup_range(&decompress, 7, 8));
  ASSERT_EQ(range_t(0, 12), fixup_range(&decompress, 0, 9));
  ASSERT_EQ(range_t(0, 12), fixup_range(&decompress, 7, 9));
  ASSERT_EQ(range_t(6, 12), fixup_range(&decompress, 8, 9));
  ASSERT_EQ(range_t(6, 12), fixup_range(&decompress, 8, 16));
  ASSERT_EQ(range_t(6, 18), fixup_range(&decompress, 8, 17));
  ASSERT_EQ(range_t(12, 18), fixup_range(&decompress, 16, 24));
  ASSERT_EQ(range_t(12, 24), fixup_range(&decompress, 16, 999));
  ASSERT_EQ(range_t(18, 24), fixup_range(&decompress, 998, 999));
}

// initialize a CephContext
int main(int argc, char** argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
