// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "os/bluestore/BlueStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

using namespace std;

TEST(rocksdb_bluefs_vselector, basic) {

  uint64_t db_size = 168ull << 30;
  uint64_t level_base = 1ull << 30;
  size_t level_multi = 8;

  RocksDBBlueFSVolumeSelector selector(
    10ull << 30,
    db_size,
    1000ull << 30,
    1ull << 30,
    level_base,
    level_multi,
    g_ceph_context->_conf->bluestore_volume_selection_policy.find("use_some_extra")
      == 0);
  selector.update_from_config(g_ceph_context);

  // taken from RocksDBBlueFSVolumeSelector::
  size_t log_bdev = 1; // LEVEL_LOG
  size_t wal_bdev = 2; // LEVEL_WAL
  size_t db_bdev = 3;  // LEVEL_DB
  size_t slow_bdev = 4;// LEVEL_SLOW
  bluefs_extent_t e;

  ASSERT_EQ(4, selector.get_extra_level());
  ASSERT_EQ(30ull << 30, selector.get_available_extra()); // 168GB - 1GB (L0) - 1GB (L1) - 8GB (L2) - 2*64GB (L3)

  ASSERT_EQ(0, selector.select_prefer_bdev((void*)log_bdev));
  ASSERT_EQ(0, selector.select_prefer_bdev((void*)wal_bdev));
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)db_bdev));
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));
  // 'Use' 138GB DB level data at DB vol
  for (size_t i = 0; i < 138; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.add_usage((void*)db_bdev, e);
  }
  ASSERT_EQ(0, selector.select_prefer_bdev((void*)log_bdev));
  ASSERT_EQ(0, selector.select_prefer_bdev((void*)wal_bdev));
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)db_bdev));
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Use' 30GB Slow level data at DB vol
  for (size_t i = 0; i < 30; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));
    selector.add_usage((void*)slow_bdev, e);
  }
  ASSERT_EQ(0, selector.select_prefer_bdev((void*)log_bdev));
  ASSERT_EQ(0, selector.select_prefer_bdev((void*)wal_bdev));
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)db_bdev));
  ASSERT_EQ(2, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Unuse' 10GB DB level data at DB vol, slow data still wouldn't fit
  // as it's exceeds the threshold
  for (size_t i = 0; i < 10; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.sub_usage((void*)db_bdev, e);
  }
  ASSERT_EQ(2, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Unuse' 10GB Slow level data at DB vol, slow data fits now
  for (size_t i = 0; i < 10; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.sub_usage((void*)slow_bdev, e);
  }
  ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Unuse' remaining 20GB Slow level data at DB vol, slow data fits now
  for (size_t i = 0; i < 20; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.sub_usage((void*)slow_bdev, e);
  }
  // 'Use' 30GB DB level data at DB vol to raise historic maximum, 10GB slow data fits only since now
  for (size_t i = 0; i < 30; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));
    selector.add_usage((void*)db_bdev, e);
  }
  for (size_t i = 0; i < 10; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));
    selector.add_usage((void*)slow_bdev, e);
  }
  ASSERT_EQ(2, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Unuse' remaining 10GB Slow level data at DB vol
  for (size_t i = 0; i < 10; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.sub_usage((void*)slow_bdev, e);
  }
  // 'Use' additional 10GB DB level data at DB vol to raise historic maximum, 10GB slow data wouldn't fit since now
  for (size_t i = 0; i < 10; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    ASSERT_EQ(1, selector.select_prefer_bdev((void*)slow_bdev));
    selector.add_usage((void*)db_bdev, e);
  }
  ASSERT_EQ(2, selector.select_prefer_bdev((void*)slow_bdev));

  // 'Unuse' 50GB DB level data, thi s wouldn't let slow data use DB volume anyway
  // due to updated historic maximum
  for (size_t i = 0; i < 50; i++) {
    e.bdev = 1; // DB dev
    e.length = 1ull * (1 << 30);
    selector.sub_usage((void*)db_bdev, e);
  }
  ASSERT_EQ(2, selector.select_prefer_bdev((void*)slow_bdev));


  std::stringstream ss;
  selector.dump(ss);
  std::cout << ss.str() << std::endl;
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct =
      global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
                  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
