#include <gtest/gtest.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/convenience.h"
#include <iostream>
using namespace std;

TEST(RocksDBOption, simple) {
  rocksdb::Options options;
  rocksdb::Status status;
  string options_string = ""
			  "write_buffer_size=536870912;"
			  "create_if_missing=true;"
			  "max_write_buffer_number=4;"
			  "max_background_compactions=4;"
			  "stats_dump_period_sec = 5;"
			  "min_write_buffer_number_to_merge = 2;"
			  "level0_file_num_compaction_trigger = 4;"
			  "max_bytes_for_level_base = 104857600;"
			  "target_file_size_base = 10485760;"
			  "num_levels = 3;"
			  "compression = kNoCompression;"
			  "disable_data_sync = false;";
  status = rocksdb::GetOptionsFromString(options, options_string, &options);
  if (!status.ok()) {
    cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(536870912, options.write_buffer_size);
  ASSERT_EQ(4, options.max_write_buffer_number);
  ASSERT_EQ(4, options.max_background_compactions);
  ASSERT_EQ(5, options.stats_dump_period_sec);
  ASSERT_EQ(2, options.min_write_buffer_number_to_merge);
  ASSERT_EQ(4, options.level0_file_num_compaction_trigger);
  ASSERT_EQ(104857600, options.max_bytes_for_level_base);
  ASSERT_EQ(10485760, options.target_file_size_base);
  ASSERT_EQ(3, options.num_levels);
  ASSERT_FALSE(options.disableDataSync);
 // ASSERT_EQ("none", options.compression);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
