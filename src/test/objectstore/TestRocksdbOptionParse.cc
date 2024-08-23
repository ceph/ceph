#include <gtest/gtest.h>
#include "include/Context.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/thread_status.h"
#include "kv/RocksDBStore.h"
#include <iostream>

using namespace std;

const string dir("rocksdb.test_temp_dir");

TEST(RocksDBOption, simple) {
  rocksdb::Options options;
  rocksdb::Status status;
  map<string,string> kvoptions;
  auto db = std::make_unique<RocksDBStore>(g_ceph_context, dir, kvoptions, nullptr);
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
			  "compaction_options_universal = {min_merge_width=4;size_ratio=2;max_size_amplification_percent=500}";
  int r = db->ParseOptionsFromString(options_string, options);
  ASSERT_EQ(0, r);
  ASSERT_EQ(536870912u, options.write_buffer_size);
  ASSERT_EQ(4, options.max_write_buffer_number);
  ASSERT_EQ(4, options.max_background_compactions);
  ASSERT_EQ(5u, options.stats_dump_period_sec);
  ASSERT_EQ(2, options.min_write_buffer_number_to_merge);
  ASSERT_EQ(4, options.level0_file_num_compaction_trigger);
  ASSERT_EQ(104857600u, options.max_bytes_for_level_base);
  ASSERT_EQ(10485760u, options.target_file_size_base);
  ASSERT_EQ(3, options.num_levels);
  ASSERT_EQ(rocksdb::kNoCompression, options.compression);
  ASSERT_EQ(2, options.compaction_options_universal.size_ratio);
  ASSERT_EQ(4, options.compaction_options_universal.min_merge_width);
  ASSERT_EQ(500, options.compaction_options_universal.max_size_amplification_percent);
}
TEST(RocksDBOption, interpret) {
  rocksdb::Options options;
  rocksdb::Status status;
  map<string,string> kvoptions;
  auto db = std::make_unique<RocksDBStore>(g_ceph_context, dir, kvoptions, nullptr);
  string options_string = "compact_on_mount = true; compaction_threads=10;flusher_threads=5;";
  
  int r = db->ParseOptionsFromString(options_string, options);
  ASSERT_EQ(0, r);
  ASSERT_TRUE(db->compact_on_mount);
  //check thread pool setting
  options.env->SleepForMicroseconds(100000);
  std::vector<rocksdb::ThreadStatus> thread_list;
  status = options.env->GetThreadList(&thread_list);
  ASSERT_TRUE(status.ok());

  int num_high_pri_threads = 0;
  int num_low_pri_threads = 0;
  for (vector<rocksdb::ThreadStatus>::iterator it = thread_list.begin();
	it!= thread_list.end();
	++it) {
    if (it->thread_type == rocksdb::ThreadStatus::HIGH_PRIORITY)
      num_high_pri_threads++;
    if (it->thread_type == rocksdb::ThreadStatus::LOW_PRIORITY)
      num_low_pri_threads++;
  }
  ASSERT_EQ(15u, thread_list.size());
  //low pri threads is compaction_threads
  ASSERT_EQ(10, num_low_pri_threads);
  //high pri threads is flusher_threads
  ASSERT_EQ(5, num_high_pri_threads);
}
