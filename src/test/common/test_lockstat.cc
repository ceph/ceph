// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <future>
#include <string>
#include <thread>
#include <vector>

#include "common/lockstat.h"
#include "gtest/gtest.h"
#ifdef CEPH_LOCKSTAT

using namespace ceph::lockstat_detail;

TEST(LockStat, HashFunctions)
{
  const char* name = "test_lock";
  const char* file = "test_lockstat.cc";
  const char* func = "TEST";
  uint64_t line = 100;

  uint64_t h1 = hash(name, file, line, func);
  uint64_t h2 = hash(name, file, line, func);
  ASSERT_EQ(h1, h2);

  uint64_t h3 = hash("other", file, line, func);
  ASSERT_NE(h1, h3);

  // Test hash_str and hash_u64 directly
  uint64_t hs1 = hash_str("abc", FNV1_64_INIT);
  uint64_t hs2 = hash_str("abc", FNV1_64_INIT);
  ASSERT_EQ(hs1, hs2);
  ASSERT_NE(hs1, FNV1_64_INIT);

  uint64_t hu1 = hash_u64(12345, FNV1_64_INIT);
  uint64_t hu2 = hash_u64(12345, FNV1_64_INIT);
  ASSERT_EQ(hu1, hu2);
  ASSERT_NE(hu1, FNV1_64_INIT);
}

TEST(LockStat, LockStatTraits)
{
  const char* name = "my_lock";
  const char* file = "file.cc";
  const char* func = "func";
  int line = 42;
  uint64_t h = hash(name, file, line, func);
  uint64_t lock_index =
      h % ceph::lockstat_detail::LockStatTraits::kHash_table_max_size;

  // get_lockstat_traits is a static method that manages a table
  LockStatTraits* traits = LockStatTraits::get_lockstat_traits(
      name, h, lock_index, file, line, func);
  if (traits) {
    ASSERT_EQ(
        traits->get_name(),
        LockStatTraits::get_lockstat_name(name, file, line, func));
    traits->set_lock_type(LockStatTraits::LockStatType::MUTEX);
    ASSERT_EQ(traits->get_lock_type(), LockStatTraits::LockStatType::MUTEX);
  } else {
    // If lockstat is disabled, it should return nullptr
    ASSERT_EQ(traits, nullptr);
  }
}

TEST(LockStat, LockStats)
{
  LockStatEntry::LockStats stats;
  stats.reset();

  for (int i = 0; i < static_cast<int>(LockMode::COUNT); ++i) {
    ASSERT_EQ(stats.m_wait_count[i], 0u);
    ASSERT_EQ(stats.m_wait_duration[i].count(), 0);
  }

  // Simulate some wait time
  lockstat_clock::duration d{tsc_rep{100}};
  stats.m_wait_duration[static_cast<int>(LockMode::WRITE)] += d;
  stats.m_wait_count[static_cast<int>(LockMode::WRITE)]++;

  ASSERT_EQ(stats.m_wait_count[static_cast<int>(LockMode::WRITE)], 1u);
  ASSERT_EQ(stats.m_wait_duration[static_cast<int>(LockMode::WRITE)].count().ticks, 100);

  LockStatEntry::LockStats stats2;
  stats2.reset();
  stats2.m_wait_duration[static_cast<int>(LockMode::WRITE)] += lockstat_clock::duration{tsc_rep{50}};
  stats2.m_wait_count[static_cast<int>(LockMode::WRITE)]++;

  stats += stats2;
  ASSERT_EQ(stats.m_wait_count[static_cast<int>(LockMode::WRITE)], 2u);
  ASSERT_EQ(stats.m_wait_duration[static_cast<int>(LockMode::WRITE)].count().ticks, 150);
}

TEST(LockStat, LockStatEntry)
{
  LockStatEntry entry;
  entry.reset();

  // Testing start/stop
  entry.start(lockstat_clock::duration{tsc_rep{0}});
  entry.stop();

  auto sum = entry.get_stats_sum();
  (void)sum;
}

TEST(LockStat, DumpFormatted)
{
  LockStatEntry::start(lockstat_clock::duration{tsc_rep{0}});

  // Record some data
  const char* name = "dump_lock";
  uint64_t h = hash(name, "test.cc", 1, "test");
  uint64_t lock_index = h % LockStatTraits::kHash_table_max_size;
  LockStatTraits* traits = LockStatTraits::get_lockstat_traits(
      name, h, lock_index, "test.cc", 1, "test");

  if (traits) {
    LockStat ls(LockStatTraits::LockStatType::MUTEX, traits);
    ls.record_wait_time(lockstat_clock::duration{tsc_rep{tsc_tick::from_duration(std::chrono::nanoseconds(100))}}, LockMode::WRITE);
    ls.record_wait_time(lockstat_clock::duration{tsc_rep{tsc_tick::from_duration(std::chrono::nanoseconds(200))}}, LockMode::WRITE);
  }

  std::unique_ptr<ceph::Formatter> f{
      ceph::Formatter::create("json", "json", "json")};
  LockStatEntry::dump_formatted(f.get());

  std::stringstream ss;
  f->flush(ss);
  std::string output = ss.str();

  // Basic checks on output
  EXPECT_NE(output.find("Profiling data dumped"), std::string::npos);
  EXPECT_NE(output.find("bin_ranges"), std::string::npos);
  EXPECT_NE(output.find("total_usec"), std::string::npos);
  EXPECT_NE(output.find("\"entries\":"), std::string::npos);
  if (traits) {
    EXPECT_NE(output.find("dump_lock[test]test.cc@1"), std::string::npos);
    EXPECT_NE(output.find("wait_count"), std::string::npos);
    EXPECT_NE(output.find("300"), std::string::npos);
    EXPECT_NE(output.find("wait_time_histogram"), std::string::npos);
    EXPECT_NE(output.find("[0,0,0,1,1,0"), std::string::npos);
  }

  LockStatEntry::stop();
}

TEST(LockStat, LockStatObject)
{
  // Use the LOCKSTAT macro or create lock_stat object
  {
    LockStat ls(LockStatTraits::LockStatType::MUTEX, nullptr);
    ASSERT_EQ(ls.get_traits(), nullptr);
    // Should not crash when traits is null
    ls.record_wait_time(lockstat_clock::duration{tsc_rep{100}}, LockMode::WRITE);
  }

  // With actual traits
  const char* name = "obj_lock";
  uint64_t h = hash(name, "test.cc", 1, "test");
  uint64_t lock_index =
      h % ceph::lockstat_detail::LockStatTraits::kHash_table_max_size;

  LockStatTraits* traits = LockStatTraits::get_lockstat_traits(
      name, h, lock_index, "test.cc", 1, "test");

  if (traits) {
    LockStat ls(LockStatTraits::LockStatType::MUTEX, traits);
    ASSERT_EQ(ls.get_traits(), traits);
    ls.record_wait_time(lockstat_clock::duration{tsc_rep{200}}, LockMode::WRITE);
  }

  // Test the macro
  auto ls_macro = LOCKSTAT("macro_lock");
  (void)ls_macro;
}

TEST(LockStat, TimeCalculation)
{
  LockStatEntry::start(lockstat_clock::duration{tsc_rep{0}});

  const char* name = "time_calc_lock";
  uint64_t h = hash(name, "test_lockstat.cc", 1, "TEST");
  uint64_t lock_index = h % LockStatTraits::kHash_table_max_size;
  LockStatTraits* traits = LockStatTraits::get_lockstat_traits(
      name, h, lock_index, "test_lockstat.cc", 1, "TEST");

  if (traits) {
    LockStat ls(LockStatTraits::LockStatType::MUTEX, traits);

    auto sleep_time = std::chrono::milliseconds(50);
    auto start = lockstat_clock::now();
    std::this_thread::sleep_for(sleep_time);
    auto duration = lockstat_clock::now() - start;

    ls.record_wait_time(duration, LockMode::WRITE);

    auto stats = traits->get_lockstat_entry()->get_stats_sum();
    auto recorded_ns = stats.m_wait_duration[static_cast<int>(LockMode::WRITE)].count();

    // Verify recorded time is similar to sleep time.
    // Allow for some slack due to sleep inaccuracies.
    auto sleep_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(sleep_time).count();
    auto max_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(sleep_time + std::chrono::milliseconds(20)).count();
    EXPECT_GE(recorded_ns, sleep_ns);
    EXPECT_LT(recorded_ns, max_ns);
  }

  LockStatEntry::stop();
}

TEST(LockStat, GetTimeoutTripwire)
{
  struct timespec timeout1, timeout2;
  const auto threshold = std::chrono::seconds(2);
  LockStat::set_tripwire_threshold(
      lockstat_clock::duration{tsc_rep{tsc_tick::from_duration(threshold)}});

  clock_gettime(CLOCK_REALTIME, &timeout1);
  // Re-load tripwire threshold to get timespec offset
  LockStat::get_timeout_tripwire(&timeout2);

  // Since we take the current time twice, there may be a small difference.
  // timeout2 should be roughly timeout1 + threshold.
  EXPECT_GE(timeout2.tv_sec, timeout1.tv_sec + 1);
  EXPECT_LE(timeout2.tv_sec, timeout1.tv_sec + 3);

  // Set threshold to something that causes nanosecond overflow
  const auto threshold_ns = std::chrono::nanoseconds(1500000000); // 1.5 seconds
  LockStat::set_tripwire_threshold(
      lockstat_clock::duration{tsc_rep{tsc_tick::from_duration(threshold_ns)}});
  LockStat::get_timeout_tripwire(&timeout2);

  EXPECT_GE(timeout2.tv_sec, timeout1.tv_sec + 1);
}

#endif // CEPH_LOCKSTAT