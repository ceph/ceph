// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <gtest/gtest.h>
#include "rgw/rgw_ratelimit.h"


using namespace std::chrono_literals;

TEST(RGWRateLimit, op_limit_not_enabled)
{
  // info.enabled = false, so no limit
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, reject_op_over_limit)
{
  // check that request is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, accept_op_after_giveback)
{
  // check that giveback is working fine
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.giveback_tokens("GET", key);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, accept_op_after_refill)
{
  // check that tokens are being filled properly
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  time += 61s;
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, reject_bw_over_limit)
{
  // check that a newer request is rejected if there is no enough tokens (bw)
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.decrease_bytes("GET",key, 2, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, accept_bw)
{
  // check that when there are enough tokens (bw) the request is still being served
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.decrease_bytes("GET",key, 1, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, check_bw_debt_at_max_120secs)
{
  // check that the bandwidth debt is not larger than 120 seconds
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.decrease_bytes("GET",key, 100, &info);
  time += 121s;
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, check_that_bw_limit_not_affect_ops)
{
  // check that high read bytes limit, does not affect ops limit
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.decrease_bytes("GET",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, read_limit_does_not_affect_writes)
{
  // read limit does not affect writes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info);
  ratelimit.decrease_bytes("PUT",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("PUT", key, time, &info);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, write_limit_does_not_affect_reads)
{
  // write limit does not affect reads
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_write_ops = 1;
  info.max_write_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  ratelimit.decrease_bytes("GET",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, allow_unlimited_access)
{
  // 0 values in RGWRateLimitInfo should allow unlimited access
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitGC, NO_GC_AHEAD_OF_TIME)
{
  // Test if GC is not starting the replace before getting to map_size * 0.9
  // Please make sure to change those values when you change the map_size in the code

  std::shared_ptr<ActiveRateLimiter> ratelimit(new ActiveRateLimiter(g_ceph_context));
  ratelimit->start();
  auto active = ratelimit->get_active();
  RGWRateLimitInfo info;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  active->should_rate_limit("GET", key, time, &info);
  auto activegc = ratelimit->get_active();
  EXPECT_EQ(activegc, active);
}
TEST(RGWRateLimiterGC, GC_IS_WORKING)
{
  // Test if GC is replacing the active RateLimiter
  // Please make sure to change those values when you change the map_size in the code

  std::shared_ptr<ActiveRateLimiter> ratelimit(new ActiveRateLimiter(g_ceph_context));
  ratelimit->start();
  auto active = ratelimit->get_active();
  RGWRateLimitInfo info;
  info.enabled = true;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "-1";
  for(int i = 0; i < 2000000; i++)
  {
    active->should_rate_limit("GET", key, time, &info);
    key = std::to_string(i);
  }
  auto activegc = ratelimit->get_active();
  EXPECT_NE(activegc, active);
}
  
  
TEST(RGWRateLimitEntry, op_limit_not_enabled)
{
  // info.enabled = false, so no limit
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(false, &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, reject_op_over_limit)
{
  // check that request is being rejected because there are not enough tokens

  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true, &info, time);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimitEntry, accept_op_after_giveback)
{
  // check that giveback is working fine
  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.giveback_tokens(true);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, accept_op_after_refill)
{
  // check that tokens are being filled properly
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  time += 61s;
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, reject_bw_over_limit)
{
  // check that a newer request is rejected if there is no enough tokens (bw)
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.decrease_bytes(true, 2, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimitEntry, accept_bw)
{
  // check that when there are enough tokens (bw) the request is still being served
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.decrease_bytes(true, 1, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, check_bw_debt_at_max_120secs)
{
  // check that the bandwidth debt is not larger than 120 seconds
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.decrease_bytes(true, 100, &info);
  time += 121s;
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, check_that_bw_limit_not_affect_ops)
{
  // check that high read bytes limit, does not affect ops limit
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.decrease_bytes(true, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimitEntry, read_limit_does_not_affect_writes)
{
  // read limit does not affect writes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(false,  &info, time);
  entry.decrease_bytes(false, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(false,  &info, time);
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimitEntry, write_limit_does_not_affect_reads)
{
  // write limit does not affect reads
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_write_ops = 1;
  info.max_write_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  std::string key = "uuser123";
  bool success = entry.should_rate_limit(true,  &info, time);
  entry.decrease_bytes(true, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, allow_unlimited_access)
{
  // 0 values in RGWRateLimitInfo should allow unlimited access (default value)
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(true,  &info, time);
  EXPECT_EQ(false, success);
}
