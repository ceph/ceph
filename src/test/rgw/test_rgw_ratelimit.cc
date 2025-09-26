// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <gtest/gtest.h>
#include "rgw_ratelimit.h"


using namespace std::chrono_literals;


TEST(RGWRateLimit, op_limit_not_enabled)
{
  // info.enabled = false, so no limit
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, reject_op_over_limit)
{
  // check that request is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, accept_op_after_giveback)
{
  // check that giveback is working fine
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.giveback_tokens("GET", key, "", &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, accept_op_after_refill)
{
  // check that tokens are being filled properly
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  time += 61s;
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, reject_bw_over_limit)
{
  // check that a newer request is rejected if there is no enough tokens (bw)
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.decrease_bytes("GET",key, 2, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, accept_bw)
{
  // check that when there are enough tokens (bw) the request is still being served
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.decrease_bytes("GET",key, 1, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, check_bw_debt_at_max_120secs)
{
  // check that the bandwidth debt is not larger than 120 seconds
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_bytes = 2;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.decrease_bytes("GET",key, 100, &info);
  time += 121s;
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, check_that_bw_limit_not_affect_ops)
{
  // check that high read bytes limit, does not affect ops limit
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.decrease_bytes("GET",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(true, success);
}
TEST(RGWRateLimit, read_limit_does_not_affect_writes)
{
  // read limit does not affect writes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_read_ops = 1;
  info.max_read_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  ratelimit.decrease_bytes("PUT",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  EXPECT_EQ(false, success);
}
TEST(RGWRateLimit, write_limit_does_not_affect_reads)
{
  // write limit does not affect reads
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_write_ops = 1;
  info.max_write_bytes = 100000000;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  ratelimit.decrease_bytes("GET",key, 10000, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, allow_unlimited_access)
{
  // 0 values in RGWRateLimitInfo should allow unlimited access
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser123";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
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
  active->should_rate_limit("GET", key, time, &info, "");
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
    active->should_rate_limit("GET", key, time, &info, "");
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read,  &info, time);
  entry.giveback_tokens(OpType::Read);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read,  &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read,  &info, time);
  time += 61s;
  success = entry.should_rate_limit(OpType::Read,  &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read,  &info, time);
  entry.decrease_bytes(true, 2, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read,  &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  entry.decrease_bytes(true, 1, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  entry.decrease_bytes(true, 100, &info);
  time += 121s;
  success = entry.should_rate_limit(OpType::Read, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  entry.decrease_bytes(true, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Write, &info, time);
  entry.decrease_bytes(false, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Write, &info, time);
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
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  entry.decrease_bytes(true, 10000, &info);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, allow_unlimited_access)
{
  // 0 values in RGWRateLimitInfo should allow unlimited access (default value)
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  EXPECT_EQ(false, success);
}


// Helpers for LIST op resource string
// ref uri: "/aaa-gonzo-staging-bbb-checkpoint1-us-west-0000?list-type=2&delimiter=%2F&max-keys=2&prefix=spark%2Fgonzo-avro%2Fsplunk_hec_test%2Fchunk-commits%2F%2F00000007999&encoding-type=url";
const std::string RES_LIST_TYPE_2 = "?list-type=2";
const std::string RES_DELIMITER = "&delimiter=%2F";
const std::string RES_PREFIX = "&prefix=spark%2Fgonzo-avro%2Fsplunk_hec_test%2Fchunk-commits%2F%2F00000007999";

TEST(RGWRateLimit, reject_list_op_over_limit)
{
  // check that LIST op is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimit, accept_list_op_after_giveback)
{
  // check that giveback is working for LIST ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  ratelimit.giveback_tokens("GET", key, RES_LIST_TYPE_2, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, accept_list_op_after_refill)
{
  // check that tokens are being filled properly for LIST ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  time += 61s;
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, list_limit_does_not_affect_reads)
{
  // list limit does not affect reads
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  // Should still be able to do a normal GET (read)
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, read_limit_does_not_affect_lists)
{
  // read limit does not affect lists
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  // Should still be able to do a LIST op
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, list_limit_does_not_affect_writes)
{
  // list limit does not affect writes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  // Should still be able to do a PUT (write)
  success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, write_limit_does_not_affect_lists)
{
  // write limit does not affect lists
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  // Should still be able to do a LIST op
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, list_limit_does_not_affect_deletes)
{
  // list limit does not affect deletes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  // Should still be able to do a DELETE op
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, delete_limit_does_not_affect_lists)
{
  // delete limit does not affect lists
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  // Should still be able to do a LIST op
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_LIST_TYPE_2);
  EXPECT_EQ(false, success);
}

// LIST RES_DELIMITER minimal tests
TEST(RGWRateLimit, reject_delimiter_op_over_limit)
{
  // check that DELIMITER op is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimit, accept_delimiter_op_after_giveback)
{
  // check that giveback is working for DELIMITER ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  ratelimit.giveback_tokens("GET", key, RES_DELIMITER, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, accept_delimiter_op_after_refill)
{
  // check that tokens are being filled properly for DELIMITER ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  time += 61s;
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_DELIMITER);
  EXPECT_EQ(false, success);
}

// LIST RES_PREFIX minimal tests
TEST(RGWRateLimit, reject_prefix_op_over_limit)
{
  // check that PREFIX op is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimit, accept_prefix_op_after_giveback)
{
  // check that giveback is working for PREFIX ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  ratelimit.giveback_tokens("GET", key, RES_PREFIX, &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, accept_prefix_op_after_refill)
{
  // check that tokens are being filled properly for PREFIX ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_list";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  time += 61s;
  success = ratelimit.should_rate_limit("GET", key, time, &info, RES_PREFIX);
  EXPECT_EQ(false, success);
}


TEST(RGWRateLimitEntry, reject_list_op_over_limit)
{
  // check that LIST request is being rejected because there are not enough tokens
  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimitEntry, accept_list_op_after_giveback)
{
  // check that giveback is working fine for LIST ops
  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  entry.giveback_tokens(OpType::List);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, accept_list_op_after_refill)
{
  // check that tokens are being filled properly for LIST ops
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  time += 61s;
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, list_limit_does_not_affect_reads)
{
  // list limit does not affect reads
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, read_limit_does_not_affect_lists)
{
  // read limit does not affect lists
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, list_limit_does_not_affect_writes)
{
  // list limit does not affect writes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Write, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, write_limit_does_not_affect_lists)
{
  // write limit does not affect lists
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Write, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, list_limit_does_not_affect_deletes)
{
  // list limit does not affect deletes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::List, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, delete_limit_does_not_affect_lists)
{
  // delete limit does not affect lists
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_list_ops = 1;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::List, &info, time);
  EXPECT_EQ(false, success);
}


TEST(RGWRateLimit, reject_delete_op_over_limit)
{
  // check that DELETE op is being rejected because there are not enough tokens
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimit, accept_delete_op_after_giveback)
{
  // check that giveback is working for DELETE ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  ratelimit.giveback_tokens("DELETE", key, "", &info);
  time = ceph::coarse_real_clock::now();
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, accept_delete_op_after_refill)
{
  // check that tokens are being filled properly for DELETE ops
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  time += 61s;
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, delete_limit_does_not_affect_reads)
{
  // delete limit does not affect reads
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  // Should still be able to do a normal GET (read)
  success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, read_limit_does_not_affect_deletes)
{
  // read limit does not affect deletes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("GET", key, time, &info, "");
  // Should still be able to do a DELETE op
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, write_limit_does_not_affect_deletes)
{
  // write limit does not affect deletes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  // Should still be able to do a DELETE op
  success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimit, delete_limit_does_not_affect_writes)
{
  // delete limit does not affect writes
  std::atomic_bool replacing;
  std::condition_variable cv;
  RateLimiter ratelimit(g_ceph_context, replacing, cv);
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now();
  std::string key = "uuser_delete";
  bool success = ratelimit.should_rate_limit("DELETE", key, time, &info, "");
  // Should still be able to do a PUT (write)
  success = ratelimit.should_rate_limit("PUT", key, time, &info, "");
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, reject_delete_op_over_limit)
{
  // check that DELETE request is being rejected because there are not enough tokens
  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(true, success);
}

TEST(RGWRateLimitEntry, accept_delete_op_after_giveback)
{
  // check that giveback is working fine for DELETE ops
  RGWRateLimitInfo info;
  RateLimiterEntry entry;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  entry.giveback_tokens(OpType::Delete);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, accept_delete_op_after_refill)
{
  // check that tokens are being filled properly for DELETE ops
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  time += 61s;
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, delete_limit_does_not_affect_reads)
{
  // delete limit does not affect reads
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Read, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, read_limit_does_not_affect_deletes)
{
  // read limit does not affect deletes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_read_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Read, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, write_limit_does_not_affect_deletes)
{
  // write limit does not affect deletes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Write, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Delete, &info, time);
  EXPECT_EQ(false, success);
}

TEST(RGWRateLimitEntry, delete_limit_does_not_affect_writes)
{
  // delete limit does not affect writes
  RateLimiterEntry entry;
  RGWRateLimitInfo info;
  info.enabled = true;
  info.max_delete_ops = 1;
  info.max_write_ops = 1;
  auto time = ceph::coarse_real_clock::now().time_since_epoch();
  bool success = entry.should_rate_limit(OpType::Delete, &info, time);
  time = ceph::coarse_real_clock::now().time_since_epoch();
  success = entry.should_rate_limit(OpType::Write, &info, time);
  EXPECT_EQ(false, success);
}

