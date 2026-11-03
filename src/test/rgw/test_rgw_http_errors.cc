// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "rgw_common.h"
#include "rgw_http_errors.h"

#include <gtest/gtest.h>

TEST(RGWHttpErrors, BucketSuspendedS3)
{
  const auto it = rgw_http_s3_errors.find(ERR_BUCKET_SUSPENDED);
  ASSERT_NE(it, rgw_http_s3_errors.end());
  EXPECT_EQ(403, it->second.first);
  EXPECT_STREQ("BucketSuspended", it->second.second);
}

TEST(RGWHttpErrors, BucketSuspendedSwift)
{
  const auto it = rgw_http_swift_errors.find(ERR_BUCKET_SUSPENDED);
  ASSERT_NE(it, rgw_http_swift_errors.end());
  EXPECT_EQ(403, it->second.first);
  EXPECT_STREQ("BucketSuspended", it->second.second);
}

TEST(RGWHttpErrors, BucketSuspendedDistinctFromUser)
{
  const auto bucket_it = rgw_http_s3_errors.find(ERR_BUCKET_SUSPENDED);
  const auto user_it = rgw_http_s3_errors.find(ERR_USER_SUSPENDED);
  ASSERT_NE(bucket_it, rgw_http_s3_errors.end());
  ASSERT_NE(user_it, rgw_http_s3_errors.end());
  EXPECT_NE(std::string(bucket_it->second.second),
            std::string(user_it->second.second));
}
