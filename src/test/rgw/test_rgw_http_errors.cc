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

TEST(RGWHttpErrors, RgwConnectionResetDistinctCode)
{
  const auto it = rgw_http_s3_errors.find(ERR_RGW_CONNECTION_RESET);
  ASSERT_NE(it, rgw_http_s3_errors.end());
  EXPECT_EQ(500, it->second.first);
  EXPECT_STREQ("ConnectionReset", it->second.second);
}

TEST(RGWHttpErrors, EioNormalizesToInternalError)
{
  rgw_err err;
  set_req_state_err(err, EIO, RGW_REST_S3);
  EXPECT_EQ(500, err.http_ret);
  EXPECT_EQ("InternalError", err.err_code);
  EXPECT_EQ(-EIO, err.ret);
}

TEST(RGWHttpErrors, BusyReshardingDistinctCode)
{
  const auto it = rgw_http_s3_errors.find(ERR_BUSY_RESHARDING);
  ASSERT_NE(it, rgw_http_s3_errors.end());
  EXPECT_EQ(503, it->second.first);
  EXPECT_STREQ("BucketResharding", it->second.second);
}

TEST(RGWHttpErrors, SetReqStateErrConnResetNotUnknown)
{
  rgw_err err;
  set_req_state_err(err, ECONNRESET, RGW_REST_S3);
  EXPECT_EQ(500, err.http_ret);
  EXPECT_EQ("ConnectionReset", err.err_code);
  EXPECT_EQ(-ECONNRESET, err.ret);
}

TEST(RGWHttpErrors, SetReqStateErrConnResetDistinctFromInternalError)
{
  rgw_err conn;
  rgw_err io;
  set_req_state_err(conn, ECONNRESET, RGW_REST_S3);
  set_req_state_err(io, ERR_INTERNAL_ERROR, RGW_REST_S3);
  EXPECT_NE(conn.err_code, io.err_code);
  EXPECT_EQ("ConnectionReset", conn.err_code);
  EXPECT_EQ("InternalError", io.err_code);
}
