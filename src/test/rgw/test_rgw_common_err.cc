#include <errno.h>
#include <gtest/gtest.h>

#include "rgw_common.h"

TEST(TestRGWCommonErr, set_req_state_err_ENAMETOOLONG)
{
  // S3
  {
    rgw_err err;
    set_req_state_err(err, ENAMETOOLONG, RGW_REST_S3);
    ASSERT_EQ(err.http_ret, 400);
    ASSERT_EQ(err.err_code, "InvalidArgument");
  }

  // S3 (by default if prot_flags is not passed)
  {
    rgw_err err;
    set_req_state_err(err, ENAMETOOLONG, 0);
    ASSERT_EQ(err.http_ret, 400);
    ASSERT_EQ(err.err_code, "InvalidArgument");
  }

  // Swift
  {
    rgw_err err;
    set_req_state_err(err, ENAMETOOLONG, RGW_REST_SWIFT);
    ASSERT_EQ(err.http_ret, 400);
    ASSERT_EQ(err.err_code, "Metadata name too long");
  }
}
