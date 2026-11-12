// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/rados/librados.hpp"
#include "include/types.h"

#include "cls/otp/cls_otp_client.h"
#include "cls/otp/cls_otp_ops.h"
#include "cls/otp/cls_otp_types.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"

#include <liboath/oath.h>

#include <ctime>
#include <string>

using namespace std;
using namespace rados::cls::otp;
using ceph::test::PoolType;
using ceph::test::pool_type_name;

// hex seed for the test token. the OSD and the test generate pins from the
// same seed and clock, so the check has to succeed for a freshly generated pin
static const string seed_hex = "3132333435363738393031323334353637383930";

class TestClsOtp : public ceph::test::ClsTestFixture {
  // Inherits: rados, ioctx, pool_name, pool_type, SetUp(), TearDown()
protected:
  CephContext *cct() {
    return reinterpret_cast<CephContext*>(ioctx.cct());
  }

  void create_otp(const string& oid, const string& id) {
    otp_info_t info;
    info.type = OTP_TOTP;
    info.id = id;
    info.seed = seed_hex;
    info.seed_type = OTP_SEED_HEX;

    librados::ObjectWriteOperation op;
    OTP::create(&op, info);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }

  // generate the pin that is valid now for the default step size
  static string current_pin() {
    char secret[64];
    size_t secret_len = sizeof(secret);
    EXPECT_EQ(OATH_OK, oath_hex2bin(seed_hex.c_str(), secret, &secret_len));

    char pin[7];
    EXPECT_EQ(OATH_OK, oath_totp_generate(secret, secret_len, time(nullptr),
                                          OATH_TOTP_DEFAULT_TIME_STEP_SIZE,
                                          0, 6, pin));
    return pin;
  }

  // flip one digit of the valid pin so it can't match any step in the window
  static string wrong_pin() {
    string pin = current_pin();
    pin[0] = (pin[0] == '0' ? '1' : '0');
    return pin;
  }

  // run the check method with a caller-chosen token, as OTP::check() does
  int exec_check(const string& oid, const string& id, const string& val,
                 const string& token, bufferlist *out, int flags) {
    cls_otp_check_otp_op op;
    op.id = id;
    op.val = val;
    op.token = token;
    bufferlist in;
    encode(op, in);

    librados::ObjectWriteOperation wop;
    wop.exec(method::check, in, out, nullptr);
    return ioctx.operate(oid, &wop, flags);
  }

  int get_result(const string& oid, const string& id, const string& token,
                 otp_check_t *result) {
    cls_otp_check_otp_op op;
    op.id = id;
    op.token = token;
    bufferlist in, out;
    encode(op, in);

    int r = ioctx.exec(oid, method::get_result, in, out);
    if (r < 0) {
      return r;
    }
    cls_otp_get_result_reply reply;
    auto iter = out.cbegin();
    decode(reply, iter);
    *result = reply.result;
    return 0;
  }
};

TEST_P(TestClsOtp, check_success_then_replay)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  const string id = "token";
  create_otp(oid, id);

  const string pin = current_pin();

  otp_check_t result;
  ASSERT_EQ(0, OTP::check(cct(), ioctx, oid, id, pin, &result));
  ASSERT_EQ(OTP_CHECK_SUCCESS, result.result);
  ASSERT_FALSE(result.token.empty());

  // a pin can be used only once
  ASSERT_EQ(0, OTP::check(cct(), ioctx, oid, id, pin, &result));
  ASSERT_EQ(OTP_CHECK_FAIL, result.result);
}

TEST_P(TestClsOtp, check_wrong_pin)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  const string id = "token";
  create_otp(oid, id);

  otp_check_t result;
  ASSERT_EQ(0, OTP::check(cct(), ioctx, oid, id, wrong_pin(), &result));
  ASSERT_EQ(OTP_CHECK_FAIL, result.result);
}

TEST_P(TestClsOtp, check_missing_id)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  create_otp(oid, "token");

  otp_check_t result;
  ASSERT_EQ(-ENOENT, OTP::check(cct(), ioctx, oid, "other", "123456", &result));
}

TEST_P(TestClsOtp, check_too_many_attempts)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  const string id = "token";
  create_otp(oid, id);

  // the check method records at most 5 attempts per time step. once that
  // limit is reached it doesn't record the check, so the result is unknown
  otp_check_t result;
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(0, OTP::check(cct(), ioctx, oid, id, wrong_pin(), &result));
    ASSERT_EQ(OTP_CHECK_FAIL, result.result);
  }
  ASSERT_EQ(0, OTP::check(cct(), ioctx, oid, id, current_pin(), &result));
  ASSERT_EQ(OTP_CHECK_UNKNOWN, result.result);
}

TEST_P(TestClsOtp, check_reply_matches_get_result)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  const string id = "token";
  create_otp(oid, id);

  const string token = "check-reply";
  bufferlist out;
  ASSERT_EQ(0, exec_check(oid, id, current_pin(), token, &out,
                          librados::OPERATION_RETURNVEC));
  ASSERT_GT(out.length(), 0u);

  cls_otp_check_otp_reply reply;
  auto iter = out.cbegin();
  decode(reply, iter);
  ASSERT_EQ(OTP_CHECK_SUCCESS, reply.result);

  // the reply must agree with what get_result reports for the same token
  otp_check_t result;
  ASSERT_EQ(0, get_result(oid, id, token, &result));
  ASSERT_EQ(token, result.token);
  ASSERT_EQ(reply.result, result.result);
  ASSERT_EQ(reply.timestamp, result.timestamp);
}

TEST_P(TestClsOtp, check_without_returnvec)
{
  SKIP_IF_OMAP_NOT_SUPPORTED();
  const string oid = "otp";
  const string id = "token";
  create_otp(oid, id);

  // an older client doesn't set RETURNVEC, so the OSD drops the reply data
  // and the client reads the result back with get_result
  const string token = "no-returnvec";
  bufferlist out;
  ASSERT_EQ(0, exec_check(oid, id, current_pin(), token, &out, 0));
  ASSERT_EQ(0u, out.length());

  otp_check_t result;
  ASSERT_EQ(0, get_result(oid, id, token, &result));
  ASSERT_EQ(OTP_CHECK_SUCCESS, result.result);
}

INSTANTIATE_TEST_SUITE_P(, TestClsOtp,
    ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
    [](const ::testing::TestParamInfo<PoolType>& info) {
      return pool_type_name(info.param);
    }
);
