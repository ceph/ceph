// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Tests for the rgw_s3_auth_disable_signature_url guard in
 * AWSGeneralAbstractor::get_auth_data().
 *
 * The option denies presigned (query-string) urls. Two kinds of test:
 *
 *  - REGRESSION tests, marked [REGRESSION], fail if the guard denies the
 *    wrong requests: a header-signed request, or a presigned v4 url whose
 *    signature is replayed through the Authorization header.
 *  - INVARIANT guards, marked [INVARIANT], pin that a presigned url is
 *    still denied and that nothing is denied while the option is off.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "rgw_common.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"

namespace {

/* A header-signed request carries its signature in Authorization. */
const std::string V4_HEADER{
    "AWS4-HMAC-SHA256 Credential=testid/20260101/us-east-1/s3/aws4_request, "
    "SignedHeaders=host, Signature="
    "0000000000000000000000000000000000000000000000000000000000000000"};
const std::string V2_HEADER{"AWS testid:c2lnbmF0dXJl"};

/* A presigned url carries the signing parameters in the query string. */
const std::string V4_QUERY{
    "X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=testid%2F20260101%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260101T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host"
    "&X-Amz-Signature="
    "0000000000000000000000000000000000000000000000000000000000000000"};
const std::string V2_QUERY{
    "AWSAccessKeyId=testid&Expires=1700000000&Signature=c2ln"};

/* The replay: the presigned signature is moved into the Authorization
 * header and X-Amz-Signature is dropped, but X-Amz-Algorithm stays in the
 * query. */
const std::string V4_QUERY_NO_SIG{
    "X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=testid%2F20260101%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260101T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host"};

class DisableSignatureUrl : public ::testing::Test {
protected:
  CephContext* cct = g_ceph_context;

  void
  set_option(bool on)
  {
    cct->_conf.set_val(
        "rgw_s3_auth_disable_signature_url", on ? "true" : "false");
    cct->_conf.apply_changes(nullptr);
  }

  /* Run get_auth_data() over a request with the given Authorization header
   * (if any) and query string, and return the value it throws, or 0. */
  int
  run(const std::map<std::string, std::string>& extra_env,
      const std::string& query)
  {
    RGWEnv env;
    env.init(cct);
    env.set("REQUEST_METHOD", "GET");
    env.set("HTTP_HOST", "rgw.example.com");
    for (const auto& [key, val] : extra_env) {
      env.set(key, val);
    }

    RGWProcessEnv penv;
    req_state s(cct, penv, &env, 0);
    if (!query.empty()) {
      s.info.args.set(query);
      s.info.args.parse(&s);
    }

    rgw::auth::s3::AWSGeneralAbstractor abstractor(cct);
    try {
      abstractor.get_auth_data(&s);
    } catch (int err) {
      return err;
    }
    return 0;
  }
};

/* [REGRESSION] a header-signed SigV4 request is not a presigned url. */
TEST_F(DisableSignatureUrl, HeaderSignedV4NotDenied)
{
  set_option(true);
  EXPECT_NE(-ERR_PRESIGNED_URL_DISABLED,
            run({{"HTTP_AUTHORIZATION", V4_HEADER}}, ""));
}

/* [REGRESSION] a header-signed SigV2 request is not a presigned url. */
TEST_F(DisableSignatureUrl, HeaderSignedV2NotDenied)
{
  set_option(true);
  EXPECT_NE(-ERR_PRESIGNED_URL_DISABLED,
            run({{"HTTP_AUTHORIZATION", V2_HEADER}}, ""));
}

/* [REGRESSION] a presigned v4 url whose signature is replayed through the
 * Authorization header is still denied, because X-Amz-Algorithm rides in
 * the query. */
TEST_F(DisableSignatureUrl, PresignedV4ReplayedInHeaderDenied)
{
  set_option(true);
  EXPECT_EQ(-ERR_PRESIGNED_URL_DISABLED,
            run({{"HTTP_AUTHORIZATION", V4_HEADER}}, V4_QUERY_NO_SIG));
}

/* [INVARIANT] a presigned v4 url is denied. */
TEST_F(DisableSignatureUrl, PresignedV4Denied)
{
  set_option(true);
  EXPECT_EQ(-ERR_PRESIGNED_URL_DISABLED, run({}, V4_QUERY));
}

/* [INVARIANT] a presigned v2 url is denied. */
TEST_F(DisableSignatureUrl, PresignedV2Denied)
{
  set_option(true);
  EXPECT_EQ(-ERR_PRESIGNED_URL_DISABLED, run({}, V2_QUERY));
}

/* [INVARIANT] with the option off nothing is denied for it. */
TEST_F(DisableSignatureUrl, OptionOffDeniesNothing)
{
  set_option(false);
  EXPECT_NE(-ERR_PRESIGNED_URL_DISABLED, run({}, V4_QUERY));
  EXPECT_NE(-ERR_PRESIGNED_URL_DISABLED,
            run({{"HTTP_AUTHORIZATION", V4_HEADER}}, V4_QUERY_NO_SIG));
}

} // anonymous namespace

int
main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
      nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  (void)cct;
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
