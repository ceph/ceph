// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Tests for the SigV4 CanonicalHeaders/SignedHeaders checks that got added
 * to fix CVE-2026-54330.
 *
 * Theres basically two kinds of test in here, and its worth knowing which
 * one youre reading:
 *
 *  - REGRESSION tests, marked [REGRESSION], fail without the "unsigned
 *    content-type is legal" fix and pass with it.
 *  - INVARIANT guards, marked [INVARIANT], pass either way. Theyre only
 *    here so that a future refactor cant quietly reopen CVE-2026-54330.
 *    Please dont "simplify" them away because they look redundant, thats
 *    literally the whole point of them.
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

#include <boost/optional.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "rgw_auth_s3.h"
#include "rgw_common.h"

using rgw::auth::s3::get_v4_canonical_headers;

namespace {

/* The headers a conformant SigV4 client always signs. Per AWS, `host` plus
 * every `x-amz-*` header actually sent are the only ones that MUST be
 * signed. */
const std::string BASE_SIGNED{"host;x-amz-content-sha256;x-amz-date"};

class SigV4CanonicalHeaders : public ::testing::Test {
protected:
  /* rgw_auth_s3.cc logs through `dout_context g_ceph_context`, so the
   * tests have to run against the global context that global_init() sets
   * up in main(). */
  CephContext* cct = g_ceph_context;

  void
  SetUp() override
  {
    set_insecure(false);
  }

  void
  set_insecure(bool val)
  {
    cct->_conf.set_val("rgw_sigv4_insecure", val ? "true" : "false");
    cct->_conf.apply_changes(nullptr);
  }

  /* Build an RGWEnv shaped the way rgw::asio::ClientIO::init_env() shapes
   * it (see rgw_asio_client.cc), then run get_v4_canonical_headers() over
   * it. `extra_env` gets merged on top of a minimal well-formed request. */
  boost::optional<std::string>
  canonicalize(
      const std::map<std::string, std::string>& extra_env,
      const std::string& signedheaders,
      bool using_qs = false,
      bool force_boto2_compat = false,
      bool with_default_xamz = true)
  {
    RGWEnv env;
    env.init(cct);
    env.set("REQUEST_METHOD", "PUT");
    env.set("REQUEST_URI", "/testbucket/testobj");
    env.set("SCRIPT_URI", "/testbucket/testobj");
    env.set("HTTP_HOST", "rgw.example.com");
    /* init_env() always sets these, and the boto2-compat path reads them. */
    env.set("SERVER_PORT", "80");
    if (with_default_xamz) {
      env.set("HTTP_X_AMZ_DATE", "20260819T000000Z");
      env.set("HTTP_X_AMZ_CONTENT_SHA256", "UNSIGNED-PAYLOAD");
    }
    for (const auto& [key, val] : extra_env) {
      env.set(key, val);
    }
    req_info info(cct, &env);
    return get_v4_canonical_headers(
        cct, info, signedheaders, using_qs, force_boto2_compat);
  }

  /* A presigned request: SignedHeaders is just `host` and the X-Amz-*
   * parameters ride in the query string instead of as headers, so the
   * environment has no x-amz-* request headers of its own. */
  boost::optional<std::string>
  canonicalize_presigned(
      const std::map<std::string, std::string>& extra_env,
      bool force_boto2_compat = false)
  {
    return canonicalize(
        extra_env, "host", true /* using_qs */, force_boto2_compat,
        false /* with_default_xamz */);
  }
};

/* [INVARIANT] Sanity check: a fully-covered request gets accepted. */
TEST_F(SigV4CanonicalHeaders, FullySignedRequestIsAccepted)
{
  const auto canon = canonicalize({}, BASE_SIGNED);
  ASSERT_TRUE(canon);
  EXPECT_NE(canon->find("host:rgw.example.com\n"), std::string::npos);
}

/* [INVARIANT] `host` has to be in SignedHeaders. */
TEST_F(SigV4CanonicalHeaders, UnsignedHostIsRejected)
{
  EXPECT_FALSE(canonicalize({}, "x-amz-content-sha256;x-amz-date"));
}

/*
 * [INVARIANT] CVE-2026-54330 regression guard. This is the most important
 * test in this file.
 *
 * A presigned-URL holder must not be able to bolt on `x-amz-*` headers
 * that the signature never covered. If any of these start passing, the
 * privilege escalation is back.
 *
 * The list is deliberately weighted towards headers that grant authority
 * or change how the request gets interpreted, not just ACLs.
 */
TEST_F(SigV4CanonicalHeaders, UnsignedXAmzHeadersAreRejected)
{
  static const char* const attack_headers[] = {
      /* the original CVE vectors */
      "HTTP_X_AMZ_ACL",
      "HTTP_X_AMZ_COPY_SOURCE",
      "HTTP_X_AMZ_GRANT_FULL_CONTROL",
      "HTTP_X_AMZ_GRANT_READ",
      "HTTP_X_AMZ_GRANT_WRITE",
      "HTTP_X_AMZ_GRANT_READ_ACP",
      "HTTP_X_AMZ_GRANT_WRITE_ACP",
      "HTTP_X_AMZ_META_INJECTED",
      "HTTP_X_AMZ_METADATA_DIRECTIVE",
      /* credential inputs. x-amz-content-sha256 gets covered in the
     * presigned test instead, because BASE_SIGNED already signs it here. */
      "HTTP_X_AMZ_SECURITY_TOKEN",
      /* object-lock: an unrecoverable storage DoS if you can forge it */
      "HTTP_X_AMZ_BYPASS_GOVERNANCE_RETENTION",
      "HTTP_X_AMZ_OBJECT_LOCK_MODE",
      "HTTP_X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE",
      "HTTP_X_AMZ_OBJECT_LOCK_LEGAL_HOLD",
      /* attacker picks the key protecting the victims upload */
      "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION",
      "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID",
      "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM",
      "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY",
      "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5",
      /* body-framing switches */
      "HTTP_X_AMZ_DECODED_CONTENT_LENGTH",
      "HTTP_X_AMZ_TRAILER",
      /* stored open redirect, tagging, tiering */
      "HTTP_X_AMZ_WEBSITE_REDIRECT_LOCATION",
      "HTTP_X_AMZ_TAGGING",
      "HTTP_X_AMZ_TAGGING_DIRECTIVE",
      "HTTP_X_AMZ_STORAGE_CLASS",
      /* an unsigned checksum header isnt any more trustworthy than any other
     * unsigned x-amz- header when it shows up as a *request header* */
      "HTTP_X_AMZ_CHECKSUM_CRC32C",
  };
  for (const char* const hdr : attack_headers) {
    EXPECT_FALSE(canonicalize({{hdr, "attacker-supplied"}}, BASE_SIGNED))
        << "unsigned " << hdr << " was accepted, CVE-2026-54330 is back";
  }
}

/*
 * [INVARIANT] The presigned-URL shape specifically.
 *
 * A presigned URL signs just `host` and carries its SignedHeaders in the
 * query string (using_qs=true). Thats the exact shape CVE-2026-54330 got
 * reported against, and the fixtures default (using_qs=false) doesnt
 * exercise that path. Also covers the boto2-compat abstractor, which
 * passes force_boto2_compat=true.
 */
TEST_F(SigV4CanonicalHeaders, PresignedUnsignedXAmzHeadersAreRejected)
{
  for (const bool boto2 : {false, true}) {
    EXPECT_TRUE(canonicalize_presigned({}, boto2))
        << "a bare presigned request should still canonicalize";
    EXPECT_FALSE(
        canonicalize_presigned({{"HTTP_X_AMZ_ACL", "public-read"}}, boto2));
    EXPECT_FALSE(canonicalize_presigned(
        {{"HTTP_X_AMZ_COPY_SOURCE", "/victim/secret"}}, boto2));
    EXPECT_FALSE(canonicalize_presigned(
        {{"HTTP_X_AMZ_SECURITY_TOKEN", "forged"}}, boto2));
  }
}

/*
 * [INVARIANT] The combination case. This is the one this patch actually
 * puts at risk. An unsigned `x-amz-*` header still has to be rejected
 * when theres also an unsigned content-type in the same request. A future
 * refactor that relaxed the header checks "because content-type is
 * unsigned anyway" would reopen the CVE, and only this test would catch
 * it.
 */
TEST_F(
    SigV4CanonicalHeaders,
    UnsignedXAmzStillRejectedAlongsideUnsignedContentType)
{
  EXPECT_FALSE(canonicalize(
      {{"CONTENT_TYPE", "application/json"}, {"HTTP_X_AMZ_ACL", "public-read"}},
      BASE_SIGNED));
  EXPECT_FALSE(canonicalize(
      {{"CONTENT_TYPE", "application/json"},
       {"HTTP_X_AMZ_COPY_SOURCE", "/victim/secret"}},
      BASE_SIGNED));
  EXPECT_FALSE(canonicalize(
      {{"CONTENT_TYPE", "application/json"}, {"HTTP_X_AMZ_ACL", "public-read"}},
      "host", true, false, false));
}

/*
 * [INVARIANT] The x-amz scan pairs emap.lower_bound(), which uses the
 * maps case-INsensitive ltstr_nocase comparator, with a separate
 * boost::istarts_with guard. Thats a subtle pairing thats easy to break,
 * so pin it with a case-variant key.
 */
TEST_F(SigV4CanonicalHeaders, UnsignedXAmzRejectedRegardlessOfEnvKeyCase)
{
  EXPECT_FALSE(canonicalize({{"http_x_amz_acl", "public-read"}}, BASE_SIGNED));
  EXPECT_FALSE(canonicalize({{"Http_X_Amz_Acl", "public-read"}}, BASE_SIGNED));
}

/* [INVARIANT] rgw_sigv4_insecure=true restores the pre-CVE behaviour, and
 * has to keep doing so. Multisite upgrades depend on it. */
TEST_F(SigV4CanonicalHeaders, InsecureOptionRestoresOldBehaviour)
{
  set_insecure(true);
  EXPECT_TRUE(canonicalize({{"HTTP_X_AMZ_ACL", "public-read"}}, BASE_SIGNED));
  EXPECT_TRUE(canonicalize({{"CONTENT_TYPE", "application/json"}}, BASE_SIGNED));
}

/*
 * [REGRESSION] The real-world breakage.
 *
 * AWS documents content-type as OPTIONAL in the signature:
 *   "You must include the host header (HTTP/1.1) [...] and any `x-amz-*`
 *    headers in the signature. You can optionally include other standard
 *    headers in the signature, such as content-type."
 *   "For the purpose of calculating an authorization signature, only the
 *    host and any `x-amz-*` headers are required[.]"
 * (IAM UG, "Create a signed AWS API request")
 *
 * Real AWS S3 accepts these requests, and so does every SDK-generated
 * presigned PUT URL that a browser uploads to (of course browsers always
 * set a content-type). minio-go's streaming signer also drops content-type
 * from SignedHeaders every time (see
 * pkg/signer/request-signature-streaming.go, ignoredStreamingHeaders),
 * which is the path every minio-go PutObject over plain HTTP takes.
 */
TEST_F(SigV4CanonicalHeaders, UnsignedContentTypeIsAccepted)
{
  EXPECT_TRUE(canonicalize({{"CONTENT_TYPE", "application/json"}}, BASE_SIGNED));
  /* and in the presigned shape, which is the common case in the wild */
  EXPECT_TRUE(canonicalize_presigned({{"CONTENT_TYPE", "application/json"}}));
}

/*
 * [INVARIANT] A signed content-type still lands in CanonicalHeaders,
 * carrying the value that was actually in the request. Thats what makes a
 * tampered content-type fail: it changes the string-to-sign, so the
 * signature comparison downstream doesnt match.
 */
TEST_F(SigV4CanonicalHeaders, SignedContentTypeIsCanonicalized)
{
  const auto canon = canonicalize(
      {{"CONTENT_TYPE", "application/json"}}, "content-type;" + BASE_SIGNED);
  ASSERT_TRUE(canon);
  EXPECT_NE(canon->find("content-type:application/json\n"), std::string::npos);

  /* tampering changes the canonical string, so different string-to-sign */
  const auto tampered = canonicalize(
      {{"CONTENT_TYPE", "text/html"}}, "content-type;" + BASE_SIGNED);
  ASSERT_TRUE(tampered);
  EXPECT_NE(*canon, *tampered);
}

/*
 * [REGRESSION] The request shape minio-go emits for a small single-shot
 * PutObject over plain HTTP with TrailingHeaders enabled. Every x-amz-
 * header IS signed; only content-type isnt.
 *
 * Note this only covers the canonicalization step. The trailing checksum
 * itself rides in the aws-chunked body and gets parsed much later by
 * AWSv4ComplMulti::complete() (rgw_auth_s3.cc), which doesnt run here.
 * Thats also precisely why a trailer can never be visible to this
 * function.
 */
TEST_F(SigV4CanonicalHeaders, MinioGoStreamingTrailerPutObjectIsAccepted)
{
  const auto canon = canonicalize(
      {
          {"CONTENT_TYPE", "application/json"},
          {"CONTENT_LENGTH", "1234"},
          {"HTTP_CONTENT_ENCODING", "aws-chunked"},
          {"HTTP_X_AMZ_CONTENT_SHA256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
          {"HTTP_X_AMZ_DECODED_CONTENT_LENGTH", "1024"},
          {"HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c"},
          {"HTTP_X_AMZ_CHECKSUM_ALGORITHM", "CRC32C"},
      },
      "content-encoding;content-length;host;x-amz-checksum-algorithm;"
      "x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;"
      "x-amz-trailer");
  ASSERT_TRUE(canon);
  EXPECT_NE(
      canon->find("x-amz-trailer:x-amz-checksum-crc32c\n"), std::string::npos);
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
