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

#include <algorithm>
#include <array>
#include <ctime>
#include <map>
#include <string>

#include <boost/optional.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "rgw_auth_s3.h"
#include "rgw_common.h"
#include "rgw_client_io.h"
#include "rgw_log.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"

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
    ASSERT_EQ(0, cct->_conf.set_val("rgw_sigv4_insecure", val ? "true" : "false"));
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

using Abstractor = rgw::auth::s3::AWSEngine::VersionAbstractor;
using rgw::auth::s3::AWSv4ComplMulti;
using rgw::auth::s3::get_v4_signature_from_signing_key;

const std::string VECTOR_SCOPE = "20130524/us-east-1/s3/aws4_request";
const std::string VECTOR_DATE = "20130524T000000Z";
const std::string VECTOR_PROOF =
  "AWS4-HMAC-SHA256\n20130524T000000Z\n"
  "20130524/us-east-1/s3/aws4_request\n"
  "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";

sha256_digest_t binary_signing_key()
{
  sha256_digest_t key{};
  for (size_t i = 0; i < key.SIZE; ++i) {
    key.v[i] = i;
  }
  key.v[31] = 0xff;
  return key;
}

TEST(SigV4SigningKey, PublishedAWSVectorAndLegacyRootAgree)
{
  // AWS S3 GET /test.txt example; public example secret, not a real key.
  // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
  const std::array<unsigned char, 32> bytes = {
    0xdb, 0xb8, 0x93, 0xac, 0xc0, 0x10, 0x96, 0x49,
    0x18, 0xf1, 0xfd, 0x43, 0x3a, 0xdd, 0x87, 0xc7,
    0x0e, 0x8b, 0x0d, 0xb6, 0xbe, 0x30, 0xc1, 0xfb,
    0xea, 0xfe, 0xfa, 0x5e, 0xc6, 0xba, 0x83, 0x78
  };
  sha256_digest_t key{};
  std::copy(bytes.begin(), bytes.end(), key.v);
  constexpr std::string_view expected =
    "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
  const auto derived = get_v4_signature_from_signing_key(key, VECTOR_PROOF);
  EXPECT_EQ(expected, std::string_view(derived.c_str(), derived.size()));

  const std::string root = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  RGWProcessEnv penv;
  RGWEnv env;
  env.init(g_ceph_context);
  env.set("REQUEST_URI", "/test.txt");
  req_state s(g_ceph_context, penv, &env, 0);
  const auto legacy = rgw::auth::s3::get_v4_signature(
    VECTOR_SCOPE, g_ceph_context, root, VECTOR_PROOF, &s);
  EXPECT_EQ(expected, std::string_view(legacy.c_str(), legacy.size()));
}

TEST(SigV4SigningKey, BinaryKeyIncludesNulAndHighBitBytes)
{
  // Independent Python stdlib hmac/sha256 fixture, with bytes 00..1e ff.
  const auto key = binary_signing_key();
  const auto signature = get_v4_signature_from_signing_key(key, VECTOR_PROOF);
  EXPECT_EQ("72f3dcf2cc9c3d33379d5df35149e1ec734c3adec0fa4ca82949d050f380096a",
            std::string(signature.c_str(), signature.size()));
  auto changed = key;
  changed.v[31] ^= 0x80;
  const auto other = get_v4_signature_from_signing_key(changed, VECTOR_PROOF);
  EXPECT_NE(std::string(signature.c_str(), signature.size()),
            std::string(other.c_str(), other.size()));
  const auto tampered = get_v4_signature_from_signing_key(key, VECTOR_PROOF + "x");
  EXPECT_NE(std::string(signature.c_str(), signature.size()),
            std::string(tampered.c_str(), tampered.size()));
}

class BodyClient : public rgw::io::RestfulClient {
  RGWEnv& env;
  std::string body;
  size_t position = 0;
  size_t fragment;

  int init_env(CephContext*) override { return 0; }

public:
  BodyClient(RGWEnv& env, std::string body, size_t fragment)
    : env(env), body(std::move(body)), fragment(fragment) {}

  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
  size_t send_100_continue() override { return 0; }
  size_t send_status(int, const char*) override { return 0; }
  size_t send_header(const std::string_view&, const std::string_view&) override {
    return 0;
  }
  size_t send_content_length(uint64_t) override { return 0; }
  size_t complete_header() override { return 0; }
  size_t send_body(const char*, size_t len) override { return len; }
  void flush() override {}
  size_t recv_body(char* buf, size_t max) override {
    const size_t count = std::min({max, fragment, body.size() - position});
    std::copy_n(body.data() + position, count, buf);
    position += count;
    return count;
  }
};

class SigV4DerivedRequest : public ::testing::Test {
protected:
  RGWProcessEnv penv;
  RGWEnv env;
  std::unique_ptr<req_state> s;

  void SetUp() override {
    env.init(g_ceph_context);
    env.set("REQUEST_METHOD", "PUT");
    env.set("REQUEST_URI", "/testbucket/testobj");
    env.set("SCRIPT_URI", "/testbucket/testobj");
    env.set("HTTP_HOST", "rgw.example.com");
    env.set("SERVER_PORT", "80");
    s = std::make_unique<req_state>(g_ceph_context, penv, &env, 0);
    s->op = OP_PUT;
    s->op_type = RGW_OP_PUT_OBJ;
    s->info.request_uri_aws4 = s->info.request_uri;
    ASSERT_EQ(0, g_ceph_context->_conf.set_val("rgw_sigv4_insecure", "false"));
    g_ceph_context->_conf.apply_changes(nullptr);
  }

  Abstractor::auth_data_t header_auth(const char* payload_hash,
                                    int64_t content_length) {
    const auto now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    char date[32];
    std::strftime(date, sizeof(date), "%Y%m%dT%H%M%SZ", &tm);
    env.set("HTTP_X_AMZ_DATE", date);
    env.set("HTTP_X_AMZ_CONTENT_SHA256", payload_hash);
    const std::string scope = std::string(date, 8) +
      "/test-region-1/s3/aws4_request";
    env.set("HTTP_AUTHORIZATION", "AWS4-HMAC-SHA256 Credential=example/" +
      scope + ",SignedHeaders=" + BASE_SIGNED + ",Signature=" +
      std::string(64, '0'));
    s->content_length = content_length;
    rgw::auth::s3::AWSGeneralAbstractor abstractor(g_ceph_context);
    return abstractor.get_auth_data(s.get());
  }

  // Use the production filter installation and fragmented client reads. The
  // decoded length is six bytes; complete() must consume and check the terminal.
  bool complete_stream(std::string body, uint32_t flags = 0,
                       size_t fragment = 7, bool legacy = false,
                       std::string initial_seed = std::string(64, '0'),
                       size_t read_size = 6, std::string decoded_length = "6",
                       bool read_terminal = false) {
    env.set("HTTP_X_AMZ_DECODED_CONTENT_LENGTH", decoded_length);
    BodyClient client(env, std::move(body), fragment);
    RGWRestfulIO io(g_ceph_context, &client);
    s->cio = &io;
    rgw::auth::Completer::cmplptr_t completer;
    if (legacy) {
      completer = AWSv4ComplMulti::create(
        s.get(), VECTOR_DATE, VECTOR_SCOPE, initial_seed, flags,
        std::string("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
    } else {
      // Destroy all temporary date/scope/key storage before I/O starts.
      auto date = VECTOR_DATE;
      auto scope = VECTOR_SCOPE;
      auto seed = std::move(initial_seed);
      auto key = binary_signing_key();
      completer = AWSv4ComplMulti::create_from_signing_key(
        s.get(), date, scope, seed, flags, key);
      std::fill(date.begin(), date.end(), 'x');
      std::fill(scope.begin(), scope.end(), 'x');
      std::fill(seed.begin(), seed.end(), 'x');
      std::fill(std::begin(key.v), std::end(key.v), 0);
    }
    char decoded[6];
    try {
      completer->modify_request_state(s.get(), s.get());
      size_t count = 0;
      while (count < sizeof(decoded)) {
        const auto received = io.recv_body(decoded + count,
          std::min(read_size, sizeof(decoded) - count));
        if (received == 0) {
          return false;
        }
        count += received;
      }
      if (std::string(decoded, count) != "abcdef") {
        return false;
      }
      if (read_terminal) {
        char extra;
        if (io.recv_body(&extra, 1) != 0) {
          return false;
        }
      }
      return completer->complete();
    } catch (const rgw::io::Exception&) {
      return false;
    } catch (const int) {
      return false;
    }
  }
};

// These fixed chunk/trailer HMACs were independently calculated with Python's
// stdlib hmac, not with the production helper under test.
const std::string CHUNK1 =
  "0f3c5f9c6dbac62238e205cb23754ecfa7d9d7cd2e07caa0696691ec2756873b";
const std::string CHUNK2 =
  "377e250377254ca31f6dee17db3af5d466943899af2c26002e17f12df3b40f33";
const std::string FINAL_CHUNK =
  "14ae2b564bf0155f19bdd825dd6c2eaf57637343573c25ed8fe52eec14edb576";
const std::string SIGNED_BODY = "3;chunk-signature=" + CHUNK1 +
  "\r\nabc\r\n3;chunk-signature=" + CHUNK2 + "\r\ndef\r\n";
const std::string FINAL_FRAME = "0;chunk-signature=" + FINAL_CHUNK + "\r\n";
const std::string CHECKSUM_TRAILER = "x-amz-checksum-crc32c:U7zv8Q==\r\n";
const std::string SIGNED_TRAILER = "x-amz-trailer-signature:"
  "daa836c32abc0d65eaa7186bca964c5461918e93f7541ce86bdacf6fadab5fc0\r\n";

TEST_F(SigV4DerivedRequest, StreamOwnsDateScopeSeedAndKey)
{
  EXPECT_TRUE(complete_stream(SIGNED_BODY + FINAL_FRAME + "\r\n"));
}

TEST_F(SigV4DerivedRequest, LegacyRootCompleterStillVerifiesStream)
{
  const std::string body =
    "3;chunk-signature="
    "3bd93527583553f7cd03a10b7f1bfe545bd41298c0768f2f349da293e29240e1"
    "\r\nabc\r\n3;chunk-signature="
    "db3ac6f51e5a85d12f62e1b728078f4538616e7fc80823b28caa426137dab818"
    "\r\ndef\r\n0;chunk-signature="
    "79545635be094f8778f40f9ce94846eecaed6ac29bd1d7d9f4219de420d5d498"
    "\r\n\r\n";
  EXPECT_TRUE(complete_stream(body, 0, 7, true));
}

TEST_F(SigV4DerivedRequest, RejectsChangedChunkAndFinalSignatures)
{
  auto changed_body = SIGNED_BODY;
  changed_body[changed_body.find("abc")] = 'x';
  EXPECT_FALSE(complete_stream(changed_body + FINAL_FRAME + "\r\n"));
  auto changed_final = FINAL_FRAME;
  changed_final[changed_final.find(FINAL_CHUNK)] = '0';
  EXPECT_FALSE(complete_stream(SIGNED_BODY + changed_final + "\r\n"));
  EXPECT_FALSE(complete_stream(SIGNED_BODY));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME.substr(0, 40)));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + "\r\n", 0, 7,
                               false, std::string(64, '1')));
}

TEST_F(SigV4DerivedRequest, SignedTrailerHasIndependentValidation)
{
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                     AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  EXPECT_TRUE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
                              SIGNED_TRAILER + "\r\n", flags));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
                               "\r\n", flags));
  auto bad_trailer = SIGNED_TRAILER;
  bad_trailer[bad_trailer.find("daa8")] = '0';
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
                               bad_trailer + "\r\n", flags));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + SIGNED_TRAILER +
                               "\r\n", flags));
  auto bad_final = FINAL_FRAME;
  bad_final[bad_final.find(FINAL_CHUNK)] = '0';
  EXPECT_FALSE(complete_stream(SIGNED_BODY + bad_final + CHECKSUM_TRAILER +
                               SIGNED_TRAILER + "\r\n", flags));
}

TEST_F(SigV4DerivedRequest, UnsignedChecksumStreamDoesNotRequireSignatures)
{
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  EXPECT_TRUE(complete_stream("3\r\nabc\r\n3\r\ndef\r\n0\r\n" +
                              CHECKSUM_TRAILER + "\r\n", flags));
  EXPECT_FALSE(complete_stream("3\r\nabc\r\n3\r\ndef\r\n", flags));
}

// Independently generated HMACs cover the actual six payload bytes. Changing
// only the declared frame length must not turn these into a valid other frame.
std::string single_frame(bool legacy, std::string_view declared_length = "6")
{
  const std::string data_signature = legacy ?
    "9470d7f2a9a830b4b76b0cc2913eacd150c247554866d112a456316ba0f6f83b" :
    "aa8e4af0dc264283bc0af57d04e7e17dc1527be53939e6e2fffea36d82fe9c60";
  const std::string final_signature = legacy ?
    "baf63f59d0e527f1d333a73dd7a75feb15a2caf6eec9cc2715d249bb7ef9b040" :
    "0040666b79b52c47b9dd5eb9bce4cf4f0e9766d741c7308f3b31509df482d9d7";
  return std::string(declared_length) + ";chunk-signature=" + data_signature +
    "\r\nabcdef\r\n0;chunk-signature=" + final_signature + "\r\n\r\n";
}

TEST_F(SigV4DerivedRequest, OneByteDecodedReadsAcceptRootAndDerivedStreams)
{
  for (const bool legacy : {false, true}) {
    for (const size_t transport_fragment : {size_t{1}, size_t{7}, size_t{101}}) {
      EXPECT_TRUE(complete_stream(single_frame(legacy), 0, transport_fragment,
                                  legacy, std::string(64, '0'), 1));
      EXPECT_TRUE(complete_stream(single_frame(legacy), 0, transport_fragment,
                                  legacy, std::string(64, '0'), 1, "6", true));
    }
  }
  EXPECT_TRUE(complete_stream(SIGNED_BODY + FINAL_FRAME + "\r\n", 0, 101,
                              false, std::string(64, '0'), 1));
}

TEST_F(SigV4DerivedRequest, OneByteDecodedReadsAcceptUnsignedTrailerStream)
{
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  for (const size_t transport_fragment : {size_t{1}, size_t{7}, size_t{101}}) {
    EXPECT_TRUE(complete_stream("3\r\nabc\r\n3\r\ndef\r\n0\r\n" +
      CHECKSUM_TRAILER + "\r\n", flags, transport_fragment, false,
      std::string(64, '0'), 1));
    EXPECT_TRUE(complete_stream("3\r\nabc\r\n3\r\ndef\r\n0\r\n" +
      CHECKSUM_TRAILER + "\r\n", flags, transport_fragment, false,
      std::string(64, '0'), 1, "6", true));
  }
}

TEST_F(SigV4DerivedRequest, DeclaredChunkLengthMustEqualConsumedPayload)
{
  for (const bool legacy : {false, true}) {
    // The HMAC covers abcdef correctly, but a frame claiming seven bytes is
    // still incomplete after the six-byte caller read. This is the S2 guard.
    EXPECT_FALSE(complete_stream(single_frame(legacy, "7"), 0, 7, legacy));
    EXPECT_FALSE(complete_stream(single_frame(legacy, "5"), 0, 7, legacy));
    EXPECT_FALSE(complete_stream(single_frame(legacy, "ffffffffffffffff"),
                                 0, 7, legacy));
    EXPECT_TRUE(complete_stream(single_frame(legacy), 0, 7, legacy));
  }
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  for (const std::string length : {"5", "7"}) {
    EXPECT_FALSE(complete_stream(length + "\r\nabcdef\r\n0\r\n" +
      CHECKSUM_TRAILER + "\r\n", flags));
  }
  auto underfilled_signed = SIGNED_BODY;
  underfilled_signed[underfilled_signed.find("\r\n3;chunk-signature=") + 2] = '4';
  EXPECT_FALSE(complete_stream(underfilled_signed + FINAL_FRAME + CHECKSUM_TRAILER +
    SIGNED_TRAILER + "\r\n", AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE));
}

TEST_F(SigV4DerivedRequest, DecodedContentLengthMustMatchReturnedBytes)
{
  for (const bool legacy : {false, true}) {
    for (const std::string length : {"5", "7", "+6", "6x", " 6", "-1",
                                    "18446744073709551616"}) {
      EXPECT_FALSE(complete_stream(single_frame(legacy), 0, 7, legacy,
                                    std::string(64, '0'), 6, length));
    }
  }
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  for (const std::string length : {"5", "7", "6x", "+6"}) {
    EXPECT_FALSE(complete_stream("6\r\nabcdef\r\n0\r\n" + CHECKSUM_TRAILER +
      "\r\n", flags, 7, false, std::string(64, '0'), 6, length));
  }
}

TEST_F(SigV4DerivedRequest, TerminalValidationSurvivesSmallCallerReads)
{
  auto bad_final = FINAL_FRAME;
  bad_final[bad_final.find(FINAL_CHUNK)] = '0';
  for (const auto& terminal : {std::string{}, bad_final + "\r\n",
                              FINAL_FRAME.substr(0, 40), FINAL_FRAME,
                              FINAL_FRAME + "\r\njunk"}) {
    EXPECT_FALSE(complete_stream(SIGNED_BODY + terminal, 0, 7, false,
                                 std::string(64, '0'), 1));
  }
  EXPECT_FALSE(complete_stream(SIGNED_BODY.substr(0, SIGNED_BODY.size() - 2) +
                               FINAL_FRAME + "\r\n"));
  auto truncated_payload = SIGNED_BODY;
  truncated_payload.erase(truncated_payload.find("def") + 2, 1);
  EXPECT_FALSE(complete_stream(truncated_payload + FINAL_FRAME + "\r\n"));
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                     AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  EXPECT_TRUE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
    SIGNED_TRAILER + "\r\n", flags, 7, false, std::string(64, '0'), 1));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
    "\r\n", flags, 7, false, std::string(64, '0'), 1));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + SIGNED_TRAILER +
    "\r\n", flags, 7, false, std::string(64, '0'), 1));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
    SIGNED_TRAILER, flags));
  EXPECT_FALSE(complete_stream(SIGNED_BODY + FINAL_FRAME + CHECKSUM_TRAILER +
    SIGNED_TRAILER + "\r\njunk", flags));
  const auto unsigned_flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  EXPECT_FALSE(complete_stream("6\r\nabcdef\r\n0\r\n" + CHECKSUM_TRAILER,
                               unsigned_flags));
  EXPECT_FALSE(complete_stream("6\r\nabcdef\r\n0\r\n" + CHECKSUM_TRAILER +
    "\r\njunk", unsigned_flags));
}

// B2 fixtures use the fixed single-frame payload/final HMACs above and
// independently generated Python stdlib trailer HMACs. No private parser API.
std::string b2_signed_prefix(bool legacy)
{
  auto body = single_frame(legacy);
  body.resize(body.size() - 2); // Keep the zero-frame CRLF, not its blank line.
  return body;
}

std::string b2_trailer_mac(bool legacy, bool with_note = false)
{
  if (with_note) {
    return legacy ?
      "76cbde6a8044dda7e50a1e3649828bbe7026878768e03a43ba4cb45a9fa0a3a3" :
      "95a223a0d6947919366617416360de24f347fef3836c235f65fb80614427fc9b";
  }
  return legacy ?
    "072020126d58d9c84acba63ec948b24fc52fd5793b3a7cf7d54271393b15540d" :
    "ed4923d07e45a88dc6ffe547d1f9a4fe9452b3d8e8fbd919cbc6278cfc0f6a81";
}

TEST_F(SigV4DerivedRequest, TrailerSignatureRequiresExactFieldAndFullValue)
{
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                     AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  for (const bool legacy : {false, true}) {
    const auto prefix = b2_signed_prefix(legacy);
    const auto mac = b2_trailer_mac(legacy);
    const auto signature = "x-amz-trailer-signature:" + mac + "\r\n";
    for (const bool read_terminal : {false, true}) {
      EXPECT_TRUE(complete_stream(prefix + CHECKSUM_TRAILER + signature + "\r\n",
        flags, 7, legacy, std::string(64, '0'), 1, "6", read_terminal));
      for (const auto& bad_signature : {
           "junkx-amz-trailer-signature:" + mac + "\r\n",
           "x-amz-trailer-signature:" + mac + ":junk\r\n",
           "x-amz-trailer-signature:" + mac + "0\r\n",
           "x-amz-trailer-signature:" + mac.substr(0, 63) + "\r\n",
           "junk\r\n" + signature,
           "x-amz-trailer-signature :" + mac + "\r\n",
           " x-amz-trailer-signature:" + mac + "\r\n"}) {
        EXPECT_FALSE(complete_stream(prefix + CHECKSUM_TRAILER + bad_signature +
          "\r\n", flags, 7, legacy, std::string(64, '0'), 1, "6", read_terminal));
      }
      EXPECT_FALSE(complete_stream(prefix + signature + "\r\n", flags, 7,
        legacy, std::string(64, '0'), 1, "6", read_terminal));
    }
  }
}

TEST_F(SigV4DerivedRequest, SignedTrailerDoesNotTruncateColonValuesOrSkipLines)
{
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                     AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  for (const bool legacy : {false, true}) {
    const auto prefix = b2_signed_prefix(legacy);
    const auto signature = "x-amz-trailer-signature:" + b2_trailer_mac(legacy) +
                           "\r\n";
    for (const auto& bad_fields : {
         std::string("x-amz-checksum-crc32c:U7zv8Q==:junk\r\n"),
         "unknown:value\r\n" + CHECKSUM_TRAILER,
         CHECKSUM_TRAILER + "unknown:value\r\n",
         CHECKSUM_TRAILER + CHECKSUM_TRAILER,
         "junk" + CHECKSUM_TRAILER,
         std::string("x-amz-checksum-crc32c:U7zv8Q==\n"),
         std::string("x-amz-checksum-crc32c:U7zv8Q==\rjunk\r\n"),
         std::string("x-amz-checksum-crc32c:U7zv8Q==\r\n folded\r\n")}) {
      EXPECT_FALSE(complete_stream(prefix + bad_fields + signature + "\r\n",
        flags, 7, legacy));
    }
    EXPECT_FALSE(complete_stream(prefix + CHECKSUM_TRAILER + signature +
      signature + "\r\n", flags, 7, legacy, std::string(64, '0'), 1, "6", true));
    EXPECT_FALSE(complete_stream(prefix + signature + CHECKSUM_TRAILER + "\r\n",
      flags, 7, legacy));
    EXPECT_FALSE(complete_stream(prefix + CHECKSUM_TRAILER + signature,
      flags, 7, legacy));
    EXPECT_FALSE(complete_stream(prefix + CHECKSUM_TRAILER + signature +
      "\r\njunk", flags, 7, legacy));
  }
}

TEST_F(SigV4DerivedRequest, TrailerCanonicalNamesOwsAndRawOrderAreSupported)
{
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                     AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
  for (const bool legacy : {false, true}) {
    const auto prefix = b2_signed_prefix(legacy);
    env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
    EXPECT_TRUE(complete_stream(prefix + "X-Amz-Checksum-Crc32c: \tU7zv8Q== \t\r\n"
      "X-Amz-Trailer-Signature: \t" + b2_trailer_mac(legacy) + "\t \r\n\r\n",
      flags, 7, legacy));
    env.set("HTTP_X_AMZ_TRAILER", "x-example-note, x-amz-checksum-crc32c");
    const std::string note = "X-Example-Note: \talpha: \t beta\t \r\n";
    const auto signature = "x-amz-trailer-signature:" +
                           b2_trailer_mac(legacy, true) + "\r\n";
    for (const auto& fields : {note + CHECKSUM_TRAILER, CHECKSUM_TRAILER + note}) {
      EXPECT_TRUE(complete_stream(prefix + fields + signature + "\r\n", flags, 7,
        legacy, std::string(64, '0'), 1, "6", true));
      EXPECT_STREQ("alpha: \t beta", env.get("HTTP_X_EXAMPLE_NOTE"));
    }
  }
}

TEST_F(SigV4DerivedRequest, DeclaredTrailerNamesCannotRepeatOrDisappear)
{
  for (const bool legacy : {false, true}) {
    const auto prefix = b2_signed_prefix(legacy);
    const auto signature = "x-amz-trailer-signature:" + b2_trailer_mac(legacy) +
                           "\r\n";
    const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
                       AWSv4ComplMulti::FLAG_TRAILER_SIGNATURE;
    for (const auto* declaration : {
         "x-amz-checksum-crc32c,x-amz-checksum-crc32c",
         "x-amz-checksum-crc32c,X-Amz-Checksum-Crc32c",
         "x-amz-checksum-crc32c,x-example-missing",
         "x-amz-checksum-crc32c,",
         ""}) {
      env.set("HTTP_X_AMZ_TRAILER", declaration);
      EXPECT_FALSE(complete_stream(prefix + CHECKSUM_TRAILER + signature + "\r\n",
                                   flags, 7, legacy));
    }
  }
}

TEST_F(SigV4DerivedRequest, UnsignedTrailersRetainFullValuesAndStrictFraming)
{
  const std::string prefix = "6\r\nabcdef\r\n0\r\n";
  const auto flags = AWSv4ComplMulti::FLAG_TRAILING_CHECKSUM |
    AWSv4ComplMulti::FLAG_UNSIGNED_PAYLOAD | AWSv4ComplMulti::FLAG_UNSIGNED_CHUNKED;
  env.set("HTTP_X_AMZ_TRAILER", "x-example-note,x-amz-checksum-crc32c");
  const std::string note = "X-Example-Note: alpha: beta\r\n";
  for (const auto& fields : {note + CHECKSUM_TRAILER, CHECKSUM_TRAILER + note}) {
    EXPECT_TRUE(complete_stream(prefix + fields + "\r\n", flags, 7, false,
                                std::string(64, '0'), 1, "6", true));
    EXPECT_STREQ("alpha: beta", env.get("HTTP_X_EXAMPLE_NOTE"));
  }
  env.set("HTTP_X_AMZ_TRAILER", "x-amz-checksum-crc32c");
  for (const auto& fields : {"unknown:value\r\n" + CHECKSUM_TRAILER,
       CHECKSUM_TRAILER + CHECKSUM_TRAILER, CHECKSUM_TRAILER + "unknown:value\r\n",
       CHECKSUM_TRAILER + "x-amz-trailer-signature:" + std::string(64, '0') + "\r\n",
       std::string("x-amz-checksum-crc32c:U7zv8Q==\n"),
       std::string("x-amz-checksum-crc32c:U7zv8Q==\r\n\r\ntrailing")}) {
    EXPECT_FALSE(complete_stream(prefix + fields + "\r\n", flags));
  }
  // A generic field value may contain ':'; an unsigned checksum value is not
  // authenticated here, and must reach the object-operation checker intact.
  auto& emap = const_cast<env_map_t&>(env.get_map());
  emap.erase("HTTP_X_AMZ_CHECKSUM_CRC32C");
  EXPECT_TRUE(complete_stream(prefix + "x-amz-checksum-crc32c:U7zv8Q==:junk\r\n\r\n",
                             flags));
  EXPECT_STREQ("U7zv8Q==:junk", env.get("HTTP_X_AMZ_CHECKSUM_CRC32C"));
}

TEST_F(SigV4DerivedRequest, MetadataOwnsActualRequestScopeOnEveryPayloadBranch)
{
  for (const auto payload : {"UNSIGNED-PAYLOAD",
       "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
       "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}) {
    auto auth = header_auth(payload, 3);
    EXPECT_EQ(Abstractor::ProofKind::s3_v4, auth.proof_kind);
    ASSERT_TRUE(auth.derived_key);
    auto metadata = *auth.derived_key;
    EXPECT_EQ(env.get("HTTP_X_AMZ_DATE"), metadata.request_date);
    EXPECT_EQ(metadata.request_date.substr(0, 8), metadata.date);
    EXPECT_EQ("test-region-1", metadata.region);
    EXPECT_EQ("s3", metadata.service);
    EXPECT_EQ(metadata.date + "/test-region-1/s3/aws4_request",
              metadata.credential_scope);
    auth = {};
    env.set("HTTP_AUTHORIZATION", "destroyed");
    env.set("HTTP_X_AMZ_DATE", "destroyed");
    EXPECT_EQ(metadata.date + "/test-region-1/s3/aws4_request",
              metadata.credential_scope);
    ASSERT_TRUE(metadata.signature_factory);
    const auto signature = metadata.signature_factory(binary_signing_key(), VECTOR_PROOF);
    EXPECT_EQ("72f3dcf2cc9c3d33379d5df35149e1ec734c3adec0fa4ca82949d050f380096a",
              std::string(signature.c_str(), signature.size()));
    ASSERT_TRUE(metadata.completer_factory);
    auto completer = metadata.completer_factory(binary_signing_key());
    if (std::string_view(payload) == "UNSIGNED-PAYLOAD") {
      EXPECT_FALSE(completer);
    } else {
      EXPECT_TRUE(completer);
    }
  }
}

TEST_F(SigV4DerivedRequest, TypedSingleCompleterChecksPayload)
{
  for (const std::string body : {"abc", "abd"}) {
    auto auth = header_auth(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", 3);
    ASSERT_TRUE(auth.derived_key);
    auto completer = auth.derived_key->completer_factory(binary_signing_key());
    auth = {};
    BodyClient client(env, body, 1);
    RGWRestfulIO io(g_ceph_context, &client);
    s->cio = &io;
    completer->modify_request_state(s.get(), s.get());
    char received[3];
    size_t count = 0;
    while (count < sizeof(received)) {
      const auto read = io.recv_body(received + count, sizeof(received) - count);
      ASSERT_GT(read, 0u);
      count += read;
    }
    EXPECT_EQ(body == "abc", completer->complete());
  }
}

TEST_F(SigV4DerivedRequest, TypedStreamingFactoryOutlivesMetadata)
{
  auto auth = header_auth("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", 6);
  ASSERT_TRUE(auth.derived_key);
  const auto date = auth.derived_key->request_date;
  const auto scope = auth.derived_key->credential_scope;
  const auto key = binary_signing_key();
  // Wiring test: fixed independent vector tests above separately pin the HMAC.
  const auto sign_chunk = [&](const std::string& previous, const char* payload) {
    const std::string proof = "AWS4-HMAC-SHA256-PAYLOAD\n" + date + "\n" + scope +
      "\n" + previous + "\n" + rgw::auth::s3::AWS4_EMPTY_PAYLOAD_HASH + "\n" +
      rgw::auth::s3::calc_v4_payload_hash(payload);
    const auto signature = get_v4_signature_from_signing_key(key, proof);
    return std::string(signature.c_str(), signature.size());
  };
  const auto first = sign_chunk(std::string(64, '0'), "abc");
  const auto second = sign_chunk(first, "def");
  const auto final = sign_chunk(second, "");
  auto factory = std::move(auth.derived_key->completer_factory);
  auth = {};
  env.set("HTTP_AUTHORIZATION", "destroyed");
  env.set("HTTP_X_AMZ_DATE", "destroyed");
  auto completer = factory(key);
  factory = {};
  const std::string body = "3;chunk-signature=" + first +
    "\r\nabc\r\n3;chunk-signature=" + second +
    "\r\ndef\r\n0;chunk-signature=" + final + "\r\n\r\n";
  BodyClient client(env, body, 5);
  RGWRestfulIO io(g_ceph_context, &client);
  s->cio = &io;
  completer->modify_request_state(s.get(), s.get());
  char received[6];
  ASSERT_EQ(sizeof(received), io.recv_body(received, sizeof(received)));
  EXPECT_EQ("abcdef", std::string(received, sizeof(received)));
  EXPECT_TRUE(completer->complete());
}

TEST_F(SigV4DerivedRequest, QueryMetadataAndCanonicalRequestAreFresh)
{
  auto header = header_auth("UNSIGNED-PAYLOAD", 0);
  ASSERT_TRUE(header.derived_key);
  const auto date = header.derived_key->request_date;
  const auto scope = header.derived_key->credential_scope;
  env.set("HTTP_AUTHORIZATION", "");
  // These are query parameters, not additional x-amz request headers.
  env.set("HTTP_X_AMZ_DATE", "");
  env.set("HTTP_X_AMZ_CONTENT_SHA256", "");
  auto& emap = const_cast<env_map_t&>(env.get_map());
  emap.erase("HTTP_X_AMZ_DATE");
  emap.erase("HTTP_X_AMZ_CONTENT_SHA256");
  s->info.args.append("x-amz-algorithm", "AWS4-HMAC-SHA256");
  s->info.args.append("x-amz-credential", "example/" + scope);
  s->info.args.append("x-amz-date", date);
  s->info.args.append("x-amz-expires", "600");
  s->info.args.append("x-amz-signedheaders", "host");
  s->info.args.append("x-amz-signature", std::string(64, '0'));
  rgw::auth::s3::AWSGeneralAbstractor abstractor(g_ceph_context);
  const auto query = abstractor.get_auth_data(s.get());
  EXPECT_EQ(Abstractor::ProofKind::s3_v4, query.proof_kind);
  ASSERT_TRUE(query.derived_key);
  EXPECT_EQ(scope, query.derived_key->credential_scope);
  EXPECT_EQ(date, query.derived_key->request_date);
  s->info.request_uri_aws4 = "/different/object";
  const auto tampered = abstractor.get_auth_data(s.get());
  EXPECT_NE(query.string_to_sign, tampered.string_to_sign);
}

TEST_F(SigV4DerivedRequest, GeneralV2IsExplicitAndHasNoDerivedKey)
{
  env.set("HTTP_AUTHORIZATION", "");
  const auto expires = std::to_string(std::time(nullptr) + 600);
  s->info.args.append("AWSAccessKeyId", "example");
  s->info.args.append("Signature", "example-signature");
  s->info.args.append("Expires", expires);
  rgw::auth::s3::AWSGeneralAbstractor abstractor(g_ceph_context);
  const auto auth = abstractor.get_auth_data(s.get());
  EXPECT_EQ(Abstractor::ProofKind::s3_v2, auth.proof_kind);
  EXPECT_FALSE(auth.derived_key);
  EXPECT_NE(std::string::npos, auth.string_to_sign.find(expires));
}

TEST_F(SigV4DerivedRequest, BrowserPolicyIsNotGeneralV2Proof)
{
  rgw::auth::s3::AWSBrowserUploadAbstractor browser(g_ceph_context);
  const auto auth = browser.get_auth_data(s.get());
  EXPECT_EQ(Abstractor::ProofKind::browser_v2, auth.proof_kind);
  EXPECT_FALSE(auth.derived_key);
  EXPECT_TRUE(auth.signature_factory);
}

class FixedAbstractor : public Abstractor {
public:
  auth_data_t get_auth_data(const req_state*) const override {
    return {"access", "signature", {}, "proof", {}, {}, ProofKind::s3_v4, {}};
  }
};

class LegacyDispatchEngine : public rgw::auth::s3::AWSEngine {
public:
  LegacyDispatchEngine(CephContext* cct, const Abstractor& abstractor)
    : AWSEngine(cct, abstractor) {}
  using AWSEngine::authenticate;
  mutable bool legacy_called = false;
  const char* get_name() const noexcept override { return "legacy-test"; }

protected:
  result_t authenticate(const DoutPrefixProvider*, const std::string_view&,
                        const std::string_view&, const std::string_view&,
                        const string_to_sign_t&, const signature_factory_t&,
                        const completer_factory_t&, const req_state*,
                        optional_yield) const override {
    legacy_called = true;
    return result_t::deny();
  }
};

class MetadataDispatchEngine : public LegacyDispatchEngine {
public:
  using LegacyDispatchEngine::LegacyDispatchEngine;
  using LegacyDispatchEngine::authenticate;
  mutable Abstractor::ProofKind seen = Abstractor::ProofKind::unknown;

protected:
  result_t authenticate(const DoutPrefixProvider*,
                        const Abstractor::auth_data_t& auth,
                        const req_state*, optional_yield) const override {
    seen = auth.proof_kind;
    return result_t::deny();
  }
};

TEST(SigV4SigningKey, MetadataDispatchRetainsLegacyVirtualCompatibility)
{
  FixedAbstractor abstractor;
  LegacyDispatchEngine legacy(g_ceph_context, abstractor);
  legacy.authenticate(nullptr, nullptr, null_yield);
  EXPECT_TRUE(legacy.legacy_called);
  MetadataDispatchEngine migrated(g_ceph_context, abstractor);
  migrated.authenticate(nullptr, nullptr, null_yield);
  EXPECT_EQ(Abstractor::ProofKind::s3_v4, migrated.seen);
  EXPECT_FALSE(migrated.legacy_called);
}

TEST(SigV4SigningKey, SixFieldLegacyAggregateDefaultsToUnknown)
{
  Abstractor::auth_data_t legacy{"access", "signature", {}, "proof", {}, {}};
  EXPECT_EQ(Abstractor::ProofKind::unknown, legacy.proof_kind);
  EXPECT_FALSE(legacy.derived_key);
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
