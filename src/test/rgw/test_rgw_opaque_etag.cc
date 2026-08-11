// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "rgw_common.h"
#include "rgw_hex.h"

#include <cstring>
#include <string>

TEST(RGWOpaqueEtag, DashedAndUnique)
{
  const auto a = rgw_make_opaque_etag("txid-aaa", 111);
  const auto b = rgw_make_opaque_etag("txid-bbb", 111);
  const auto c = rgw_make_opaque_etag("txid-aaa", 222);

  ASSERT_NE(std::string::npos, a.find('-'));
  ASSERT_EQ(0u, a.find("mtime-"));
  ASSERT_NE(std::string::npos, a.find("-req-"));
  ASSERT_NE(a, std::string(CEPH_CRYPTO_MD5_DIGESTSIZE * 2, '0'));
  ASSERT_NE(a, b);
  ASSERT_NE(a, c);

  const auto empty_id = rgw_make_opaque_etag({}, 0);
  ASSERT_NE(std::string::npos, empty_id.find('-'));
  ASSERT_NE(std::string::npos, empty_id.find("req-"));
}

TEST(RGWOpaqueEtag, PartDigestKeepsClassicMd5)
{
  constexpr char md5hex[] = "d41d8cd98f00b204e9800998ecf8427e";
  char decoded[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};
  char via_helper[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};

  ASSERT_EQ(CEPH_CRYPTO_MD5_DIGESTSIZE,
            hex_to_buf(md5hex, decoded, CEPH_CRYPTO_MD5_DIGESTSIZE));
  rgw_part_etag_to_digest(md5hex, via_helper);
  ASSERT_EQ(0, std::memcmp(decoded, via_helper, CEPH_CRYPTO_MD5_DIGESTSIZE));
}

TEST(RGWOpaqueEtag, PartDigestKeepsCompositeMd5Prefix)
{
  // Append/MPU object ETag: hex_to_buf() fails on "-3" but still fills
  // the first 16 bytes. The helper must match that, not MD5 the whole string.
  constexpr char md5hex[] = "d41d8cd98f00b204e9800998ecf8427e";
  constexpr char composite[] = "d41d8cd98f00b204e9800998ecf8427e-3";
  char expected[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};
  char via_hex[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};
  char via_helper[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};

  ASSERT_EQ(CEPH_CRYPTO_MD5_DIGESTSIZE,
            hex_to_buf(md5hex, expected, CEPH_CRYPTO_MD5_DIGESTSIZE));
  ASSERT_LT(hex_to_buf(composite, via_hex, CEPH_CRYPTO_MD5_DIGESTSIZE), 0);
  ASSERT_EQ(0, std::memcmp(expected, via_hex, CEPH_CRYPTO_MD5_DIGESTSIZE));

  rgw_part_etag_to_digest(composite, via_helper);
  ASSERT_EQ(0, std::memcmp(expected, via_helper, CEPH_CRYPTO_MD5_DIGESTSIZE));
}

TEST(RGWOpaqueEtag, PartDigestAcceptsOpaqueDashed)
{
  const auto opaque = rgw_make_opaque_etag("tx0001", 42);
  char buf[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};

  ASSERT_LT(hex_to_buf(opaque.c_str(), buf, CEPH_CRYPTO_MD5_DIGESTSIZE), 0);

  rgw_part_etag_to_digest(opaque, buf);
  bool nonzero = false;
  for (int i = 0; i < CEPH_CRYPTO_MD5_DIGESTSIZE; ++i) {
    if (buf[i] != 0) {
      nonzero = true;
      break;
    }
  }
  ASSERT_TRUE(nonzero);

  char again[CEPH_CRYPTO_MD5_DIGESTSIZE] = {};
  rgw_part_etag_to_digest(opaque, again);
  ASSERT_EQ(0, std::memcmp(buf, again, CEPH_CRYPTO_MD5_DIGESTSIZE));
}
