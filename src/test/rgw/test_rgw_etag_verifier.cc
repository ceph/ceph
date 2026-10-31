// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_etag.h"
#include "rgw_etag_verifier.h"
#include "rgw_obj_manifest.h"

#include <gtest/gtest.h>

#include "common/dout.h"
#include "global/global_context.h"

using rgw::putobj::create_etag_verifier;
using rgw::putobj::etag_verifier_ptr;

namespace {

constexpr std::string_view MD5_HEX = "0123456789abcdef0123456789ABCDEF";

/// the manifest of an object stored in one piece
bufferlist atomic_manifest()
{
  RGWObjManifest manifest;
  manifest.set_trivial_rule(4 << 20, 4 << 20);
  bufferlist bl;
  encode(manifest, bl);
  return bl;
}

int create(std::string_view etag)
{
  NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);
  etag_verifier_ptr verifier;
  int r = create_etag_verifier(&dpp, g_ceph_context, nullptr,
                               atomic_manifest(), std::nullopt, verifier,
                               etag);
  if (r == 0) {
    EXPECT_NE(nullptr, verifier.get());
  }
  return r;
}

} // anonymous namespace

TEST(RgwEtagVerifier, AtomicObject)
{
  EXPECT_EQ(0, create({}));
  EXPECT_EQ(0, create(MD5_HEX));
}

TEST(RgwEtagVerifier, AtomicObjectWithMultipartEtag)
{
  // an OSD-direct PUT stores the MD5 of its stripes' CRCs and their
  // count, which no MD5 of the data can match
  const std::string etag = std::string(MD5_HEX) + "-3";
  EXPECT_EQ(-ENOTSUP, create(etag));
  // as the attr holds it, and quoted
  EXPECT_EQ(-ENOTSUP, create(std::string_view(etag.c_str(), etag.size() + 1)));
  EXPECT_EQ(-ENOTSUP, create("\"" + etag + "\""));
  EXPECT_EQ(-ENOTSUP, create(std::string(MD5_HEX) + "-10000"));
}

TEST(RgwEtagVerifier, AtomicObjectWithOtherEtag)
{
  // verified as before, and failing as before
  EXPECT_EQ(0, create(std::string(MD5_HEX) + "-"));
  EXPECT_EQ(0, create(std::string(MD5_HEX) + "-3a"));
  EXPECT_EQ(0, create(std::string(MD5_HEX.substr(1)) + "-3"));
  EXPECT_EQ(0, create("g" + std::string(MD5_HEX.substr(1)) + "-3"));
}

TEST(RgwEtag, MultipartForm)
{
  // the one test of the form, which the verifier and an out-of-band GET
  // of a whole object share
  using rgw::is_multipart_etag;
  const std::string etag = std::string(MD5_HEX) + "-3";
  EXPECT_TRUE(is_multipart_etag(etag));
  EXPECT_TRUE(is_multipart_etag(std::string_view(etag.c_str(),
                                                 etag.size() + 1)));
  EXPECT_TRUE(is_multipart_etag("\"" + etag + "\""));
  EXPECT_FALSE(is_multipart_etag(MD5_HEX));
  EXPECT_FALSE(is_multipart_etag(std::string(MD5_HEX) + "-"));
  EXPECT_FALSE(is_multipart_etag(std::string(MD5_HEX) + "-3a"));
  EXPECT_FALSE(is_multipart_etag("g" + std::string(MD5_HEX.substr(1)) +
                                 "-3"));
  EXPECT_FALSE(is_multipart_etag({}));
}
