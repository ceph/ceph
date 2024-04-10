// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <fstream>
#include <string>

#include "gtest/gtest.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "rgw/rgw_cksum.h"
#include "rgw/rgw_cksum_pipe.h"
#include <openssl/sha.h>
#include "rgw/rgw_hex.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using namespace rgw::cksum;

  bool verbose = false;

  cksum::Type t1 = cksum::Type::blake3;
  cksum::Type t2 = cksum::Type::sha1;
  cksum::Type t3 = cksum::Type::sha256;
  cksum::Type t4 = cksum::Type::sha512;
  cksum::Type t5 = cksum::Type::crc32;
  cksum::Type t6 = cksum::Type::crc32c;
  cksum::Type t7 = cksum::Type::xxh3;

  std::string lorem =
    "Lorem ipsum dolor sit amet";

  std::string dolor =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.)";

TEST(RGWCksum, Ctor)
{
  cksum::Cksum ck1;
  cksum::Cksum ck2(cksum::Type::none);

  auto ck3 = rgw::putobj::GetHeaderCksumResult(ck1, "");

  ASSERT_EQ(ck1.to_armor(), ck2.to_armor());
  ASSERT_EQ(ck2.to_armor(), ck3.first.to_armor());
}

TEST(RGWCksum, Output)
{
  auto o_mode = std::ios::out|std::ios::trunc;
  std::ofstream of;
  of.open("/tmp/lorem", o_mode);
  of << lorem;
  of.close();

  of.open("/tmp/dolor", o_mode);
  of << dolor;
  of.close();
}

TEST(RGWCksum, DigestCRC32)
{
  auto t = cksum::Type::crc32;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  
  /* compare w/known value https://crccalc.com/ */
  ASSERT_EQ(cksum.hex(), "98b2c5bd");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "OThiMmM1YmQ=");
  /* compare with aws-sdk-cpp encoded value */
  ASSERT_EQ(cksum.to_armor(), "mLLFvQ==");
}

TEST(RGWCksum, DigestCRC32c)
{
  auto t = cksum::Type::crc32c;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value https://crccalc.com/ */
  ASSERT_EQ(cksum.hex(), "95dc2e4b");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "OTVkYzJlNGI=");
  /* compare with aws-sdk-cpp encoded value */
  ASSERT_EQ(cksum.to_armor(), "ldwuSw==");
}

TEST(RGWCksum, DigestXXH3)
{
  auto t = cksum::Type::xxh3;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value xxhsum -H3 */
  ASSERT_EQ(cksum.hex(), "5a164e0145351d01");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "NWExNjRlMDE0NTM1MWQwMQ==");
}

TEST(RGWCksum, DigestSha1)
{
  auto t = cksum::Type::sha1;
  for (const auto input_str : {&lorem, &dolor}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest *digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)input_str->c_str(),
		   input_str->length());

    /* try by hand */
    unsigned char sha1_hash[SHA_DIGEST_LENGTH]; // == 20
    ::SHA1((unsigned char *)input_str->c_str(), input_str->length(), sha1_hash);
    // do some stuff with the hash

    char buf[20 * 2 + 1];
    memset(buf, 0, sizeof(buf));
    buf_to_hex(sha1_hash, SHA_DIGEST_LENGTH, buf);
    if (verbose) {
      std::cout << "byhand sha1 " << buf << std::endl;
    }

    auto cksum = rgw::cksum::finalize_digest(digest, t);
    if (verbose) {
      std::cout << "computed sha1: " << cksum.hex() << std::endl;
    }

    /* check match with direct OpenSSL mech */
    ASSERT_TRUE(memcmp(buf, cksum.hex().c_str(),
		       cksum.hex().length()) == 0);

    if (input_str == &lorem) {
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "38f00f8738e241daea6f37f6f55ae8414d7b0219");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"MzhmMDBmODczOGUyNDFkYWVhNmYzN2Y2ZjU1YWU4NDE0ZDdiMDIxOQ==");
    } else { // &dolor
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "cd36b370758a259b34845084a6cc38473cb95e27");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"Y2QzNmIzNzA3NThhMjU5YjM0ODQ1MDg0YTZjYzM4NDczY2I5NWUyNw==");
      /* compare with aws-sdk-cpp encoded value */
      ASSERT_EQ(cksum.to_armor(), "zTazcHWKJZs0hFCEpsw4Rzy5Xic=");
    }
  }
}

TEST(RGWCksum, DigestSha256)
{
  auto t = cksum::Type::sha256;
  for (const auto input_str : {&lorem, &dolor}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest *digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)input_str->c_str(),
		   input_str->length());

    auto cksum = rgw::cksum::finalize_digest(digest, t);
    if (verbose) {
      std::cout << "computed sha256: " << cksum.hex() << std::endl;
    }

    if (input_str == &lorem) {
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "16aba5393ad72c0041f5600ad3c2c52ec437a2f0c7fc08fadfc3c0fe9641d7a3");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"MTZhYmE1MzkzYWQ3MmMwMDQxZjU2MDBhZDNjMmM1MmVjNDM3YTJmMGM3ZmMwOGZhZGZjM2MwZmU5NjQxZDdhMw==");
    } else { // &dolor
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "2d8c2f6d978ca21712b5f6de36c9d31fa8e96a4fa5d8ff8b0188dfb9e7c171bb");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"MmQ4YzJmNmQ5NzhjYTIxNzEyYjVmNmRlMzZjOWQzMWZhOGU5NmE0ZmE1ZDhmZjhiMDE4OGRmYjllN2MxNzFiYg==");
      /* compare with aws-sdk-cpp encoded value */
      ASSERT_EQ(cksum.to_armor(), "LYwvbZeMohcStfbeNsnTH6jpak+l2P+LAYjfuefBcbs=");
    }
  }
}

TEST(RGWCksum, DigestSha512)
{
  auto t = cksum::Type::sha512;
  for (const auto input_str : {&lorem, &dolor}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest *digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)input_str->c_str(),
		   input_str->length());

    auto cksum = rgw::cksum::finalize_digest(digest, t);

    if (input_str == &lorem) {
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "b1f4aaa6b51c19ffbe4b1b6fa107be09c8acafd7c768106a3faf475b1e27a940d3c075fda671eadf46c68f93d7eabcf604bcbf7055da0dc4eae6743607a2fc3f");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"YjFmNGFhYTZiNTFjMTlmZmJlNGIxYjZmYTEwN2JlMDljOGFjYWZkN2M3NjgxMDZhM2ZhZjQ3NWIxZTI3YTk0MGQzYzA3NWZkYTY3MWVhZGY0NmM2OGY5M2Q3ZWFiY2Y2MDRiY2JmNzA1NWRhMGRjNGVhZTY3NDM2MDdhMmZjM2Y=");
    } else { // &dolor
      /* compare w/known value, openssl sha1 */
      ASSERT_EQ(cksum.hex(), "8ba760cac29cb2b2ce66858ead169174057aa1298ccd581514e6db6dee3285280ee6e3a54c9319071dc8165ff061d77783100d449c937ff1fb4cd1bb516a69b9");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"OGJhNzYwY2FjMjljYjJiMmNlNjY4NThlYWQxNjkxNzQwNTdhYTEyOThjY2Q1ODE1MTRlNmRiNmRlZTMyODUyODBlZTZlM2E1NGM5MzE5MDcxZGM4MTY1ZmYwNjFkNzc3ODMxMDBkNDQ5YzkzN2ZmMWZiNGNkMWJiNTE2YTY5Yjk=");
    }
  }
}

TEST(RGWCksum, DigestBlake3)
{
  auto t = cksum::Type::blake3;
  for (const auto input_str : {&lorem, &dolor}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest *digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)input_str->c_str(),
		   input_str->length());

    auto cksum = rgw::cksum::finalize_digest(digest, t);

    if (input_str == &lorem) {
      /* compare w/known value, b3sum */
      ASSERT_EQ(cksum.hex(), "f1da5f4e2bd5669307bcdb2e223dad05af7425207cbee59e73526235f50f76ad");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"ZjFkYTVmNGUyYmQ1NjY5MzA3YmNkYjJlMjIzZGFkMDVhZjc0MjUyMDdjYmVlNTllNzM1MjYyMzVmNTBmNzZhZA==");
    } else { // &dolor
      /* compare w/known value, b3sum */
      ASSERT_EQ(cksum.hex(), "71fe44583a6268b56139599c293aeb854e5c5a9908eca00105d81ad5e22b7bb6");
      /* compare w/known value https://www.base64encode.org/ */
      ASSERT_EQ(cksum.to_base64(),
		"NzFmZTQ0NTgzYTYyNjhiNTYxMzk1OTljMjkzYWViODU0ZTVjNWE5OTA4ZWNhMDAxMDVkODFhZDVlMjJiN2JiNg==");
    }
  }
} /* blake3 */

TEST(RGWCksum, DigestSTR)
{
  for (auto t : {t1, t2, t3, t4, t5, t6, t7}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest* digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)dolor.c_str(), dolor.length());
    auto cksum = rgw::cksum::finalize_digest(digest, t);
    if (verbose) {
      std::cout << "type: " << to_string(t)
		<< " digest: " << cksum.to_string()
		<< std::endl;
    }
  }
}

TEST(RGWCksum, DigestBL)
{
  std::string lacrimae = dolor + dolor;

  ceph::buffer::list dolor_bl;
  for ([[maybe_unused]] const auto& ix : {1, 2}) {
    dolor_bl.push_back(
      buffer::create_static(dolor.length(),
			    const_cast<char*>(dolor.data())));
  }

  for (auto t : {t1, t2, t3, t4, t5, t6, t7}) {
    DigestVariant dv1 = rgw::cksum::digest_factory(t);
    Digest* digest1 = get_digest(dv1);
    ASSERT_NE(digest1, nullptr);

    DigestVariant dv2 = rgw::cksum::digest_factory(t);
    Digest* digest2 = get_digest(dv2);
    ASSERT_NE(digest2, nullptr);

    digest1->Update((const unsigned char *)lacrimae.c_str(),
		    lacrimae.length());
    digest2->Update(dolor_bl);

    auto cksum1 = rgw::cksum::finalize_digest(digest1, t);
    auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

    ASSERT_EQ(cksum1.to_string(), cksum2.to_string());

    /* serialization */
    buffer::list bl_out;
    encode(cksum1, bl_out);

    /* unserialization */
    buffer::list bl_in;
    bl_in.append(bl_out.c_str(), bl_out.length());

    rgw::cksum::Cksum cksum3;
    auto iter = bl_in.cbegin();
    decode(cksum3, iter);

    /* all that way for a Strohs */
    ASSERT_EQ(cksum1.to_string(), cksum3.to_string());
  } /* for t1, ... */
}




  //foop
TEST(RGWCksum, CtorUnarmor)
{
  auto t = cksum::Type::sha256;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest *digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *) lorem.c_str(),
		 lorem.length());

  auto cksum1 = rgw::cksum::finalize_digest(digest, t);
  auto armored_text1 = cksum1.to_armor();
  auto cksum2 = rgw::cksum::Cksum(cksum1.type, armored_text1.c_str());

  ASSERT_EQ(armored_text1, cksum2.to_armor());
}

} /* namespace */

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  std::string val;
  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
     if (ceph_argparse_flag(args, arg_iter, "--verbose",
			    (char*) nullptr)) {
       verbose = true;
     } else {
       ++arg_iter;
     }
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
