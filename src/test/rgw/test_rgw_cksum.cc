// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <cstdint>
#include <iostream>
#include <fstream>
#include <span>
#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "rgw/rgw_cksum.h"
#include "rgw/rgw_cksum_pipe.h"
#include "rgw/rgw_rdma_cksum.h"
#include <openssl/sha.h>
#include "rgw/rgw_hex.h"

extern "C" {
#include "common/madler/crc64nvme.h"
#include "common/madler/crc32iso_hdlc.h"
#include "common/madler/crc32iscsi.h"
#include "common/spdk/crc64.h"
} // extern "C"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using namespace rgw::cksum;

  bool verbose = false;
  bool gen_test_data = false;

  cksum::Type t1 = cksum::Type::blake3;
  cksum::Type t2 = cksum::Type::sha1;
  cksum::Type t3 = cksum::Type::sha256;
  cksum::Type t4 = cksum::Type::sha512;
  cksum::Type t5 = cksum::Type::crc32;
  cksum::Type t6 = cksum::Type::crc32c;
  cksum::Type t7 = cksum::Type::xxhash3;
  cksum::Type t8 = cksum::Type::md5;
  cksum::Type t9 = cksum::Type::xxhash64;
  cksum::Type t10 = cksum::Type::xxhash128;

  std::string lorem =
    "Lorem ipsum dolor sit amet";

  std::string dolor =
    R"(Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.)";

std::string lacrimae = dolor + dolor;
std::string dolorem = dolor + lorem;

TEST(RGWCksum, Output)
{
  if (gen_test_data) {
    auto o_mode = std::ios::out|std::ios::trunc;
    std::ofstream of;

    std::cout << "writing lorem text to /tmp/lorem " << std::endl;
    of.open("/tmp/lorem", o_mode);
    of << lorem;
    of.close();

    std::cout << "writing dolor text to /tmp/dolor " << std::endl;
    of.open("/tmp/dolor", o_mode);
    of << dolor;
    of.close();

    std::cout << "writing lacrimae text to /tmp/lacrimae " << std::endl;
    of.open("/tmp/lacrimae", o_mode);
    of << lacrimae;
    of.close();

    std::cout << "writing dolorem text to /tmp/dolorem " << std::endl;
    of.open("/tmp/dolorem", o_mode);
    of << dolorem;
    of.close();
  }
}
 
TEST(RGWCksum, Ctor)
{
  cksum::Cksum ck1;
  cksum::Cksum ck2(cksum::Type::none);

  auto ck3 = rgw::putobj::GetHeaderCksumResult(ck1, "");

  ASSERT_EQ(ck1.to_armor(), ck2.to_armor());
  ASSERT_EQ(ck2.to_armor(), ck3.first.to_armor());
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

TEST(RGWCksum, DigestXXHash3)
{
  auto t = cksum::Type::xxhash3;
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

TEST(RGWCksum, DigestMD5)
{
  auto t = cksum::Type::md5;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value openssl md5 */
  ASSERT_EQ(cksum.hex(), "db89bb5ceab87f9c0fcc2ab36c189c2c");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "ZGI4OWJiNWNlYWI4N2Y5YzBmY2MyYWIzNmMxODljMmM=");
}

TEST(RGWCksum, DigestXXHash64)
{
  auto t = cksum::Type::xxhash64;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value xxhsum -H64 */
  ASSERT_EQ(cksum.hex(), "c5a8b11443765630");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "YzVhOGIxMTQ0Mzc2NTYzMA==");
}

TEST(RGWCksum, DigestXXHash128)
{
  auto t = cksum::Type::xxhash128;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value xxhsum -H128 */
  ASSERT_EQ(cksum.hex(), "0b7155cf20619db85a164e0145351d01");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "MGI3MTU1Y2YyMDYxOWRiODVhMTY0ZTAxNDUzNTFkMDE=");
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

    std::string buf;
    buf_to_hex(std::span{sha1_hash, SHA_DIGEST_LENGTH}, std::back_inserter(buf));
    if (verbose) {
      std::cout << "byhand sha1 " << buf << std::endl;
    }

    auto cksum = rgw::cksum::finalize_digest(digest, t);
    if (verbose) {
      std::cout << "computed sha1: " << cksum.hex() << std::endl;
    }

    /* check match with direct OpenSSL mech */
    ASSERT_TRUE(memcmp(buf.c_str(), cksum.hex().c_str(),
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
  for (auto t : {t1, t2, t3, t4, t5, t6, t7, t8, t9, t10}) {
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
  ceph::buffer::list dolor_bl;
  for ([[maybe_unused]] const auto& ix : {1, 2}) {
    dolor_bl.push_back(
      buffer::create_static(dolor.length(),
			    const_cast<char*>(dolor.data())));
  }

  for (auto t : {t1, t2, t3, t4, t5, t6, t7, t8, t9, t10}) {
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

TEST(RGWCksum, CRC64NVME1)
{
  /* from SPDK crc64_ut.c */
  static constexpr unsigned int buf_size = 4096;
  char buf[buf_size];
  uint64_t crc;
  unsigned int i, j;

  /* All the expected CRC values are compliant with
   * the NVM Command Set Specification 1.0c */

  /* Input buffer = 0s */
  memset(buf, 0, buf_size);
  crc = spdk_crc64_nvme(buf, buf_size, 0);
  ASSERT_TRUE(crc == 0x6482D367EB22B64E);

  /* Input buffer = 1s */
  memset(buf, 0xFF, buf_size);
  crc = spdk_crc64_nvme(buf, buf_size, 0);
  ASSERT_TRUE(crc == 0xC0DDBA7302ECA3AC);

  /* Input buffer = 0x00, 0x01, 0x02, ... */
  memset(buf, 0, buf_size);
  j = 0;
  for (i = 0; i < buf_size; i++) {
    buf[i] = (char)j;
    if (j == 0xFF) {
      j = 0;
    } else {
      j++;
    }
  }
  crc = spdk_crc64_nvme(buf, buf_size, 0);
  ASSERT_TRUE(crc == 0x3E729F5F6750449C);

  /* Input buffer = 0xFF, 0xFE, 0xFD, ... */
  memset(buf, 0, buf_size);
  j = 0xFF;
  for (i = 0; i < buf_size ; i++) {
		buf[i] = (char)j;
		if (j == 0) {
		  j = 0xFF;
		} else {
		  j--;
		}
  }
  crc = spdk_crc64_nvme(buf, buf_size, 0);
  ASSERT_TRUE(crc == 0x9A2DF64B8E9E517E);
}

TEST(RGWCksum, CRC64NVME_UNDIGEST)
{
  auto t = cksum::Type::crc64nvme;

  /* digest 1 */
  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest *digest1 = get_digest(dv1);
  ASSERT_NE(digest1, nullptr);

  digest1->Update((const unsigned char *)lacrimae.c_str(), lacrimae.length());

  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  uint64_t crc1 = rgw::digest::byteswap(std::get<uint64_t>(*cksum1.get_crc()));

  uint64_t crc2 = spdk_crc64_nvme((const unsigned char *)lacrimae.c_str(),
				  lacrimae.length(), 0ULL);
  ASSERT_EQ(crc1, crc2);
}

TEST(RGWCksum, CRC64NVME2)
{
  auto t = cksum::Type::crc64nvme;

  /* digest 1 */
  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest *digest1 = get_digest(dv1);
  ASSERT_NE(digest1, nullptr);

  digest1->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  /* the armored value produced by awscliv2 2.24.5 */
  ASSERT_EQ(cksum1.to_armor(), "wiBA+PSv41M=");

  /* digest 2 */
  DigestVariant dv2 = rgw::cksum::digest_factory(t);
  Digest* digest2 = get_digest(dv2);
  ASSERT_NE(digest2, nullptr);

  digest2->Update((const unsigned char *)lacrimae.c_str(), lacrimae.length());

  auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

  /* the armored value produced by awscliv2 2.24.5 */
  ASSERT_EQ(cksum2.to_armor(), "oa2U66pdPLk=");
}

TEST(RGWCksum, CRC64NVME_COMBINE1)
{
  /* do crc64nvme and combining by hand */

  uint64_t crc1 = spdk_crc64_nvme((const unsigned char *)dolor.c_str(),
				  dolor.length(), 0ULL);

  uint64_t crc2 = spdk_crc64_nvme((const unsigned char *)lacrimae.c_str(),
				  lacrimae.length(), 0ULL);

  uint64_t crc4 =  crc64nvme_comb(crc1, crc1, dolor.length());

  ASSERT_EQ(crc2, crc4);
}

TEST(RGWCksum, CRC64NVME_COMBINE2)
{
  /* do crc64nvme and combining by hand, non-uniform strings */

  uint64_t crc1 = spdk_crc64_nvme((const unsigned char *)dolor.c_str(),
				  dolor.length(), 0ULL);
  
  uint64_t crc2 = spdk_crc64_nvme((const unsigned char *)lorem.c_str(),
				  lorem.length(), 0ULL);

  uint64_t crc3 = spdk_crc64_nvme((const unsigned char *)dolorem.c_str(),
				  dolorem.length(), 0ULL);
  
  uint64_t crc4 = crc64nvme_comb(crc1, crc2, lorem.length());

  if (verbose) {
    std::cout << "\ncrc1/dolor: " << crc1
	      << "\ncrc2/lorem: " << crc2
	      << "\ncrc3/dolorem: " << crc3
	      << "\ncrc4/crc1+crc2: " << crc4
	      << std::endl;
  }

  ASSERT_EQ(crc3, crc4);
}

TEST(RGWCksum, CRC64NVME_COMBINE3)
{
  auto t = cksum::Type::crc64nvme;

  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest* digest1 = get_digest(dv1);
  ASSERT_NE(digest1, nullptr);

  DigestVariant dv2 = rgw::cksum::digest_factory(t);
  Digest* digest2 = get_digest(dv2);
  ASSERT_NE(digest2, nullptr);

  DigestVariant dv3 = rgw::cksum::digest_factory(t);
  Digest* digest3 = get_digest(dv3);
  ASSERT_NE(digest3, nullptr);

  /* dolor */
  digest1->Update((const unsigned char *)dolor.c_str(), dolor.length());
  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  uint64_t spdk_crc1 = spdk_crc64_nvme((const unsigned char *)dolor.c_str(),
				       dolor.length(), 0ULL);
  auto cksum_crc1 =
    rgw::digest::byteswap(std::get<uint64_t>(*cksum1.get_crc()));

  ASSERT_EQ(cksum_crc1, spdk_crc1);

  /* lorem */
  digest2->Update((const unsigned char *)lorem.c_str(), lorem.length());
  auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

  uint64_t spdk_crc2 = spdk_crc64_nvme((const unsigned char *)lorem.c_str(),
				       lorem.length(), 0ULL);
  auto cksum_crc2 =
    rgw::digest::byteswap(std::get<uint64_t>(*cksum2.get_crc()));

  ASSERT_EQ(cksum_crc2, spdk_crc2);

  /* dolorem */
  digest3->Update((const unsigned char *)dolorem.c_str(), dolorem.length());
  auto cksum3 = rgw::cksum::finalize_digest(digest3, t);

  uint64_t spdk_crc3 = spdk_crc64_nvme((const unsigned char *)dolorem.c_str(),
				       dolorem.length(), 0ULL);
  auto cksum_crc3 =
    rgw::digest::byteswap(std::get<uint64_t>(*cksum3.get_crc()));

  ASSERT_EQ(cksum_crc3, spdk_crc3);

  /* API combine check */
  auto cksum4 = rgw::cksum::combine_crc_cksum(cksum1, cksum2, lorem.length());
  ASSERT_TRUE(cksum4);

  auto cksum_crc4 =
    rgw::digest::byteswap(std::get<uint64_t>(*cksum4->get_crc()));

  auto armor3 = cksum3.to_armor();
  auto armor4 = cksum4->to_armor();

  if (verbose) {
    std::cout << "\ncrc1/dolor spdk: " << spdk_crc1
	      << " cksum_crc1: " << cksum_crc1
	      << "\ncrc2/lorem spdk: " << spdk_crc2
	      << " cksum_crc2: " << cksum_crc2
	      << "\ncrc3/dolorem spdk: " << spdk_crc3
	      << " cksum_crc3: " << cksum_crc3
	      << " cksum_crc3 armored: " << armor3
	      << "\ncrc4/crc1+crc2: " << cksum_crc4
	      << " crc4 armored: " << armor4
	      << std::endl;
  }

  /* the CRC of dolor+lorem == gf combination of cksum1 and cksum2 */
  ASSERT_EQ(cksum3.to_armor(), cksum4->to_armor());
} /* crc64nvme */

TEST(RGWCksum, CRC32_COMBINE3)
{
  auto t = cksum::Type::crc32;

  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest* digest1 = get_digest(dv1);
  ASSERT_NE(digest1, nullptr);

  DigestVariant dv2 = rgw::cksum::digest_factory(t);
  Digest* digest2 = get_digest(dv2);
  ASSERT_NE(digest2, nullptr);

  DigestVariant dv3 = rgw::cksum::digest_factory(t);
  Digest* digest3 = get_digest(dv3);
  ASSERT_NE(digest3, nullptr);

  /* dolor */
  digest1->Update((const unsigned char *)dolor.c_str(), dolor.length());
  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  uint32_t madler_crc1 =
    crc32iso_hdlc_word(0U, (const unsigned char *)dolor.c_str(),
		       dolor.length());

  auto cksum_crc1 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum1.get_crc()));

  ASSERT_EQ(cksum_crc1, madler_crc1);

  /* lorem */
  digest2->Update((const unsigned char *)lorem.c_str(), lorem.length());
  auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

  uint32_t madler_crc2 =
    crc32iso_hdlc_word(0U, (const unsigned char *)lorem.c_str(),
		       lorem.length());
  auto cksum_crc2 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum2.get_crc()));

  ASSERT_EQ(cksum_crc2, madler_crc2);

  /* dolorem */
  digest3->Update((const unsigned char *)dolorem.c_str(), dolorem.length());
  auto cksum3 = rgw::cksum::finalize_digest(digest3, t);

  uint32_t madler_crc3 =
    crc32iso_hdlc_word(0U, (const unsigned char *)dolorem.c_str(),
		       dolorem.length());
  auto cksum_crc3 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum3.get_crc()));

  ASSERT_EQ(cksum_crc3, madler_crc3);

  /* API combine check */
  auto cksum4 = rgw::cksum::combine_crc_cksum(cksum1, cksum2, lorem.length());
  ASSERT_TRUE(cksum4);

  auto cksum_crc4 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum4->get_crc()));

  if (verbose) {
    std::cout << "\ncrc1/dolor spdk: " << madler_crc1
	      << " cksum_crc1: " << cksum_crc1
	      << "\ncrc2/lorem spdk: " << madler_crc2
      	      << " cksum_crc2: " << cksum_crc2
	      << "\ncrc3/dolorem spdk: " << madler_crc3
      	      << " cksum_crc3: " << cksum_crc3
	      << "\ncrc4/crc1+crc2: " << cksum_crc4
	      << std::endl;
  }

  /* the CRC of dolor+lorem == gf combination of cksum1 and cksum2 */
  ASSERT_EQ(cksum3.to_armor(), cksum4->to_armor());
} /* crc32 */

TEST(RGWCksum, CRC32C_COMBINE3)
{
  auto t = cksum::Type::crc32c;

  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest* digest1 = get_digest(dv1);
  ASSERT_NE(digest1, nullptr);

  DigestVariant dv2 = rgw::cksum::digest_factory(t);
  Digest* digest2 = get_digest(dv2);
  ASSERT_NE(digest2, nullptr);

  DigestVariant dv3 = rgw::cksum::digest_factory(t);
  Digest* digest3 = get_digest(dv3);
  ASSERT_NE(digest3, nullptr);

  /* dolor */
  digest1->Update((const unsigned char *)dolor.c_str(), dolor.length());
  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  uint32_t madler_crc1 = crc32iscsi_word(0U, (const unsigned char *)dolor.c_str(),
				       dolor.length());

  auto cksum_crc1 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum1.get_crc()));

  ASSERT_EQ(cksum_crc1, madler_crc1);

  /* lorem */
  digest2->Update((const unsigned char *)lorem.c_str(), lorem.length());
  auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

  uint32_t madler_crc2 = crc32iscsi_word(0U, (const unsigned char *)lorem.c_str(),
					 lorem.length());
  auto cksum_crc2 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum2.get_crc()));

  ASSERT_EQ(cksum_crc2, madler_crc2);

  /* dolorem */
  digest3->Update((const unsigned char *)dolorem.c_str(), dolorem.length());
  auto cksum3 = rgw::cksum::finalize_digest(digest3, t);

  uint32_t madler_crc3 = crc32iscsi_word(0U, (const unsigned char *)dolorem.c_str(),
					 dolorem.length());
  auto cksum_crc3 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum3.get_crc()));

  ASSERT_EQ(cksum_crc3, madler_crc3);

  /* API combine check */
  auto cksum4 = rgw::cksum::combine_crc_cksum(cksum1, cksum2, lorem.length());
  ASSERT_TRUE(cksum4);

  auto cksum_crc4 =
    rgw::digest::byteswap(std::get<uint32_t>(*cksum4->get_crc()));

  if (verbose) {
    std::cout << "\ncrc1/dolor spdk: " << madler_crc1
	      << " cksum_crc1: " << cksum_crc1
	      << "\ncrc2/lorem spdk: " << madler_crc2
      	      << " cksum_crc2: " << cksum_crc2
	      << "\ncrc3/dolorem spdk: " << madler_crc3
      	      << " cksum_crc3: " << cksum_crc3
	      << "\ncrc4/crc1+crc2: " << cksum_crc4
	      << std::endl;
  }

  /* the CRC of dolor+lorem == gf combination of cksum1 and cksum2 */
  ASSERT_EQ(cksum3.to_armor(), cksum4->to_armor());
} /* crc32c */

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
  auto cksum2 = rgw::cksum::Cksum(cksum1.type, armored_text1.c_str(),
				  rgw::cksum::Cksum::CtorStyle::from_armored);

  ASSERT_EQ(armored_text1, cksum2.to_armor());
}

TEST(RGWCksum, RdmaCanonical)
{
  /* S3 over RDMA renders the canonical CRCs that the OSDs report; the
   * rendering must be the one a digest stores for the same bytes, on
   * either byte order, or a whole-object GET or an OSD-direct PUT finds
   * a mismatch */
  for (const auto* s : {&lorem, &dolor, &lacrimae}) {
    const auto* p = (const unsigned char *) s->c_str();

    auto t = cksum::Type::crc64nvme;
    DigestVariant dv1 = rgw::cksum::digest_factory(t);
    Digest* digest1 = get_digest(dv1);
    ASSERT_NE(digest1, nullptr);
    digest1->Update(p, s->length());
    auto stored1 = rgw::cksum::finalize_digest(digest1, t);
    EXPECT_EQ(stored1.to_armor(),
	      rgw::rdma::cksum_crc64nvme(
		spdk_crc64_nvme(p, s->length(), 0)).to_armor());

    t = cksum::Type::crc32c;
    DigestVariant dv2 = rgw::cksum::digest_factory(t);
    Digest* digest2 = get_digest(dv2);
    ASSERT_NE(digest2, nullptr);
    digest2->Update(p, s->length());
    auto stored2 = rgw::cksum::finalize_digest(digest2, t);
    EXPECT_EQ(stored2.to_armor(),
	      rgw::rdma::cksum_crc32c(
		rgw::rdma::crc32c_of(p, s->length())).to_armor());
  }

  /* the values that DigestCRC32c and CRC64NVME2 compare with */
  const auto* p = (const unsigned char *) dolor.c_str();
  EXPECT_EQ("wiBA+PSv41M=",
	    rgw::rdma::cksum_crc64nvme(
	      spdk_crc64_nvme(p, dolor.length(), 0)).to_armor());
  EXPECT_EQ("ldwuSw==",
	    rgw::rdma::cksum_crc32c(
	      rgw::rdma::crc32c_of(p, dolor.length())).to_armor());
}

TEST(RGWCksum, RdmaStoredOrder)
{
  /* a stored CRC holds the big-endian bytes of the CRC, on either byte
   * order */
  const uint32_t crc32 = 0x01020304;
  const uint64_t crc64 = 0x0102030405060708;
  const uint32_t stored32 = rgw::cksum::stored_crc_order(crc32);
  const uint64_t stored64 = rgw::cksum::stored_crc_order(crc64);
  const auto* b32 = reinterpret_cast<const unsigned char*>(&stored32);
  const auto* b64 = reinterpret_cast<const unsigned char*>(&stored64);
  EXPECT_EQ(0x01, b32[0]);
  EXPECT_EQ(0x04, b32[3]);
  EXPECT_EQ(0x01, b64[0]);
  EXPECT_EQ(0x08, b64[7]);
  EXPECT_EQ(crc32, rgw::cksum::stored_crc_order(stored32));
  EXPECT_EQ(crc64, rgw::cksum::stored_crc_order(stored64));
}

/* what an OSD reports of the bytes of s, delivered as one contiguous
 * extent */
librados::ObjectOperation::rdma_delivery_result
rdma_result(const std::string& s)
{
  using R = librados::ObjectOperation::rdma_delivery_result;
  const auto* p = (const unsigned char *) s.c_str();
  R r;
  r.bytes = s.length();
  r.crc64 = spdk_crc64_nvme(p, s.length(), 0);
  r.crc32c = rgw::rdma::crc32c_of(p, s.length());
  r.flags = R::COMBINABLE | R::CRC64_VALID |
    R::CRC32C_VALID;
  return r;
}

TEST(RGWCksum, RdmaDeliveredCrcs)
{
  using R = librados::ObjectOperation::rdma_delivery_result;
  const auto whole = rdma_result(dolorem);

  /* one contiguous extent: each CRC that is valid */
  auto c = rgw::rdma::delivered_crcs(whole);
  EXPECT_EQ(whole.crc64, c.crc64);
  EXPECT_EQ(whole.crc32c, c.crc32c);
  auto r = whole;
  r.flags &= ~R::CRC32C_VALID;
  c = rgw::rdma::delivered_crcs(r);
  EXPECT_EQ(whole.crc64, c.crc64);
  EXPECT_FALSE(c.crc32c);

  /* placed as ranges, in any order, that cover the bytes: their fold */
  const auto d = rdma_result(dolor);
  const auto l = rdma_result(lorem);
  r = whole;
  r.flags = R::RANGES | R::CRC64_VALID |
    R::CRC32C_VALID;
  r.ranges = {{dolor.length(), lorem.length(), l.crc64, l.crc32c},
	      {0, dolor.length(), d.crc64, d.crc32c}};
  c = rgw::rdma::delivered_crcs(r);
  EXPECT_EQ(whole.crc64, c.crc64);
  EXPECT_EQ(whole.crc32c, c.crc32c);

  /* not when they leave bytes out */
  r.bytes += 1;
  c = rgw::rdma::delivered_crcs(r);
  EXPECT_FALSE(c.crc64);
  EXPECT_FALSE(c.crc32c);
  r.bytes = whole.bytes;
  r.ranges[0].ofs += 1;
  c = rgw::rdma::delivered_crcs(r);
  EXPECT_FALSE(c.crc64);
  EXPECT_FALSE(c.crc32c);

  /* nor from a result that is neither */
  r = whole;
  r.flags &= ~R::COMBINABLE;
  c = rgw::rdma::delivered_crcs(r);
  EXPECT_FALSE(c.crc64);
  EXPECT_FALSE(c.crc32c);
}

TEST(RGWCksum, RdmaCrcFold)
{
  /* the CRCs of consecutive extents fold into those of the whole */
  const auto whole = rgw::rdma::delivered_crcs(rdma_result(dolorem));
  const auto d = rgw::rdma::delivered_crcs(rdma_result(dolor));
  const auto l = rgw::rdma::delivered_crcs(rdma_result(lorem));

  rgw::rdma::crc_fold both({true, true});
  EXPECT_FALSE(both.result().crc64);
  EXPECT_FALSE(both.result().crc32c);
  both.append(d, dolor.length());
  both.append(l, lorem.length());
  EXPECT_EQ(whole.crc64, both.result().crc64);
  EXPECT_EQ(whole.crc32c, both.result().crc32c);

  /* only those wanted */
  rgw::rdma::crc_fold only64({.crc64nvme = true});
  only64.append(d, dolor.length());
  only64.append(l, lorem.length());
  EXPECT_EQ(whole.crc64, only64.result().crc64);
  EXPECT_FALSE(only64.result().crc32c);

  /* an extent without one of them ends that one, and only that one */
  const auto all = rgw::rdma::delivered_crcs(rdma_result(dolorem + dolor));
  rgw::rdma::crc_fold cut({true, true});
  cut.append(d, dolor.length());
  cut.append({l.crc64, std::nullopt}, lorem.length());
  cut.append(d, dolor.length());
  EXPECT_EQ(all.crc64, cut.result().crc64);
  EXPECT_FALSE(cut.result().crc32c);
}

} /* namespace */

using cksum_3tuple
    = std::tuple<rgw::cksum::Cksum, rgw::cksum::Cksum, rgw::cksum::Cksum>;

cksum_3tuple
mpu_checksum_helper(cksum::Type t, uint16_t flags)
{
  DigestVariant dv1 = rgw::cksum::digest_factory(t);
  Digest* digest1 = get_digest(dv1);

  DigestVariant dv2 = rgw::cksum::digest_factory(t);
  Digest* digest2 = get_digest(dv2);

  DigestVariant dv3 = rgw::cksum::digest_factory(t);
  Digest* digest3 = get_digest(dv3);

  /* dolor */
  digest1->Update((const unsigned char *)dolor.c_str(), dolor.length());
  auto cksum1 = rgw::cksum::finalize_digest(digest1, t);

  /* lorem */
  digest2->Update((const unsigned char *)lorem.c_str(), lorem.length());
  auto cksum2 = rgw::cksum::finalize_digest(digest2, t);

  /* dolorem */
  digest3->Update((const unsigned char *)dolorem.c_str(), dolorem.length());
  auto cksum3 = rgw::cksum::finalize_digest(digest3, t);

  return cksum_3tuple(cksum1, cksum2, cksum3);
}

TEST(RGWCksum, Combiner1)
{
  using std::get;

  auto t = cksum::Type::crc64nvme;
  uint16_t flags = rgw::cksum::Cksum::FLAG_CKSUM_NONE;

  auto cksums = mpu_checksum_helper(t, flags);
  auto& [cksum1, cksum2, cksum3] = cksums;

  auto armor1 = cksum1.to_armor();
  auto armor2 = cksum2.to_armor();
  auto armor3 = cksum3.to_armor();

  if (verbose) {
    std::cout << "\ncksums: cksum type: " << to_string(t)
	      << "\narmor1: " << armor1
	      << "\narmor2: " << armor2
	      << "\narmor3: " << armor3
	      << std::endl;
  }

  auto cmbnr = rgw::cksum::CombinerFactory(t, flags);

  ASSERT_TRUE(cmbnr);
  ASSERT_EQ(t, cmbnr->get_type());

  cmbnr->append(get<0>(cksums), dolor.length());
  cmbnr->append(get<1>(cksums), lorem.length());

  /* depending on the checksum type and flags, cksum4 is
   * either equivalent to cksum3, or, a composite digest
   * of cksum1 and cksum2 */
  auto cksum4 = cmbnr->final();

  /* if cksums(0) is !composite, then cksums(2) is
   * == cksums(0) + cksums(1) and also == cksum4 */
  bool crc = get<0>(cksums).crc();
  if (crc) {
    ASSERT_EQ(get<2>(cksums).to_armor(), cksum4.to_armor());
  } else {
    /* all we can do is assert match against an external
     * reference */
  }
  if (verbose) {
  /* pretty-print armored cksum */
  std::string cksum_flags =
    (! get<0>(cksums).crc()) ? "COMPOSITE" : "FULL_OBJECT";
  std::cout << "\ncomposite cksum (delorem) "
	    << "\n\tcksum-type " << to_string(t)
	    << "\n\tflags "
	    << cksum_flags
	    << "\n\tarmored cksum " << cksum4.to_armor()
	    << std::endl;
  }
} /* Combiner1 */

using cksum_4tuple
    = std::tuple<rgw::cksum::Cksum, rgw::cksum::Cksum, rgw::cksum::Cksum,
		 rgw::cksum::Cksum>;

class CksumCombinerFixture : public testing::Test
{
public:

  static std::string long_a; // 5M string "AAAA...."
  static std::string long_b;
  static std::string long_c;

  static std::vector<cksum::Type> cksum_types;

  static void SetUpTestSuite() {

    using std::get;
    using ST = std::tuple<std::string&, std::string&>;


    /* generate unique strings that match ones we use in
     * a checksum test matrix in s3-tests */
    std::string a{"A"};
    std::string b{"B"};
    std::string c{"C"};

    for (const auto& elt : {ST(a, long_a),
			    ST(b, long_b),
			    ST(c, long_c)}) {

      for (int ix = 0; ix < (5 * 1024 * 1024); ++ix) {
	get<1>(elt) += get<0>(elt);
      }
    }

    /* generate check types */
    for (uint16_t ix = 1; ix <= uint16_t(cksum::Type::xxhash128); ++ix) {
      cksum_types.push_back(cksum::Type(ix));
    }
  } /* SetUpTestSuite */

  static cksum_4tuple mpu_checksums(cksum::Type t) {

    DigestVariant dva = rgw::cksum::digest_factory(t);
    Digest *digesta = get_digest(dva);
    digesta->Update((const unsigned char *)long_a.c_str(), long_a.length());
    auto cksum1 = rgw::cksum::finalize_digest(digesta, t);

    DigestVariant dvb = rgw::cksum::digest_factory(t);
    Digest *digestb = get_digest(dvb);
    digestb->Update((const unsigned char *)long_b.c_str(), long_b.length());
    auto cksum2 = rgw::cksum::finalize_digest(digestb, t);

    DigestVariant dvc = rgw::cksum::digest_factory(t);
    Digest *digestc = get_digest(dvc);
    digestc->Update((const unsigned char *)long_c.c_str(), long_c.length());
    auto cksum3 = rgw::cksum::finalize_digest(digestc, t);

    std::string long_d = long_a + long_b + long_c;
    DigestVariant dvd = rgw::cksum::digest_factory(t);
    Digest *digestd = get_digest(dvd);
    digestd->Update((const unsigned char *)long_d.c_str(), long_d.length());
    auto cksum4 = rgw::cksum::finalize_digest(digestd, t);

    return cksum_4tuple(cksum1, cksum2, cksum3, cksum4);
  }

  static void TearDownTestSuite() {
  }
}; /* CksumCombinerfixture */

std::string CksumCombinerFixture::long_a;
std::string CksumCombinerFixture::long_b;
std::string CksumCombinerFixture::long_c;
std::vector<cksum::Type> CksumCombinerFixture::cksum_types;

/* TODO: multipart test matrix using fixture */
TEST_F(CksumCombinerFixture, Test1) {

  for (const auto t : CksumCombinerFixture::cksum_types) {
    using std::get;

    auto cksums = CksumCombinerFixture::mpu_checksums(t);

    auto cksum1 = get<0>(cksums);
    auto flags = cksum1.flags;
    auto cmbnr = rgw::cksum::CombinerFactory(t, flags);

    ASSERT_TRUE(cmbnr);
    ASSERT_EQ(t, cmbnr->get_type());

    auto lena = CksumCombinerFixture::long_a.length();
    auto lenb = CksumCombinerFixture::long_b.length();
    auto lenc = CksumCombinerFixture::long_c.length();

    ASSERT_EQ(lena, lenb);
    ASSERT_EQ(lenb, lenc);
    ASSERT_EQ(lenc, (5*1024*1024));

    cmbnr->append(get<0>(cksums), lena);
    cmbnr->append(get<1>(cksums), lena);
    cmbnr->append(get<2>(cksums), lena);

    /* depending on the checksum type and flags, cksum4 is
     * either equivalent to cksum3, or, a composite digest
     * of cksum1 and cksum2 */
    auto cksum4 = cmbnr->final();

    /* if cksums(0) is !composite, then cksums(3) is
     * == SUM(cksums(0)..cksums(2)) and also == cksum4 */
    if (get<0>(cksums).crc()) {
      ASSERT_EQ(get<3>(cksums).to_armor(), cksum4.to_armor());
  } else {
    /* all we can do is assert match against an external
     * reference */
  }
  /* pretty-print armored cksum */
  std::string cksum_flags =
    (! get<0>(cksums).crc()) ? "COMPOSITE" : "FULL_OBJECT";
  std::cout << "\ncomposite cksum (long_a+long_b+long_c) "
	    << "\n\tcksum-type " << to_string(t)
	    << "\n\tarmored cksum1 " << get<0>(cksums).to_armor()
	    << "\n\tarmored cksum2 " << get<1>(cksums).to_armor()
	    << "\n\tarmored cksum3 " << get<2>(cksums).to_armor()
	    << "\n\tflags "
	    << cksum_flags
	    << "\n\tarmored cksum4 (composite) " << cksum4.to_armor()
	    << std::endl;
  }
} /* CksumCombinerFixture, Test1 */


int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
     if (ceph_argparse_flag(args, arg_iter, "--verbose",
			    (char*) nullptr)) {
       verbose = true;
     } else if (ceph_argparse_flag(args, arg_iter, "--gen_test_data",
				   (char*) nullptr)) {
       gen_test_data = true;
     } else {
       ++arg_iter;
     }
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
