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
#include <string>

#include "gtest/gtest.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "rgw/rgw_cksum.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using namespace rgw::cksum;

  cksum::Type t1 = cksum::Type::blake3;
  cksum::Type t2 = cksum::Type::sha1;
  cksum::Type t3 = cksum::Type::sha256;
  cksum::Type t4 = cksum::Type::sha512;
  cksum::Type t5 = cksum::Type::crc32;
  cksum::Type t6 = cksum::Type::crc32c;

  std::string dolor =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim "
    "veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea "
    "commodo consequat. Duis aute irure dolor in reprehenderit in voluptate "
    "velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
    "occaecat cupidatat non proident, sunt in culpa qui officia deserunt "
    "mollit anim id est laborum.";

TEST(RGWCksum, DigestCRC32)
{

  auto t = cksum::Type::crc32;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  //std::cout << dolor << std::endl;
  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  
  /* compare w/known value https://crccalc.com/ */
  ASSERT_EQ(cksum.hex(), "98b2c5bd" /* not quite bdc5b298 */);
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "OThiMmM1YmQ=");
}

TEST(RGWCksum, DigestCRC32c)
{

  auto t = cksum::Type::crc32c;
  DigestVariant dv = rgw::cksum::digest_factory(t);
  Digest* digest = get_digest(dv);

  ASSERT_NE(digest, nullptr);

  //std::cout << dolor << std::endl;
  digest->Update((const unsigned char *)dolor.c_str(), dolor.length());

  auto cksum = rgw::cksum::finalize_digest(digest, t);
  /* compare w/known value https://crccalc.com/ */
  ASSERT_EQ(cksum.hex(), "4b2edc95");
  /* compare w/known value https://www.base64encode.org/ */
  ASSERT_EQ(cksum.to_base64(), "NGIyZWRjOTU=");
}

TEST(RGWCksum, DigestSTR)
{
  for (auto t : {t1, t2, t3, t4, t5, t6}) {
    DigestVariant dv = rgw::cksum::digest_factory(t);
    Digest* digest = get_digest(dv);

    ASSERT_NE(digest, nullptr);

    digest->Update((const unsigned char *)dolor.c_str(), dolor.length());
      auto cksum = rgw::cksum::finalize_digest(digest, t);
      std::cout << "type: " << to_string(t)
		<< " digest: " << cksum.to_string()
		<< std::endl;
  }
}

TEST(RGWCksum, DigestBL)
{
  std::string lacrimae = dolor + dolor;

  ceph::buffer::list dolor_bl;
  for (const auto& ix : {1, 2}) {
    dolor_bl.push_back(
      buffer::create_static(dolor.length(),
			    const_cast<char*>(dolor.data())));
  }

  for (auto t : {t1, t2, t3, t4, t5, t6}) {
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
  }
}

int main(int argc, char **argv)
{

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

  return 0;
}

} /* namespace */

