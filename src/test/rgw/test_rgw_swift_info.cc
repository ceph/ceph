// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "rgw_rest_swift.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/config.h"
#include <sstream>

using namespace std;

TEST(RGWSwiftInfo, TempurlAllowedDigests)
{
  // Create a JSON formatter to capture the output
  JSONFormatter formatter;

  // Wrap in parent section like production code does
  formatter.open_object_section("info");
  RGWInfo_ObjStore_SWIFT::list_tempurl_data(formatter, g_conf(), nullptr);
  formatter.close_section();

  // Get the formatted output
  stringstream ss;
  formatter.flush(ss);
  string output = ss.str();

  // Parse the JSON output
  JSONParser parser;
  ASSERT_TRUE(parser.parse(output.c_str(), output.length()));

  // Verify the tempurl object exists (parser.find_obj searches recursively)
  JSONObj *tempurl_obj = parser.find_obj("tempurl");
  ASSERT_TRUE(tempurl_obj != nullptr);

  // Verify methods array exists and contains expected values
  JSONObj *methods_obj = tempurl_obj->find_obj("methods");
  ASSERT_TRUE(methods_obj != nullptr);

  vector<string> methods;
  JSONDecoder::decode_json("methods", methods, tempurl_obj);
  ASSERT_EQ(5u, methods.size());
  EXPECT_EQ("GET", methods[0]);
  EXPECT_EQ("HEAD", methods[1]);
  EXPECT_EQ("PUT", methods[2]);
  EXPECT_EQ("POST", methods[3]);
  EXPECT_EQ("DELETE", methods[4]);

  // Verify allowed_digests array exists and contains sha1, sha256, sha512
  JSONObj *digests_obj = tempurl_obj->find_obj("allowed_digests");
  ASSERT_TRUE(digests_obj != nullptr);

  vector<string> allowed_digests;
  JSONDecoder::decode_json("allowed_digests", allowed_digests, tempurl_obj);
  ASSERT_EQ(3u, allowed_digests.size());

  // Verify all expected digests are present (order doesn't matter)
  EXPECT_NE(find(allowed_digests.begin(), allowed_digests.end(), "sha1"), allowed_digests.end());
  EXPECT_NE(find(allowed_digests.begin(), allowed_digests.end(), "sha256"), allowed_digests.end());
  EXPECT_NE(find(allowed_digests.begin(), allowed_digests.end(), "sha512"), allowed_digests.end());
}
