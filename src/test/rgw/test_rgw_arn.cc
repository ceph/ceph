// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_arn.h"
#include <gtest/gtest.h>

using namespace rgw;

const int BASIC_ENTRIES = 6;

const std::string basic_str[BASIC_ENTRIES] = {"arn:aws:s3:us-east-1:12345:resource",
                                  "arn:aws:s3:us-east-1:12345:resourceType/resource",
                                  "arn:aws:s3:us-east-1:12345:resourceType/resource/qualifier",
                                  "arn:aws:s3:us-east-1:12345:resourceType/resource:qualifier",
                                  "arn:aws:s3:us-east-1:12345:resourceType:resource",
                                  "arn:aws:s3:us-east-1:12345:resourceType:resource/qualifier"};

const std::string expected_basic_resource[BASIC_ENTRIES] = {"resource", 
                                                "resourceType/resource", 
                                                "resourceType/resource/qualifier",
                                                "resourceType/resource:qualifier",
                                                "resourceType:resource",
                                                "resourceType:resource/qualifier"};
TEST(TestARN, Basic)
{
  for (auto i = 0; i < BASIC_ENTRIES; ++i) {
    boost::optional<ARN> arn = ARN::parse(basic_str[i]);
    ASSERT_TRUE(arn);
    EXPECT_EQ(arn->partition, Partition::aws);
    EXPECT_EQ(arn->service, Service::s3);
    EXPECT_STREQ(arn->region.c_str(), "us-east-1");
    EXPECT_STREQ(arn->account.c_str(), "12345");
    EXPECT_STREQ(arn->resource.c_str(), expected_basic_resource[i].c_str());
  }
}

TEST(TestARN, ToString)
{
  for (auto i = 0; i < BASIC_ENTRIES; ++i) {
    boost::optional<ARN> arn = ARN::parse(basic_str[i]);
    ASSERT_TRUE(arn);
    EXPECT_STREQ(to_string(*arn).c_str(), basic_str[i].c_str());
  }
}

const std::string expected_basic_resource_type[BASIC_ENTRIES] = 
    {"", "resourceType", "resourceType", "resourceType", "resourceType", "resourceType"};
const std::string expected_basic_qualifier[BASIC_ENTRIES] = 
    {"", "", "qualifier", "qualifier", "", "qualifier"};

TEST(TestARNResource, Basic)
{
  for (auto i = 0; i < BASIC_ENTRIES; ++i) {
    boost::optional<ARN> arn = ARN::parse(basic_str[i]);
    ASSERT_TRUE(arn);
    ASSERT_FALSE(arn->resource.empty());
    boost::optional<ARNResource> resource = ARNResource::parse(arn->resource);
    ASSERT_TRUE(resource);
    EXPECT_STREQ(resource->resource.c_str(), "resource");
    EXPECT_STREQ(resource->resource_type.c_str(), expected_basic_resource_type[i].c_str());
    EXPECT_STREQ(resource->qualifier.c_str(), expected_basic_qualifier[i].c_str());
  }
}

const int EMPTY_ENTRIES = 4;

const std::string empty_str[EMPTY_ENTRIES] = {"arn:aws:s3:::resource",
                                  "arn:aws:s3::12345:resource",
                                  "arn:aws:s3:us-east-1::resource",
                                  "arn:aws:s3:us-east-1:12345:"};

TEST(TestARN, Empty)
{
  for (auto i = 0; i < EMPTY_ENTRIES; ++i) {
    boost::optional<ARN> arn = ARN::parse(empty_str[i]);
    ASSERT_TRUE(arn);
    EXPECT_EQ(arn->partition, Partition::aws);
    EXPECT_EQ(arn->service, Service::s3);
    EXPECT_TRUE(arn->region.empty() || arn->region == "us-east-1");
    EXPECT_TRUE(arn->account.empty() || arn->account == "12345");
    EXPECT_TRUE(arn->resource.empty() || arn->resource == "resource");
  }
}

const int WILDCARD_ENTRIES = 3;

const std::string wildcard_str[WILDCARD_ENTRIES] = {"arn:aws:s3:*:*:resource",
                                  "arn:aws:s3:*:12345:resource",
                                  "arn:aws:s3:us-east-1:*:resource"};

// FIXME: currently the following: "arn:aws:s3:us-east-1:12345:*"
// does not fail, even if "wildcard" is not set to "true" 

TEST(TestARN, Wildcard)
{
  for (auto i = 0; i < WILDCARD_ENTRIES; ++i) {
    EXPECT_FALSE(ARN::parse(wildcard_str[i]));
    boost::optional<ARN> arn = ARN::parse(wildcard_str[i], true);
    ASSERT_TRUE(arn);
    EXPECT_EQ(arn->partition, Partition::aws);
    EXPECT_EQ(arn->service, Service::s3);
    EXPECT_TRUE(arn->region == "*" || arn->region == "us-east-1");
    EXPECT_TRUE(arn->account == "*" || arn->account == "12345");
    EXPECT_TRUE(arn->resource == "*" || arn->resource == "resource");
  }
}

