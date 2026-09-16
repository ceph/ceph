/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "gtest/gtest.h"

#include <string_view>

#include "rgw_common.h"

namespace {

static_assert(requires(const RGWHTTPArgs::name_value_map& args,
                       std::string_view name) {
  args.find(name);
});

TEST(RGWRest, HttpArgsTracksSubresources)
{
  RGWHTTPArgs args;
  args.append("acl", "");
  args.append("uploads", "1");
  args.append("plain", "value");

  EXPECT_TRUE(args.sub_resource_exists("acl"));
  EXPECT_TRUE(args.sub_resource_exists("uploads"));
  EXPECT_FALSE(args.sub_resource_exists("plain"));
}

TEST(RGWRest, HttpArgsTracksResponseModifiers)
{
  RGWHTTPArgs args;
  args.append("response-content-type", "text/plain");

  EXPECT_TRUE(args.sub_resource_exists("response-content-type"));
  EXPECT_TRUE(args.has_response_modifier());
}

TEST(RGWRest, HttpArgsPreservesSingleAdminSubresource)
{
  RGWHTTPArgs args;
  args.append("subuser", "one");
  args.append("key", "two");

  EXPECT_TRUE(args.sub_resource_exists("subuser"));
  EXPECT_FALSE(args.sub_resource_exists("key"));
}

TEST(RGWRest, HttpArgsRemoveReopensAdminSubresourceSlot)
{
  RGWHTTPArgs args;
  args.append("subuser", "one");
  args.remove("subuser");
  args.append("key", "two");

  EXPECT_FALSE(args.sub_resource_exists("subuser"));
  EXPECT_TRUE(args.sub_resource_exists("key"));
}

TEST(RGWRest, HttpArgsSetResetsCachedState)
{
  RGWHTTPArgs args;
  args.append("subuser", "one");
  args.append("response-content-type", "text/plain");
  args.append("rgwx-control", "system");

  args.set("?key=two");
  ASSERT_EQ(0, args.parse(nullptr));

  bool exists = true;
  EXPECT_FALSE(args.sub_resource_exists("subuser"));
  EXPECT_TRUE(args.sub_resource_exists("key"));
  EXPECT_FALSE(args.has_response_modifier());
  EXPECT_EQ("", args.sys_get("rgwx-control", &exists));
  EXPECT_FALSE(exists);
}

TEST(RGWRest, HttpArgsParsesQueryAndLowercasesAmzNames)
{
  RGWHTTPArgs args;

  args.set("?X-Amz-Foo=bar&uploadId=123&password=secret");
  ASSERT_EQ(0, args.parse(nullptr));

  bool exists = false;
  EXPECT_EQ("bar", args.get("x-amz-foo", &exists));
  EXPECT_TRUE(exists);
  EXPECT_TRUE(args.sub_resource_exists("uploadId"));
  EXPECT_EQ("secret", args.get("password"));
}

TEST(RGWRest, HttpArgsFindsUncachedArgsByStringView)
{
  RGWHTTPArgs args;
  args.append("plain", "value");

  const std::string storage = "xxplainyy";
  const std::string_view name { storage.data() + 2, 5 };

  EXPECT_TRUE(args.exists(name));
  EXPECT_EQ("value", args.get(name));
  const auto value = args.get_optional(name);
  ASSERT_TRUE(value);
  EXPECT_EQ("value", *value);
}

TEST(RGWRest, HttpArgsTracksCachedOrdinaryArgs)
{
  RGWHTTPArgs args;
  args.append("bulk-delete", "");
  args.append("extract-archive", "");
  args.append("format", "json");
  args.append("multipart-manifest", "delete");
  args.append("replication", "");
  args.append("restore", "");

  using enum RGWHTTPArgs::http_arg;

  EXPECT_TRUE(args.exists("bulk-delete"));
  EXPECT_TRUE(args.exists("extract-archive"));
  EXPECT_TRUE(args.exists("format"));
  EXPECT_TRUE(args.exists("multipart-manifest"));
  EXPECT_TRUE(args.exists("replication"));
  EXPECT_TRUE(args.exists("restore"));
  EXPECT_TRUE(args.exists(bulk_delete));
  EXPECT_TRUE(args.exists(extract_archive));
  EXPECT_TRUE(args.exists(format));
  EXPECT_TRUE(args.exists(multipart_manifest));
  EXPECT_TRUE(args.exists(replication));
  EXPECT_TRUE(args.exists(restore));
  EXPECT_FALSE(args.sub_resource_exists("bulk-delete"));
  EXPECT_FALSE(args.sub_resource_exists("extract-archive"));
  EXPECT_FALSE(args.sub_resource_exists("format"));
  EXPECT_FALSE(args.sub_resource_exists("multipart-manifest"));
  EXPECT_FALSE(args.sub_resource_exists("replication"));
  EXPECT_FALSE(args.sub_resource_exists("restore"));
  EXPECT_FALSE(args.sub_resource_exists(bulk_delete));
  EXPECT_FALSE(args.sub_resource_exists(extract_archive));
  EXPECT_FALSE(args.sub_resource_exists(format));
  EXPECT_FALSE(args.sub_resource_exists(multipart_manifest));
  EXPECT_FALSE(args.sub_resource_exists(replication));
  EXPECT_FALSE(args.sub_resource_exists(restore));
}

TEST(RGWRest, HttpArgsUpdatesCachedClassificationOnRemove)
{
  RGWHTTPArgs args;
  args.append("uploadId", "123");

  using enum RGWHTTPArgs::http_arg;

  EXPECT_TRUE(args.exists("uploadId"));
  EXPECT_TRUE(args.sub_resource_exists("uploadId"));
  EXPECT_TRUE(args.exists(upload_id));
  EXPECT_TRUE(args.sub_resource_exists(upload_id));
  EXPECT_TRUE(args.exist_obj_excl_sub_resource());

  args.remove("uploadId");

  EXPECT_FALSE(args.exists("uploadId"));
  EXPECT_FALSE(args.sub_resource_exists("uploadId"));
  EXPECT_FALSE(args.exists(upload_id));
  EXPECT_FALSE(args.sub_resource_exists(upload_id));
  EXPECT_FALSE(args.exist_obj_excl_sub_resource());
}

} // namespace
