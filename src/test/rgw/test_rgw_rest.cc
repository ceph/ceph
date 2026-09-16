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

#include <memory>
#include <string_view>
#include <utility>

#include "rgw_common.h"
#include "rgw_rest.h"

namespace {

RGWRESTMgr *register_test_resource(RGWRESTMgr& mgr, std::string resource)
{
  auto resource_mgr = std::make_unique<RGWRESTMgr>();
  auto *out = resource_mgr.get();

  mgr.register_resource(std::move(resource), std::move(resource_mgr));

  return out;
}

RGWRESTMgr *register_test_default_mgr(RGWRESTMgr& mgr)
{
  auto default_mgr = std::make_unique<RGWRESTMgr>();
  auto *out = default_mgr.get();

  mgr.register_default_mgr(std::move(default_mgr));

  return out;
}

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

TEST(RGWRest, RestManagerDoesNotConcatenateEmptyFrontendPrefix)
{
  RGWRESTMgr mgr;
  std::string out_uri;

  EXPECT_EQ(&mgr, mgr.get_manager(nullptr, "", "/bucket", &out_uri));
  EXPECT_EQ("/bucket", out_uri);
}

TEST(RGWRest, RestManagerConcatenatesNonEmptyFrontendPrefix)
{
  RGWRESTMgr mgr;
  std::string out_uri;

  EXPECT_EQ(&mgr, mgr.get_manager(nullptr, "/prefix", "/bucket", &out_uri));
  EXPECT_EQ("/prefix/bucket", out_uri);
}

TEST(RGWRest, RestManagerUsesLongestPrefixMatch)
{
  RGWRESTMgr mgr;
  register_test_resource(mgr, "admin");
  auto *usage = register_test_resource(mgr, "admin/usage");
  std::string out_uri;

  EXPECT_EQ(usage, mgr.get_manager(nullptr, "", "/admin/usage/show", &out_uri));
  EXPECT_EQ("/show", out_uri);
}

TEST(RGWRest, RestManagerRequiresPathBoundary)
{
  RGWRESTMgr mgr;
  register_test_resource(mgr, "admin");
  std::string out_uri;

  EXPECT_EQ(&mgr, mgr.get_manager(nullptr, "", "/administrator", &out_uri));
  EXPECT_EQ("/administrator", out_uri);
}

TEST(RGWRest, RestManagerUsesDefaultOnlyWhenNoRouteMatches)
{
  RGWRESTMgr mgr;
  auto *admin = register_test_resource(mgr, "admin");
  auto *default_mgr = register_test_default_mgr(mgr);
  std::string out_uri;

  EXPECT_EQ(default_mgr, mgr.get_manager(nullptr, "", "/unknown", &out_uri));
  EXPECT_EQ("/unknown", out_uri);

  EXPECT_EQ(admin, mgr.get_manager(nullptr, "", "/admin", &out_uri));
  EXPECT_EQ("", out_uri);
}

TEST(RGWRest, RestManagerReplacesDuplicateResource)
{
  RGWRESTMgr mgr;
  register_test_resource(mgr, "admin");
  auto *second = register_test_resource(mgr, "admin");
  std::string out_uri;

  EXPECT_EQ(second, mgr.get_manager(nullptr, "", "/admin", &out_uri));
  EXPECT_EQ("", out_uri);
}

TEST(RGWRest, RestManagerCreatesIntermediateManagers)
{
  RGWRESTMgr mgr;
  auto *v1 = register_test_resource(mgr, "auth/v1.0");
  std::string out_uri;

  auto *intermediate = mgr.get_manager(nullptr, "", "/auth/status", &out_uri);

  EXPECT_NE(&mgr, intermediate);
  EXPECT_NE(v1, intermediate);
  EXPECT_EQ("/status", out_uri);
}

} // namespace
