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

#include <array>
#include <memory>
#include <string_view>
#include <utility>

#include "common/ceph_context.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"
#include "rgw_rest_bucket_logging.h"

void parse_post_action(std::string_view post_body, req_state *s);

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

struct ParserState final {
  RGWProcessEnv penv;
  RGWEnv env;
  req_state state { g_ceph_context, penv, &env, 0 };
};

using CreateBucketLoggingOp = RGWOp *(*)();

struct BucketLoggingOpCase final {
  CreateBucketLoggingOp create;
  RGWOpType type;
  std::string_view name;
};

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

TEST(RGWRest, BucketLoggingFactoriesCreateControlOps)
{
  const std::array cases {
    BucketLoggingOpCase {
      RGWHandler_REST_BucketLogging_S3::create_get_op,
      RGW_OP_GET_BUCKET_LOGGING,
      "get_bucket_logging",
    },
    BucketLoggingOpCase {
      RGWHandler_REST_BucketLogging_S3::create_put_op,
      RGW_OP_PUT_BUCKET_LOGGING,
      "put_bucket_logging",
    },
    BucketLoggingOpCase {
      RGWHandler_REST_BucketLogging_S3::create_post_op,
      RGW_OP_POST_BUCKET_LOGGING,
      "post_bucket_logging",
    },
  };

  ParserState parser;

  for (const auto& test_case : cases) {
    const auto op = std::unique_ptr<RGWOp> { test_case.create() };
    ASSERT_NE(nullptr, op);

    op->init(nullptr, &parser.state, nullptr);

    EXPECT_EQ(test_case.type, op->get_type());
    EXPECT_EQ(test_case.name, op->name());
    EXPECT_FALSE(op->always_do_bucket_logging());
  }
}

TEST(RGWRest, PostActionParserDecodesOrdinaryArguments)
{
  ParserState parser;

  parse_post_action("Action=CreateTopic&Name=topic+one&Policy=%7B%7D", &parser.state);

  EXPECT_EQ("CreateTopic", parser.state.info.args.get("Action"));
  EXPECT_EQ("topic one", parser.state.info.args.get("Name"));
  EXPECT_EQ("{}", parser.state.info.args.get("Policy"));
  EXPECT_TRUE(parser.state.info.args.exists("PayloadHash"));
}

TEST(RGWRest, PostActionParserPreservesExistingPayloadHash)
{
  ParserState parser;
  parser.state.info.args.append("PayloadHash", "already-present");

  parse_post_action("Action=ListTopics", &parser.state);

  EXPECT_EQ("ListTopics", parser.state.info.args.get("Action"));
  EXPECT_EQ("already-present", parser.state.info.args.get("PayloadHash"));
}

TEST(RGWRest, PostActionParserAggregatesAttributesByIndex)
{
  ParserState parser;

  parse_post_action("Action=CreateTopic&Attributes.entry.2.value=value+two&"
                    "Attributes.entry.1.key=first&Attributes.entry.2.key=second&"
                    "Attributes.entry.1.value=value+one", &parser.state);

  EXPECT_EQ("CreateTopic", parser.state.info.args.get("Action"));
  EXPECT_EQ("value one", parser.state.info.args.get("first"));
  EXPECT_EQ("value two", parser.state.info.args.get("second"));
}

TEST(RGWRest, PostActionParserPreservesDotsInAttributeValues)
{
  ParserState parser;

  parse_post_action("Action=CreateTopic&Attributes.entry.1.key=endpoint&"
                    "Attributes.entry.1.value=https%3A%2F%2Fexample.com%2Fa.b.c",
                    &parser.state);

  EXPECT_EQ("https://example.com/a.b.c", parser.state.info.args.get("endpoint"));
}

TEST(RGWRest, PostActionParserPreservesMalformedAttributeEntryBehavior)
{
  ParserState parser;

  parse_post_action("Action=CreateTopic&Attributes.entry.1.unknown=value", &parser.state);

  EXPECT_TRUE(parser.state.info.args.exists(""));
  EXPECT_EQ("", parser.state.info.args.get(""));
}

TEST(RGWRest, PostActionParserAlwaysAddsPayloadHash)
{
  ParserState parser;

  parse_post_action("", &parser.state);

  EXPECT_FALSE(parser.state.info.args.exists("Action"));
  EXPECT_TRUE(parser.state.info.args.exists("PayloadHash"));
}

TEST(RGWRest, PostActionParserUsesBroadActionGate)
{
  ParserState parser;

  parse_post_action("NotAction=CreateTopic&Name=topic", &parser.state);

  EXPECT_EQ("CreateTopic", parser.state.info.args.get("NotAction"));
  EXPECT_EQ("topic", parser.state.info.args.get("Name"));
}

} // namespace
