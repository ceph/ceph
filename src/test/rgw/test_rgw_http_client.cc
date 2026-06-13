// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>
#include <sstream>

#include "rgw_http_client.h"

using namespace std;

// Tests for RGWEndpoint

TEST(RGWEndpointTest, default_constructor) {
  RGWEndpoint ep;
  EXPECT_TRUE(ep.get_url().empty());
  EXPECT_TRUE(ep.get_endpoint_url_lookup_id().empty());
  EXPECT_TRUE(ep.get_connect_to().empty());
}

TEST(RGWEndpointTest, constructor_sets_url_and_lookup_id) {
  RGWEndpoint ep("http://example.com:8080");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
}

TEST(RGWEndpointTest, constructor_with_connect_to) {
  RGWEndpoint ep("http://example.com:8080", "example.com:8080:192.168.1.1:8080");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
  EXPECT_EQ(ep.get_connect_to(), "example.com:8080:192.168.1.1:8080");
}

TEST(RGWEndpointTest, set_url_on_default_constructed_sets_base) {
  RGWEndpoint ep;
  EXPECT_TRUE(ep.get_endpoint_url_lookup_id().empty());

  ep.set_url("http://first.example.com");
  EXPECT_EQ(ep.get_url(), "http://first.example.com");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://first.example.com");

  // Second set_url should NOT change endpoint_url_lookup_id
  ep.set_url("http://second.example.com");
  EXPECT_EQ(ep.get_url(), "http://second.example.com");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://first.example.com");
}

TEST(RGWEndpointTest, set_path) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_path("/path/to/resource");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/path/to/resource");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
}

TEST(RGWEndpointTest, set_query) {
  RGWEndpoint ep("http://example.com:8080/path");
  ep.set_query("key=val&k2=v2");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/path?key=val&k2=v2");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080/path");
}

TEST(RGWEndpointTest, set_query_with_leading_question_mark) {
  RGWEndpoint ep("http://example.com:8080/path");
  ep.set_query("?key=val");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/path?key=val");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080/path");
}

TEST(RGWEndpointTest, set_host) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_host("bucket.example.com");
  EXPECT_EQ(ep.get_url(), "http://bucket.example.com:8080");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
}

TEST(RGWEndpointTest, add_trailing_slash) {
  RGWEndpoint ep("http://example.com:8080");
  ep.add_trailing_slash();
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");

  // Should not add another slash
  ep.add_trailing_slash();
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/");
}

TEST(RGWEndpointTest, set_query_empty_is_noop) {
  RGWEndpoint ep("http://example.com:8080/path");
  ep.set_query("");
  // Empty query should not add a '?' to the URL
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/path");
}

TEST(RGWEndpointTest, set_path_without_leading_slash) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_path("admin/bucket");
  // boost::urls should auto-add leading '/' for URLs with authority
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/admin/bucket");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
}

TEST(RGWEndpointTest, set_path_then_set_query) {
  // This mirrors the forward_request() usage pattern
  RGWEndpoint ep("http://example.com:8080");
  ep.set_path("/admin/bucket");
  ep.set_query("?max-entries=100&stats=true");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/admin/bucket?max-entries=100&stats=true");
  EXPECT_EQ(ep.get_endpoint_url_lookup_id(), "http://example.com:8080");
}

TEST(RGWEndpointTest, set_connect_to) {
  RGWEndpoint ep("http://example.com:8080");
  EXPECT_TRUE(ep.get_connect_to().empty());

  ep.set_connect_to("example.com:8080:192.168.1.1:8080");
  EXPECT_EQ(ep.get_connect_to(), "example.com:8080:192.168.1.1:8080");
}

// Tests for operator<<

TEST(RGWEndpointTest, ostream_operator_url_only) {
  RGWEndpoint ep("http://example.com:8080");
  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://example.com:8080 endpoint_url_lookup_id=http://example.com:8080 connect_to=<empty>");
}

TEST(RGWEndpointTest, ostream_operator_with_modified_url) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_path("/modified");

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://example.com:8080/modified endpoint_url_lookup_id=http://example.com:8080 connect_to=<empty>");
}

TEST(RGWEndpointTest, ostream_operator_with_connect_to) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_connect_to("example.com:8080:192.168.1.1:8080");

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://example.com:8080 endpoint_url_lookup_id=http://example.com:8080 connect_to=example.com:8080:192.168.1.1:8080");
}

TEST(RGWEndpointTest, ostream_operator_full) {
  RGWEndpoint ep;
  ep.set_url("http://original.example.com:8080");
  ep.set_url("http://192.168.1.1:8080");
  ep.set_connect_to("original.example.com:8080:192.168.1.1:8080");

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://192.168.1.1:8080 endpoint_url_lookup_id=http://original.example.com:8080 connect_to=original.example.com:8080:192.168.1.1:8080");
}
