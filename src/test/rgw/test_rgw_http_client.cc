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
  EXPECT_TRUE(ep.get_original_url().empty());
  EXPECT_TRUE(ep.get_connect_to().empty());
}

TEST(RGWEndpointTest, constructor_sets_url_and_original_url) {
  RGWEndpoint ep("http://example.com:8080");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_original_url(), "http://example.com:8080");
}

TEST(RGWEndpointTest, constructor_with_connect_to) {
  RGWEndpoint ep("http://example.com:8080", "example.com:8080:192.168.1.1:8080");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_original_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_connect_to(), "example.com:8080:192.168.1.1:8080");
}

TEST(RGWEndpointTest, set_url_on_default_constructed_sets_original) {
  RGWEndpoint ep;
  EXPECT_TRUE(ep.get_original_url().empty());

  ep.set_url("http://first.example.com");
  EXPECT_EQ(ep.get_url(), "http://first.example.com");
  EXPECT_EQ(ep.get_original_url(), "http://first.example.com");

  // Second set_url should NOT change original_url
  ep.set_url("http://second.example.com");
  EXPECT_EQ(ep.get_url(), "http://second.example.com");
  EXPECT_EQ(ep.get_original_url(), "http://first.example.com");
}

TEST(RGWEndpointTest, set_url_does_not_change_original_after_constructor) {
  RGWEndpoint ep("http://original.example.com");

  ep.set_url("http://modified.example.com");
  EXPECT_EQ(ep.get_url(), "http://modified.example.com");
  EXPECT_EQ(ep.get_original_url(), "http://original.example.com");
}

TEST(RGWEndpointTest, with_url_returns_copy_with_new_url) {
  RGWEndpoint ep("http://original.example.com");
  ep.set_connect_to("original.example.com:80:192.168.1.1:80");

  RGWEndpoint ep2 = ep.with_url("http://modified.example.com");

  // Original unchanged
  EXPECT_EQ(ep.get_url(), "http://original.example.com");
  EXPECT_EQ(ep.get_original_url(), "http://original.example.com");

  // Copy has new url but preserves original_url and connect_to
  EXPECT_EQ(ep2.get_url(), "http://modified.example.com");
  EXPECT_EQ(ep2.get_original_url(), "http://original.example.com");
  EXPECT_EQ(ep2.get_connect_to(), "original.example.com:80:192.168.1.1:80");
}

TEST(RGWEndpointTest, set_connect_to) {
  RGWEndpoint ep("http://example.com:8080");
  EXPECT_TRUE(ep.get_connect_to().empty());

  ep.set_connect_to("example.com:8080:192.168.1.1:8080");
  EXPECT_EQ(ep.get_connect_to(), "example.com:8080:192.168.1.1:8080");
}

TEST(RGWEndpointTest, add_trailing_slash) {
  RGWEndpoint ep("http://example.com:8080");
  ep.add_trailing_slash();
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/");

  // Should not add another slash
  ep.add_trailing_slash();
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/");
}

TEST(RGWEndpointTest, append_to_url) {
  RGWEndpoint ep("http://example.com:8080");
  ep.append_to_url("/path/to/resource");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080/path/to/resource");
}

// Tests for operator<<

TEST(RGWEndpointTest, ostream_operator_url_only) {
  RGWEndpoint ep("http://example.com:8080");
  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://example.com:8080");
}

TEST(RGWEndpointTest, ostream_operator_with_different_original_url) {
  RGWEndpoint ep;
  ep.set_url("http://original.example.com:8080");
  ep.set_url("http://modified.example.com:8080");  // original_url stays the same

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://modified.example.com:8080 original_url=http://original.example.com:8080");
}

TEST(RGWEndpointTest, ostream_operator_with_connect_to) {
  RGWEndpoint ep("http://example.com:8080");
  ep.set_connect_to("example.com:8080:192.168.1.1:8080");

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://example.com:8080 connect_to=example.com:8080:192.168.1.1:8080");
}

TEST(RGWEndpointTest, ostream_operator_full) {
  RGWEndpoint ep;
  ep.set_url("http://original.example.com:8080");
  ep.set_url("http://192.168.1.1:8080");
  ep.set_connect_to("original.example.com:8080:192.168.1.1:8080");

  std::ostringstream oss;
  oss << ep;
  EXPECT_EQ(oss.str(), "RGWEndpoint: url=http://192.168.1.1:8080 original_url=http://original.example.com:8080 connect_to=original.example.com:8080:192.168.1.1:8080");
}
