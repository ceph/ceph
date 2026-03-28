// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "rgw_rest_conn.h"
#include "rgw_resolve.h"

using namespace std;
using ::testing::_;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::Return;

// Mock RGWResolver to control DNS resolution behavior
class MockRGWResolver : public RGWResolver {
public:
  MockRGWResolver() : RGWResolver() {}

  MOCK_METHOD(int, resolve_all_addrs, (const std::string& hostname,
                                       std::vector<entity_addr_t>* addrs), (override));
};

struct RGWRESTConnTest : public ::testing::Test {
  boost::intrusive_ptr<CephContext> cct;

protected:

  MockRGWResolver* mock_resolver;
  RGWResolver* original_resolver;

  void SetUp() override {
    if (!cct) {
      cct.reset(new CephContext(CEPH_ENTITY_TYPE_ANY), false);
    }
    cct.get()->_conf->rgw_rest_conn_connect_to_resolved_ips = true;

    mock_resolver = new MockRGWResolver();
    // Save original resolver and replace with mock
    original_resolver = rgw_resolver;
    rgw_resolver = mock_resolver;
  }

  void TearDown() override {
    // Restore original resolver
    rgw_resolver = original_resolver;
    delete mock_resolver;
    cct.reset();
  }
};

TEST_F(RGWRESTConnTest, resolve_endpoints_single_ipv4) {
  // Setup mock to return a single IPv4 address
  entity_addr_t addr;
  addr.parse("192.168.1.100");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"http://example.com:8080"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("http://example.com:8080");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;
  EXPECT_EQ(res_ep.host, "example.com");
  EXPECT_EQ(res_ep.scheme, "http");
  EXPECT_EQ(res_ep.port, 8080);
  EXPECT_EQ(res_ep.resolved_ips.size(), 1u);
  ASSERT_EQ(res_ep.resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep.resolved_ips[0].connect_to, "example.com:8080:192.168.1.100:8080");
  EXPECT_TRUE(ceph::real_clock::is_zero(res_ep.resolved_ips[0].last_failure.load()));
}

TEST_F(RGWRESTConnTest, resolve_endpoints_multiple_ips) {
  // Setup mock to return multiple IP addresses
  entity_addr_t addr1, addr2, addr3;
  addr1.parse("192.168.1.100");
  addr2.parse("192.168.1.101");
  addr3.parse("2001:db8::1");
  std::vector<entity_addr_t> mock_addrs = {addr1, addr2, addr3};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("multi.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"https://multi.example.com"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("https://multi.example.com");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;
  EXPECT_EQ(res_ep.host, "multi.example.com");
  EXPECT_EQ(res_ep.scheme, "https");
  EXPECT_EQ(res_ep.port, 443);  // Default HTTPS port
  EXPECT_EQ(res_ep.resolved_ips.size(), 3u);
  ASSERT_EQ(res_ep.resolved_ips.size(), 3u);


  // Verify all connect_to strings
  std::set<std::string> expected_connect_to = {
    "multi.example.com:443:192.168.1.100:443",
    "multi.example.com:443:192.168.1.101:443",
    "multi.example.com:443:2001:db8::1:443"
  };
  std::set<std::string> actual_connect_to;
  for (const auto& ip_status : res_ep.resolved_ips) {
    actual_connect_to.insert(ip_status.connect_to);
  }
  EXPECT_EQ(actual_connect_to, expected_connect_to);
}

TEST_F(RGWRESTConnTest, resolve_endpoints_default_http_port) {
  // Setup mock resolver to return an IP addr
  entity_addr_t addr;
  addr.parse("10.0.0.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("noport.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"http://noport.example.com"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("http://noport.example.com");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;
  EXPECT_EQ(res_ep.port, 80);  // Default HTTP port
  EXPECT_EQ(res_ep.scheme, "http");
  EXPECT_EQ(res_ep.resolved_ips.size(), 1u);
  ASSERT_EQ(res_ep.resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep.resolved_ips[0].connect_to, "noport.example.com:80:10.0.0.1:80");
}

TEST_F(RGWRESTConnTest, resolve_endpoints_custom_https_port) {
  // Setup mock resolver to return an IP addr
  entity_addr_t addr;
  addr.parse("10.0.0.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("custom.secure.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"https://custom.secure.example.com:8443"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("https://custom.secure.example.com:8443");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;
  EXPECT_EQ(res_ep.port, 8443);
  EXPECT_EQ(res_ep.scheme, "https");
  EXPECT_EQ(res_ep.resolved_ips.size(), 1u);
  ASSERT_EQ(res_ep.resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep.resolved_ips[0].connect_to, "custom.secure.example.com:8443:10.0.0.1:8443");
}

TEST_F(RGWRESTConnTest, resolve_endpoints_resolution_failure) {
  // Return empty addresses to simulate resolution failure
  std::vector<entity_addr_t> empty_addrs;

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("nonexistent.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(empty_addrs), Return(-1)));

  std::list<std::string> endpoints = {"http://nonexistent.example.com:8080"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("http://nonexistent.example.com:8080");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;
  EXPECT_EQ(res_ep.host, "nonexistent.example.com");
  EXPECT_TRUE(res_ep.resolved_ips.empty());
  EXPECT_TRUE(res_ep.resolved_ips.empty());
}

TEST_F(RGWRESTConnTest, resolve_endpoints_invalid_url) {
  // Invalid URL should be skipped, no resolver call expected
  EXPECT_CALL(*mock_resolver, resolve_all_addrs(_, _)).Times(0);

  std::list<std::string> endpoints = {"not-a-valid-url"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  EXPECT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("not-a-valid-url");
  ASSERT_NE(res_ep_ptr, nullptr);
  EXPECT_TRUE(res_ep_ptr->resolved_ips.empty());
}

TEST_F(RGWRESTConnTest, resolve_endpoints_empty_host) {
  // URL with empty host should be skipped
  EXPECT_CALL(*mock_resolver, resolve_all_addrs(_, _)).Times(0);

  std::list<std::string> endpoints = {"http://:8080/path"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  EXPECT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("http://:8080/path");
  ASSERT_NE(res_ep_ptr, nullptr);
  EXPECT_TRUE(res_ep_ptr->resolved_ips.empty());
}

TEST_F(RGWRESTConnTest, resolve_endpoints_multiple_endpoints) {
  entity_addr_t addr1, addr2;
  addr1.parse("192.168.1.1");
  addr2.parse("192.168.1.2");
  std::vector<entity_addr_t> mock_addrs1 = {addr1};
  std::vector<entity_addr_t> mock_addrs2 = {addr2};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("host1.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs1), Return(0)));
  EXPECT_CALL(*mock_resolver, resolve_all_addrs("host2.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs2), Return(0)));

  std::list<std::string> endpoints = {
    "http://host1.example.com:8080",
    "https://host2.example.com/rgw"
  };
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 2u);

  // Check first endpoint
  auto* res_ep1_ptr = conn.find_resolved_endpoint("http://host1.example.com:8080");
  ASSERT_NE(res_ep1_ptr, nullptr);
  EXPECT_EQ(res_ep1_ptr->host, "host1.example.com");
  EXPECT_EQ(res_ep1_ptr->scheme, "http");
  EXPECT_EQ(res_ep1_ptr->port, 8080);
  EXPECT_EQ(res_ep1_ptr->resolved_ips.size(), 1u);
  ASSERT_EQ(res_ep1_ptr->resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep1_ptr->resolved_ips[0].connect_to, "host1.example.com:8080:192.168.1.1:8080");
  EXPECT_TRUE(ceph::real_clock::is_zero(res_ep1_ptr->resolved_ips[0].last_failure.load()));

  // Check second endpoint
  auto* res_ep2_ptr = conn.find_resolved_endpoint("https://host2.example.com/rgw");
  ASSERT_NE(res_ep2_ptr, nullptr);
  EXPECT_EQ(res_ep2_ptr->host, "host2.example.com");
  EXPECT_EQ(res_ep2_ptr->scheme, "https");
  EXPECT_EQ(res_ep2_ptr->port, 443);  // default HTTPS port
  EXPECT_EQ(res_ep2_ptr->resolved_ips.size(), 1u);
  ASSERT_EQ(res_ep2_ptr->resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep2_ptr->resolved_ips[0].connect_to, "host2.example.com:443:192.168.1.2:443");
  EXPECT_TRUE(ceph::real_clock::is_zero(res_ep2_ptr->resolved_ips[0].last_failure.load()));
}

TEST_F(RGWRESTConnTest, resolve_endpoints_with_path) {
  entity_addr_t addr;
  addr.parse("10.0.0.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("api.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  // URL with path - host extraction should still work
  std::list<std::string> endpoints = {"http://api.example.com:9000/datacenter1/rgw"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);

  auto* res_ep_ptr = conn.find_resolved_endpoint("http://api.example.com:9000/datacenter1/rgw");
  ASSERT_NE(res_ep_ptr, nullptr);
  const auto& res_ep = *res_ep_ptr;

  EXPECT_EQ(res_ep.host, "api.example.com");
  EXPECT_EQ(res_ep.port, 9000);
  EXPECT_EQ(res_ep.resolved_ips.size(), 1u);
  EXPECT_EQ(res_ep.resolved_ips[0].connect_to, "api.example.com:9000:10.0.0.1:9000");
}

TEST_F(RGWRESTConnTest, populate_connect_to_round_robin) {
  // Setup mock to return multiple IP addresses
  entity_addr_t addr1, addr2;
  addr1.parse("192.168.1.1");
  addr2.parse("192.168.1.2");
  std::vector<entity_addr_t> mock_addrs = {addr1, addr2};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("rr.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  const string test_url = "http://rr.example.com:8080";

  std::list<std::string> endpoints = {test_url};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  ResolvedEndpoint* res_ep = conn.find_resolved_endpoint(test_url);
  ASSERT_NE(res_ep, nullptr);

  // First call should get first IP (index 0)
  RGWEndpoint ep1;
  ep1.set_url(test_url);
  conn.populate_connect_to(ep1, *res_ep);
  EXPECT_EQ(ep1.get_connect_to(), "rr.example.com:8080:192.168.1.1:8080");

  // Second call should get second IP (index 1)
  RGWEndpoint ep2;
  ep2.set_url(test_url);
  conn.populate_connect_to(ep2, *res_ep);
  EXPECT_EQ(ep2.get_connect_to(), "rr.example.com:8080:192.168.1.2:8080");

  // Third call should wrap around to first IP (index 2 % 2 = 0)
  RGWEndpoint ep3;
  ep3.set_url(test_url);
  conn.populate_connect_to(ep3, *res_ep);
  EXPECT_EQ(ep3.get_connect_to(), "rr.example.com:8080:192.168.1.1:8080");
}

TEST_F(RGWRESTConnTest, endpoint_constructor_sets_original_url) {
  RGWEndpoint ep("http://example.com:8080");
  EXPECT_EQ(ep.get_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_original_url(), "http://example.com:8080");
}

TEST_F(RGWRESTConnTest, endpoint_set_url_on_default_constructed_sets_original) {
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

TEST_F(RGWRESTConnTest, endpoint_with_url_preserves_original) {
  RGWEndpoint ep("http://original.example.com");
  RGWEndpoint ep2 = ep.with_url("http://modified.example.com");

  EXPECT_EQ(ep2.get_url(), "http://modified.example.com");
  EXPECT_EQ(ep2.get_original_url(), "http://original.example.com");
}

TEST_F(RGWRESTConnTest, set_endpoint_unconnectable_uses_original_url) {
  entity_addr_t addr;
  addr.parse("192.168.1.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"http://example.com:8080"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  // Get endpoint and verify it's initially connectable
  RGWEndpoint ep;
  int ret = conn.get_endpoint(ep);
  ASSERT_EQ(ret, 0);
  EXPECT_EQ(ep.get_original_url(), "http://example.com:8080");

  // Mark it unconnectable
  conn.set_endpoint_unconnectable(ep);

  // Verify status was updated (endpoint should now be skipped initially)
  auto* res_ep_ptr = conn.find_resolved_endpoint("http://example.com:8080");
  ASSERT_NE(res_ep_ptr, nullptr);
  EXPECT_FALSE(ceph::real_clock::is_zero(res_ep_ptr->resolved_ips[0].last_failure.load()));
}

TEST_F(RGWRESTConnTest, set_endpoint_unconnectable_after_url_modification) {
  entity_addr_t addr;
  addr.parse("192.168.1.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"http://example.com:8080"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  // Get endpoint
  RGWEndpoint ep;
  int ret = conn.get_endpoint(ep);
  ASSERT_EQ(ret, 0);

  // Simulate URL modification (e.g., connect_to mapping changed the effective URL)
  ep.set_url("http://192.168.1.1:8080");

  // original_url should still be the original
  EXPECT_EQ(ep.get_original_url(), "http://example.com:8080");
  EXPECT_EQ(ep.get_url(), "http://192.168.1.1:8080");

  // Mark unconnectable - should use original_url for lookup
  conn.set_endpoint_unconnectable(ep);

  // Should find by original URL
  auto* res_ep_ptr = conn.find_resolved_endpoint("http://example.com:8080");
  ASSERT_NE(res_ep_ptr, nullptr);
  EXPECT_FALSE(ceph::real_clock::is_zero(res_ep_ptr->resolved_ips[0].last_failure.load()));

  // Should NOT have created entry for modified URL
  auto* res_ep2_ptr = conn.find_resolved_endpoint("http://192.168.1.1:8080");
  EXPECT_EQ(res_ep2_ptr, nullptr);
}

TEST_F(RGWRESTConnTest, set_endpoint_unconnectable_with_unknown_original_url) {
  entity_addr_t addr;
  addr.parse("192.168.1.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("known.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"http://known.example.com:8080"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  // Create an endpoint with URL not in resolved_endpoints
  RGWEndpoint ep;
  ep.set_url("http://unknown.example.com:8080");

  // This should safely do nothing (log error, but not crash)
  conn.set_endpoint_unconnectable(ep);

  // Verify original endpoint status unchanged
  auto* res_ep_ptr = conn.find_resolved_endpoint("http://known.example.com:8080");
  ASSERT_NE(res_ep_ptr, nullptr);
  EXPECT_TRUE(ceph::real_clock::is_zero(res_ep_ptr->resolved_ips[0].last_failure.load()));
}

TEST_F(RGWRESTConnTest, populate_connect_to_skips_failed_ips) {
  // Setup mock to return multiple IP addresses
  entity_addr_t addr1, addr2, addr3;
  addr1.parse("192.168.1.1");
  addr2.parse("192.168.1.2");
  addr3.parse("192.168.1.3");
  std::vector<entity_addr_t> mock_addrs = {addr1, addr2, addr3};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("multi.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  const string test_url = "http://multi.example.com:8080";
  std::list<std::string> endpoints = {test_url};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  ResolvedEndpoint* res_ep = conn.find_resolved_endpoint(test_url);
  ASSERT_NE(res_ep, nullptr);
  ASSERT_EQ(res_ep->resolved_ips.size(), 3u);

  // Mark second IP (192.168.1.2) as down
  res_ep->resolved_ips[1].mark_down();

  // Round robin should skip the failed IP
  RGWEndpoint ep1;
  ep1.set_url(test_url);
  conn.populate_connect_to(ep1, *res_ep);
  EXPECT_EQ(ep1.get_connect_to(), "multi.example.com:8080:192.168.1.1:8080");

  // Next call should skip failed IP and go to third IP
  RGWEndpoint ep2;
  ep2.set_url(test_url);
  conn.populate_connect_to(ep2, *res_ep);
  EXPECT_EQ(ep2.get_connect_to(), "multi.example.com:8080:192.168.1.3:8080");

  // Should wrap back to first IP (skipping second)
  RGWEndpoint ep3;
  ep3.set_url(test_url);
  conn.populate_connect_to(ep3, *res_ep);
  EXPECT_EQ(ep3.get_connect_to(), "multi.example.com:8080:192.168.1.1:8080");
}

TEST_F(RGWRESTConnTest, populate_connect_to_ip_failure_expiration) {
  entity_addr_t addr1, addr2;
  addr1.parse("10.0.0.1");
  addr2.parse("10.0.0.2");
  std::vector<entity_addr_t> mock_addrs = {addr1, addr2};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("expire.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  const string test_url = "http://expire.example.com:8080";
  std::list<std::string> endpoints = {test_url};

  // Set a very short timeout for testing (1 second)
  cct.get()->_conf->rgw_rest_conn_ip_fail_timeout_secs = 1;

  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  ResolvedEndpoint* res_ep = conn.find_resolved_endpoint(test_url);
  ASSERT_NE(res_ep, nullptr);

  // Mark first IP as failed with a timestamp in the past (more than 1 second ago)
  res_ep->resolved_ips[0].last_failure.store(
      ceph::real_clock::now() - std::chrono::seconds(2));

  // The failure should be expired, so first IP should be selected and marked up
  RGWEndpoint ep1;
  ep1.set_url(test_url);
  conn.populate_connect_to(ep1, *res_ep);
  EXPECT_EQ(ep1.get_connect_to(), "expire.example.com:8080:10.0.0.1:8080");

  // Verify IP was marked up (last_failure reset to zero)
  EXPECT_TRUE(ceph::real_clock::is_zero(res_ep->resolved_ips[0].last_failure.load()));
}

TEST_F(RGWRESTConnTest, populate_connect_to_all_ips_down_fallback) {
  entity_addr_t addr1, addr2;
  addr1.parse("172.16.0.1");
  addr2.parse("172.16.0.2");
  std::vector<entity_addr_t> mock_addrs = {addr1, addr2};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("alldown.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  const string test_url = "http://alldown.example.com:8080";
  std::list<std::string> endpoints = {test_url};

  // Use longer timeout so failures don't expire during test
  cct.get()->_conf->rgw_rest_conn_ip_fail_timeout_secs = 60;

  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  ResolvedEndpoint* res_ep = conn.find_resolved_endpoint(test_url);
  ASSERT_NE(res_ep, nullptr);

  // Mark ALL IPs as down
  res_ep->resolved_ips[0].mark_down();
  res_ep->resolved_ips[1].mark_down();

  // Should leave connect_to empty if no IPs are up
  RGWEndpoint ep1;
  ep1.set_url(test_url);
  conn.populate_connect_to(ep1, *res_ep);

  // Should get one of the IPs (round-robin among failed IPs)
  std::string connect_to = ep1.get_connect_to();
  EXPECT_TRUE(connect_to.empty());
}

TEST_F(RGWRESTConnTest, get_endpoint_skips_endpoint_when_all_ips_down) {
  entity_addr_t addr1, addr2;
  addr1.parse("10.1.1.1");
  addr2.parse("10.2.2.2");
  std::vector<entity_addr_t> mock_addrs1 = {addr1};
  std::vector<entity_addr_t> mock_addrs2 = {addr2};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("ep1.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs1), Return(0)));
  EXPECT_CALL(*mock_resolver, resolve_all_addrs("ep2.example.com", _))
       .WillOnce(DoAll(SetArgPointee<1>(mock_addrs2), Return(0)));

  std::list<std::string> endpoints = {
      "http://ep1.example.com:8080",
      "http://ep2.example.com:8080"
  };

  cct.get()->_conf->rgw_rest_conn_ip_fail_timeout_secs = 60;

  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  // Mark ALL IPs on first endpoint as down
  ResolvedEndpoint* res_ep1 = conn.find_resolved_endpoint("http://ep1.example.com:8080");
  ASSERT_NE(res_ep1, nullptr);
  res_ep1->resolved_ips[0].mark_down();
  // Also set endpoint-level failure time for fast-path skip
  res_ep1->last_failure_time.store(ceph::real_clock::now());

  // get_endpoint should skip ep1 and return ep2
  RGWEndpoint ep;
  int ret = conn.get_endpoint(ep);
  ASSERT_EQ(ret, 0);
  EXPECT_EQ(ep.get_url(), "http://ep2.example.com:8080");

  // Calling again should continue to prefer ep2
  RGWEndpoint ep2;
  ret = conn.get_endpoint(ep2);
  ASSERT_EQ(ret, 0);
  EXPECT_EQ(ep2.get_url(), "http://ep2.example.com:8080");
}
