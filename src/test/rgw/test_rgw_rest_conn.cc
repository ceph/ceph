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
  EXPECT_EQ(resolved[0].host, "example.com");
  EXPECT_EQ(resolved[0].scheme, "http");
  EXPECT_EQ(resolved[0].port, 8080);
  EXPECT_EQ(resolved[0].ips.size(), 1u);
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
  EXPECT_EQ(resolved[0].host, "multi.example.com");
  EXPECT_EQ(resolved[0].scheme, "https");
  EXPECT_EQ(resolved[0].port, 443);  // Default HTTPS port
  EXPECT_EQ(resolved[0].ips.size(), 3u);

  // Verify all IP contents
  std::set<std::string> expected_ips = {
    "v2:192.168.1.100:0/0",
    "v2:192.168.1.101:0/0",
    "v2:[2001:db8::1]:0/0"
  };
  std::set<std::string> actual_ips;
  for (const auto& ip : resolved[0].ips) {
    std::ostringstream os;
    os << ip;
    actual_ips.insert(os.str());
  }
  EXPECT_EQ(actual_ips, expected_ips);
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
  EXPECT_EQ(resolved[0].port, 80);  // Default HTTP port
  EXPECT_EQ(resolved[0].ips.size(), 1u);
}

TEST_F(RGWRESTConnTest, resolve_endpoints_custom_https_port) {
  // Setup mock resolver to return an IP addr
  entity_addr_t addr;
  addr.parse("10.0.0.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("secure.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  std::list<std::string> endpoints = {"https://secure.example.com:8443"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);
  EXPECT_EQ(resolved[0].port, 8443);
  EXPECT_EQ(resolved[0].ips.size(), 1u);
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
  EXPECT_EQ(resolved[0].host, "nonexistent.example.com");
  EXPECT_TRUE(resolved[0].ips.empty());
}

TEST_F(RGWRESTConnTest, resolve_endpoints_invalid_url) {
  // Invalid URL should be skipped, no resolver call expected
  EXPECT_CALL(*mock_resolver, resolve_all_addrs(_, _)).Times(0);

  std::list<std::string> endpoints = {"not-a-valid-url"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  EXPECT_TRUE(resolved.empty());
}

TEST_F(RGWRESTConnTest, resolve_endpoints_empty_host) {
  // URL with empty host should be skipped
  EXPECT_CALL(*mock_resolver, resolve_all_addrs(_, _)).Times(0);

  std::list<std::string> endpoints = {"http://:8080/path"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  EXPECT_TRUE(resolved.empty());
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
    "https://host2.example.com:443"
  };
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 2u);

  EXPECT_EQ(resolved[0].host, "host1.example.com");
  EXPECT_EQ(resolved[0].scheme, "http");
  EXPECT_EQ(resolved[0].port, 8080);
  EXPECT_EQ(resolved[0].ips.size(), 1u);

  EXPECT_EQ(resolved[1].host, "host2.example.com");
  EXPECT_EQ(resolved[1].scheme, "https");
  EXPECT_EQ(resolved[1].port, 443);
  EXPECT_EQ(resolved[1].ips.size(), 1u);
}

TEST_F(RGWRESTConnTest, resolve_endpoints_with_path) {
  entity_addr_t addr;
  addr.parse("10.0.0.1");
  std::vector<entity_addr_t> mock_addrs = {addr};

  EXPECT_CALL(*mock_resolver, resolve_all_addrs("api.example.com", _))
      .WillOnce(DoAll(SetArgPointee<1>(mock_addrs), Return(0)));

  // URL with path - host extraction should still work
  std::list<std::string> endpoints = {"http://api.example.com:9000/rgw"};
  RGWRESTConn conn(cct.get(), nullptr, "remote-zone", endpoints, std::nullopt);

  const auto& resolved = conn.get_resolved_endpoints();
  ASSERT_EQ(resolved.size(), 1u);
  EXPECT_EQ(resolved[0].host, "api.example.com");
  EXPECT_EQ(resolved[0].port, 9000);
  EXPECT_EQ(resolved[0].ips.size(), 1u);
}
