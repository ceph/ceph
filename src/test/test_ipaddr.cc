#include "include/ipaddr.h"
#include "common/pick_address.h"
#include "gtest/gtest.h"
#include "include/stringify.h"
#include "common/ceph_context.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#if defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#endif
#include <arpa/inet.h>
#include <ifaddrs.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <net/if.h>
#endif

using namespace std;

static void ipv4(struct sockaddr_in *addr, const char *s) {
  int err;

  addr->sin_family = AF_INET;
  err = inet_pton(AF_INET, s, &addr->sin_addr);
  ASSERT_EQ(1, err);
}

static void ipv6(struct sockaddr_in6 *addr, const char *s) {
  int err;

  addr->sin6_family = AF_INET6;
  err = inet_pton(AF_INET6, s, &addr->sin6_addr);
  ASSERT_EQ(1, err);
}

static char eth0[] = "eth0";
static char eth1[] = "eth1";

TEST(CommonIPAddr, TestNotFound)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv4(&net, "10.11.234.56");
  ASSERT_FALSE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 24));
  ASSERT_FALSE(matches_ipv6_in_subnet(two, (struct sockaddr_in6*)&net, 24));
}

TEST(CommonIPAddr, TestV4_Simple)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv4(&net, "10.11.12.42");

  ASSERT_TRUE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 24));
  ASSERT_FALSE(matches_ipv4_in_subnet(two, (struct sockaddr_in*)&net, 24));
}

TEST(CommonIPAddr, TestV4_Prefix25)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv4(&a_two, "10.11.12.129");
  ipv4(&net, "10.11.12.128");

  ASSERT_FALSE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 25));
  ASSERT_TRUE(matches_ipv4_in_subnet(two, (struct sockaddr_in*)&net, 25));
}

TEST(CommonIPAddr, TestV4_Prefix16)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.1.1.2");
  ipv4(&a_two, "10.2.1.123");
  ipv4(&net, "10.2.0.0");

  ASSERT_FALSE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 16));
  ASSERT_TRUE(matches_ipv4_in_subnet(two, (struct sockaddr_in*)&net, 16));
}

TEST(CommonIPAddr, TestV4_PrefixTooLong)
{
  struct ifaddrs one;
  struct sockaddr_in a_one;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv4(&a_one, "10.11.12.13");
  ipv4(&net, "10.11.12.12");

  ASSERT_FALSE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 42));
}

TEST(CommonIPAddr, TestV4_PrefixZero)
{
  struct ifaddrs one, two;
  struct sockaddr_in6 a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv4(&a_two, "10.1.2.3");
  ipv4(&net, "255.0.1.2");

  ASSERT_FALSE(matches_ipv4_in_subnet(one, (struct sockaddr_in*)&net, 0));
  ASSERT_TRUE(matches_ipv4_in_subnet(two, (struct sockaddr_in*)&net, 0));
}

static char lo[] = "lo";
static char lo0[] = "lo:0";

TEST(CommonIPAddr, TestV4_SkipLoopback)
{
  struct ifaddrs one, two, three;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in a_three;

  one.ifa_next = &two;
  one.ifa_flags &= ~IFF_UP;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = lo;

  two.ifa_next = &three;
  two.ifa_flags = IFF_UP;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = lo0;

  three.ifa_next = NULL;
  three.ifa_flags = IFF_UP;
  three.ifa_addr = (struct sockaddr*)&a_three;
  three.ifa_name = eth0;

  ipv4(&a_one, "127.0.0.1");
  ipv4(&a_two, "127.0.0.1");
  ipv4(&a_three, "10.1.2.3");

  const struct sockaddr *result = nullptr;
  // we prefer the non-loopback address despite the loopback addresses
  result =
    find_ip_in_subnet_list(nullptr, (struct ifaddrs*)&one,
                           CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6,
                           "", "");
  ASSERT_EQ(three.ifa_addr, result);
  // the subnet criteria leaves us no choice but the UP loopback address
  result =
    find_ip_in_subnet_list(nullptr, (struct ifaddrs*)&one,
                           CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6,
                           "127.0.0.0/8", "");
  ASSERT_EQ(two.ifa_addr, result);
}

TEST(CommonIPAddr, TestV6_Simple)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv6(&net, "2001:1234:5678:90ab::dead:beef");

  ASSERT_FALSE(matches_ipv6_in_subnet(one, (struct sockaddr_in6*)&net, 64));
  ASSERT_TRUE(matches_ipv6_in_subnet(two, (struct sockaddr_in6*)&net, 64));
}

TEST(CommonIPAddr, TestV6_Prefix57)
{
  struct ifaddrs one, two;
  struct sockaddr_in6 a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv6(&net, "2001:1234:5678:90ab::dead:beef");

  ASSERT_FALSE(matches_ipv6_in_subnet(one, (struct sockaddr_in6*)&net, 57));
  ASSERT_TRUE(matches_ipv6_in_subnet(two, (struct sockaddr_in6*)&net, 57));
}

TEST(CommonIPAddr, TestV6_PrefixTooLong)
{
  struct ifaddrs one;
  struct sockaddr_in6 a_one;
  struct sockaddr_in6 net;

  memset(&net, 0, sizeof(net));

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv6(&net, "2001:1234:5678:900F::cdee");

  ASSERT_FALSE(matches_ipv6_in_subnet(one, (struct sockaddr_in6*)&net, 9000));
}

TEST(CommonIPAddr, TestV6_PrefixZero)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.2.3.4");
  ipv6(&a_two, "2001:f00b::1");
  ipv6(&net, "ff00::1");

  ASSERT_FALSE(matches_ipv6_in_subnet(one, (struct sockaddr_in6*)&net, 0));
  ASSERT_TRUE(matches_ipv6_in_subnet(two, (struct sockaddr_in6*)&net, 0));
}

TEST(CommonIPAddr, TestV6_SkipLoopback)
{
  struct ifaddrs one, two, three;
  struct sockaddr_in6 a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 a_three;

  one.ifa_next = &two;
  one.ifa_flags &= ~IFF_UP;
  ipv6(&a_one, "::1");
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = lo;

  two.ifa_next = &three;
  two.ifa_flags = IFF_UP;
  ipv6(&a_two, "::1");
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = lo0;

  three.ifa_next = NULL;
  three.ifa_flags = IFF_UP;
  ipv6(&a_three, "2001:1234:5678:90ab::beef");
  three.ifa_addr = (struct sockaddr*)&a_three;
  three.ifa_name = eth0;

  const struct sockaddr *result = nullptr;
  // we prefer the non-loopback address despite the loopback addresses
  result =
    find_ip_in_subnet_list(nullptr, (struct ifaddrs*)&one,
                           CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6,
                           "", "");
  ASSERT_EQ(three.ifa_addr, result);
  // the subnet criteria leaves us no choice but the UP loopback address
  result =
    find_ip_in_subnet_list(nullptr, (struct ifaddrs*)&one,
                           CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6,
                           "::1/128", "");
  ASSERT_EQ(two.ifa_addr, result);
}

TEST(CommonIPAddr, ParseNetwork_Empty)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_Junk)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("foo", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_SlashNum)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("/24", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_Slash)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("/", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv4)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv4Slash)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv4SlashNegative)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/-3", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv4SlashJunk)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/foo", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv6)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv6Slash)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv6SlashNegative)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/-3", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_Bad_IPv6SlashJunk)
{
  struct sockaddr_storage network;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/foo", &network, &prefix_len);
  ASSERT_EQ(ok, false);
}

TEST(CommonIPAddr, ParseNetwork_IPv4_0)
{
  struct sockaddr_in network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/0", &net_storage, &prefix_len);
  network = *(struct sockaddr_in *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(0U, prefix_len);
  ASSERT_EQ(AF_INET, network.sin_family);
  ASSERT_EQ(0, network.sin_port);
  struct sockaddr_in want;
  ipv4(&want, "123.123.123.123");
  ASSERT_EQ(want.sin_addr.s_addr, network.sin_addr.s_addr);
}

TEST(CommonIPAddr, ParseNetwork_IPv4_13)
{
  struct sockaddr_in network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/13", &net_storage, &prefix_len);
  network = *(struct sockaddr_in *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(13U, prefix_len);
  ASSERT_EQ(AF_INET, network.sin_family);
  ASSERT_EQ(0, network.sin_port);
  struct sockaddr_in want;
  ipv4(&want, "123.123.123.123");
  ASSERT_EQ(want.sin_addr.s_addr, network.sin_addr.s_addr);
}

TEST(CommonIPAddr, ParseNetwork_IPv4_32)
{
  struct sockaddr_in network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/32", &net_storage, &prefix_len);
  network = *(struct sockaddr_in *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(32U, prefix_len);
  ASSERT_EQ(AF_INET, network.sin_family);
  ASSERT_EQ(0, network.sin_port);
  struct sockaddr_in want;
  ipv4(&want, "123.123.123.123");
  ASSERT_EQ(want.sin_addr.s_addr, network.sin_addr.s_addr);
}

TEST(CommonIPAddr, ParseNetwork_IPv4_42)
{
  struct sockaddr_in network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("123.123.123.123/42", &net_storage, &prefix_len);
  network = *(struct sockaddr_in *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(42U, prefix_len);
  ASSERT_EQ(AF_INET, network.sin_family);
  ASSERT_EQ(0, network.sin_port);
  struct sockaddr_in want;
  ipv4(&want, "123.123.123.123");
  ASSERT_EQ(want.sin_addr.s_addr, network.sin_addr.s_addr);
}

TEST(CommonIPAddr, ParseNetwork_IPv6_0)
{
  struct sockaddr_in6 network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/0", &net_storage, &prefix_len);
  network = *(struct sockaddr_in6 *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(0U, prefix_len);
  ASSERT_EQ(AF_INET6, network.sin6_family);
  ASSERT_EQ(0, network.sin6_port);
  struct sockaddr_in6 want;
  ipv6(&want, "2001:1234:5678:90ab::dead:beef");
  ASSERT_EQ(0, memcmp(want.sin6_addr.s6_addr, network.sin6_addr.s6_addr, sizeof(network.sin6_addr.s6_addr)));
}

TEST(CommonIPAddr, ParseNetwork_IPv6_67)
{
  struct sockaddr_in6 network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/67", &net_storage, &prefix_len);
  network = *(struct sockaddr_in6 *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(67U, prefix_len);
  ASSERT_EQ(AF_INET6, network.sin6_family);
  ASSERT_EQ(0, network.sin6_port);
  struct sockaddr_in6 want;
  ipv6(&want, "2001:1234:5678:90ab::dead:beef");
  ASSERT_EQ(0, memcmp(want.sin6_addr.s6_addr, network.sin6_addr.s6_addr, sizeof(network.sin6_addr.s6_addr)));
}

TEST(CommonIPAddr, ParseNetwork_IPv6_128)
{
  struct sockaddr_in6 network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/128", &net_storage, &prefix_len);
  network = *(struct sockaddr_in6 *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(128U, prefix_len);
  ASSERT_EQ(AF_INET6, network.sin6_family);
  ASSERT_EQ(0, network.sin6_port);
  struct sockaddr_in6 want;
  ipv6(&want, "2001:1234:5678:90ab::dead:beef");
  ASSERT_EQ(0, memcmp(want.sin6_addr.s6_addr, network.sin6_addr.s6_addr, sizeof(network.sin6_addr.s6_addr)));
}

TEST(CommonIPAddr, ParseNetwork_IPv6_9000)
{
  struct sockaddr_in6 network;
  struct sockaddr_storage net_storage;
  unsigned int prefix_len;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/9000", &net_storage, &prefix_len);
  network = *(struct sockaddr_in6 *) &net_storage;
  ASSERT_EQ(ok, true);
  ASSERT_EQ(9000U, prefix_len);
  ASSERT_EQ(AF_INET6, network.sin6_family);
  ASSERT_EQ(0, network.sin6_port);
  struct sockaddr_in6 want;
  ipv6(&want, "2001:1234:5678:90ab::dead:beef");
  ASSERT_EQ(0, memcmp(want.sin6_addr.s6_addr, network.sin6_addr.s6_addr, sizeof(network.sin6_addr.s6_addr)));
}

TEST(CommonIPAddr, ambiguous)
{
  entity_addr_t a;
  bool ok;

  ok = a.parse("1.2.3.4", nullptr, entity_addr_t::TYPE_ANY);
  ASSERT_TRUE(ok);
  ASSERT_EQ(entity_addr_t::TYPE_ANY, a.get_type());

  ok = a.parse("any:1.2.3.4", nullptr, entity_addr_t::TYPE_ANY);
  ASSERT_TRUE(ok);
  ASSERT_EQ(entity_addr_t::TYPE_ANY, a.get_type());

  ok = a.parse("v1:1.2.3.4", nullptr, entity_addr_t::TYPE_ANY);
  ASSERT_TRUE(ok);
  ASSERT_EQ(entity_addr_t::TYPE_LEGACY, a.get_type());

  ok = a.parse("v2:1.2.3.4", nullptr, entity_addr_t::TYPE_ANY);
  ASSERT_TRUE(ok);
  ASSERT_EQ(entity_addr_t::TYPE_MSGR2, a.get_type());
}

TEST(CommonIPAddr, network_contains)
{
  entity_addr_t network, addr;
  unsigned int prefix;
  bool ok;

  ok = parse_network("2001:1234:5678:90ab::dead:beef/32", &network, &prefix);
  ASSERT_TRUE(ok);
  ASSERT_EQ(32U, prefix);
  ok = addr.parse("2001:1234:5678:90ab::dead:beef", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_TRUE(network_contains(network, prefix, addr));
  ok = addr.parse("2001:1334:5678:90ab::dead:beef", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(network_contains(network, prefix, addr));
  ok = addr.parse("127.0.0.1", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(network_contains(network, prefix, addr));

  ok = parse_network("10.1.2.3/16", &network, &prefix);
  ASSERT_TRUE(ok);
  ASSERT_EQ(16U, prefix);
  ok = addr.parse("2001:1234:5678:90ab::dead:beef", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(network_contains(network, prefix, addr));
  ok = addr.parse("1.2.3.4", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(network_contains(network, prefix, addr));
  ok = addr.parse("10.1.22.44", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_TRUE(network_contains(network, prefix, addr));
  ok = addr.parse("10.2.22.44", nullptr);
  ASSERT_TRUE(ok);
  ASSERT_FALSE(network_contains(network, prefix, addr));
}

TEST(pick_address, find_ip_in_subnet_list)
{
  struct ifaddrs one, two, three;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in6 a_three;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = &three;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  three.ifa_next = NULL;
  three.ifa_addr = (struct sockaddr*)&a_three;
  three.ifa_name = eth1;

  ipv4(&a_one, "10.1.1.2");
  ipv4(&a_two, "10.2.1.123");
  ipv6(&a_three, "2001:1234:5678:90ab::cdef");

  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_OSD), false};

  // match by network
  result = find_ip_in_subnet_list(
    cct.get(),
    &one,
    CEPH_PICK_ADDRESS_IPV4,
    "10.1.0.0/16",
    "eth0");
  ASSERT_EQ(one.ifa_addr, result);

  result = find_ip_in_subnet_list(
    cct.get(),
    &one,
    CEPH_PICK_ADDRESS_IPV4,
    "10.2.0.0/16",
    "eth1");
  ASSERT_EQ(two.ifa_addr, result);

  // match by eth name
  result = find_ip_in_subnet_list(
    cct.get(),
    &one,
    CEPH_PICK_ADDRESS_IPV4,
    "10.0.0.0/8",
    "eth0");
  ASSERT_EQ(one.ifa_addr, result);

  result = find_ip_in_subnet_list(
    cct.get(),
    &one,
    CEPH_PICK_ADDRESS_IPV4,
    "10.0.0.0/8",
    "eth1");
  ASSERT_EQ(two.ifa_addr, result);

  result = find_ip_in_subnet_list(
    cct.get(),
    &one,
    CEPH_PICK_ADDRESS_IPV6,
    "2001::/16",
    "eth1");
  ASSERT_EQ(three.ifa_addr, result);
}

TEST(pick_address, filtering)
{
  struct ifaddrs one, two, three;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in6 a_three;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = &three;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  three.ifa_next = NULL;
  three.ifa_addr = (struct sockaddr*)&a_three;
  three.ifa_name = eth1;

  ipv4(&a_one, "10.1.1.2");
  ipv4(&a_two, "10.2.1.123");
  ipv6(&a_three, "2001:1234:5678:90ab::cdef");

  boost::intrusive_ptr<CephContext> cct = new CephContext(CEPH_ENTITY_TYPE_MON);
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs

  cct->_conf.set_val("public_addr", "");
  cct->_conf.set_val("public_network", "");
  cct->_conf.set_val("public_network_interface", "");
  cct->_conf.set_val("cluster_addr", "");
  cct->_conf.set_val("cluster_network", "");
  cct->_conf.set_val("cluster_network_interface", "");

  entity_addrvec_t av;
  {
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v1:0.0.0.0:0/0"), stringify(av.v[0]));
  }
  {
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV6 |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v1:[::]:0/0"), stringify(av.v[0]));
  }
  {
    cct->_conf.set_val("public_network", "10.2.0.0/16");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v1:10.2.1.123:0/0"), stringify(av.v[0]));
    cct->_conf.set_val("public_network", "");
  }
  {
    cct->_conf.set_val("public_network", "10.0.0.0/8");
    cct->_conf.set_val("public_network_interface", "eth1");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v2:10.2.1.123:0/0"), stringify(av.v[0]));
    cct->_conf.set_val("public_network", "");
    cct->_conf.set_val("public_network_interface", "");
  }
  {
    cct->_conf.set_val("public_network", "10.2.0.0/16");
    cct->_conf.set_val("cluster_network", "10.1.0.0/16");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v2:10.2.1.123:0/0"), stringify(av.v[0]));
    cct->_conf.set_val("public_network", "");
    cct->_conf.set_val("cluster_network", "");
  }
  {
    cct->_conf.set_val("public_network", "10.2.0.0/16");
    cct->_conf.set_val("cluster_network", "10.1.0.0/16");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_CLUSTER |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v1:10.1.1.2:0/0"), stringify(av.v[0]));
    cct->_conf.set_val("public_network", "");
    cct->_conf.set_val("cluster_network", "");
  }

  {
    cct->_conf.set_val("public_network", "2001::/16");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV6 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(1u, av.v.size());
    ASSERT_EQ(string("v2:[2001:1234:5678:90ab::cdef]:0/0"), stringify(av.v[0]));
    cct->_conf.set_val("public_network", "");
  }
  {
    cct->_conf.set_val("public_network", "2001::/16 10.0.0.0/8");
    cct->_conf.set_val("public_network_interface", "eth1");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_IPV6 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(2u, av.v.size());
    ASSERT_EQ(string("v2:[2001:1234:5678:90ab::cdef]:0/0"), stringify(av.v[0]));
    ASSERT_EQ(string("v2:10.2.1.123:0/0"), stringify(av.v[1]));
    cct->_conf.set_val("public_network", "");
    cct->_conf.set_val("public_network_interface", "");
  }
  {
    cct->_conf.set_val("public_network", "2001::/16 10.0.0.0/8");
    cct->_conf.set_val("public_network_interface", "eth1");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_IPV6 |
			   CEPH_PICK_ADDRESS_MSGR1 |
			   CEPH_PICK_ADDRESS_PREFER_IPV4,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(2u, av.v.size());
    ASSERT_EQ(string("v1:10.2.1.123:0/0"), stringify(av.v[0]));
    ASSERT_EQ(string("v1:[2001:1234:5678:90ab::cdef]:0/0"), stringify(av.v[1]));
    cct->_conf.set_val("public_network", "");
    cct->_conf.set_val("public_network_interface", "");
  }

  {
    cct->_conf.set_val("public_network", "2001::/16");
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV6 |
			   CEPH_PICK_ADDRESS_MSGR1 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(2u, av.v.size());
    ASSERT_EQ(string("v2:[2001:1234:5678:90ab::cdef]:0/0"), stringify(av.v[0]));
    ASSERT_EQ(string("v1:[2001:1234:5678:90ab::cdef]:0/0"), stringify(av.v[1]));
    cct->_conf.set_val("public_network", "");
  }

  {
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_IPV4 |
			   CEPH_PICK_ADDRESS_MSGR1 |
			   CEPH_PICK_ADDRESS_MSGR2,
			   &one, &av);
    ASSERT_EQ(0, r);
    ASSERT_EQ(2u, av.v.size());
    ASSERT_EQ(string("v2:0.0.0.0:0/0"), stringify(av.v[0]));
    ASSERT_EQ(string("v1:0.0.0.0:0/0"), stringify(av.v[1]));
  }
}

TEST(pick_address, ipv4_ipv6_enabled)
{
  struct ifaddrs one;
  struct sockaddr_in a_one;

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv4(&a_one, "10.1.1.2");

  boost::intrusive_ptr<CephContext> cct = new CephContext(CEPH_ENTITY_TYPE_OSD);
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs

  cct->_conf.set_val("public_addr", "");
  cct->_conf.set_val("public_network", "10.1.1.0/24");
  cct->_conf.set_val("public_network_interface", "");
  cct->_conf.set_val("cluster_addr", "");
  cct->_conf.set_val("cluster_network", "");
  cct->_conf.set_val("cluster_network_interface", "");
  cct->_conf.set_val("ms_bind_ipv6", "true");

  entity_addrvec_t av;
  {
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(-1, r);
  }
}

TEST(pick_address, ipv4_ipv6_enabled2)
{
  struct ifaddrs one;
  struct sockaddr_in6 a_one;

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv6(&a_one, "2001:1234:5678:90ab::cdef");

  boost::intrusive_ptr<CephContext> cct = new CephContext(CEPH_ENTITY_TYPE_OSD);
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs

  cct->_conf.set_val("public_addr", "");
  cct->_conf.set_val("public_network", "2001::/16");
  cct->_conf.set_val("public_network_interface", "");
  cct->_conf.set_val("cluster_addr", "");
  cct->_conf.set_val("cluster_network", "");
  cct->_conf.set_val("cluster_network_interface", "");
  cct->_conf.set_val("ms_bind_ipv6", "true");

  entity_addrvec_t av;
  {
    int r = pick_addresses(cct.get(),
			   CEPH_PICK_ADDRESS_PUBLIC |
			   CEPH_PICK_ADDRESS_MSGR1,
			   &one, &av);
    ASSERT_EQ(-1, r);
  }
}
