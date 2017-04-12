#include "include/ipaddr.h"
#include "gtest/gtest.h"

#include <arpa/inet.h>

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
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv4(&net, "10.11.234.56");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 24);
  ASSERT_EQ(0, result);
}

TEST(CommonIPAddr, TestV4_Simple)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv4(&net, "10.11.12.42");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 24);
  ASSERT_EQ((struct sockaddr*)&a_one, result);
}

TEST(CommonIPAddr, TestV4_Prefix25)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv4(&a_two, "10.11.12.129");
  ipv4(&net, "10.11.12.128");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 25);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}

TEST(CommonIPAddr, TestV4_Prefix16)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.1.1.2");
  ipv4(&a_two, "10.2.1.123");
  ipv4(&net, "10.2.0.0");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 16);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}

TEST(CommonIPAddr, TestV4_PrefixTooLong)
{
  struct ifaddrs one;
  struct sockaddr_in a_one;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv4(&a_one, "10.11.12.13");
  ipv4(&net, "10.11.12.12");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 42);
  ASSERT_EQ(0, result);
}

TEST(CommonIPAddr, TestV4_PrefixZero)
{
  struct ifaddrs one, two;
  struct sockaddr_in6 a_one;
  struct sockaddr_in a_two;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv4(&a_two, "10.1.2.3");
  ipv4(&net, "255.0.1.2");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 0);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}

TEST(CommonIPAddr, TestV6_Simple)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.11.12.13");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv6(&net, "2001:1234:5678:90ab::dead:beef");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 64);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}

TEST(CommonIPAddr, TestV6_Prefix57)
{
  struct ifaddrs one, two;
  struct sockaddr_in6 a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv6(&a_two, "2001:1234:5678:90ab::cdef");
  ipv6(&net, "2001:1234:5678:90ab::dead:beef");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 57);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}

TEST(CommonIPAddr, TestV6_PrefixTooLong)
{
  struct ifaddrs one;
  struct sockaddr_in6 a_one;
  struct sockaddr_in6 net;
  const struct sockaddr *result;

  one.ifa_next = NULL;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  ipv6(&a_one, "2001:1234:5678:900F::cdef");
  ipv6(&net, "2001:1234:5678:900F::cdee");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 9000);
  ASSERT_EQ(0, result);
}

TEST(CommonIPAddr, TestV6_PrefixZero)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in6 net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;
  one.ifa_name = eth0;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;
  two.ifa_name = eth1;

  ipv4(&a_one, "10.2.3.4");
  ipv6(&a_two, "2001:f00b::1");
  ipv6(&net, "ff00::1");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 0);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
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
