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

TEST(CommonIPAddr, TestNotFound)
{
  struct ifaddrs one, two;
  struct sockaddr_in a_one;
  struct sockaddr_in6 a_two;
  struct sockaddr_in net;
  const struct sockaddr *result;

  one.ifa_next = &two;
  one.ifa_addr = (struct sockaddr*)&a_one;

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

  ipv4(&a_one, "10.11.12.13");
  ipv4(&a_two, "10.11.12.129");
  ipv4(&net, "10.11.12.128");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 25);
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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

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

  two.ifa_next = NULL;
  two.ifa_addr = (struct sockaddr*)&a_two;

  ipv4(&a_one, "10.2.3.4");
  ipv6(&a_two, "2001:f00b::1");
  ipv6(&net, "ff00::1");

  result = find_ip_in_subnet(&one, (struct sockaddr*)&net, 0);
  ASSERT_EQ((struct sockaddr*)&a_two, result);
}
