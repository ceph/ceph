
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <stdlib.h>
#include <string.h>
#if defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#endif

#include "include/ipaddr.h"
#include "msg/msg_types.h"
#include "common/pick_address.h"

void netmask_ipv4(const struct in_addr *addr,
			 unsigned int prefix_len,
			 struct in_addr *out) {
  uint32_t mask;

  if (prefix_len >= 32) {
    // also handle 32 in this branch, because >>32 is not defined by
    // the C standards
    mask = ~uint32_t(0);
  } else {
    mask = htonl(~(~uint32_t(0) >> prefix_len));
  }
  out->s_addr = addr->s_addr & mask;
}


static bool match_numa_node(const string& if_name, int numa_node)
{
#ifdef WITH_SEASTAR
  return true;
#else
  int if_node = -1;
  int r = get_iface_numa_node(if_name, &if_node);
  if (r < 0) {
    return false;
  }
  return if_node == numa_node;
#endif
}

const struct ifaddrs *find_ipv4_in_subnet(const struct ifaddrs *addrs,
					  const struct sockaddr_in *net,
					  unsigned int prefix_len,
					  int numa_node) {
  struct in_addr want, temp;

  netmask_ipv4(&net->sin_addr, prefix_len, &want);

  for (; addrs != NULL; addrs = addrs->ifa_next) {

    if (addrs->ifa_addr == NULL)
      continue;

    if (strcmp(addrs->ifa_name, "lo") == 0)
      continue;

    if (numa_node >= 0 && !match_numa_node(addrs->ifa_name, numa_node))
      continue;

    if (addrs->ifa_addr->sa_family != net->sin_family)
      continue;

    struct in_addr *cur = &((struct sockaddr_in*)addrs->ifa_addr)->sin_addr;
    netmask_ipv4(cur, prefix_len, &temp);

    if (temp.s_addr == want.s_addr) {
      return addrs;
    }
  }

  return NULL;
}


void netmask_ipv6(const struct in6_addr *addr,
		  unsigned int prefix_len,
		  struct in6_addr *out) {
  if (prefix_len > 128)
    prefix_len = 128;

  memcpy(out->s6_addr, addr->s6_addr, prefix_len/8);
  if (prefix_len < 128)
    out->s6_addr[prefix_len/8] = addr->s6_addr[prefix_len/8] & ~( 0xFF >> (prefix_len % 8) );
  if (prefix_len/8 < 15)
    memset(out->s6_addr+prefix_len/8+1, 0, 16-prefix_len/8-1);
}


const struct ifaddrs *find_ipv6_in_subnet(const struct ifaddrs *addrs,
					  const struct sockaddr_in6 *net,
					  unsigned int prefix_len,
					  int numa_node) {
  struct in6_addr want, temp;

  netmask_ipv6(&net->sin6_addr, prefix_len, &want);

  for (; addrs != NULL; addrs = addrs->ifa_next) {

    if (addrs->ifa_addr == NULL)
      continue;

    if (strcmp(addrs->ifa_name, "lo") == 0)
      continue;

    if (numa_node >= 0 && !match_numa_node(addrs->ifa_name, numa_node))
      continue;

    if (addrs->ifa_addr->sa_family != net->sin6_family)
      continue;

    struct in6_addr *cur = &((struct sockaddr_in6*)addrs->ifa_addr)->sin6_addr;
    if (IN6_IS_ADDR_LINKLOCAL(cur))
      continue;
    netmask_ipv6(cur, prefix_len, &temp);

    if (IN6_ARE_ADDR_EQUAL(&temp, &want))
      return addrs;
  }

  return NULL;
}


const struct ifaddrs *find_ip_in_subnet(const struct ifaddrs *addrs,
					const struct sockaddr *net,
					unsigned int prefix_len,
					int numa_node) {
  switch (net->sa_family) {
    case AF_INET:
      return find_ipv4_in_subnet(addrs, (struct sockaddr_in*)net, prefix_len,
				 numa_node);

    case AF_INET6:
      return find_ipv6_in_subnet(addrs, (struct sockaddr_in6*)net, prefix_len,
				 numa_node);
    }

  return NULL;
}


bool parse_network(const char *s, struct sockaddr_storage *network, unsigned int *prefix_len) {
  char *slash = strchr((char*)s, '/');
  if (!slash) {
    // no slash
    return false;
  }
  if (*(slash+1) == '\0') {
    // slash is the last character
    return false;
  }

  char *end;
  long int num = strtol(slash+1, &end, 10);
  if (*end != '\0') {
    // junk after the prefix_len
    return false;
  }
  if (num < 0) {
    return false;
  }
  *prefix_len = num;

  // copy the part before slash to get nil termination
  char *addr = (char*)alloca(slash-s + 1);
  strncpy(addr, s, slash-s);
  addr[slash-s] = '\0';

  // caller expects ports etc to be zero
  memset(network, 0, sizeof(*network));

  // try parsing as ipv4
  int ok;
  ok = inet_pton(AF_INET, addr, &((struct sockaddr_in*)network)->sin_addr);
  if (ok) {
    network->ss_family = AF_INET;
    return true;
  }

  // try parsing as ipv6
  ok = inet_pton(AF_INET6, addr, &((struct sockaddr_in6*)network)->sin6_addr);
  if (ok) {
    network->ss_family = AF_INET6;
    return true;
  }

  return false;
}

bool parse_network(const char *s,
		   entity_addr_t *network,
		   unsigned int *prefix_len)
{
  sockaddr_storage ss;
  bool ret = parse_network(s, &ss, prefix_len);
  if (ret) {
    network->set_type(entity_addr_t::TYPE_LEGACY);
    network->set_sockaddr((sockaddr *)&ss);
  }
  return ret;
}

bool network_contains(
  const struct entity_addr_t& network,
  unsigned int prefix_len,
  const struct entity_addr_t& addr)
{
  if (addr.get_family() != network.get_family()) {
    return false;
  }
  switch (network.get_family()) {
  case AF_INET:
    {
      struct in_addr a, b;
      netmask_ipv4(
	&((const sockaddr_in*)network.get_sockaddr())->sin_addr, prefix_len, &a);
      netmask_ipv4(
	&((const sockaddr_in*)addr.get_sockaddr())->sin_addr, prefix_len, &b);
      if (memcmp(&a, &b, sizeof(a)) == 0) {
	return true;
      }
    }
    break;
  case AF_INET6:
    {
      struct in6_addr a, b;
      netmask_ipv6(
	&((const sockaddr_in6*)network.get_sockaddr())->sin6_addr, prefix_len, &a);
      netmask_ipv6(
	&((const sockaddr_in6*)addr.get_sockaddr())->sin6_addr, prefix_len, &b);
      if (memcmp(&a, &b, sizeof(a)) == 0) {
	return true;
      }
    }
    break;
  }
  return false;
}
