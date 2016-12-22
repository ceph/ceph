#include "include/ipaddr.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>


static void netmask_ipv4(const struct in_addr *addr,
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


const struct sockaddr *find_ipv4_in_subnet(const struct ifaddrs *addrs,
					   const struct sockaddr_in *net,
					   unsigned int prefix_len) {
  struct in_addr want, temp;

  netmask_ipv4(&net->sin_addr, prefix_len, &want);//将net正式化，例如1.1.1.1/8=>1.0.0.0/8

  for (; addrs != NULL; addrs = addrs->ifa_next) {//遍历每个地址

    if (addrs->ifa_addr == NULL)
      continue;

    if (strcmp(addrs->ifa_name, "lo") == 0)//跳过loop口
      continue;

    if (addrs->ifa_addr->sa_family != net->sin_family)//跳过地址类型不一致的
      continue;

    struct in_addr *cur = &((struct sockaddr_in*)addrs->ifa_addr)->sin_addr;
    netmask_ipv4(cur, prefix_len, &temp);
    //这种处理方式比较不推荐，这个prefix_len是由上层传入的，并没有取此接口对应的prefix_len

    if (temp.s_addr == want.s_addr) {
      return addrs->ifa_addr;//用第一个找到的地址
    }
  }

  return NULL;
}


static void netmask_ipv6(const struct in6_addr *addr,
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


const struct sockaddr *find_ipv6_in_subnet(const struct ifaddrs *addrs,
					   const struct sockaddr_in6 *net,
					   unsigned int prefix_len) {
  struct in6_addr want, temp;

  netmask_ipv6(&net->sin6_addr, prefix_len, &want);

  for (; addrs != NULL; addrs = addrs->ifa_next) {

    if (addrs->ifa_addr == NULL)
      continue;

    if (strcmp(addrs->ifa_name, "lo") == 0)
      continue;

    if (addrs->ifa_addr->sa_family != net->sin6_family)
      continue;

    struct in6_addr *cur = &((struct sockaddr_in6*)addrs->ifa_addr)->sin6_addr;
    netmask_ipv6(cur, prefix_len, &temp);

    if (IN6_ARE_ADDR_EQUAL(&temp, &want))
      return addrs->ifa_addr;
  }

  return NULL;
}

//找一个地址
const struct sockaddr *find_ip_in_subnet(const struct ifaddrs *addrs,
					 const struct sockaddr *net,
					 unsigned int prefix_len) {
  switch (net->sa_family) {
    case AF_INET:
      return find_ipv4_in_subnet(addrs, (struct sockaddr_in*)net, prefix_len);

    case AF_INET6:
      return find_ipv6_in_subnet(addrs, (struct sockaddr_in6*)net, prefix_len);
    }

  return NULL;
}


//解释a.a.a.a/len格式，将解析结果放入netowrk,prefix_len中，解释失败返回false
bool parse_network(const char *s, struct sockaddr *network, unsigned int *prefix_len) {
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
    network->sa_family = AF_INET;
    return true;
  }

  // try parsing as ipv6
  ok = inet_pton(AF_INET6, addr, &((struct sockaddr_in6*)network)->sin6_addr);
  if (ok) {
    network->sa_family = AF_INET6;
    return true;
  }

  return false;
}
