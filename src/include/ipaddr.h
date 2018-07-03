#ifndef CEPH_IPADDR_H
#define CEPH_IPADDR_H

/*
  Find an IP address that is in the wanted subnet.

  If there are multiple matches, the first one is returned; this order
  is system-dependent and should not be relied on.
 */
const struct ifaddrs *find_ip_in_subnet(const struct ifaddrs *addrs,
					 const struct sockaddr *net,
					 unsigned int prefix_len);


bool parse_network(const char *s, struct sockaddr_storage *network, unsigned int *prefix_len);

void netmask_ipv6(const struct in6_addr *addr,
		  unsigned int prefix_len,
		  struct in6_addr *out);

void netmask_ipv4(const struct in_addr *addr,
		  unsigned int prefix_len,
		  struct in_addr *out);
#endif
