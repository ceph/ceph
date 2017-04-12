#ifndef CEPH_IPADDR_H
#define CEPH_IPADDR_H

#include <netinet/in.h>
#include <sys/types.h>
#include <ifaddrs.h>

/*
  Find an IP address that is in the wanted subnet.

  If there are multiple matches, the first one is returned; this order
  is system-dependent and should not be relied on.
 */
const struct sockaddr *find_ip_in_subnet(const struct ifaddrs *addrs,
					 const struct sockaddr *net,
					 unsigned int prefix_len);


bool parse_network(const char *s, struct sockaddr_storage *network, unsigned int *prefix_len);

#endif
