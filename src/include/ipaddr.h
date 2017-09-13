#ifndef CEPH_IPADDR_H
#define CEPH_IPADDR_H

/*
 * Find an IP address that is in the wanted subnet.
 *
 * If there are multiple matches, the first one is returned; this order
 * is system-dependent and should not be relied on.
 */
const struct ifaddrs *find_ip_in_subnet(const struct ifaddrs *addrs,
					 const struct sockaddr *net,
					 unsigned int prefix_len);

/*
 * Validate and parse IPv4 or IPv6 network
 *
 * Given a network (e.g. "192.168.0.0/24") and pointers to a sockaddr_storage
 * struct and an unsigned int:
 *
 * if the network string is valid, return true and populate sockaddr_storage
 * and prefix_len;
 *
 * if the network string is invalid, return false.
 */
bool parse_network(const char *s, struct sockaddr_storage *network, unsigned int *prefix_len);

#endif
