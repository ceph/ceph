#ifndef CEPH_IPADDR_H
#define CEPH_IPADDR_H

class entity_addr_t;

/*
 * Check if an IP address that is in the wanted subnet.
 */
bool matches_ipv4_in_subnet(const struct ifaddrs& addrs,
                            const struct sockaddr_in* net,
                            unsigned int prefix_len);
bool matches_ipv6_in_subnet(const struct ifaddrs& addrs,
                            const struct sockaddr_in6* net,
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
bool parse_network(const char *s,
		   struct sockaddr_storage *network,
		   unsigned int *prefix_len);
bool parse_network(const char *s,
		   entity_addr_t *network,
		   unsigned int *prefix_len);

void netmask_ipv6(const struct in6_addr *addr,
		  unsigned int prefix_len,
		  struct in6_addr *out);

void netmask_ipv4(const struct in_addr *addr,
		  unsigned int prefix_len,
		  struct in_addr *out);

bool network_contains(
	const struct entity_addr_t& network,
	unsigned int prefix_len,
	const struct entity_addr_t& addr);

#endif
