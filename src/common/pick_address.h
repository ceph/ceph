// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_PICK_ADDRESS_H
#define CEPH_PICK_ADDRESS_H

#include <string>
#include <list>

class CephContext;
struct entity_addr_t;
class entity_addrvec_t;


#define CEPH_PICK_ADDRESS_PUBLIC      0x01
#define CEPH_PICK_ADDRESS_CLUSTER     0x02
#define CEPH_PICK_ADDRESS_MSGR1       0x04
#define CEPH_PICK_ADDRESS_MSGR2       0x08
#define CEPH_PICK_ADDRESS_IPV4        0x10
#define CEPH_PICK_ADDRESS_IPV6        0x20
#define CEPH_PICK_ADDRESS_PREFER_IPV4 0x40
#define CEPH_PICK_ADDRESS_DEFAULT_MON_PORTS  0x80

#ifndef WITH_SEASTAR
/*
  Pick addresses based on subnets if needed.

  If an address is not explicitly given, and a list of subnets is
  given, find an assigned IP address in the subnets and set that.

  cluster_addr is set based on cluster_network, public_addr is set
  based on public_network.

  cluster_network and public_network are a list of ip/prefix pairs.

  All IP addresses assigned to all local network interfaces are
  potential matches.

  If multiple IP addresses match the subnet, one of them will be
  picked, effectively randomly.

  This function will exit on error.
 */
void pick_addresses(CephContext *cct, int needs);

#endif	// !WITH_SEASTAR

int pick_addresses(CephContext *cct, unsigned flags, entity_addrvec_t *addrs,
		   int preferred_numa_node = -1);
int pick_addresses(CephContext *cct, unsigned flags, struct ifaddrs *ifa,
		   entity_addrvec_t *addrs,
		   int preferred_numa_node = -1);

/**
 * Find a network interface whose address matches the address/netmask
 * in `network`.
 */
std::string pick_iface(CephContext *cct, const struct sockaddr_storage &network);

/**
 * check for a locally configured address
 *
 * check if any of the listed addresses is configured on the local host.
 *
 * @param cct context
 * @param ls list of addresses
 * @param match [out] pointer to match, if an item in @a ls is found configured locally.
 */
bool have_local_addr(CephContext *cct, const std::list<entity_addr_t>& ls, entity_addr_t *match);

const struct sockaddr *find_ip_in_subnet_list(
  CephContext *cct,
  const struct ifaddrs *ifa,
  unsigned ipv,
  const std::string &networks,
  const std::string &interfaces,
  int numa_node=-1);

int get_iface_numa_node(
  const std::string& iface,
  int *node);

#endif
