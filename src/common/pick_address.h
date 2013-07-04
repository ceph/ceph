#ifndef CEPH_PICK_ADDRESS_H
#define CEPH_PICK_ADDRESS_H

#include "common/config.h"

class CephContext;


#define CEPH_PICK_ADDRESS_PUBLIC     0x01
#define CEPH_PICK_ADDRESS_CLUSTER    0x02

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

/**
 * check for a locally configured address
 *
 * check if any of the listed addresses is configured on the local host.
 *
 * @param cct context
 * @param ls list of addresses
 * @param match [out] pointer to match, if an item in @a ls is found configured locally.
 */
bool have_local_addr(CephContext *cct, const list<entity_addr_t>& ls, entity_addr_t *match);

#endif
