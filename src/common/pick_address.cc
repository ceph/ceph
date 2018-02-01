// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/pick_address.h"
#include "include/ipaddr.h"
#include "include/str_list.h"
#include "common/debug.h"
#include "common/errno.h"

#include <netdb.h>

#define dout_subsys ceph_subsys_

const struct sockaddr *find_ip_in_subnet_list(
  CephContext *cct,
  const struct ifaddrs *ifa,
  const std::string &networks,
  const std::string &interfaces)
{
  std::list<string> nets;
  get_str_list(networks, nets);
  std::list<string> ifs;
  get_str_list(interfaces, ifs);

  // filter interfaces by name
  const struct ifaddrs *filtered = nullptr;
  if (ifs.empty()) {
    filtered = ifa;
  } else {
    if (nets.empty()) {
      lderr(cct) << "interface names specified but not network names" << dendl;
      exit(1);
    }
    const struct ifaddrs *t = ifa;
    struct ifaddrs *head = 0;
    while (t) {
      bool match = false;
      for (auto& i : ifs) {
	if (strcmp(i.c_str(), t->ifa_name) == 0) {
	  match = true;
	  break;
	}
      }
      if (match) {
	struct ifaddrs *n = new ifaddrs;
	memcpy(n, t, sizeof(*t));
	n->ifa_next = head;
	head = n;
      }
      t = t->ifa_next;
    }
    if (!head) {
      lderr(cct) << "no interfaces matching " << ifs << dendl;
      exit(1);
    }
    filtered = head;
  }

  struct sockaddr *r = nullptr;
  for (auto& s : nets) {
    struct sockaddr_storage net;
    unsigned int prefix_len;

    if (!parse_network(s.c_str(), &net, &prefix_len)) {
      lderr(cct) << "unable to parse network: " << s << dendl;
      exit(1);
    }

    const struct ifaddrs *found = find_ip_in_subnet(
      filtered,
      (struct sockaddr *) &net, prefix_len);
    if (found) {
      r = found->ifa_addr;
      break;
    }
  }

  if (filtered != ifa) {
    while (filtered) {
      struct ifaddrs *t = filtered->ifa_next;
      delete filtered;
      filtered = t;
    }
  }

  return r;
}

// observe this change
struct Observer : public md_config_obs_t {
  const char *keys[2];
  explicit Observer(const char *c) {
    keys[0] = c;
    keys[1] = NULL;
  }

  const char** get_tracked_conf_keys() const override {
    return (const char **)keys;
  }
  void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed) override {
    // do nothing.
  }
};

static void fill_in_one_address(CephContext *cct,
				const struct ifaddrs *ifa,
				const string networks,
				const string interfaces,
				const char *conf_var)
{
  const struct sockaddr *found = find_ip_in_subnet_list(cct, ifa, networks,
							interfaces);
  if (!found) {
    lderr(cct) << "unable to find any IP address in networks '" << networks
	       << "' interfaces '" << interfaces << "'" << dendl;
    exit(1);
  }

  char buf[INET6_ADDRSTRLEN];
  int err;

  err = getnameinfo(found,
		    (found->sa_family == AF_INET)
		    ? sizeof(struct sockaddr_in)
		    : sizeof(struct sockaddr_in6),

		    buf, sizeof(buf),
		    nullptr, 0,
		    NI_NUMERICHOST);
  if (err != 0) {
    lderr(cct) << "unable to convert chosen address to string: " << gai_strerror(err) << dendl;
    exit(1);
  }

  Observer obs(conf_var);

  cct->_conf->add_observer(&obs);

  cct->_conf->set_val_or_die(conf_var, buf);
  cct->_conf->apply_changes(nullptr);

  cct->_conf->remove_observer(&obs);
}

void pick_addresses(CephContext *cct, int needs)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  auto public_addr = cct->_conf->get_val<entity_addr_t>("public_addr");
  auto public_network = cct->_conf->get_val<std::string>("public_network");
  auto public_network_interface =
    cct->_conf->get_val<std::string>("public_network_interface");
  auto cluster_addr = cct->_conf->get_val<entity_addr_t>("cluster_addr");
  auto cluster_network = cct->_conf->get_val<std::string>("cluster_network");
  auto cluster_network_interface =
    cct->_conf->get_val<std::string>("cluster_network_interface");

  if (r < 0) {
    string err = cpp_strerror(errno);
    lderr(cct) << "unable to fetch interfaces and addresses: " << err << dendl;
    exit(1);
  }

  if ((needs & CEPH_PICK_ADDRESS_PUBLIC) &&
    public_addr.is_blank_ip() && !public_network.empty()) {
    fill_in_one_address(cct, ifa, public_network, public_network_interface,
      "public_addr");
  }

  if ((needs & CEPH_PICK_ADDRESS_CLUSTER) && cluster_addr.is_blank_ip()) {
    if (!cluster_network.empty()) {
      fill_in_one_address(cct, ifa, cluster_network, cluster_network_interface,
	"cluster_addr");
    } else {
      if (!public_network.empty()) {
        lderr(cct) << "Public network was set, but cluster network was not set " << dendl;
        lderr(cct) << "    Using public network also for cluster network" << dendl;
        fill_in_one_address(cct, ifa, public_network, public_network_interface,
          "cluster_addr");
      }
    }
  }

  freeifaddrs(ifa);
}


std::string pick_iface(CephContext *cct, const struct sockaddr_storage &network)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    string err = cpp_strerror(errno);
    lderr(cct) << "unable to fetch interfaces and addresses: " << err << dendl;
    return {};
  }

  const unsigned int prefix_len = max(sizeof(in_addr::s_addr), sizeof(in6_addr::s6_addr)) * CHAR_BIT;
  const struct ifaddrs *found = find_ip_in_subnet(ifa,
                                  (const struct sockaddr *) &network, prefix_len);

  std::string result;
  if (found) {
    result = found->ifa_name;
  }

  freeifaddrs(ifa);

  return result;
}


bool have_local_addr(CephContext *cct, const list<entity_addr_t>& ls, entity_addr_t *match)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    lderr(cct) << "unable to fetch interfaces and addresses: " << cpp_strerror(errno) << dendl;
    exit(1);
  }

  bool found = false;
  for (struct ifaddrs *addrs = ifa; addrs != nullptr; addrs = addrs->ifa_next) {
    if (addrs->ifa_addr) {
      entity_addr_t a;
      a.set_sockaddr(addrs->ifa_addr);
      for (auto& p : ls) {
        if (a.is_same_host(p)) {
          *match = p;
          found = true;
          goto out;
        }
      }
    }
  }

 out:
  freeifaddrs(ifa);
  return found;
}
