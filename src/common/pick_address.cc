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

#include <bitset>
#include <netdb.h>
#include <netinet/in.h>
#ifdef _WIN32
#include <ws2ipdef.h>
#else
#include <arpa/inet.h> // inet_pton()
#include <net/if.h> // IFF_UP
#endif
#include <string>
#include <string.h>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <fmt/format.h>

#include "include/ipaddr.h"
#include "include/str_list.h"
#include "common/ceph_context.h"
#ifndef WITH_SEASTAR
#include "common/config.h"
#include "common/config_obs.h"
#endif
#include "common/debug.h"
#include "common/errno.h"
#include "common/numa.h"

#ifndef HAVE_IN_ADDR_T
typedef uint32_t in_addr_t;
#endif

#ifndef IN_LOOPBACKNET
#define IN_LOOPBACKNET 127
#endif

#define dout_subsys ceph_subsys_

using std::string;
using std::vector;

namespace {

bool matches_with_name(const ifaddrs& ifa, const std::string& if_name)
{
  return if_name.compare(ifa.ifa_name) == 0;
}

static int is_loopback_addr(sockaddr* addr)
{
  if (addr->sa_family == AF_INET) {
    const sockaddr_in* sin = (struct sockaddr_in *)(addr);
    const in_addr_t net = ntohl(sin->sin_addr.s_addr) >> IN_CLASSA_NSHIFT;
    return net == IN_LOOPBACKNET ? 1 : 0;
  } else if (addr->sa_family == AF_INET6) {
    sockaddr_in6* sin6 = (struct sockaddr_in6 *)(addr);
    return IN6_IS_ADDR_LOOPBACK(&sin6->sin6_addr) ? 1 : 0;
  } else {
    return -1;
  }
}

static int grade_addr(const ifaddrs& ifa)
{
  if (ifa.ifa_addr == nullptr) {
    return -1;
  }
  int score = 0;
  if (ifa.ifa_flags & IFF_UP) {
    score += 4;
  }
  switch (is_loopback_addr(ifa.ifa_addr)) {
  case 0:
    // prefer non-loopback addresses
    score += 2;
    break;
  case 1:
    score += 0;
    break;
  default:
    score = -1;
    break;
  }
  return score;
}

bool matches_with_net(const ifaddrs& ifa,
                      const sockaddr* net,
                      unsigned int prefix_len,
                      unsigned ipv)
{
  switch (net->sa_family) {
  case AF_INET:
    if (ipv & CEPH_PICK_ADDRESS_IPV4) {
      return matches_ipv4_in_subnet(ifa, (struct sockaddr_in*)net, prefix_len);
    }
    break;
  case AF_INET6:
    if (ipv & CEPH_PICK_ADDRESS_IPV6) {
      return matches_ipv6_in_subnet(ifa, (struct sockaddr_in6*)net, prefix_len);
    }
    break;
  }
  return false;
}

bool matches_with_net(CephContext *cct,
                      const ifaddrs& ifa,
                      const std::string& s,
                      unsigned ipv)
{
  struct sockaddr_storage net;
  unsigned int prefix_len;
  if (!parse_network(s.c_str(), &net, &prefix_len)) {
    lderr(cct) << "unable to parse network: " << s << dendl;
    exit(1);
  }
  return matches_with_net(ifa, (sockaddr*)&net, prefix_len, ipv);
}

int grade_with_numa_node(const ifaddrs& ifa, int numa_node)
{
#if defined(WITH_SEASTAR) || defined(_WIN32)
  return 0;
#else
  if (numa_node < 0) {
    return 0;
  }
  int if_node = -1;
  int r = get_iface_numa_node(ifa.ifa_name, &if_node);
  if (r < 0) {
    return 0;
  }
  return if_node == numa_node ? 1 : 0;
#endif
}
}

const struct sockaddr *find_ip_in_subnet_list(
  CephContext *cct,
  const struct ifaddrs *ifa,
  unsigned ipv,
  const std::string &networks,
  const std::string &interfaces,
  int numa_node)
{
  const auto ifs = get_str_list(interfaces);
  const auto nets = get_str_list(networks);
  if (!ifs.empty() && nets.empty()) {
      lderr(cct) << "interface names specified but not network names" << dendl;
      exit(1);
  }

  int best_score = 0;
  const sockaddr* best_addr = nullptr;
  for (const auto* addr = ifa; addr != nullptr; addr = addr->ifa_next) {
    if (!ifs.empty() &&
	std::none_of(std::begin(ifs), std::end(ifs),
                     [&](const auto& if_name) {
                       return matches_with_name(*addr, if_name);
                     })) {
      continue;
    }
    if (!nets.empty() &&
	std::none_of(std::begin(nets), std::end(nets),
                     [&](const auto& net) {
                       return matches_with_net(cct, *addr, net, ipv);
                     })) {
      continue;
    }
    int score = grade_addr(*addr);
    if (score < 0) {
      continue;
    }
    score += grade_with_numa_node(*addr, numa_node);
    if (score > best_score) {
      best_score = score;
      best_addr = addr->ifa_addr;
    }
  }
  return best_addr;
}

#ifndef WITH_SEASTAR
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
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override {
    // do nothing.
  }
};

static void fill_in_one_address(CephContext *cct,
				const struct ifaddrs *ifa,
				const string &networks,
				const string &interfaces,
				const char *conf_var,
				int numa_node = -1)
{
  const struct sockaddr *found = find_ip_in_subnet_list(
    cct,
    ifa,
    CEPH_PICK_ADDRESS_IPV4|CEPH_PICK_ADDRESS_IPV6,
    networks,
    interfaces,
    numa_node);
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

  cct->_conf.add_observer(&obs);

  cct->_conf.set_val_or_die(conf_var, buf);
  cct->_conf.apply_changes(nullptr);

  cct->_conf.remove_observer(&obs);
}

void pick_addresses(CephContext *cct, int needs)
{
  auto public_addr = cct->_conf.get_val<entity_addr_t>("public_addr");
  auto public_network = cct->_conf.get_val<std::string>("public_network");
  auto public_network_interface =
    cct->_conf.get_val<std::string>("public_network_interface");
  auto cluster_addr = cct->_conf.get_val<entity_addr_t>("cluster_addr");
  auto cluster_network = cct->_conf.get_val<std::string>("cluster_network");
  auto cluster_network_interface =
    cct->_conf.get_val<std::string>("cluster_network_interface");

  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    string err = cpp_strerror(errno);
    lderr(cct) << "unable to fetch interfaces and addresses: " << err << dendl;
    exit(1);
  }
  auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
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
}
#endif	// !WITH_SEASTAR

static std::optional<entity_addr_t> get_one_address(
  CephContext *cct,
  const struct ifaddrs *ifa,
  unsigned ipv,
  const string &networks,
  const string &interfaces,
  int numa_node = -1)
{
  const struct sockaddr *found = find_ip_in_subnet_list(cct, ifa, ipv,
							networks,
							interfaces,
							numa_node);
  if (!found) {
    std::string_view ip_type;
    if ((ipv & CEPH_PICK_ADDRESS_IPV4) && (ipv & CEPH_PICK_ADDRESS_IPV6)) {
      ip_type = "IPv4 or IPv6";
    } else if (ipv & CEPH_PICK_ADDRESS_IPV4) {
      ip_type = "IPv4";
    } else {
      ip_type = "IPv6";
    }
    lderr(cct) << "unable to find any " << ip_type << " address in networks '"
               << networks << "' interfaces '" << interfaces << "'" << dendl;
    return {};
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
    return {};
  }

  entity_addr_t addr;
  if (addr.parse(buf)) {
    return addr;
  } else {
    return {};
  }
}

int pick_addresses(
  CephContext *cct,
  unsigned flags,
  struct ifaddrs *ifa,
  entity_addrvec_t *addrs,
  int preferred_numa_node)
{
  addrs->v.clear();

  unsigned addrt = (flags & (CEPH_PICK_ADDRESS_PUBLIC |
			     CEPH_PICK_ADDRESS_PUBLIC_BIND |
			     CEPH_PICK_ADDRESS_CLUSTER));
  // TODO: move to std::popcount when it's available for all release lines
  // we are interested in (quincy was a blocker at the time of writing)
  if (std::bitset<sizeof(addrt)*CHAR_BIT>(addrt).count() != 1) {
    // these flags are mutually exclusive and one of them must be
    // always set (in other words: it's mode selection).
    return -EINVAL;
  }
  unsigned msgrv = flags & (CEPH_PICK_ADDRESS_MSGR1 |
			    CEPH_PICK_ADDRESS_MSGR2);
  if (msgrv == 0) {
    if (cct->_conf.get_val<bool>("ms_bind_msgr1")) {
      msgrv |= CEPH_PICK_ADDRESS_MSGR1;
    }
    if (cct->_conf.get_val<bool>("ms_bind_msgr2")) {
      msgrv |= CEPH_PICK_ADDRESS_MSGR2;
    }
    if (msgrv == 0) {
      return -EINVAL;
    }
  }
  unsigned ipv = flags & (CEPH_PICK_ADDRESS_IPV4 |
			  CEPH_PICK_ADDRESS_IPV6);
  if (ipv == 0) {
    if (cct->_conf.get_val<bool>("ms_bind_ipv4")) {
      ipv |= CEPH_PICK_ADDRESS_IPV4;
    }
    if (cct->_conf.get_val<bool>("ms_bind_ipv6")) {
      ipv |= CEPH_PICK_ADDRESS_IPV6;
    }
    if (ipv == 0) {
      return -EINVAL;
    }
    if (cct->_conf.get_val<bool>("ms_bind_prefer_ipv4")) {
      flags |= CEPH_PICK_ADDRESS_PREFER_IPV4;
    } else {
      flags &= ~CEPH_PICK_ADDRESS_PREFER_IPV4;
    }
  }

  entity_addr_t addr;
  string networks;
  string interfaces;
  if (addrt & CEPH_PICK_ADDRESS_PUBLIC) {
    addr = cct->_conf.get_val<entity_addr_t>("public_addr");
    networks = cct->_conf.get_val<std::string>("public_network");
    interfaces =
      cct->_conf.get_val<std::string>("public_network_interface");
  } else if (addrt & CEPH_PICK_ADDRESS_PUBLIC_BIND) {
    addr = cct->_conf.get_val<entity_addr_t>("public_bind_addr");
    // XXX: we don't support _network nor _network_interface for
    // the public_bind addrs yet.
    if (addr.is_blank_ip()) {
      return -ENOENT;
    }
  } else {
    addr = cct->_conf.get_val<entity_addr_t>("cluster_addr");
    networks = cct->_conf.get_val<std::string>("cluster_network");
    interfaces =
      cct->_conf.get_val<std::string>("cluster_network_interface");
    if (networks.empty()) {
      lderr(cct) << "Falling back to public interface" << dendl;
      // fall back to public_ network and interface if cluster is not set
      networks = cct->_conf.get_val<std::string>("public_network");
      interfaces =
	cct->_conf.get_val<std::string>("public_network_interface");
    }
  }
  if (addr.is_blank_ip() &&
      !networks.empty()) {
    // note: pass in ipv to filter the matching addresses
    for (auto pick_mask :  {CEPH_PICK_ADDRESS_IPV4, CEPH_PICK_ADDRESS_IPV6}) {
      if (ipv & pick_mask) {
        auto ip_addr = get_one_address(cct, ifa, pick_mask,
                                       networks, interfaces,
                                       preferred_numa_node);
        if (ip_addr) {
          addrs->v.push_back(*ip_addr);
        } else {
          // picked but not found
          return -1;
        }
      }
    }
  }

  // note: we may have a blank addr here

  // ipv4 and/or ipv6?
  if (addrs->v.empty()) {
    addr.set_type(entity_addr_t::TYPE_MSGR2);
    for (auto pick_mask : {CEPH_PICK_ADDRESS_IPV4, CEPH_PICK_ADDRESS_IPV6}) {
      if (ipv & pick_mask) {
        addr.set_family(pick_mask == CEPH_PICK_ADDRESS_IPV4 ? AF_INET : AF_INET6);
        addrs->v.push_back(addr);
      }
    }
  }

  std::sort(addrs->v.begin(), addrs->v.end(),
	    [flags] (entity_addr_t& lhs, entity_addr_t& rhs) {
	      if (flags & CEPH_PICK_ADDRESS_PREFER_IPV4) {
		return lhs.is_ipv4() && rhs.is_ipv6();
	      } else {
		return lhs.is_ipv6() && rhs.is_ipv4();
	      }
	    });

  // msgr2 or legacy or both?
  if (msgrv == (CEPH_PICK_ADDRESS_MSGR1 | CEPH_PICK_ADDRESS_MSGR2)) {
    vector<entity_addr_t> v;
    v.swap(addrs->v);
    for (auto a : v) {
      a.set_type(entity_addr_t::TYPE_MSGR2);
      if (flags & CEPH_PICK_ADDRESS_DEFAULT_MON_PORTS) {
	a.set_port(CEPH_MON_PORT_IANA);
      }
      addrs->v.push_back(a);
      a.set_type(entity_addr_t::TYPE_LEGACY);
      if (flags & CEPH_PICK_ADDRESS_DEFAULT_MON_PORTS) {
	a.set_port(CEPH_MON_PORT_LEGACY);
      }
      addrs->v.push_back(a);
    }
  } else if (msgrv == CEPH_PICK_ADDRESS_MSGR1) {
    for (auto& a : addrs->v) {
      a.set_type(entity_addr_t::TYPE_LEGACY);
    }
  } else {
    for (auto& a : addrs->v) {
      a.set_type(entity_addr_t::TYPE_MSGR2);
    }
  }

  return 0;
}

int pick_addresses(
  CephContext *cct,
  unsigned flags,
  entity_addrvec_t *addrs,
  int preferred_numa_node)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    r = -errno;
    string err = cpp_strerror(r);
    lderr(cct) << "unable to fetch interfaces and addresses: "
	       <<  cpp_strerror(r) << dendl;
    return r;
  }
  r = pick_addresses(cct, flags, ifa, addrs, preferred_numa_node);
  freeifaddrs(ifa);
  return r;
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
  auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
  const unsigned int prefix_len = std::max(sizeof(in_addr::s_addr), sizeof(in6_addr::s6_addr)) * CHAR_BIT;
  for (auto addr = ifa; addr != nullptr; addr = addr->ifa_next) {
    if (matches_with_net(*ifa, (const struct sockaddr *) &network, prefix_len,
			 CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6)) {
      return addr->ifa_name;
    }
  }
  return {};
}


bool have_local_addr(CephContext *cct, const std::list<entity_addr_t>& ls, entity_addr_t *match)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    lderr(cct) << "unable to fetch interfaces and addresses: " << cpp_strerror(errno) << dendl;
    exit(1);
  }
  auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });

  for (struct ifaddrs *addrs = ifa; addrs != nullptr; addrs = addrs->ifa_next) {
    if (addrs->ifa_addr) {
      entity_addr_t a;
      a.set_sockaddr(addrs->ifa_addr);
      for (auto& p : ls) {
        if (a.is_same_host(p)) {
          *match = p;
          return true;
        }
      }
    }
  }
  return false;
}

int get_iface_numa_node(
  const std::string& iface,
  int *node)
{
  enum class iface_t {
    PHY_PORT,
    BOND_PORT
  } ifatype = iface_t::PHY_PORT;
  std::string_view ifa{iface};
  if (auto pos = ifa.find(":"); pos != ifa.npos) {
    ifa.remove_suffix(ifa.size() - pos);
  }
  string fn = fmt::format("/sys/class/net/{}/device/numa_node", ifa);
  int fd = ::open(fn.c_str(), O_RDONLY);
  if (fd < 0) {
    fn = fmt::format("/sys/class/net/{}/bonding/slaves", ifa);
    fd = ::open(fn.c_str(), O_RDONLY);
    if (fd < 0) {
      return -errno;
    }
    ifatype = iface_t::BOND_PORT;
  }

  int r = 0;
  char buf[1024];
  char *endptr = 0;
  r = safe_read(fd, &buf, sizeof(buf));
  if (r < 0) {
    goto out;
  }
  buf[r] = 0;
  while (r > 0 && ::isspace(buf[--r])) {
    buf[r] = 0;
  }

  switch (ifatype) {
  case iface_t::PHY_PORT:
    *node = strtoll(buf, &endptr, 10);
    if (endptr != buf + strlen(buf)) {
      r = -EINVAL;
      goto out;
    }
    r = 0;
    break;
  case iface_t::BOND_PORT:
    int bond_node = -1;
    std::vector<std::string> sv;
    std::string ifacestr = buf;
    get_str_vec(ifacestr, " ", sv);
    for (auto& iter : sv) {
      int bn = -1;
      r = get_iface_numa_node(iter, &bn);
      if (r >= 0) {
        if (bond_node == -1 || bn == bond_node) {
          bond_node = bn;
        } else {
          *node = -2;
          goto out;
        }
      } else {
        goto out;
      }
    }
    *node = bond_node;
    break;
  }

  out:
  ::close(fd);
  return r;
}

bool is_addr_in_subnet(
  CephContext *cct,
  const std::string &networks,
  const std::string &addr)
{
  const auto nets = get_str_list(networks);
  ceph_assert(!nets.empty());

  unsigned ipv = CEPH_PICK_ADDRESS_IPV4;
  struct sockaddr_in public_addr;
  public_addr.sin_family = AF_INET;

  if(inet_pton(AF_INET, addr.c_str(), &public_addr.sin_addr) != 1) {
    lderr(cct) << "unable to convert chosen address to string: " << addr << dendl;
    return false;
  }

  for (const auto &net : nets) {
    struct ifaddrs ifa;
    memset(&ifa, 0, sizeof(ifa));
    ifa.ifa_next = nullptr;
    ifa.ifa_addr = (struct sockaddr*)&public_addr;
    if(matches_with_net(cct, ifa, net, ipv)) {
      return true;
    }
  }
  return false;
}
