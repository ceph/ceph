// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_config.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#ifdef __linux__
#include <sys/capability.h>
#include <sys/prctl.h>
#endif

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/pick_address.h"
#include "include/ipaddr.h"
#include "include/scope_guard.h"
#include "include/str_list.h"

namespace fs = std::filesystem;

namespace ceph::oob {

namespace {

/// addr as the 16 bytes of a GID: an IPv6 address, or an IPv4 address
/// mapped into IPv6
bool addr_to_gid(const std::string& addr, uint8_t gid[16])
{
  in_addr v4;
  if (inet_pton(AF_INET, addr.c_str(), &v4) == 1) {
    std::memset(gid, 0, 16);
    gid[10] = gid[11] = 0xff;
    std::memcpy(gid + 12, &v4, 4);
    return true;
  }
  in6_addr v6;
  if (inet_pton(AF_INET6, addr.c_str(), &v6) == 1) {
    std::memcpy(gid, &v6, 16);
    return true;
  }
  return false;
}

/// a GID as sysfs writes it, "0000:0000:0000:0000:0000:ffff:0a5a:0001",
/// which the kernel prints as an IPv6 address (%pI6)
bool parse_gid(const std::string& s, uint8_t gid[16])
{
  return inet_pton(AF_INET6, s.c_str(), gid) == 1;
}

std::string read_line(const fs::path& p)
{
  std::ifstream in(p);
  std::string s;
  if (in) {
    std::getline(in, s);
  }
  return s;
}

/// the numeric entries of a directory, in order
std::vector<unsigned> numbered(const fs::path& dir)
{
  std::vector<unsigned> out;
  std::error_code ec;
  for (const auto& e : fs::directory_iterator(dir, ec)) {
    const auto n = e.path().filename().string();
    if (!n.empty() && std::all_of(n.begin(), n.end(), [](unsigned char c) { return std::isdigit(c); })) {
      out.push_back(static_cast<unsigned>(std::stoul(n)));
    }
  }
  std::sort(out.begin(), out.end());
  return out;
}

/// the RDMA device of a network interface, from sysfs, or empty
std::string device_of_iface(const std::string& sysfs, const std::string& iface)
{
  std::error_code ec;
  std::vector<std::string> devs;
  for (const auto& e : fs::directory_iterator(
	 fs::path(sysfs) / "class/net" / iface / "device/infiniband", ec)) {
    devs.push_back(e.path().filename().string());
  }
  std::sort(devs.begin(), devs.end());
  return devs.empty() ? std::string{} : devs.front();
}

/// find_rdma_port(), following at most depth levels of bonds
bool port_under(const std::string& sysfs, const std::string& iface,
		int depth, std::string* device, unsigned* port)
{
  const auto dir = fs::path(sysfs) / "class/net" / iface;
  if (auto d = device_of_iface(sysfs, iface); !d.empty()) {
    // dev_port numbers the ports of the device from 0, as IPoIB and
    // mlx4 set it
    const auto s = read_line(dir / "dev_port");
    unsigned p = 0;
    const auto [end, err] = std::from_chars(s.data(), s.data() + s.size(), p);
    *device = std::move(d);
    *port = err == std::errc{} && end == s.data() + s.size() && p < 255 ?
      p + 1 : 1;
    return true;
  }
  // a bond names no device, but its slaves do. Only for InfiniBand
  // (ARPHRD_INFINIBAND): an Ethernet address that no RoCE GID holds has
  // no GID to use
  if (depth == 0 || read_line(dir / "type") != "32") {
    return false;
  }
  std::vector<std::string> lower;
  std::error_code ec;
  for (const auto& e : fs::directory_iterator(dir, ec)) {
    const auto n = e.path().filename().string();
    if (n.starts_with("lower_")) {
      lower.push_back(n.substr(6));
    }
  }
  std::sort(lower.begin(), lower.end());
  if (auto a = read_line(dir / "bonding/active_slave"); !a.empty()) {
    lower.insert(lower.begin(), std::move(a));
  }
  return std::any_of(lower.begin(), lower.end(), [&](const auto& l) {
    return port_under(sysfs, l, depth - 1, device, port);
  });
}

/// what is wrong with oob_network, empty when nothing is: an entry that
/// is not a subnet, on which find_ip_in_subnet_list() would exit rather
/// than return, or no entry, which would match every address (and with
/// oob_network_interface exit too). oob_network_interface needs no
/// check: a name that no interface has matches none
std::string check_networks(const std::string& nets)
{
  const auto subnets = get_str_list(nets);
  if (subnets.empty()) {
    return "oob_network '" + nets + "' names no subnet";
  }
  for (const auto& n : subnets) {
    sockaddr_storage net;
    unsigned prefix_len;
    if (!parse_network(n.c_str(), &net, &prefix_len)) {
      return "oob_network entry '" + n + "' is not a subnet, such as "
	"10.0.0.0/24 or fd00::/64";
    }
  }
  return {};
}

/// the interface that carries addr, or empty
std::string iface_of_addr(const std::string& addr)
{
  uint8_t want[16];
  if (!addr_to_gid(addr, want)) {
    return {};
  }
  ifaddrs* ifa = nullptr;
  if (getifaddrs(&ifa) < 0) {
    return {};
  }
  auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
  for (auto* a = ifa; a; a = a->ifa_next) {
    if (!a->ifa_addr) {
      continue;
    }
    char buf[INET6_ADDRSTRLEN];
    const void* raw = nullptr;
    if (a->ifa_addr->sa_family == AF_INET) {
      raw = &reinterpret_cast<const sockaddr_in*>(a->ifa_addr)->sin_addr;
    } else if (a->ifa_addr->sa_family == AF_INET6) {
      raw = &reinterpret_cast<const sockaddr_in6*>(a->ifa_addr)->sin6_addr;
    } else {
      continue;
    }
    uint8_t have[16];
    if (inet_ntop(a->ifa_addr->sa_family, raw, buf, sizeof(buf)) &&
	addr_to_gid(buf, have) && std::memcmp(have, want, 16) == 0) {
      return a->ifa_name;
    }
  }
  return {};
}

#ifdef __linux__
/// whether the calling thread holds CAP_NET_RAW in its effective set
/// and in its permitted set; 0, or -errno when it cannot tell
int net_raw_held(bool* effective, bool* permitted)
{
  cap_t caps = cap_get_proc();
  if (!caps) {
    return -errno;
  }
  auto free_caps = make_scope_guard([caps] { cap_free(caps); });
  cap_flag_value_t e = CAP_CLEAR;
  cap_flag_value_t p = CAP_CLEAR;
  if (cap_get_flag(caps, CAP_NET_RAW, CAP_EFFECTIVE, &e) < 0 ||
      cap_get_flag(caps, CAP_NET_RAW, CAP_PERMITTED, &p) < 0) {
    return -errno;
  }
  *effective = e == CAP_SET;
  *permitted = p == CAP_SET;
  return 0;
}

/// raise CAP_NET_RAW into the calling thread's effective set, or drop it
int set_net_raw(cap_flag_value_t v)
{
  cap_t caps = cap_get_proc();
  if (!caps) {
    return -errno;
  }
  auto free_caps = make_scope_guard([caps] { cap_free(caps); });
  const cap_value_t raw[1] = {CAP_NET_RAW};
  if (cap_set_flag(caps, CAP_EFFECTIVE, 1, raw, v) < 0 ||
      cap_set_proc(caps) < 0) {
    return -errno;
  }
  return 0;
}
#endif

} // anonymous namespace

std::string location_t::describe() const
{
  if (addr.empty()) {
    return "no out-of-band address";
  }
  std::string s = addr + " (" + source + ")";
  if (!iface.empty()) {
    s += " on " + iface;
  }
  if (!rdma_device.empty()) {
    s += ", RDMA device " + rdma_device + " port " +
      std::to_string(rdma_port) + " GID " + std::to_string(gid_index);
  }
  return s;
}

bool find_rdma_gid(const std::string& sysfs, const std::string& addr,
		   std::string* device, unsigned* port, int* gid_index)
{
  uint8_t want[16];
  if (!addr_to_gid(addr, want)) {
    return false;
  }
  std::error_code ec;
  std::vector<std::string> devs;
  for (const auto& e : fs::directory_iterator(
	 fs::path(sysfs) / "class/infiniband", ec)) {
    devs.push_back(e.path().filename().string());
  }
  std::sort(devs.begin(), devs.end());
  // a RoCE v2 GID first: it is the one that routes
  for (const bool v2_only : {true, false}) {
    for (const auto& d : devs) {
      const auto pdir = fs::path(sysfs) / "class/infiniband" / d / "ports";
      for (const unsigned p : numbered(pdir)) {
	const auto gdir = pdir / std::to_string(p);
	for (const unsigned i : numbered(gdir / "gids")) {
	  // an unused entry is all zeros, and holds no address
	  static constexpr uint8_t unused[16] = {};
	  uint8_t gid[16];
	  if (!parse_gid(read_line(gdir / "gids" / std::to_string(i)), gid) ||
	      std::memcmp(gid, unused, 16) == 0 ||
	      std::memcmp(gid, want, 16) != 0) {
	    continue;
	  }
	  const auto type =
	    read_line(gdir / "gid_attrs/types" / std::to_string(i));
	  if (v2_only && type != "RoCE v2") {
	    continue;
	  }
	  *device = d;
	  *port = p;
	  *gid_index = static_cast<int>(i);
	  return true;
	}
      }
    }
  }
  return false;
}

bool find_rdma_port(const std::string& sysfs, const std::string& iface,
		    std::string* device, unsigned* port)
{
  // a bond of bonds is as deep as it goes
  return port_under(sysfs, iface, 2, device, port);
}

int resolve(CephContext* cct, std::string_view fallback, location_t* out,
	    std::string* err)
{
  *out = location_t{};
  const auto& conf = cct->_conf;
  const auto addr = conf.get_val<std::string>("oob_addr");
  const auto nets = conf.get_val<std::string>("oob_network");
  const auto ifaces = conf.get_val<std::string>("oob_network_interface");
  if (!addr.empty()) {
    uint8_t g[16];
    if (!addr_to_gid(addr, g)) {
      *err = "oob_addr '" + addr + "' is not an IP address";
      return -EINVAL;
    }
    out->addr = addr;
    out->source = "oob_addr";
  } else if (!nets.empty()) {
    if (auto bad = check_networks(nets); !bad.empty()) {
      *err = std::move(bad);
      return -EINVAL;
    }
    ifaddrs* ifa = nullptr;
    if (getifaddrs(&ifa) < 0) {
      const int e = errno;
      *err = std::string("getifaddrs: ") + std::strerror(e);
      return -e;
    }
    auto free_ifa = make_scope_guard([ifa] { freeifaddrs(ifa); });
    const sockaddr* sa = find_ip_in_subnet_list(
      cct, ifa, CEPH_PICK_ADDRESS_IPV4 | CEPH_PICK_ADDRESS_IPV6, nets,
      ifaces, -1);
    if (!sa) {
      *err = "no address of this host is in oob_network " + nets +
	(ifaces.empty() ? std::string{} :
	 " on the interfaces " + ifaces);
      return -EADDRNOTAVAIL;
    }
    char buf[INET6_ADDRSTRLEN];
    const void* raw = sa->sa_family == AF_INET ?
      static_cast<const void*>(
	&reinterpret_cast<const sockaddr_in*>(sa)->sin_addr) :
      static_cast<const void*>(
	&reinterpret_cast<const sockaddr_in6*>(sa)->sin6_addr);
    if (!inet_ntop(sa->sa_family, raw, buf, sizeof(buf))) {
      *err = std::string("inet_ntop: ") + std::strerror(errno);
      return -EINVAL;
    }
    out->addr = buf;
    out->source = "oob_network";
    // the interface the match came from: under oob_network_interface, a
    // VLAN that shares its parent's address must not turn into the parent
    for (const ifaddrs* i = ifa; i; i = i->ifa_next) {
      if (i->ifa_addr == sa && i->ifa_name) {
	out->iface = i->ifa_name;
	break;
      }
    }
  } else if (!ifaces.empty()) {
    *err = "oob_network_interface needs oob_network";
    return -EINVAL;
  } else if (!fallback.empty()) {
    out->addr = std::string(fallback);
    out->source = "public address";
  } else {
    return 0;
  }
  if (out->iface.empty()) {
    out->iface = iface_of_addr(out->addr);
  }
  if (!find_rdma_gid("/sys", out->addr, &out->rdma_device, &out->rdma_port,
		     &out->gid_index) &&
      !out->iface.empty()) {
    // InfiniBand: the GIDs do not hold the IPoIB address, but the
    // interface, or a bond's slave, names its device and port
    find_rdma_port("/sys", out->iface, &out->rdma_device, &out->rdma_port);
  }
  return 0;
}

bool is_verbs_provider(std::string_view provider)
{
  return provider == "verbs" || provider.substr(0, 6) == "verbs;";
}

void ofi_where(const location_t& loc, std::string_view provider,
	       std::string* domain, std::string* node,
	       std::string* alt_domain)
{
  domain->clear();
  node->clear();
  if (alt_domain) {
    alt_domain->clear();
  }
  if (is_verbs_provider(provider)) {
    *domain = loc.rdma_device;
    *node = loc.addr;
  } else if (provider == "uet") {
    *domain = loc.iface;
    if (alt_domain && loc.rdma_device != loc.iface) {
      *alt_domain = loc.rdma_device;
    }
  } else if (provider != "shm" && provider != "efa") {
    *node = loc.addr;
  }
}

bool needs_net_raw(std::string_view provider)
{
  return provider == "uet";
}

int keep_caps(std::string* err)
{
#ifdef __linux__
  if (prctl(PR_SET_KEEPCAPS, 1) == -1) {
    const int r = -errno;
    *err = "unable to set the keepcaps flag: " + cpp_strerror(r);
    return r;
  }
#endif
  return 0;
}

int keep_only_net_raw(bool* kept, std::string* err)
{
  *kept = false;
#ifdef __linux__
  bool effective = false;
  bool permitted = false;
  if (int r = net_raw_held(&effective, &permitted); r < 0) {
    *err = "unable to read the capabilities: " + cpp_strerror(r);
    return r;
  }
  // every set empty, but CAP_NET_RAW in the permitted one
  cap_t caps = cap_init();
  if (!caps) {
    const int r = -errno;
    *err = "unable to drop the capabilities: " + cpp_strerror(r);
    return r;
  }
  auto free_caps = make_scope_guard([caps] { cap_free(caps); });
  const cap_value_t raw[1] = {CAP_NET_RAW};
  if ((permitted &&
       cap_set_flag(caps, CAP_PERMITTED, 1, raw, CAP_SET) < 0) ||
      cap_set_proc(caps) < 0) {
    const int r = -errno;
    *err = "unable to drop the capabilities: " + cpp_strerror(r);
    return r;
  }
  *kept = permitted;
  // nothing is left to keep; only a later setuid() reads the flag
  prctl(PR_SET_KEEPCAPS, 0);
#endif
  return 0;
}

int with_net_raw(const std::function<void()>& fn, std::string* err)
{
#ifdef __linux__
  bool effective = false;
  bool permitted = false;
  if (int r = net_raw_held(&effective, &permitted); r < 0) {
    *err = "unable to read the capabilities: " + cpp_strerror(r);
    fn();
    return r;
  }
  if (effective) {
    fn();
    return 0;
  }
  if (!permitted) {
    *err = "the process does not hold CAP_NET_RAW";
    fn();
    return -EPERM;
  }
  // a plain thread, not a named one: the threads that fn starts take
  // its name, the caller's
  int r = 0;
  std::exception_ptr ex;
  std::thread t([&] {
    r = set_net_raw(CAP_SET);
    try {
      fn();
    } catch (...) {
      ex = std::current_exception();
    }
    if (r == 0) {
      set_net_raw(CAP_CLEAR);
    }
  });
  t.join();
  if (ex) {
    std::rethrow_exception(ex);
  }
  if (r < 0) {
    *err = "unable to raise CAP_NET_RAW: " + cpp_strerror(r);
  }
  return r;
#else
  fn();
  return 0;
#endif
}

bool osd_keeps_net_raw(CephContext* cct)
{
  return cct->get_set_uid() != 0 &&
    cct->get_module_type() == CEPH_ENTITY_TYPE_OSD &&
    !cct->_conf.get_val<bool>("set_keepcaps") &&
    (cct->_conf.get_val<bool>("oob_keep_net_raw") ||
     needs_net_raw(read_transports(cct).ofi_provider));
}

ceph::rdma::oob_transports read_transports(CephContext* cct)
{
  return ceph::rdma::parse_oob_transports(
    cct->_conf.get_val<std::string>("oob_client_transports"),
    cct->_conf.get_val<std::string>("oob_cluster_transport"));
}

uint64_t memory(CephContext* cct)
{
  return cct->_conf.get_val<Option::size_t>("oob_memory");
}

int dc_key(CephContext* cct, uint64_t* key, std::string* err)
{
  const auto s = cct->_conf.get_val<std::string>("cuobj_dc_key");
  if (!parse_dc_key(s, key)) {
    *err = "cuobj_dc_key '" + s + "' is not a 64-bit key in hexadecimal "
      "after 0x, such as 0xffeeddcc, or in decimal";
    return -EINVAL;
  }
  return 0;
}

} // namespace ceph::oob
