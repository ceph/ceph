// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <charconv>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <system_error>

#include "common/rdma_token.h"
#include "include/common_fwd.h"

/**
 * Where a daemon runs its out-of-band transports, from oob_network,
 * oob_network_interface and oob_addr, and what it reads of the other
 * out-of-band options.
 */
namespace ceph::oob {

/// a daemon's out-of-band address, and what carries it
struct location_t {
  /// numeric address, or empty when the daemon has none
  std::string addr;
  /// the option it came from: oob_addr, oob_network or the public address
  std::string source;
  /// the network interface that carries addr, or empty
  std::string iface;
  /// the RDMA device whose RoCE GID holds addr, or under iface
  /// (InfiniBand), or empty
  std::string rdma_device;
  unsigned rdma_port = 0;
  /// that GID's index, or -1
  int gid_index = -1;

  std::string describe() const;
};

/**
 * Find the daemon's out-of-band address: oob_addr, else the first
 * address of the host in oob_network (limited to the interfaces in
 * oob_network_interface), else fallback, the daemon's public address,
 * which can be empty. Then find the interface that carries it, and the
 * RDMA device, port and GID.
 *
 * Returns 0, -EINVAL with *err for a malformed option, or -EADDRNOTAVAIL
 * with *err when no address of the host is in oob_network.
 */
int resolve(CephContext* cct, std::string_view fallback, location_t* out,
	    std::string* err);

/**
 * The RDMA device, port and GID index whose GID holds addr, from the
 * GID tables under sysfs (/sys normally): a RoCE v2 GID when one holds
 * it, else any. Returns false when none does.
 */
bool find_rdma_gid(const std::string& sysfs, const std::string& addr,
		   std::string* device, unsigned* port, int* gid_index);

/**
 * The RDMA device and port under a network interface, from sysfs, for an
 * address that no GID holds, such as an IPoIB address: the interface's
 * own device, at its dev_port plus one, or for an InfiniBand bond its
 * active slave's, else the first of its lower interfaces by name that
 * has one. A bond of bonds is followed too. Returns false when none does.
 */
bool find_rdma_port(const std::string& sysfs, const std::string& iface,
		    std::string* device, unsigned* port);

/// whether a libfabric provider runs over verbs, and so needs an RDMA
/// device
bool is_verbs_provider(std::string_view provider);

/**
 * The libfabric domain and node for a provider at loc: the RDMA device
 * and the address for verbs, the interface for uet, nothing for shm
 * and efa, and the address for others. Empty values let the provider
 * choose.
 *
 * For uet, *alt_domain is the RDMA device of the interface, when it has
 * one. The uet provider on a device model such as rocm-ernic names its
 * domain after the RDMA device, not after the interface, and a caller
 * tries that domain when the first one does not open.
 */
void ofi_where(const location_t& loc, std::string_view provider,
	       std::string* domain, std::string* node,
	       std::string* alt_domain = nullptr);

/// whether a libfabric provider needs CAP_NET_RAW to open its endpoint:
/// the UEC reference provider ("uet") sends and receives on a raw socket
bool needs_net_raw(std::string_view provider);

/**
 * Before a switch from root to another user (--setuser): have the
 * calling thread keep its permitted capabilities through its setuid(),
 * for keep_only_net_raw() to trim after it. The other threads lose
 * every capability in the switch, as they do without it. Returns 0, or
 * -errno with *err.
 */
int keep_caps(std::string* err);

/**
 * After that switch, on the same thread: drop every capability but
 * CAP_NET_RAW in the permitted set, and stop keeping capabilities
 * through a later switch. The threads that this one starts afterwards
 * inherit that permitted set. *kept says whether CAP_NET_RAW was there
 * to keep, which a container can leave out. Returns 0, or -errno with
 * *err when the thread may still hold other capabilities.
 */
int keep_only_net_raw(bool* kept, std::string* err);

/**
 * Run fn with CAP_NET_RAW in the effective set, for a provider that
 * needs it while its endpoint opens (see needs_net_raw()).
 *
 * A thread that holds it in its effective set already, as one of a
 * daemon running as root does, runs fn itself. A thread that holds it
 * in its permitted set only, as one of a daemon that kept it through
 * --setuser does, runs fn on a new thread that raises it, and drops it
 * when fn returns. The kernel keeps capabilities for each thread, and
 * raising them on the calling thread would give it credentials other
 * than those that opened its RDMA files, which the kernel then refuses
 * (ib_safe_file_access()). The threads that fn starts inherit the
 * capability. A thread that does not hold it runs fn without it.
 *
 * Returns 0 when fn ran with CAP_NET_RAW, or -errno with *err when it
 * ran without it: -EPERM when the thread does not hold it. An exception
 * from fn reaches the caller.
 */
int with_net_raw(const std::function<void()>& fn, std::string* err);

/**
 * Whether an OSD that switches from root to cct's uid in global_init()
 * keeps CAP_NET_RAW, and only it: when uet is the provider of its
 * endpoint or oob_keep_net_raw is set, and set_keepcaps does not keep
 * every capability already. cct holds only ceph.conf, the environment
 * and the command line then: the mon config arrives after the switch.
 */
bool osd_keeps_net_raw(CephContext* cct);

/// the two transport options, parsed
ceph::rdma::oob_transports read_transports(CephContext* cct);

/// oob_memory
uint64_t memory(CephContext* cct);

/**
 * Parse a cuobj_dc_key value: hexadecimal after 0x, as cufile.json
 * gives rdma_dc_key, else decimal. Returns false when it is neither, or
 * does not fit in 64 bits. Inline because the option's validator calls
 * it too, and crimson builds the options without oob_config.cc.
 */
inline bool parse_dc_key(std::string_view v, uint64_t* key)
{
  // not strtoull(), which takes a sign and leading space, and in base 0
  // reads a leading 0 as octal
  int base = 10;
  if (v.size() > 2 && v[0] == '0' && (v[1] == 'x' || v[1] == 'X')) {
    v.remove_prefix(2);
    base = 16;
  }
  uint64_t k = 0;
  const auto [end, ec] =
    std::from_chars(v.data(), v.data() + v.size(), k, base);
  if (v.empty() || ec != std::errc{} || end != v.data() + v.size()) {
    return false;
  }
  *key = k;
  return true;
}

/**
 * cuobj_dc_key, the key of every cuObject DC initiator and target.
 * Returns 0, or -EINVAL with *err when parse_dc_key() cannot read it,
 * which the option's validator should prevent.
 */
int dc_key(CephContext* cct, uint64_t* key, std::string* err);

} // namespace ceph::oob
