// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include <net/if.h>
#include <unistd.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/crc64nvme.h"
#include "common/config.h"
#include "include/crc32c.h"
#include "gtest/gtest.h"

using ceph::rdma::parse_rdma_token;
using ceph::rdma::RDMA_TOKEN_MAX_LEN;

// the shape emitted by cuObject clients:
// raddr:rsize:rkey:lid:qp:has_gid:gid
static const std::string valid_token =
  "0102030405060708:01020304:0102aabb:0102:010203:1:"
  "0102030405060708090a0b0c0d0e0f10";

TEST(RdmaToken, ParseValid)
{
  auto w = parse_rdma_token(valid_token);
  ASSERT_TRUE(w);
  EXPECT_EQ(0x0102030405060708ull, w->addr);
  EXPECT_EQ(0x01020304ull, w->size);
}

TEST(RdmaToken, ParseMinimalFields)
{
  // only the leading addr:size fields are interpreted
  auto w = parse_rdma_token("ff:10:rest-is-opaque");
  ASSERT_TRUE(w);
  EXPECT_EQ(0xffull, w->addr);
  EXPECT_EQ(0x10ull, w->size);
}

TEST(RdmaToken, RejectMalformed)
{
  EXPECT_FALSE(parse_rdma_token(""));
  EXPECT_FALSE(parse_rdma_token("deadbeef"));           // no colon
  EXPECT_FALSE(parse_rdma_token("deadbeef:"));          // no second colon
  EXPECT_FALSE(parse_rdma_token(":1234:rkey"));         // empty addr
  EXPECT_FALSE(parse_rdma_token("1234::rkey"));         // empty size
  EXPECT_FALSE(parse_rdma_token("xyz:1234:rkey"));      // non-hex addr
  EXPECT_FALSE(parse_rdma_token("1234:no pe:rkey"));    // non-hex size
  EXPECT_FALSE(parse_rdma_token("11112222333344445:1:x")); // >16 hex digits
  EXPECT_FALSE(parse_rdma_token(std::string(RDMA_TOKEN_MAX_LEN + 1, '1')));
}

TEST(RdmaToken, MaxValues)
{
  auto w = parse_rdma_token("ffffffffffffffff:ffffffff:x");
  ASSERT_TRUE(w);
  EXPECT_EQ(~0ull, w->addr);
  EXPECT_EQ(0xffffffffull, w->size);
}

TEST(RdmaToken, TransportList)
{
  using ceph::rdma::parse_transport_list;
  using V = std::vector<std::string>;
  EXPECT_EQ(parse_transport_list(""), V{});
  EXPECT_EQ(parse_transport_list("tcp"), V{"tcp"});
  // separators are commas, spaces and tabs; a provider's semicolon is
  // part of its name, and <core>_ofi_<utility> spells the same provider
  // without one
  EXPECT_EQ(parse_transport_list("cuobj,verbs;ofi_rxm"),
	    (V{"cuobj", "verbs;ofi_rxm"}));
  EXPECT_EQ(parse_transport_list("Verbs_OFI_rxm, tcp_ofi_rxm, verbs;ofi_rxm"),
	    (V{"verbs;ofi_rxm", "tcp;ofi_rxm"}));
  EXPECT_EQ(ceph::rdma::libfabric_name("ofi_rxm"), "ofi_rxm");
  EXPECT_EQ(ceph::rdma::libfabric_name("verbs"), "verbs;ofi_rxm");
  EXPECT_EQ(ceph::rdma::libfabric_name("uet"), "uet");
  EXPECT_EQ(parse_transport_list(" UET ,\tcuobj , "), (V{"uet", "cuobj"}));
  // duplicates and empty entries drop
  EXPECT_EQ(parse_transport_list("rc,,rc,tcp"), (V{"rc", "tcp"}));
}

TEST(RdmaToken, OobTransports)
{
  using ceph::rdma::parse_oob_transports;
  // nothing
  auto t = parse_oob_transports("", "");
  EXPECT_TRUE(t.ofi_provider.empty());
  EXPECT_FALSE(t.cuobj());
  EXPECT_TRUE(t.warnings.empty());

  // one provider for both roles, and cuObject clients, in either
  // spelling
  t = parse_oob_transports("verbs_ofi_rxm, cuobj", "verbs;ofi_rxm");
  EXPECT_EQ("verbs;ofi_rxm", t.ofi_provider);
  EXPECT_TRUE(t.client_ofi());
  EXPECT_TRUE(t.client_cuobj());
  EXPECT_TRUE(t.cluster_ofi());
  EXPECT_FALSE(t.cluster_cuobj());
  EXPECT_TRUE(t.names_provider("verbs;ofi_rxm"));
  EXPECT_TRUE(t.warnings.empty());

  // cuObject between the daemons, libfabric to clients
  t = parse_oob_transports("uet", "cuobj");
  EXPECT_EQ("uet", t.ofi_provider);
  EXPECT_TRUE(t.cuobj());
  EXPECT_FALSE(t.client_cuobj());
  EXPECT_TRUE(t.cluster_cuobj());
  EXPECT_TRUE(t.names_provider("uet"));

  // one libfabric endpoint: the cluster transport's provider wins
  t = parse_oob_transports("tcp, uet", "verbs;ofi_rxm");
  EXPECT_EQ("verbs;ofi_rxm", t.ofi_provider);
  EXPECT_EQ(2u, t.warnings.size());
  t = parse_oob_transports("tcp, uet", "");
  EXPECT_EQ("tcp", t.ofi_provider);
  EXPECT_EQ(1u, t.warnings.size());

  // the cluster transport is one
  t = parse_oob_transports("", "cuobj, tcp");
  EXPECT_EQ("cuobj", t.cluster);
  EXPECT_TRUE(t.ofi_provider.empty());
  EXPECT_EQ(1u, t.warnings.size());

  // verbs alone, which is also what a configuration file leaves of an
  // unescaped verbs;ofi_rxm, is the same provider
  t = parse_oob_transports("verbs", "verbs_ofi_rxm");
  EXPECT_EQ("verbs;ofi_rxm", t.ofi_provider);
  EXPECT_TRUE(t.names_provider("verbs;ofi_rxm"));
  EXPECT_TRUE(t.warnings.empty());
}

namespace {

/// a fake sysfs with RDMA devices, their GID tables, and network
/// interfaces
struct fake_sysfs {
  std::filesystem::path root;
  fake_sysfs() {
    root = std::filesystem::temp_directory_path() /
      ("oob-sysfs-" + std::to_string(getpid()));
    std::filesystem::remove_all(root);
  }
  ~fake_sysfs() { std::filesystem::remove_all(root); }
  void gid(const std::string& dev, unsigned port, unsigned i,
	   const std::string& value, const std::string& type) {
    const auto p = root / "class/infiniband" / dev / "ports" /
      std::to_string(port);
    std::filesystem::create_directories(p / "gids");
    std::filesystem::create_directories(p / "gid_attrs/types");
    std::ofstream(p / "gids" / std::to_string(i)) << value << "\n";
    if (!type.empty()) {
      std::ofstream(p / "gid_attrs/types" / std::to_string(i)) << type << "\n";
    }
  }
  /// a network interface of a type (32 for InfiniBand, 1 for Ethernet),
  /// on a port of an RDMA device, or on none
  void netdev(const std::string& name, const std::string& type,
	      const std::string& dev_port, const std::string& rdma_dev) {
    const auto p = root / "class/net" / name;
    std::filesystem::create_directories(p);
    std::ofstream(p / "type") << type << "\n";
    if (!dev_port.empty()) {
      std::ofstream(p / "dev_port") << dev_port << "\n";
    }
    if (!rdma_dev.empty()) {
      std::filesystem::create_directories(p / "device/infiniband" / rdma_dev);
    }
  }
  /// a slave of a bond
  void slave(const std::string& bond, const std::string& name) {
    const auto p = root / "class/net" / bond;
    std::filesystem::create_directories(p);
    std::filesystem::create_directory_symlink("../" + name,
					      p / ("lower_" + name));
  }
  /// the active slave of a bond, or none
  void active(const std::string& bond, const std::string& name) {
    const auto p = root / "class/net" / bond / "bonding";
    std::filesystem::create_directories(p);
    std::ofstream(p / "active_slave") << name << "\n";
  }
};

} // anonymous namespace

TEST(OobConfig, FindRdmaGid)
{
  using ceph::oob::find_rdma_gid;
  fake_sysfs fs;
  const std::string link = "fe80:0000:0000:0000:5e5d:aaff:fe01:0203";
  const std::string v4 = "0000:0000:0000:0000:0000:ffff:0a5a:0001";
  fs.gid("mlx5_0", 1, 0, link, "IB/RoCE v1");
  fs.gid("mlx5_0", 1, 1, link, "RoCE v2");
  fs.gid("mlx5_0", 1, 2, v4, "IB/RoCE v1");
  fs.gid("mlx5_0", 1, 3, v4, "RoCE v2");
  fs.gid("mlx5_0", 2, 0, "0000:0000:0000:0000:0000:ffff:0a5b:0001",
	 "RoCE v2");
  // an unused entry: no type
  fs.gid("mlx5_0", 2, 1, "0000:0000:0000:0000:0000:0000:0000:0000", "");

  std::string dev;
  unsigned port = 0;
  int idx = -1;
  // the RoCE v2 GID of the address, not its v1 twin
  ASSERT_TRUE(find_rdma_gid(fs.root.string(), "10.90.0.1", &dev, &port, &idx));
  EXPECT_EQ("mlx5_0", dev);
  EXPECT_EQ(1u, port);
  EXPECT_EQ(3, idx);
  ASSERT_TRUE(find_rdma_gid(fs.root.string(), "10.91.0.1", &dev, &port, &idx));
  EXPECT_EQ(2u, port);
  EXPECT_EQ(0, idx);
  // an IPv6 address matches its GID directly
  ASSERT_TRUE(find_rdma_gid(fs.root.string(), "fe80::5e5d:aaff:fe01:203", &dev, &port,
			    &idx));
  EXPECT_EQ(1, idx);
  // no GID holds it, or it is no address
  EXPECT_FALSE(find_rdma_gid(fs.root.string(), "10.92.0.1", &dev, &port, &idx));
  EXPECT_FALSE(find_rdma_gid(fs.root.string(), "mlx5_0", &dev, &port, &idx));
  EXPECT_FALSE(find_rdma_gid((fs.root / "none").string(), "10.90.0.1", &dev, &port,
			     &idx));
  // an unused entry holds no address, not even the unspecified one
  EXPECT_FALSE(find_rdma_gid(fs.root.string(), "::", &dev, &port, &idx));
}

// No GID holds an IPoIB address: the interface names the device, its
// dev_port the port, and for a bond its active slave both, else its
// first slave by name that has a device.
TEST(OobConfig, FindRdmaPort)
{
  using ceph::oob::find_rdma_port;
  fake_sysfs fs;
  // the two ports of one device, and a device with one port
  fs.netdev("ib0", "32", "0", "mlx4_0");
  fs.netdev("ib1", "32", "1", "mlx4_0");
  fs.netdev("ib2", "32", "0", "mlx5_0");
  // no dev_port
  fs.netdev("ib3", "32", "", "mlx5_1");

  std::string dev;
  unsigned port = 0;
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "ib0", &dev, &port));
  EXPECT_EQ("mlx4_0", dev);
  EXPECT_EQ(1u, port);
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "ib1", &dev, &port));
  EXPECT_EQ("mlx4_0", dev);
  EXPECT_EQ(2u, port);
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "ib2", &dev, &port));
  EXPECT_EQ("mlx5_0", dev);
  EXPECT_EQ(1u, port);
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "ib3", &dev, &port));
  EXPECT_EQ("mlx5_1", dev);
  EXPECT_EQ(1u, port);

  // an active-backup bond of IPoIB interfaces: its active slave
  fs.netdev("bond0", "32", "0", "");
  fs.slave("bond0", "ib0");
  fs.slave("bond0", "ib1");
  fs.slave("bond0", "ib2");
  fs.active("bond0", "ib2");
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "bond0", &dev, &port));
  EXPECT_EQ("mlx5_0", dev);
  EXPECT_EQ(1u, port);
  // and its port
  fs.active("bond0", "ib1");
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "bond0", &dev, &port));
  EXPECT_EQ("mlx4_0", dev);
  EXPECT_EQ(2u, port);
  // with no active slave, the first slave by name that has a device
  fs.netdev("ia0", "32", "0", "");
  fs.slave("bond0", "ia0");
  fs.active("bond0", "");
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "bond0", &dev, &port));
  EXPECT_EQ("mlx4_0", dev);
  EXPECT_EQ(1u, port);
  // a bond of bonds, but no deeper
  fs.netdev("bond2", "32", "0", "");
  fs.slave("bond2", "bond0");
  fs.active("bond0", "ib1");
  ASSERT_TRUE(find_rdma_port(fs.root.string(), "bond2", &dev, &port));
  EXPECT_EQ("mlx4_0", dev);
  EXPECT_EQ(2u, port);
  fs.netdev("bond3", "32", "0", "");
  fs.slave("bond3", "bond2");
  EXPECT_FALSE(find_rdma_port(fs.root.string(), "bond3", &dev, &port));

  // an Ethernet bond: a RoCE GID holds its address, or nothing does
  fs.netdev("eth0", "1", "0", "mlx5_2");
  fs.netdev("bond1", "1", "0", "");
  fs.slave("bond1", "eth0");
  fs.active("bond1", "eth0");
  dev.clear();
  port = 0;
  EXPECT_FALSE(find_rdma_port(fs.root.string(), "bond1", &dev, &port));
  // no RDMA device, or no such interface
  fs.netdev("eth1", "1", "0", "");
  EXPECT_FALSE(find_rdma_port(fs.root.string(), "eth1", &dev, &port));
  EXPECT_FALSE(find_rdma_port(fs.root.string(), "ib9", &dev, &port));
  EXPECT_EQ("", dev);
  EXPECT_EQ(0u, port);
}

TEST(OobConfig, OfiWhere)
{
  using ceph::oob::ofi_where;
  ceph::oob::location_t loc;
  loc.addr = "10.90.0.1";
  loc.iface = "uet-o3";
  loc.rdma_device = "rocep101s0f0";
  std::string domain, node;
  ofi_where(loc, "verbs;ofi_rxm", &domain, &node);
  EXPECT_EQ("rocep101s0f0", domain);
  EXPECT_EQ("10.90.0.1", node);
  ofi_where(loc, "uet", &domain, &node);
  EXPECT_EQ("uet-o3", domain);
  EXPECT_EQ("", node);
  // the uet provider on rocm-ernic names its domain after the RDMA device
  std::string alt = "stale";
  ofi_where(loc, "uet", &domain, &node, &alt);
  EXPECT_EQ("uet-o3", domain);
  EXPECT_EQ("rocep101s0f0", alt);
  ofi_where(loc, "verbs;ofi_rxm", &domain, &node, &alt);
  EXPECT_EQ("", alt);
  ceph::oob::location_t plain = loc;
  plain.rdma_device.clear();
  ofi_where(plain, "uet", &domain, &node, &alt);
  EXPECT_EQ("uet-o3", domain);
  EXPECT_EQ("", alt);
  ofi_where(loc, "tcp", &domain, &node);
  EXPECT_EQ("", domain);
  EXPECT_EQ("10.90.0.1", node);
  ofi_where(loc, "shm", &domain, &node);
  EXPECT_EQ("", domain);
  EXPECT_EQ("", node);
  // no address: the provider chooses
  ofi_where(ceph::oob::location_t{}, "verbs;ofi_rxm", &domain, &node);
  EXPECT_EQ("", domain);
  EXPECT_EQ("", node);
}

// A malformed oob_network, or oob_network_interface without a subnet, is
// an error that the daemon reports, and runs on without out-of-band
// transfers: the subnet parser that picks the address exits instead.
TEST(OobConfig, ResolveRejectsMalformedNetworks)
{
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs
  cct->_conf.set_val("oob_addr", "");
  cct->_conf.set_val("oob_network_interface", "");
  ceph::oob::location_t loc;
  std::string err;
  for (const char* bad : {"10.10.0.5", "10.10.0.0/255.255.0.0",
			  "10.10.0.0/", "10.10.0.0/16,fd00::", "nonsense/8",
			  " , "}) {
    cct->_conf.set_val("oob_network", bad);
    err.clear();
    EXPECT_EQ(-EINVAL, ceph::oob::resolve(cct.get(), "", &loc, &err))
      << bad;
    EXPECT_NE(std::string::npos, err.find("oob_network")) << err;
    EXPECT_TRUE(loc.addr.empty());
  }
  // interfaces with no subnet to pick from them
  cct->_conf.set_val("oob_network", ",");
  cct->_conf.set_val("oob_network_interface", "eth0");
  EXPECT_EQ(-EINVAL, ceph::oob::resolve(cct.get(), "", &loc, &err));
  // a well-formed subnet that no address of the host is in (TEST-NET-1)
  cct->_conf.set_val("oob_network_interface", "");
  cct->_conf.set_val("oob_network", "192.0.2.0/24");
  EXPECT_EQ(-EADDRNOTAVAIL, ceph::oob::resolve(cct.get(), "", &loc, &err));
  // a name longer than an interface's, such as an altname, is only a
  // name that no interface has: the others in the list still pick the
  // address
  const std::string altname(IFNAMSIZ + 4, 'e');
  cct->_conf.set_val("oob_network_interface", altname);
  EXPECT_EQ(-EADDRNOTAVAIL, ceph::oob::resolve(cct.get(), "", &loc, &err));
  cct->_conf.set_val("oob_network", "127.0.0.0/8");
  cct->_conf.set_val("oob_network_interface", "lo," + altname);
  err.clear();
  const int r = ceph::oob::resolve(cct.get(), "", &loc, &err);
  EXPECT_NE(-EINVAL, r) << err;
  if (r == 0) {
    // the loopback is up
    EXPECT_EQ("127.0.0.1", loc.addr);
  }
}

// cuobj_dc_key is hexadecimal after 0x, as cufile.json gives rdma_dc_key,
// or decimal. The option's validator rejects anything else when it is
// set, as it does for `ceph config set`, and the key stays as it was.
TEST(OobConfig, DcKey)
{
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_OSD), false};
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs
  uint64_t key = 0;
  std::string err;
  // the default is the client library's
  EXPECT_EQ(0, ceph::oob::dc_key(cct.get(), &key, &err)) << err;
  EXPECT_EQ(0xffeeddccull, key);
  const std::pair<const char*, uint64_t> good[] = {
    {"0x11223344", 0x11223344}, {"0X11223344", 0x11223344},
    {"0xFFEEDDCC", 0xffeeddcc}, {"4293844428", 0xffeeddcc},
    {"0", 0}, {"0x0", 0},
    // decimal, not octal
    {"010", 10},
    {"0xffffffffffffffff", UINT64_MAX},
    {"18446744073709551615", UINT64_MAX},
  };
  for (const auto& [val, want] : good) {
    key = 1;
    EXPECT_TRUE(ceph::oob::parse_dc_key(val, &key)) << val;
    EXPECT_EQ(want, key) << val;
    ASSERT_EQ(0, cct->_conf.set_val("cuobj_dc_key", val)) << val;
    key = 1;
    err.clear();
    EXPECT_EQ(0, ceph::oob::dc_key(cct.get(), &key, &err))
      << val << ": " << err;
    EXPECT_EQ(want, key) << val;
  }
  ASSERT_EQ(0, cct->_conf.set_val("cuobj_dc_key", "0x11223344"));
  for (const char* bad : {"", "0x", "x10", "ffeeddcc", "0xffeeddcg",
			  "-1", "+1", " 1", "1 ", "0x-1", "0x 1", "0x0x1",
			  "1K", "0x10000000000000000",
			  "18446744073709551616"}) {
    key = 7;
    EXPECT_FALSE(ceph::oob::parse_dc_key(bad, &key)) << bad;
    EXPECT_EQ(7u, key) << bad;
    std::stringstream ss;
    EXPECT_EQ(-EINVAL, cct->_conf.set_val("cuobj_dc_key", bad, &ss)) << bad;
    EXPECT_NE(std::string::npos, ss.str().find("0xffeeddcc")) << ss.str();
    err.clear();
    EXPECT_EQ(0, ceph::oob::dc_key(cct.get(), &key, &err))
      << bad << ": " << err;
    EXPECT_EQ(0x11223344u, key) << bad;
  }
}


TEST(RdmaToken, OfiShape)
{
  using ceph::rdma::is_ofi_token;
  const std::string ofi =
    "7f0012345000:c00000:ofi1:rxm.1:020012340a00000100ff:1f";
  EXPECT_TRUE(is_ofi_token(ofi));
  EXPECT_TRUE(is_ofi_token("0:10:ofi1:xnet.1:0200:1"));
  // the family of the tag ofi-rma writes
  EXPECT_TRUE(is_ofi_token("0:10:" + std::string(ceph::rdma::OFI_TOKEN_TAG) +
                           ":xnet.1:0200:1"));
  // any version of the family: one this build cannot parse is still not
  // a cuObject descriptor, and is declined by the libfabric executor
  EXPECT_TRUE(is_ofi_token("0:10:ofi10:xnet.1:0200:1"));
  EXPECT_TRUE(is_ofi_token("0:10:ofi2:x"));
  // a libfabric token has the addr:size prefix too, so parsing the
  // window cannot tell the two apart
  auto w = parse_rdma_token(ofi);
  ASSERT_TRUE(w);
  EXPECT_EQ(0xc00000ull, w->size);

  EXPECT_FALSE(is_ofi_token(valid_token));
  EXPECT_FALSE(is_ofi_token(""));
  EXPECT_FALSE(is_ofi_token("ofi1"));
  EXPECT_FALSE(is_ofi_token("0:10"));
  EXPECT_FALSE(is_ofi_token("0:10:"));
  // only the third field counts
  EXPECT_FALSE(is_ofi_token("ofi1:0:10:tcp"));
  EXPECT_FALSE(is_ofi_token("0:10:ofi:xnet.1:0200:1"));
  EXPECT_FALSE(is_ofi_token("0:10:ofi1x:xnet.1:0200:1"));
  EXPECT_FALSE(is_ofi_token("0:10:1:ofi1:0200:1"));
}

TEST(RdmaToken, CuobjDescriptor)
{
  using ceph::rdma::is_cuobj_descriptor;
  EXPECT_TRUE(is_cuobj_descriptor(valid_token));
  EXPECT_TRUE(is_cuobj_descriptor("ff:10:rest-is-opaque"));
  // a libfabric token parses, but is not one
  EXPECT_FALSE(is_cuobj_descriptor("0:10:ofi1:xnet.1:0200:1"));
  EXPECT_FALSE(is_cuobj_descriptor("0:10:ofi2:xnet.1:0200:1"));
  // neither is a token whose window does not parse, such as an RC
  // queue-pair token, which has no colons
  EXPECT_FALSE(is_cuobj_descriptor(std::string(88, 'a')));
  EXPECT_FALSE(is_cuobj_descriptor(""));
  EXPECT_FALSE(is_cuobj_descriptor("xyz:1234:rkey"));
  EXPECT_FALSE(is_cuobj_descriptor("0:10"));
}

TEST(RdmaToken, FormatCuobjDescriptor)
{
  using ceph::rdma::format_cuobj_descriptor;
  const uint8_t gid[16] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			   0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
  EXPECT_EQ(valid_token,
	    format_cuobj_descriptor(0x0102030405060708ull, 0x01020304,
				    0x0102aabb, 0x0102, 0x010203, gid));

  // every field has a fixed width, so a descriptor is always as long
  const uint8_t v4_gid[16] = {0, 0, 0, 0, 0, 0, 0, 0,
			      0, 0, 0xff, 0xff, 192, 168, 1, 10};
  const std::string t =
    format_cuobj_descriptor(0x7f0000001000ull, 0x800000, 0x5, 0, 0x12,
			    v4_gid);
  EXPECT_EQ("00007f0000001000:00800000:00000005:0000:000012:1:"
	    "00000000000000000000ffffc0a8010a", t);
  EXPECT_EQ(81u, t.size());
  EXPECT_EQ(valid_token.size(), t.size());

  // which the gateway and the OSD read back as a cuObject window
  auto w = parse_rdma_token(t);
  ASSERT_TRUE(w);
  EXPECT_EQ(0x7f0000001000ull, w->addr);
  EXPECT_EQ(0x800000ull, w->size);
  EXPECT_TRUE(ceph::rdma::is_cuobj_descriptor(t));
  EXPECT_FALSE(ceph::rdma::is_ofi_token(t));
}

TEST(RdmaToken, LongestOfiTokenFits)
{
  // a token as long as ofi-rma writes one, without its NUL (common/ofi_rma.h
  // checks that OFI_RMA_TOKEN_MAX - 1 fits RDMA_TOKEN_MAX_LEN)
  std::string t = "7f0012345000:c00000:ofi1:rxm.1:";
  t.resize(RDMA_TOKEN_MAX_LEN - 1, 'a');
  ASSERT_LE(t.size(), RDMA_TOKEN_MAX_LEN);
  auto w = parse_rdma_token(t);
  ASSERT_TRUE(w);
  EXPECT_EQ(0xc00000ull, w->size);
  EXPECT_TRUE(ceph::rdma::is_ofi_token(t));
}

// crc32c_combine() takes B's length whole, past 4 GiB too, where
// ceph_crc32c_zeros() runs a GiB at a time
TEST(RdmaToken, Crc32cCombineLongB)
{
  using ceph::rdma::crc32c_canonical;
  using ceph::rdma::crc32c_combine;
  // the CRC-32C register, without the final inversion, after n zeros
  auto zeros = [](uint32_t crc, uint64_t n) {
    while (n > 0) {
      const unsigned c =
	static_cast<unsigned>(std::min<uint64_t>(n, 1u << 30));
      crc = ceph_crc32c_zeros(crc, c);
      n -= c;
    }
    return crc;
  };
  ceph::buffer::list a;
  a.append("123456789");
  const uint32_t crc_a = crc32c_canonical(a);
  const uint32_t reg_a = a.crc32c(0xffffffff);
  for (uint64_t n : {uint64_t(0), uint64_t(1), uint64_t(1) << 30,
		     (uint64_t(1) << 32) - 1, uint64_t(1) << 32,
		     (uint64_t(5) << 30) + 7}) {
    // B is n zero bytes
    const uint32_t crc_b = zeros(0xffffffff, n) ^ 0xffffffff;
    const uint32_t crc_ab = zeros(reg_a, n) ^ 0xffffffff;
    EXPECT_EQ(crc_ab, crc32c_combine(crc_a, crc_b, n)) << "len_b " << n;
  }
}

TEST(OobArena, BuddyBuffers)
{
  using ceph::oob::Arena;
  std::string err;
  // rounded down to whole blocks
  auto a = Arena::create(2 * Arena::MAX + 5, &err);
  ASSERT_TRUE(a) << err;
  EXPECT_EQ(2 * Arena::MAX, a->size());

  EXPECT_EQ(Arena::MIN, Arena::round(1));
  EXPECT_EQ(Arena::MIN, Arena::round(Arena::MIN));
  EXPECT_EQ(4 * Arena::MIN, Arena::round(3 * Arena::MIN));
  EXPECT_EQ(Arena::MAX, Arena::round(Arena::MAX));
  EXPECT_EQ(0u, Arena::round(Arena::MAX + 1));

  // a buffer per transfer, in its power of two, never overlapping
  auto s1 = a->alloc(100);
  auto s2 = a->alloc(3 << 20);
  auto s3 = a->alloc(Arena::MAX);
  ASSERT_TRUE(s1 && s2 && s3);
  EXPECT_EQ(Arena::MIN, s1.len);
  EXPECT_EQ(4 * Arena::MIN, s2.len);
  EXPECT_EQ(0u, s2.offset % s2.len);
  EXPECT_TRUE(s1.offset + s1.len <= s2.offset ||
              s2.offset + s2.len <= s1.offset);
  EXPECT_FALSE(a->alloc(Arena::MAX + 1));
  // the other block is split already, so no second MAX buffer
  EXPECT_FALSE(a->alloc(Arena::MAX));
  EXPECT_EQ(Arena::MIN + 4 * Arena::MIN + Arena::MAX, a->stats().in_use);

  // freed buffers join their buddies again
  a->free(s1);
  a->free(s2.offset, 3 << 20);
  auto s4 = a->alloc(Arena::MAX);
  ASSERT_TRUE(s4);
  a->free(s3);
  a->free(s4);
  EXPECT_EQ(0u, a->stats().in_use);

  // every MIN buffer, then nothing more
  std::vector<Arena::slice_t> all;
  while (auto s = a->alloc(1)) {
    all.push_back(s);
  }
  EXPECT_EQ(a->size() / Arena::MIN, all.size());
  EXPECT_GE(a->stats().failed, 1u);
  for (auto& s : all) {
    a->free(s);
  }
  EXPECT_EQ(0u, a->stats().in_use);
}

TEST(OobArena, Reserve)
{
  using ceph::oob::Arena;
  std::string err;
  auto a = Arena::create(4 * Arena::MAX, &err);
  ASSERT_TRUE(a) << err;
  // a run of whole blocks at the top, inside the arena
  char* w = a->reserve(Arena::MAX + 1);
  ASSERT_TRUE(w);
  EXPECT_EQ(a->base() + 2 * Arena::MAX, w);
  EXPECT_EQ(2 * Arena::MAX, a->stats().reserved);
  // buffers come from the rest
  auto s = a->alloc(Arena::MAX);
  ASSERT_TRUE(s);
  EXPECT_LT(s.offset, 2 * Arena::MAX);
  // no run of three blocks is left
  EXPECT_FALSE(a->reserve(3 * Arena::MAX));
  EXPECT_TRUE(a->reserve(Arena::MAX));
  EXPECT_FALSE(a->alloc(Arena::MAX));
  a->free(s);
}

TEST(OobArena, ReserveLeavesStaging)
{
  using ceph::oob::Arena;
  std::string err;
  auto a = Arena::create(2 * Arena::MAX, &err);
  ASSERT_TRUE(a) << err;
  // mapped on a huge page, and writable from end to end
  EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(a->base()) % (2 << 20));
  std::memset(a->base(), 0xab, 4096);
  std::memset(a->base() + a->size() - 4096, 0xcd, 4096);
  // a run of every block would leave transfers nowhere to stage
  EXPECT_FALSE(a->reserve(2 * Arena::MAX));
  EXPECT_FALSE(a->reserve(Arena::MAX + 1));
  EXPECT_EQ(0u, a->stats().reserved);
  char* w = a->reserve(Arena::MAX);
  ASSERT_TRUE(w);
  EXPECT_EQ(a->base() + Arena::MAX, w);
  // the last block stays for buffers
  EXPECT_FALSE(a->reserve(Arena::MAX));
  auto s = a->alloc(Arena::MAX);
  ASSERT_TRUE(s);
  EXPECT_FALSE(a->alloc(Arena::MIN));
  a->free(s);
  // a run given back, when its windows did not come up, is for buffers
  // again, or for another run
  a->unreserve(w, Arena::MAX);
  EXPECT_EQ(0u, a->stats().reserved);
  auto s1 = a->alloc(Arena::MAX);
  auto s2 = a->alloc(Arena::MAX);
  ASSERT_TRUE(s1 && s2);
  a->free(s1);
  a->free(s2);
  EXPECT_EQ(w, a->reserve(Arena::MAX));
  a->unreserve(w, Arena::MAX);
  EXPECT_EQ(0u, a->stats().reserved);
  EXPECT_EQ(0u, a->stats().in_use);
}

TEST(RdmaToken, FoldRangesWithAnEmptyRange)
{
  // an empty range at the offset where the next range starts, as a share
  // past the end of an object reports it, folds the same in either order
  const std::string a(10, 'a'), b(5, 'b');
  ceph::buffer::list bla, blb, both;
  bla.append(a);
  blb.append(b);
  both.append(a + b);
  const ceph::rdma::crc_range_t ra{0, 10, ceph::crc64nvme(bla)};
  const ceph::rdma::crc_range_t rb{10, 5, ceph::crc64nvme(blb)};
  const ceph::rdma::crc_range_t empty{10, 0, 0};
  for (const auto& ranges :
	 {std::vector{ra, rb, empty}, std::vector{ra, empty, rb},
	  std::vector{empty, rb, ra}}) {
    const auto crc = ceph::rdma::fold_crc64_ranges(ranges);
    ASSERT_TRUE(crc);
    EXPECT_EQ(ceph::crc64nvme(both), *crc);
  }
}
