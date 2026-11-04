// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_config.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include "gtest/gtest.h"

using ceph::oob::find_rdma_gid;

namespace {

/// a fake sysfs with the GID table of one RDMA port, mlx5_0 port 1
struct fake_gids {
  std::filesystem::path root;
  std::filesystem::path port;
  fake_gids() {
    root = std::filesystem::temp_directory_path() /
      ("oob-gid-" + std::to_string(getpid()));
    std::filesystem::remove_all(root);
    port = root / "class/infiniband/mlx5_0/ports/1";
    std::filesystem::create_directories(port / "gids");
    std::filesystem::create_directories(port / "gid_attrs/types");
  }
  ~fake_gids() { std::filesystem::remove_all(root); }
  /// a RoCE v2 GID entry, written as the kernel writes it: the text,
  /// then a newline
  void gid(unsigned i, const std::string& text) {
    std::ofstream(port / "gids" / std::to_string(i)) << text << "\n";
    std::ofstream(port / "gid_attrs/types" / std::to_string(i))
      << "RoCE v2\n";
  }
  /// the index of the GID that holds addr, or -1
  int find(const std::string& addr) const {
    std::string dev;
    unsigned p = 0;
    int idx = -1;
    if (!find_rdma_gid(root.string(), addr, &dev, &p, &idx)) {
      return -1;
    }
    return idx;
  }
};

} // anonymous namespace

// sysfs prints a GID in full as an IPv6 address (%pI6), eight groups of
// four hex digits. Beyond the link-local and IPv4-mapped GIDs that
// OobConfig.FindRdmaGid finds: a unique local GID, one in upper case,
// and an IPv4-mapped address written as IPv6
TEST(OobGid, KernelForm)
{
  fake_gids g;
  g.gid(0, "0000:0000:0000:0000:0000:ffff:0a5a:0001");
  g.gid(1, "fd00:0000:0000:0000:0000:0000:0000:0001");
  g.gid(2, "FD00:0000:0000:0000:0000:0000:0000:00AB");
  EXPECT_EQ(0, g.find("::ffff:10.90.0.1"));
  EXPECT_EQ(1, g.find("fd00::1"));
  EXPECT_EQ(2, g.find("fd00::ab"));
}

// an entry that is no GID holds no address, and the search goes on to
// the next
TEST(OobGid, SkipsMalformed)
{
  fake_gids g;
  g.gid(0, "");
  g.gid(1, "fe80:0000:0000:0000:5e5d:aaff:fe01");
  g.gid(2, "fe80:0000:0000:0000:5e5d:aaff:fe01:020g");
  g.gid(3, "fe80:0000:0000:0000:5e5d:aaff:fe01:0203:0000");
  g.gid(4, "fe80:0000:0000:0000:5e5d:aaff:fe01:0203");
  EXPECT_EQ(4, g.find("fe80::5e5d:aaff:fe01:203"));
}
