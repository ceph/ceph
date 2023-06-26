// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include <cstdint>

#include "include/rados/librados.hpp"

constexpr int to_create = 10'000'000;

int main() {
  for (int i = 0; i < to_create; ++i) {
    librados::ObjectReadOperation op;
    bufferlist bl;
    std::uint64_t sz;
    struct timespec tm;
    std::map<std::string, ceph::buffer::list> xattrs;
    std::map<std::string, ceph::buffer::list> omap;
    bool more;
    op.read(0, 0, &bl, nullptr);
    op.stat2(&sz, &tm, nullptr);
    op.getxattrs(&xattrs, nullptr);
    op.omap_get_vals2({}, 1000, &omap, &more, nullptr);
  }
}
