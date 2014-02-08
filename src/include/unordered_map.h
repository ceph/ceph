#ifndef CEPH_UNORDERED_MAP_H
#define CEPH_UNORDERED_MAP_H

#include <ciso646>

#ifdef _LIBCPP_VERSION

#include <unordered_map>

namespace ceph {
  using std::unordered_map;
}

#else

#include <tr1/unordered_map>

namespace ceph {
  using std::tr1::unordered_map;
}

#endif

#endif
