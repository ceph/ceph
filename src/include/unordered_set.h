#ifndef CEPH_UNORDERED_SET_H
#define CEPH_UNORDERED_SET_H

#include <ciso646>

#ifdef _LIBCPP_VERSION

#include <unordered_set>

namespace ceph {
  using std::unordered_set;
}

#else

#include <tr1/unordered_set>

namespace ceph {
  using std::tr1::unordered_set;
}

#endif

#endif
