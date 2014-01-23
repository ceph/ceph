#ifndef CEPH_HASH_NAMESPACE_H
#define CEPH_HASH_NAMESPACE_H

#include <ciso646>

#ifdef _LIBCPP_VERSION

#include <functional>

#define CEPH_HASH_NAMESPACE_START namespace std {
#define CEPH_HASH_NAMESPACE_END }
#define CEPH_HASH_NAMESPACE std

#else

#include <tr1/functional>

#define CEPH_HASH_NAMESPACE_START namespace std { namespace tr1 {
#define CEPH_HASH_NAMESPACE_END }}
#define CEPH_HASH_NAMESPACE std::tr1

#endif

#endif
