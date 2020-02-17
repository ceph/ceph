// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBCEPHFS_PTHREAD_SELF
#define CEPH_TEST_LIBCEPHFS_PTHREAD_SELF

#include <pthread.h>

#include <type_traits>

/*
 * There is a  difference between libc shipped with FreeBSD and
 * glibc shipped with GNU/Linux for the return type of pthread_self().
 *
 * Introduced a conversion function in include/compat.h
 *     (uint64_t)ceph_pthread_self()
 *
 * libc returns an opague pthread_t that is not default convertable
 * to a uint64_t, which is what gtest expects.
 * And tests using gtest will not compile because of this difference.
 * 
 */
static uint64_t ceph_pthread_self() {
  auto me = pthread_self();
  static_assert(std::is_convertible_v<decltype(me), uint64_t> ||
                std::is_pointer_v<decltype(me)>,
                "we need to use pthread_self() for the owner parameter");
  return reinterpret_cast<uint64_t>(me);
}

#endif
