// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_COMMON_SIMPLERNG_H_
#define __CEPH_COMMON_SIMPLERNG_H_

/*
 * rand() is not thread-safe.  random_r family segfaults.
 * boost::random::* have build issues.
 */
class SimpleRNG {
  unsigned m_z, m_w;

public:
  SimpleRNG(int seed) : m_z(seed), m_w(123) {}

  unsigned operator()() {
    m_z = 36969 * (m_z & 65535) + (m_z >> 16);
    m_w = 18000 * (m_w & 65535) + (m_w >> 16);
    return (m_z << 16) + m_w;
  }
};

#endif
