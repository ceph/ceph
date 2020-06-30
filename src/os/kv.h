// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_KV_H
#define CEPH_OS_KV_H

#include <string>
#include "include/byteorder.h"

// some key encoding helpers
template<typename T>
inline static void _key_encode_u32(uint32_t u, T *key) {
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 4);
}

template<typename T>
inline static void _key_encode_u32(uint32_t u, size_t pos, T *key) {
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->replace(pos, sizeof(bu), (char*)&bu, sizeof(bu));
}

inline static const char *_key_decode_u32(const char *key, uint32_t *pu) {
  uint32_t bu;
  memcpy(&bu, key, 4);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 4;
}

template<typename T>
inline static void _key_encode_u64(uint64_t u, T *key) {
  uint64_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 8);
}

inline static const char *_key_decode_u64(const char *key, uint64_t *pu) {
  uint64_t bu;
  memcpy(&bu, key, 8);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 8;
}

#endif
