// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "kv.h"

#include "include/byteorder.h"
#include <string.h>

void _key_encode_u32(uint32_t u, std::string *key)
{
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab32(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 4);
}

const char *_key_decode_u32(const char *key, uint32_t *pu)
{
  uint32_t bu;
  memcpy(&bu, key, 4);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab32(bu);
#else
# error wtf
#endif
  return key + 4;
}

void _key_encode_u64(uint64_t u, std::string *key)
{
  uint64_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab64(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 8);
}

const char *_key_decode_u64(const char *key, uint64_t *pu)
{
  uint64_t bu;
  memcpy(&bu, key, 8);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab64(bu);
#else
# error wtf
#endif
  return key + 8;
}
