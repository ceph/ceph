// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_NEWSTORE_KV_H
#define CEPH_OS_NEWSTORE_KV_H

#include <string>

// some key encoding helpers
void _key_encode_u32(uint32_t u, std::string *key);
const char *_key_decode_u32(const char *key, uint32_t *pu);
void _key_encode_u64(uint64_t u, std::string *key);
const char *_key_decode_u64(const char *key, uint64_t *pu);

#endif
