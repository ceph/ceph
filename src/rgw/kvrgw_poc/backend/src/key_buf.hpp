// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "constants.hpp"
#include "typed_ids.hpp"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <string_view>

namespace kvrgw {

struct KeyBuf {
  static constexpr size_t kMaxSize = 1100;
  uint8_t data[kMaxSize];
  size_t len{};

  template<typename H>
  void set_header(const H& hdr) {
    static_assert(sizeof(H) <= kMaxSize);
    std::memcpy(data, &hdr, sizeof(H));
    len = sizeof(H);
  }

  bool append(const void* src, size_t n) {
    if (len + n > kMaxSize) {
      return false;
    }
    std::memcpy(data + len, src, n);
    len += n;
    return true;
  }

  bool append_byte(uint8_t b) {
    if (len >= kMaxSize) return false;
    data[len++] = b;
    return true;
  }

  std::string_view view() const {
    return {reinterpret_cast<const char*>(data), len};
  }

  const uint8_t* ptr() const { return data; }
  size_t size() const { return len; }
};

#pragma pack(push, 1)

struct KeyHeaderS {
  char ns;
  uint16_t shard_count;
  uint16_t shard_id;
  uint8_t bucket_id[sizeof(bucket_id_t)];
  char cat;

  KeyHeaderS(char ns_, uint16_t sc, uint16_t si, const void* bid, char cat_)
      : ns(ns_), shard_count(htons(sc)), shard_id(htons(si)), cat(cat_) {
    std::memcpy(bucket_id, bid, sizeof(bucket_id));
  }
};

struct KeyHeaderG {
  char ns;
  uint8_t size_tier;
  uint16_t shard_count;
  uint16_t shard_id;
  uint8_t bucket_id[sizeof(bucket_id_t)];
  char cat;

  KeyHeaderG(uint8_t tier, uint16_t sc, uint16_t si, const void* bid, char cat_)
      : ns('G'), size_tier(tier), shard_count(htons(sc)), shard_id(htons(si)), cat(cat_) {
    std::memcpy(bucket_id, bid, sizeof(bucket_id));
  }
};

struct KeyHeaderD {
  char ns;
  uint16_t shard_count;
  uint16_t shard_id;
  uint8_t bucket_id[sizeof(bucket_id_t)];
  uint8_t size_tier;
  uint8_t hash_prefix;
  uint32_t mtime;
  uint8_t ref_tag[kRefTagSize];

  KeyHeaderD(uint16_t sc, uint16_t si, const void* bid,
             uint8_t st, uint8_t hp, uint32_t mt, const void* rt)
      : ns('D'), shard_count(htons(sc)), shard_id(htons(si)),
        size_tier(st), hash_prefix(hp), mtime(htonl(mt)) {
    std::memcpy(bucket_id, bid, sizeof(bucket_id));
    std::memcpy(ref_tag, rt, sizeof(ref_tag));
  }
};

struct KeyHeaderB {
  char ns;
  tenant_id_t tenant_id;

  KeyHeaderB(tenant_id_t tid)
      : ns('B'), tenant_id(htonl(tid)) {}
};

struct KeyHeaderL {
  char ns;
  char type;

  KeyHeaderL(char t) : ns('L'), type(t) {}
};

#pragma pack(pop)

static_assert(sizeof(KeyHeaderS) == 14);
static_assert(sizeof(KeyHeaderG) == 15);
static_assert(sizeof(KeyHeaderD) == 31);
static_assert(sizeof(KeyHeaderB) == 5);
static_assert(sizeof(KeyHeaderL) == 2);

}  // namespace kvrgw
