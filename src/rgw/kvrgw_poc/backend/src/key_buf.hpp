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
#include "ref_tag.hpp"
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

  KeyBuf() = default;
  KeyBuf(const KeyBuf& other) noexcept : len(other.len) {
    std::memcpy(data, other.data, other.len);
  }

  KeyBuf& operator=(const KeyBuf& other) noexcept {
    if (this != &other) {
      len = other.len;
      std::memcpy(data, other.data, other.len);
    }
    return *this;
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
  bucket_id_t bucket_id;
  char cat;

  KeyHeaderS(char ns_, uint16_t sc, uint16_t si, bucket_id_t bid, char cat_)
      : ns(ns_), shard_count(htons(sc)), shard_id(htons(si)), cat(cat_) {
    bucket_id = bid;
  }
};

struct KeyHeaderG {
  char ns;
  uint8_t size_tier;
  uint16_t shard_count;
  uint16_t shard_id;
  bucket_id_t bucket_id;
  char cat;

  KeyHeaderG(uint8_t tier, uint16_t sc, uint16_t si, bucket_id_t bid, char cat_)
      : ns('G'), size_tier(tier), shard_count(htons(sc)), shard_id(htons(si)), cat(cat_) {
    bucket_id = bid;
  }
};

// Compact header for group G:G keys — no bucket_id (each GcDescriptor is self-contained).
// Wire layout: [ns='G'][cat='G'] — 2 bytes, followed by ref_tag (12B) as unique discriminator.
struct KeyHeaderGGroup {
  char ns{'G'};
  char cat{'G'};
};
static_assert(sizeof(KeyHeaderGGroup) == 2);

// Single P:O key header.
// Wire layout: [ns='P'][bucket_id 8B][cat='O'] — 10B, followed by ref_tag (12B) → 22B total key.
struct KeyHeaderPO {
  char ns{'P'};
  char cat{'O'};
  bucket_id_t bucket_id;

  KeyHeaderPO() = default;
  explicit KeyHeaderPO(bucket_id_t bid) {
    bucket_id = bid;
  }
};
static_assert(sizeof(KeyHeaderPO) == 10);

// Group P:O key header.
// Wire layout: [ns='P'][cat='G'] — 2B, followed by ref_tag_0 (12B) → 14B total key.
struct KeyHeaderPOGroup {
  char ns{'P'};
  char cat{'G'};
};
static_assert(sizeof(KeyHeaderPOGroup) == 2);

// Single P:M key — Sweeper-driven multipart cleanup entry (see
// plans/fixing_agent_mess.md Section 4). Same layout convention as P:O.
// Wire layout: [ns='P'][cat='M'][bucket_id 8B] — 10B, followed by ref_tag
// (12B) → 22B total key.
struct KeyHeaderPM {
  char ns{'P'};
  char cat{'M'};
  bucket_id_t bucket_id;

  KeyHeaderPM() = default;
  explicit KeyHeaderPM(bucket_id_t bid) {
    bucket_id = bid;
  }
};
static_assert(sizeof(KeyHeaderPM) == 10);

struct KeyHeaderD {
  char ns;
  uint16_t shard_count;
  uint16_t shard_id;
  bucket_id_t bucket_id;
  uint8_t size_tier;
  uint8_t hash_prefix;
  uint32_t mtime;
  RefTag ref_tag;

  KeyHeaderD(uint16_t sc, uint16_t si, bucket_id_t bid,
             uint8_t st, uint8_t hp, uint32_t mt, const char* rt)
      : ns('D'), shard_count(htons(sc)), shard_id(htons(si)),
        size_tier(st), hash_prefix(hp), mtime(htonl(mt)) {
    bucket_id = bid;
    ref_tag.load(rt);
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
