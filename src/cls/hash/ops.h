// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 * Copyright (C) 2025 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once
#include "include/encoding.h"
#include <time.h>
#include "include/utime.h"
#include "common/ceph_time.h"
#include "BLAKE3/c/blake3.h"

namespace cls::hash {
  //#define DEBUG_ENDIANNESS

#ifndef DEBUG_ENDIANNESS
#define CEPHTOH_16 be16toh
#define CEPHTOH_32 be32toh
#define CEPHTOH_64 be64toh
#define HTOCEPH_16 htobe16
#define HTOCEPH_32 htobe32
#define HTOCEPH_64 htobe64

#else
  // DEBUG Mode - use BE format to test the code
#define CEPHTOH_16 le16toh
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_16 htole16
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64
#endif

  //----------------------------------------------------------------------------
  static inline void blake3_to_ceph(blake3_hasher *hasher)
  {
    char *p_end = (char*)hasher->key + sizeof(hasher->key);
    for (uint32_t *p = hasher->key; p < (uint32_t *)p_end; p++) {
      *p = HTOCEPH_32(*p);
    }

    p_end = (char*)hasher->chunk.cv + sizeof(hasher->chunk.cv);
    for (uint32_t *p = hasher->chunk.cv; p < (uint32_t *)p_end; p++) {
      *p = HTOCEPH_32(*p);
    }
    hasher->chunk.chunk_counter = HTOCEPH_64(hasher->chunk.chunk_counter);
  }

  //----------------------------------------------------------------------------
  static inline void blake3_to_host(blake3_hasher *hasher)
  {
    char *p_end = (char*)hasher->key + sizeof(hasher->key);
    for (uint32_t *p = hasher->key; p < (uint32_t *)p_end; p++) {
      *p = CEPHTOH_32(*p);
    }

    p_end = (char*)hasher->chunk.cv + sizeof(hasher->chunk.cv);
    for (uint32_t *p = hasher->chunk.cv; p < (uint32_t *)p_end; p++) {
      *p = CEPHTOH_32(*p);
    }
    hasher->chunk.chunk_counter = CEPHTOH_64(hasher->chunk.chunk_counter);
  }

  enum hash_type_t {
    HASH_NONE,
    HASH_MD5,
    HASH_SHA256,
    HASH_BLAKE3
  };

  struct cls_hash_flags_t {
  private:
    static constexpr uint8_t CLS_BLK3_FLAG_FIRST_PART = 0x01;
    static constexpr uint8_t CLS_BLK3_FLAG_LAST_PART  = 0x02;

  public:
    cls_hash_flags_t() : flags(0) {}
    cls_hash_flags_t(uint8_t _flags) : flags(_flags) {}

    inline void clear() {
      flags = 0;
    }

    inline uint8_t get_flags() const {
      return flags;
    }

    inline void set_flags(uint8_t _flags) {
      flags = _flags;
    }

    inline bool is_first_part() const {
      return ((CLS_BLK3_FLAG_FIRST_PART & flags) != 0);
    }

    inline void set_first_part() {
      flags |= CLS_BLK3_FLAG_FIRST_PART;
    }

    inline bool is_last_part() const {
      return ((CLS_BLK3_FLAG_LAST_PART & flags) != 0);
    }

    inline void set_last_part() {
      flags |= CLS_BLK3_FLAG_LAST_PART;
    }

  private:
    uint8_t flags;
  };

  struct cls_hash_op {
    int32_t hash_type;
    uint64_t offset;
    cls_hash_flags_t flags;
    ceph::bufferlist hash_state_bl;
  };

  inline void encode(const cls_hash_op& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.hash_type, bl);
    encode(o.offset, bl);
    encode(o.flags.get_flags(), bl);
    encode(o.hash_state_bl, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(cls_hash_op& o, ceph::bufferlist::const_iterator& bl)
  {
    uint8_t _flags;
    DECODE_START(1, bl);
    decode(o.hash_type, bl);
    decode(o.offset, bl);
    decode(_flags, bl);
    o.flags.set_flags(_flags);
    decode(o.hash_state_bl, bl);
    DECODE_FINISH(bl);
  }
} // namespace cls::hash
