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

namespace cls::blake3_hash {
  struct cls_blake3_flags_t {
  private:
    static constexpr uint8_t CLS_BLK3_FLAG_FIRST_PART = 0x01;
    static constexpr uint8_t CLS_BLK3_FLAG_LAST_PART  = 0x02;

  public:
    cls_blake3_flags_t() : flags(0) {}
    cls_blake3_flags_t(uint8_t _flags) : flags(_flags) {}

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

  struct cls_blake3_op {
    cls_blake3_flags_t flags;
    ceph::bufferlist blake3_state_bl;
  };

  inline void encode(const cls_blake3_op& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.flags.get_flags(), bl);
    encode(o.blake3_state_bl, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(cls_blake3_op& o, ceph::bufferlist::const_iterator& bl)
  {
    uint8_t _flags;
    DECODE_START(1, bl);
    decode(_flags, bl);
    o.flags.set_flags(_flags);
    decode(o.blake3_state_bl, bl);
    DECODE_FINISH(bl);
  }
} // namespace cls::blake3_hash
