// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "types.h"
#include "server.h"
#include "include/encoding.h"
#include <time.h>
#include "include/utime.h"
#include "common/ceph_time.h"

namespace cls::cmpxattr {

  struct cmp_vals_set_vals_op {
    Mode mode;
    Op comparison;
    ComparisonMap cmp_pairs;
    std::map<std::string, ceph::bufferlist> set_pairs;
  };

  inline void encode(const cmp_vals_set_vals_op& o, ceph::bufferlist& bl, uint64_t f=0)
  {
    ENCODE_START(1, 1, bl);
    encode(o.mode, bl);
    encode(o.comparison, bl);
    encode(o.cmp_pairs, bl);
    encode(o.set_pairs, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(cmp_vals_set_vals_op& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.mode, bl);
    decode(o.comparison, bl);
    decode(o.cmp_pairs, bl);
    decode(o.set_pairs, bl);
    DECODE_FINISH(bl);
  }

  //===========================================================================
  struct operation_flags_t {
    friend inline void encode(const operation_flags_t& o, ceph::bufferlist& bl);
    friend inline void decode(operation_flags_t& o, ceph::bufferlist::const_iterator& bl);
    static constexpr uint8_t LOCK_UPDATE_OP_SET_LOCK       = 0x01;
    static constexpr uint8_t LOCK_UPDATE_OP_SET_EPOCH      = 0x02;
    static constexpr uint8_t LOCK_UPDATE_OP_MARK_COMPLETED = 0x04;
    static constexpr uint8_t LOCK_UPDATE_OP_URGENT_MSG     = 0x08;

    operation_flags_t() : flags(0) {}
    operation_flags_t(uint8_t _flags) : flags(_flags) {}
    inline void clear() { this->flags = 0; }
    inline operator uint16_t() const {
      return this->flags;
    }

    inline void set_lock()              {this->flags |= LOCK_UPDATE_OP_SET_LOCK; }
    inline bool is_set_lock() const     { return ((flags & LOCK_UPDATE_OP_SET_LOCK) != 0); }

    inline void set_epoch()              {this->flags |= LOCK_UPDATE_OP_SET_EPOCH; }
    inline bool is_set_epoch() const     { return ((flags & LOCK_UPDATE_OP_SET_EPOCH) != 0); }

    inline bool is_mark_completed() const {
      return ((flags & LOCK_UPDATE_OP_MARK_COMPLETED) != 0);
    }

    inline void set_urgent_msg()      {this->flags |= LOCK_UPDATE_OP_URGENT_MSG; }
    inline bool is_urgent_msg() const {
      return ((flags & LOCK_UPDATE_OP_URGENT_MSG) != 0);
    }
  private:
    uint16_t flags;
  };

  inline void encode(const operation_flags_t& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.flags, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(operation_flags_t& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.flags, bl);
    DECODE_FINISH(bl);
  }

  struct lock_update_op {
    bool is_urgent_stop_msg() const {
      return (op_flags.is_urgent_msg() &&
	      ((urgent_msg == URGENT_MSG_ABORT) || (urgent_msg == URGENT_MSG_PASUE)));
    }

    bool is_lock_revert_msg() const {
      return (op_flags.is_urgent_msg() && (urgent_msg == URGENT_MSG_RESUME));
    }

    bool is_urgent_msg() const { return op_flags.is_urgent_msg(); }

    bool is_mark_completed_msg() const { return op_flags.is_mark_completed(); }

    bool verify() const {
      if (op_flags.is_urgent_msg()) {
	return (op_flags.is_set_lock()        &&
		!op_flags.is_set_epoch()      &&
		!op_flags.is_mark_completed() &&
		in_bl.length() == 0           &&
		urgent_msg != URGENT_MSG_NONE &&
		!progress_a && !progress_b);
      }

      if (op_flags.is_set_epoch()) {
	return (op_flags.is_set_lock()        &&
		!op_flags.is_mark_completed() &&
		in_bl.length() == 0           &&
		urgent_msg == URGENT_MSG_NONE &&
		!progress_a && !progress_b);
      }

      if (op_flags.is_mark_completed() ) {
	return (in_bl.length() > 0 && urgent_msg == URGENT_MSG_NONE);
      }

      return true;
    }
    utime_t           max_lock_duration; // max duration for holding a lock
    uint64_t          progress_a = 0;
    uint64_t          progress_b = 0;
    std::string       owner;
    std::string       key_name;
    operation_flags_t op_flags = 0;
    ceph::bufferlist  in_bl;
    int32_t           urgent_msg = URGENT_MSG_NONE;
  };

  inline void encode(const lock_update_op& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.max_lock_duration, bl);
    encode(o.progress_a, bl);
    encode(o.progress_b, bl);
    encode(o.owner, bl);
    encode(o.key_name, bl);
    encode(o.op_flags, bl);
    encode(o.in_bl, bl);
    encode(o.urgent_msg, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(lock_update_op& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.max_lock_duration, bl);
    decode(o.progress_a, bl);
    decode(o.progress_b, bl);
    decode(o.owner, bl);
    decode(o.key_name, bl);
    decode(o.op_flags, bl);
    decode(o.in_bl, bl);
    decode(o.urgent_msg, bl);
    DECODE_FINISH(bl);
  }

} // namespace cls::cmpxattr
