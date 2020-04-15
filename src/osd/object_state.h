// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd_types.h"

struct ObjectState {
  object_info_t oi;
  bool exists;         ///< the stored object exists (i.e., we will remember the object_info_t)

  ObjectState() : exists(false) {}

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
  ObjectState(object_info_t &&oi_, bool exists_)
    : oi(std::move(oi_)), exists(exists_) {}
  ObjectState(const hobject_t &obj) : oi(obj), exists(false) {}
};

struct RWState {
  enum State {
    RWNONE,
    RWREAD,
    RWWRITE,
    RWEXCL,
  };
  static const char *get_state_name(State s) {
    switch (s) {
    case RWNONE: return "none";
    case RWREAD: return "read";
    case RWWRITE: return "write";
    case RWEXCL: return "excl";
    default: return "???";
    }
  }
  const char *get_state_name() const {
    return get_state_name(state);
  }

  int count;              ///< number of readers or writers
  int waiters = 0;        ///< number waiting

  State state:4;               ///< rw state
  /// if set, restart backfill when we can get a read lock
  bool recovery_read_marker:1;
  /// if set, requeue snaptrim on lock release
  bool snaptrimmer_write_marker:1;

  RWState()
    : count(0),
      state(RWNONE),
      recovery_read_marker(false),
      snaptrimmer_write_marker(false)
  {}

  /// this function adjusts the counts if necessary
  bool get_read_lock() {
    // don't starve anybody!
    if (waiters > 0) {
      return false;
    }
    switch (state) {
    case RWNONE:
      ceph_assert(count == 0);
      state = RWREAD;
      // fall through
    case RWREAD:
      count++;
      return true;
    case RWWRITE:
      return false;
    case RWEXCL:
      return false;
    default:
      ceph_abort_msg("unhandled case");
      return false;
    }
  }

  bool get_write_lock(bool greedy=false) {
    if (!greedy) {
      // don't starve anybody!
      if (waiters > 0 ||
	  recovery_read_marker) {
	return false;
      }
    }
    switch (state) {
    case RWNONE:
      ceph_assert(count == 0);
      state = RWWRITE;
      // fall through
    case RWWRITE:
      count++;
      return true;
    case RWREAD:
      return false;
    case RWEXCL:
      return false;
    default:
      ceph_abort_msg("unhandled case");
      return false;
    }
  }
  bool get_excl_lock() {
    switch (state) {
    case RWNONE:
      ceph_assert(count == 0);
      state = RWEXCL;
      count = 1;
      return true;
    case RWWRITE:
      return false;
    case RWREAD:
      return false;
    case RWEXCL:
      return false;
    default:
      ceph_abort_msg("unhandled case");
      return false;
    }
  }
  /// same as get_write_lock, but ignore starvation
  bool take_write_lock() {
    if (state == RWWRITE) {
      count++;
      return true;
    }
    return get_write_lock();
  }
  bool dec() {
    ceph_assert(count > 0);
    count--;
    if (count == 0) {
      state = RWNONE;
      return true;
    } else {
      return false;
    }
  }
  bool put_read() {
    ceph_assert(state == RWREAD);
    return dec();
  }
  bool put_write() {
    ceph_assert(state == RWWRITE);
    return dec();
  }
  bool put_excl() {
    ceph_assert(state == RWEXCL);
    return dec();
  }
  void inc_waiters() {
    ++waiters;
  }
  void release_waiters() {
    waiters = 0;
  }
  void dec_waiters(int count) {
    ceph_assert(waiters >= count);
    waiters -= count;
  }
  bool empty() const { return state == RWNONE; }

  bool get_snaptrimmer_write(bool mark_if_unsuccessful) {
    if (get_write_lock()) {
      return true;
    } else {
      if (mark_if_unsuccessful)
	snaptrimmer_write_marker = true;
      return false;
    }
  }
  bool get_recovery_read() {
    recovery_read_marker = true;
    if (get_read_lock()) {
      return true;
    }
    return false;
  }
};

inline std::ostream& operator<<(std::ostream& out, const RWState& rw)
{
  return out << "rwstate(" << rw.get_state_name()
	     << " n=" << rw.count
	     << " w=" << rw.waiters
	     << ")";
}
