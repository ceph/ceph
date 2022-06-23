// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * ============
 *
 * This is a common read/write reference framework, which will work
 * simliarly to a RW lock, the difference here is that for the "readers"
 * they won't hold any lock but will increase a reference instead when
 * the "require" state is matched, or set a flag to tell the callers
 * that the "require" state is not matched and also there is no any
 * wait mechanism for "readers" to wait the state until it matches. It
 * will let the callers determine what to do next.
 *
 * The usage, such as in libcephfs's client/Client.cc case:
 *
 * The Readers:
 *
 *   For the ll_read()/ll_write(), etc functions, they will work as
 *   "readers", in the beginning they just need to define a RWRef
 *   object and in RWRef constructor it will check if the state is
 *   MOUNTED or MOUNTING, if not it will fail and return directly with
 *   doing nothing, or it will increase the reference and continue.
 *   And when destructing the RWRef object, in the RWRef destructor
 *   it will decrease the reference and notify the "writers" who maybe
 *   waiting.
 *
 * The Writers:
 *
 *   And for the _unmount() function , as a "writer", in the beginning
 *   it will also just need to define a RWRef object and in RWRef
 *   constructor it will update the state to next stage first, which then
 *   will fail all the new coming "readers", and then wait for all the
 *   "readers" to finish.
 *
 * With this we can get rid of the locks for all the "readers" and they
 * can run in parallel. And we won't have any potential deadlock issue
 * with RWRef, such as:
 *
 * With RWLock:
 *
 *     ThreadA:                           ThreadB:
 *
 *     write_lock<RWLock1>.lock();        another_lock.lock();
 *     state = NEXT_STATE;                ...
 *     another_lock.lock();               read_lock<RWLock1>.lock();
 *     ...                                if (state == STATE) {
 *                                          ...
 *                                        }
 *                                        ...
 *
 * With RWRef:
 *
 *     ThreadA:                           ThreadB:
 *
 *     w = RWRef(myS, NEXT_STATE, false); another_lock.lock();
 *     another_lock.lock();               r = RWRef(myS, STATE);
 *     ...                                if (r.is_state_satisfied()) {
 *                                          ...
 *                                        }
 *                                        ...
 *
 * And also in ThreadA, if it needs to do the cond.wait(&another_lock),
 * it will goto sleep by holding the write_lock<RWLock1> for the RWLock
 * case, if the ThreadBs are for some IOs, they may stuck for a very long
 * time that may get timedout in the uplayer which may keep retrying.
 * With the RWRef, the ThreadB will fail or continue directly without any
 * stuck, and the uplayer will knew what next to do quickly.
 */

#ifndef CEPH_RWRef_Posix__H
#define CEPH_RWRef_Posix__H

#include <string>
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"

/* The status mechanism info */
template<typename T>
struct RWRefState {
  public:
    template <typename T1> friend class RWRef;

    /*
     * This will be status mechanism. Currently you need to define
     * it by yourself.
     */
    T state;

    /*
     * User defined method to check whether the "require" state
     * is in the proper range we need.
     *
     * For example for the client/Client.cc:
     * In some reader operation cases we need to make sure the
     * client state is in mounting or mounted states, then it
     * will set the "require = mounting" in class RWRef's constructor.
     * Then the check_reader_state() should return truth if the
     * state is already in mounting or mounted state.
     */
    virtual int check_reader_state(T require) const = 0;

    /*
     * User defined method to check whether the "require" state
     * is in the proper range we need.
     *
     * This will usually be the state migration check.
     */
    virtual int check_writer_state(T require) const = 0;

    /*
     * User defined method to check whether the "require"
     * state is valid or not.
     */
    virtual bool is_valid_state(T require) const = 0;

    int64_t get_state() const {
      std::scoped_lock l{lock};
      return state;
    }

    bool check_current_state(T require) const {
      ceph_assert(is_valid_state(require));

      std::scoped_lock l{lock};
      return state == require;
    }

    RWRefState(T init_state, const char *lockname, uint64_t _reader_cnt=0)
      : state(init_state), lock(ceph::make_mutex(lockname)), reader_cnt(_reader_cnt) {}
    virtual ~RWRefState() {}

  private:
    mutable ceph::mutex lock;
    ceph::condition_variable cond;
    uint64_t reader_cnt = 0;
};

template<typename T>
class RWRef {
public:
  RWRef(const RWRef& other) = delete;
  const RWRef& operator=(const RWRef& other) = delete;

  RWRef(RWRefState<T> &s, T require, bool ir=true)
    :S(s), is_reader(ir) {
    ceph_assert(S.is_valid_state(require));

    std::scoped_lock l{S.lock};
    if (likely(is_reader)) { // Readers will update the reader_cnt
      if (S.check_reader_state(require)) {
        S.reader_cnt++;
        satisfied = true;
      }
    } else { // Writers will update the state
      is_reader = false;

      /*
       * If the current state is not the same as "require"
       * then update the state and we are the first writer.
       *
       * Or if there already has one writer running or
       * finished, it will let user to choose to continue
       * or just break.
       */
      if (S.check_writer_state(require)) {
        first_writer = true;
        S.state = require;
      }
      satisfied = true;
    }
  }

  /*
   * Whether the "require" state is in the proper range of
   * the states.
   */
  bool is_state_satisfied() const {
    return satisfied;
  }

  /*
   * Update the state, and only the writer could do the update.
   */
  void update_state(T new_state) {
    ceph_assert(!is_reader);
    ceph_assert(S.is_valid_state(new_state));

    std::scoped_lock l{S.lock};
    S.state = new_state;
  }

  /*
   * For current state whether we are the first writer or not
   */
  bool is_first_writer() const {
    return first_writer;
  }

  /*
   * Will wait for all the in-flight "readers" to finish
   */
  void wait_readers_done() {
    // Only writers can wait
    ceph_assert(!is_reader);

    std::unique_lock l{S.lock};

    S.cond.wait(l, [this] {
      return !S.reader_cnt;
    });
  }

  ~RWRef() {
    std::scoped_lock l{S.lock};
    if (!is_reader)
      return;

    if (!satisfied)
      return;

    /*
     * Decrease the refcnt and notify the waiters
     */
    if (--S.reader_cnt == 0)
      S.cond.notify_all();
  }

private:
  RWRefState<T> &S;
  bool satisfied = false;
  bool first_writer = false;
  bool is_reader = true;
};

#endif // !CEPH_RWRef_Posix__H
