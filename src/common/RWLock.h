// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#ifndef CEPH_RWLock_Posix__H
#define CEPH_RWLock_Posix__H

#include <pthread.h>
#include <string>
#include "include/ceph_assert.h"
#include "acconfig.h"
#include "lockdep.h"
#include "common/valgrind.h"

#include <atomic>

class RWLock final
{
  mutable pthread_rwlock_t L;
  std::string name;
  mutable int id;
  mutable std::atomic<unsigned> nrlock = { 0 }, nwlock = { 0 };
  bool track, lockdep;

  std::string unique_name(const char* name) const;

public:
  RWLock(const RWLock& other) = delete;
  const RWLock& operator=(const RWLock& other) = delete;

  RWLock(const std::string &n, bool track_lock=true, bool ld=true, bool prioritize_write=false)
    : name(n), id(-1), track(track_lock),
      lockdep(ld) {
#if defined(HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP)
    if (prioritize_write) {
      pthread_rwlockattr_t attr;
      pthread_rwlockattr_init(&attr);
      // PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
      //   Setting the lock kind to this avoids writer starvation as long as
      //   long as any read locking is not done in a recursive fashion.
      pthread_rwlockattr_setkind_np(&attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
      pthread_rwlock_init(&L, &attr);
      pthread_rwlockattr_destroy(&attr);
    } else 
#endif 
    // Next block is in {} to possibly connect to the above if when code is used.
    {
      pthread_rwlock_init(&L, NULL);
    }
    ANNOTATE_BENIGN_RACE_SIZED(&id, sizeof(id), "RWLock lockdep id");
    ANNOTATE_BENIGN_RACE_SIZED(&nrlock, sizeof(nrlock), "RWlock nrlock");
    ANNOTATE_BENIGN_RACE_SIZED(&nwlock, sizeof(nwlock), "RWlock nwlock");
    if (lockdep && g_lockdep) id = lockdep_register(name.c_str());
  }

  bool is_locked() const {
    ceph_assert(track);
    return (nrlock > 0) || (nwlock > 0);
  }

  bool is_wlocked() const {
    ceph_assert(track);
    return (nwlock > 0);
  }
  ~RWLock() {
    // The following check is racy but we are about to destroy
    // the object and we assume that there are no other users.
    if (track)
      ceph_assert(!is_locked());
    pthread_rwlock_destroy(&L);
    if (lockdep && g_lockdep) {
      lockdep_unregister(id);
    }
  }

  void unlock(bool lockdep=true) const {
    if (track) {
      if (nwlock > 0) {
        nwlock--;
      } else {
        ceph_assert(nrlock > 0);
        nrlock--;
      }
    }
    if (lockdep && this->lockdep && g_lockdep)
      id = lockdep_will_unlock(name.c_str(), id);
    int r = pthread_rwlock_unlock(&L);
    ceph_assert(r == 0);
  }

  // read
  void get_read() const {
    if (lockdep && g_lockdep) id = lockdep_will_lock(name.c_str(), id);
    int r = pthread_rwlock_rdlock(&L);
    ceph_assert(r == 0);
    if (lockdep && g_lockdep) id = lockdep_locked(name.c_str(), id);
    if (track)
      nrlock++;
  }
  bool try_get_read() const {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      if (track)
         nrlock++;
      if (lockdep && g_lockdep) id = lockdep_locked(name.c_str(), id);
      return true;
    }
    return false;
  }
  void put_read() const {
    unlock();
  }
  void lock_shared() {
    get_read();
  }
  void unlock_shared() {
    put_read();
  }
  // write
  void get_write(bool lockdep=true) {
    if (lockdep && this->lockdep && g_lockdep)
      id = lockdep_will_lock(name.c_str(), id);
    int r = pthread_rwlock_wrlock(&L);
    ceph_assert(r == 0);
    if (lockdep && this->lockdep && g_lockdep)
      id = lockdep_locked(name.c_str(), id);
    if (track)
      nwlock++;

  }
  bool try_get_write(bool lockdep=true) {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      if (lockdep && this->lockdep && g_lockdep)
	id = lockdep_locked(name.c_str(), id);
      if (track)
         nwlock++;
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }
  void lock() {
    get_write();
  }
  void get(bool for_write) {
    if (for_write) {
      get_write();
    } else {
      get_read();
    }
  }

public:
  class RLocker {
    const RWLock &m_lock;

    bool locked;

  public:
   explicit  RLocker(const RWLock& lock) : m_lock(lock) {
      m_lock.get_read();
      locked = true;
    }
    void unlock() {
      ceph_assert(locked);
      m_lock.unlock();
      locked = false;
    }
    ~RLocker() {
      if (locked) {
        m_lock.unlock();
      }
    }
  };

  class WLocker {
    RWLock &m_lock;

    bool locked;

  public:
    explicit WLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_write();
      locked = true;
    }
    void unlock() {
      ceph_assert(locked);
      m_lock.unlock();
      locked = false;
    }
    ~WLocker() {
      if (locked) {
        m_lock.unlock();
      }
    }
  };

  class Context {
    RWLock& lock;

  public:
    enum LockState {
      Untaken = 0,
      TakenForRead = 1,
      TakenForWrite = 2,
    };

  private:
    LockState state;

  public:
    explicit Context(RWLock& l) : lock(l), state(Untaken) {}
    Context(RWLock& l, LockState s) : lock(l), state(s) {}

    void get_write() {
      ceph_assert(state == Untaken);

      lock.get_write();
      state = TakenForWrite;
    }

    void get_read() {
      ceph_assert(state == Untaken);

      lock.get_read();
      state = TakenForRead;
    }

    void unlock() {
      ceph_assert(state != Untaken);
      lock.unlock();
      state = Untaken;
    }

    void promote() {
      ceph_assert(state == TakenForRead);
      unlock();
      get_write();
    }

    LockState get_state() { return state; }
    void set_state(LockState s) {
      state = s;
    }

    bool is_locked() {
      return (state != Untaken);
    }

    bool is_rlocked() {
      return (state == TakenForRead);
    }

    bool is_wlocked() {
      return (state == TakenForWrite);
    }
  };
};

#endif // !CEPH_RWLock_Posix__H
