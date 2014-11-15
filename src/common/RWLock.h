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
#include "lockdep.h"
#include "include/atomic.h"

class RWLock
{
  mutable pthread_rwlock_t L;
  const char *name;
  mutable int id;
  mutable atomic_t nrlock, nwlock;

public:
  RWLock(const RWLock& other);
  const RWLock& operator=(const RWLock& other);

  RWLock(const char *n) : name(n), id(-1), nrlock(0), nwlock(0) {
    pthread_rwlock_init(&L, NULL);
    if (g_lockdep) id = lockdep_register(name);
  }

  bool is_locked() const {
    return (nrlock.read() > 0) || (nwlock.read() > 0);
  }

  bool is_wlocked() const {
    return (nwlock.read() > 0);
  }
  virtual ~RWLock() {
    // The following check is racy but we are about to destroy
    // the object and we assume that there are no other users.
    assert(!is_locked());
    pthread_rwlock_destroy(&L);
  }

  void unlock() const {
    if (nwlock.read() > 0) {
      nwlock.dec();
    } else {
      nrlock.dec();
    }
    if (g_lockdep) id = lockdep_will_unlock(name, id);
    pthread_rwlock_unlock(&L);
  }

  // read
  void get_read() const {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    pthread_rwlock_rdlock(&L);
    if (g_lockdep) id = lockdep_locked(name, id);
    nrlock.inc();
  }
  bool try_get_read() const {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      nrlock.inc();
      if (g_lockdep) id = lockdep_locked(name, id);
      return true;
    }
    return false;
  }
  void put_read() const {
    unlock();
  }

  // write
  void get_write() {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    pthread_rwlock_wrlock(&L);
    if (g_lockdep) id = lockdep_locked(name, id);
    nwlock.inc();
  }
  bool try_get_write() {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      if (g_lockdep) id = lockdep_locked(name, id);
      nwlock.inc();
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }

public:
  class RLocker {
    const RWLock &m_lock;

  public:
    RLocker(const RWLock& lock) : m_lock(lock) {
      m_lock.get_read();
    }
    ~RLocker() {
      m_lock.put_read();
    }
  };

  class WLocker {
    RWLock &m_lock;

  public:
    WLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_write();
    }
    ~WLocker() {
      m_lock.put_write();
    }
  };
};

#endif // !_Mutex_Posix_
