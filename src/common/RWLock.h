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

class RWLock
{
  mutable pthread_rwlock_t L;
  const char *name;
  int id;

public:
  RWLock(const RWLock& other);
  const RWLock& operator=(const RWLock& other);

  RWLock(const char *n) : name(n), id(-1) {
    pthread_rwlock_init(&L, NULL);
    if (g_lockdep) id = lockdep_register(name);
  }

  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() {
    if (g_lockdep) id = lockdep_will_unlock(name, id);
    int r = pthread_rwlock_unlock(&L);
    assert(r == 0);
  }

  // read
  void get_read() {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    int r = pthread_rwlock_rdlock(&L);
    assert(r == 0);
    if (g_lockdep) id = lockdep_locked(name, id);
  }
  bool try_get_read() {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      if (g_lockdep) id = lockdep_locked(name, id);
      return true;
    }
    return false;
  }
  void put_read() {
    unlock();
  }

  // write
  void get_write() {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    int r = pthread_rwlock_wrlock(&L);
    assert(r == 0);
    if (g_lockdep) id = lockdep_locked(name, id);
  }
  bool try_get_write() {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      if (g_lockdep) id = lockdep_locked(name, id);
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }

public:
  class RLocker {
    RWLock &m_lock;

  public:
    RLocker(RWLock& lock) : m_lock(lock) {
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
