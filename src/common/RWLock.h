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



#ifndef _RWLock_Posix_
#define _RWLock_Posix_

#include <pthread.h>
#include "lockdep.h"

class RWLock
{
  mutable pthread_rwlock_t L;
  const char *name;
  int id;

  public:

  RWLock(const char *n) : name(n), id(-1) {
    pthread_rwlock_init(&L, NULL);
    if (g_lockdep) id = lockdep_register(name);
  }

  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() {
    if (g_lockdep) id = lockdep_unlocked(name, id);
    pthread_rwlock_unlock(&L);
  }
  void get_read() {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    pthread_rwlock_rdlock(&L);    
    if (g_lockdep) id = lockdep_locked(name, id);
  }
  void put_read() { unlock(); }
  void get_write() {
    if (g_lockdep) id = lockdep_will_lock(name, id);
    pthread_rwlock_wrlock(&L);
    if (g_lockdep) id = lockdep_locked(name, id);
  }
  void put_write() { unlock(); }
};

#endif // !_Mutex_Posix_
