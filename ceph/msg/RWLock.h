// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

class RWLock
{
  mutable pthread_rwlock_t L;

  public:

  RWLock() {
    pthread_rwlock_init(&L, NULL);
  }

  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() {
    pthread_rwlock_unlock(&L);
  }
  void get_read() {
    pthread_rwlock_rdlock(&L);    
  }
  void put_read() { unlock(); }
  void get_write() {
    pthread_rwlock_wrlock(&L);
  }
  void put_write() { unlock(); }
};

#endif // !_Mutex_Posix_
