// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
