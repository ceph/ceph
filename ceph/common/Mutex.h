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

/////////////////////////////////////////////////////////////////////
//  Written by Phillip Sitbon
//  Copyright 2003
//
//  Posix/Mutex.h
//    - Resource locking mechanism using Posix mutexes
//
/////////////////////////////////////////////////////////////////////

#ifndef _Mutex_Posix_
#define _Mutex_Posix_

#include <pthread.h>
#include <cassert>

class Mutex
{
  mutable pthread_mutex_t M;
  void operator=(Mutex &M) {}
  Mutex( const Mutex &M ) {}
  bool tag;

  public:

  Mutex() : tag(false)
  {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&M,&attr);
    //cout << this << " mutex init = " << r << endl;
    pthread_mutexattr_destroy(&attr);
  }

  Mutex(bool t) : tag(t)
  {
	assert(0);
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&M,&attr);
    //cout << this << " mutex init = " << r << endl;
    pthread_mutexattr_destroy(&attr);
  }

  virtual ~Mutex()
  { 
	//pthread_mutex_unlock(&M); 
	pthread_mutex_destroy(&M); 
  }

  int Lock()  { 
	if (tag) cout << this << " " << pthread_self() << endl; 
	int r = pthread_mutex_lock(&M);
	if (tag) cout << "lock = " << r << endl;
	return r;
  }

  int Lock(char *s)  { 
	cout << "Lock: " << s << endl;
	int r = pthread_mutex_lock(&M);
	cout << this << " " << pthread_self() << " lock = " << r << endl;
	return r;
  }

  int Lock_Try() const
  { 
	return pthread_mutex_trylock(&M); 
  }

  int Unlock() 
  { 
	if (tag) cout << this << " " << pthread_self() << endl;
	int r = pthread_mutex_unlock(&M);
	if (tag) cout << "lock = " << r << endl;
	return r;
  }

  int Unlock(char *s) 
  { 
	cout << "Unlock: " << s << endl;
	int r = pthread_mutex_unlock(&M);
	cout << this << " " << pthread_self() << " unlock = " << r << endl;
	return r;
  }

  friend class Cond;
};

#endif // !_Mutex_Posix_
