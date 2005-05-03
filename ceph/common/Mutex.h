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

  bool locked;

  public:

  Mutex()
  {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
    cout << "mutex init = " << pthread_mutex_init(&M,NULL) << endl;
    pthread_mutexattr_destroy(&attr);
	locked = false;
  }

  virtual ~Mutex()
  { pthread_mutex_unlock(&M); pthread_mutex_destroy(&M); }

  int Lock()  { 
	int r = pthread_mutex_lock(&M);
	cout << pthread_self() << " lock = " << r << endl;
	assert(!locked);
	locked = true;
	return r;
  }

  int Lock_Try() const
  { return pthread_mutex_trylock(&M); }

  int Unlock() 
  { 
	assert(locked);
	locked = false;
	int r = pthread_mutex_unlock(&M);
	cout << pthread_self() << " unlock = " << r << endl;
	return r;
  }
};

#endif // !_Mutex_Posix_
