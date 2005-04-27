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

class Mutex
{
  mutable pthread_mutex_t M;
  void operator=(Mutex &M) {}
  Mutex( const Mutex &M ) {}

  public:

  Mutex()
  {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&M,&attr);
    pthread_mutexattr_destroy(&attr);
  }

  virtual ~Mutex()
  { pthread_mutex_unlock(&M); pthread_mutex_destroy(&M); }

  int Lock() const
  { return pthread_mutex_lock(&M); }

  int Lock_Try() const
  { return pthread_mutex_trylock(&M); }

  int Unlock() const
  { return pthread_mutex_unlock(&M); }
};

#endif // !_Mutex_Posix_
