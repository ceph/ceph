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


#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>
#include <errno.h>

class Thread {
 private:
  pthread_t thread_id;

 public:
  Thread() : thread_id(0) {}
  virtual ~Thread() {}

 protected:
  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg) {
    return ((Thread*)arg)->entry();
  }

 public:
  pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }
  bool am_self() { return (pthread_self() == thread_id); }

  int create() {
    return pthread_create( &thread_id, NULL, _entry_func, (void*)this );
  }
  int join(void **prval = 0) {
    if (thread_id == 0) {
      cerr << "WARNING: join on thread that was never started" << endl;
      //assert(0);
      return -EINVAL;   // never started.
    }
    
    int status = pthread_join(thread_id, prval);
    if (status != 0) {
      switch (status) {
      case -EINVAL:
	cerr << "thread " << thread_id << " join status = EINVAL" << endl;
	break;
      case -ESRCH:
	cerr << "thread " << thread_id << " join status = ESRCH" << endl;
	assert(0);
	break;
      case -EDEADLK:
	cerr << "thread " << thread_id << " join status = EDEADLK" << endl;
	break;
      default:
	cerr << "thread " << thread_id << " join status = " << status << endl;
      }
      assert(0); // none of these should happen.
    }
    thread_id = 0;
    return status;
  }

};

#endif
