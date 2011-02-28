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


#ifndef CEPH_THREAD_H
#define CEPH_THREAD_H

#include "common/signal.h"
#include "common/config.h"
#include "include/atomic.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>

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
    void *r = ((Thread*)arg)->entry();
    return r;
  }

 public:
  static int get_num_threads(void);

  pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }
  bool am_self() { return (pthread_self() == thread_id); }

  int kill(int signal) {
    if (thread_id)
      return pthread_kill(thread_id, signal);
    else
      return -EINVAL;
  }
  int create(size_t stacksize = 0) {

    pthread_attr_t *thread_attr = NULL;
    stacksize &= PAGE_MASK;  // must be multiple of page
    if (stacksize) {
      thread_attr = (pthread_attr_t*) malloc(sizeof(pthread_attr_t));
      pthread_attr_init(thread_attr);
      pthread_attr_setstacksize(thread_attr, stacksize);
    }

    // The child thread will inherit our signal mask.  We want it to block all
    // signals, so set our mask to that.  (It's ok to block signals for a
    // little while-- they will just be delivered to another thread or
    // delieverd to this thread later.)
    sigset_t old_sigset;
    block_all_signals(&old_sigset);
    int r = pthread_create(&thread_id, thread_attr, _entry_func, (void*)this);
    restore_sigset(&old_sigset);

    if (thread_attr) 
      free(thread_attr);

    if (r) {
      char buf[80];
      generic_dout(0) << "pthread_create failed with message: " << strerror_r(r, buf, sizeof(buf)) << dendl;
    } else {
      generic_dout(10) << "thread " << thread_id << " start" << dendl;
    }
    return r;
  }
  int join(void **prval = 0) {
    if (thread_id == 0) {
      generic_dout(0) << "WARNING: join on thread that was never started" << dendl;
      assert(0);
      return -EINVAL;   // never started.
    }

    int status = pthread_join(thread_id, prval);
    if (status != 0) {
      switch (status) {
      case -EINVAL:
	generic_dout(0) << "thread " << thread_id << " join status = EINVAL" << dendl;
	break;
      case -ESRCH:
	generic_dout(0) << "thread " << thread_id << " join status = ESRCH" << dendl;
	assert(0);
	break;
      case -EDEADLK:
	generic_dout(0) << "thread " << thread_id << " join status = EDEADLK" << dendl;
	break;
      default:
	generic_dout(0) << "thread " << thread_id << " join status = " << status << dendl;
      }
      assert(0); // none of these should happen.
    }
    generic_dout(10) << "thread " << thread_id << " stop" << dendl;
    thread_id = 0;
    return status;
  }

};

#endif
