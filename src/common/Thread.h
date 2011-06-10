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

#include "common/code_environment.h"
#include "common/signal.h"

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

  const pthread_t &get_thread_id() { return thread_id; }
  bool is_started() { return thread_id != 0; }
  bool am_self() { return (pthread_self() == thread_id); }

  int kill(int signal) {
    if (thread_id)
      return pthread_kill(thread_id, signal);
    else
      return -EINVAL;
  }
  int try_create(size_t stacksize) {
    pthread_attr_t *thread_attr = NULL;
    stacksize &= PAGE_MASK;  // must be multiple of page
    if (stacksize) {
      thread_attr = (pthread_attr_t*) malloc(sizeof(pthread_attr_t));
      pthread_attr_init(thread_attr);
      pthread_attr_setstacksize(thread_attr, stacksize);
    }

    int r;

    // The child thread will inherit our signal mask.  Set our signal mask to
    // the set of signals we want to block.  (It's ok to block signals more
    // signals than usual for a little while-- they will just be delivered to
    // another thread or delieverd to this thread later.)
    sigset_t old_sigset;
    if (g_code_env == CODE_ENVIRONMENT_LIBRARY) {
      block_signals(&old_sigset, NULL);
    }
    else {
      int to_block[] = { SIGPIPE , 0 };
      block_signals(&old_sigset, to_block);
    }
    r = pthread_create(&thread_id, thread_attr, _entry_func, (void*)this);
    restore_sigset(&old_sigset);

    if (thread_attr) 
      free(thread_attr);
    return r;
  }

  void create(size_t stacksize = 0) {
    int ret = try_create(stacksize);
    assert(ret == 0);
  }

  int join(void **prval = 0) {
    if (thread_id == 0) {
      assert("join on thread that was never started" == 0);
      return -EINVAL;
    }

    int status = pthread_join(thread_id, prval);
    assert(status == 0);
    thread_id = 0;
    return status;
  }

};

#endif
