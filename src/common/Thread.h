 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_THREAD_H
#define CEPH_THREAD_H

#include <pthread.h>

class Thread {
 public:
  Thread();
  virtual ~Thread();

  pthread_t get_thread_id() const;
  pid_t get_pid() const;
  bool is_started() const;
  bool am_self() const;
  int kill(int signal);
  int try_create(size_t stacksize);
  void create(size_t stacksize = 0);
  int join(void **prval = 0);
  int detach();
  int set_ioprio(int cls, int prio);
  int set_affinity(int cpuid);

 protected:
  virtual void *entry() = 0;

 private:
  Thread(const Thread& other);
  Thread& operator=(const Thread& other);

  void *entry_wrapper();
  static void *_entry_func(void *arg);

  pthread_t thread_id;
  pid_t pid;
  int ioprio_class, ioprio_priority;
  int cpuid;
};

#endif
