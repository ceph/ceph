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

#include <memory>
#include <utility>

#include <pthread.h>
#include <sys/types.h>

class Thread {
 private:
  pthread_t thread_id;
  pid_t pid;
  int ioprio_class, ioprio_priority;
  int cpuid;
  const char *thread_name;

  void *entry_wrapper();

 public:
  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;

  Thread();
  virtual ~Thread();

 protected:
  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg);

 public:
  const pthread_t &get_thread_id() const;
  pid_t get_pid() const { return pid; }
  bool is_started() const;
  bool am_self() const;
  int kill(int signal);
  int try_create(size_t stacksize);
  void create(const char *name, size_t stacksize = 0);
  int join(void **prval = 0);
  int detach();
  int set_ioprio(int cls, int prio);
  int set_affinity(int cpuid);
};


template <typename F>
class LambdaThread : public Thread {
  F f;

  void* entry() override final {
    // TODO: use SFINAE to forward the result if F's return type
    // is void*. This should be possible with a thing in type of
    // std::result_of.
    // Anyway, a lot of derivates just call their main loops and
    // never go with something different than nullptr, so Lambda
    // Thread still can be useful and help avoid making closures
    // manually.
    f();
    return nullptr;
  }

public:
  LambdaThread(F &&f) : f(std::forward<F>(f)) {}
};

template <typename F>
std::unique_ptr<LambdaThread<F>> make_lambda_thread(F &&f) {
  return std::make_unique<LambdaThread<F>>(std::move(f));
}

#endif
