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

#include <functional>
#include <string_view>
#include <system_error>
#include <thread>

#include <pthread.h>
#include <sys/types.h>

#include "include/compat.h"

extern pid_t ceph_gettid();

class Thread {
 private:
  pthread_t thread_id;
  pid_t pid;
  int cpuid;
  std::string thread_name;

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
  int set_affinity(int cpuid);
};

// Functions for with std::thread

void set_thread_name(std::thread& t, const std::string& s);
std::string get_thread_name(const std::thread& t);
void kill(std::thread& t, int signal);

template<typename Fun, typename... Args>
std::thread make_named_thread(std::string_view n,
			      Fun&& fun,
			      Args&& ...args) {

  return std::thread([n = std::string(n)](auto&& fun, auto&& ...args) {
		       ceph_pthread_setname(pthread_self(), n.data());
		       std::invoke(std::forward<Fun>(fun),
				   std::forward<Args>(args)...);
		     }, std::forward<Fun>(fun), std::forward<Args>(args)...);
}
#endif
