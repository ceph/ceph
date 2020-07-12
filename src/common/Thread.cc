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

#include <signal.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#endif

#ifdef WITH_SEASTAR
#include "crimson/os/alienstore/alien_store.h"
#endif

#include "common/Thread.h"
#include "common/code_environment.h"
#include "common/debug.h"
#include "common/signal.h"

#ifdef HAVE_SCHED
#include <sched.h>
#endif


pid_t ceph_gettid(void)
{
#ifdef __linux__
  return syscall(SYS_gettid);
#else
  return -ENOSYS;
#endif
}

static int _set_affinity(int id)
{
#ifdef HAVE_SCHED
  if (id >= 0 && id < CPU_SETSIZE) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    CPU_SET(id, &cpuset);

    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0)
      return -errno;
    /* guaranteed to take effect immediately */
    sched_yield();
  }
#endif
  return 0;
}

Thread::Thread()
  : thread_id(0),
    pid(0),
    cpuid(-1),
    thread_name(NULL)
{
}

Thread::~Thread()
{
}

void *Thread::_entry_func(void *arg) {
  void *r = ((Thread*)arg)->entry_wrapper();
  return r;
}

void *Thread::entry_wrapper()
{
  int p = ceph_gettid(); // may return -ENOSYS on other platforms
  if (p > 0)
    pid = p;
  if (pid && cpuid >= 0)
    _set_affinity(cpuid);

  ceph_pthread_setname(pthread_self(), thread_name);
#ifdef WITH_SEASTAR
  crimson::os::AlienStore::configure_thread_memory();
#endif
  return entry();
}

const pthread_t &Thread::get_thread_id() const
{
  return thread_id;
}

bool Thread::is_started() const
{
  return thread_id != 0;
}

bool Thread::am_self() const
{
  return (pthread_self() == thread_id);
}

int Thread::kill(int signal)
{
  if (thread_id)
    return pthread_kill(thread_id, signal);
  else
    return -EINVAL;
}

int Thread::try_create(size_t stacksize)
{
  pthread_attr_t *thread_attr = NULL;
  pthread_attr_t thread_attr_loc;
  
  stacksize &= CEPH_PAGE_MASK;  // must be multiple of page
  if (stacksize) {
    thread_attr = &thread_attr_loc;
    pthread_attr_init(thread_attr);
    pthread_attr_setstacksize(thread_attr, stacksize);
  }

  int r;

  // The child thread will inherit our signal mask.  Set our signal mask to
  // the set of signals we want to block.  (It's ok to block signals more
  // signals than usual for a little while-- they will just be delivered to
  // another thread or delieverd to this thread later.)

  #ifndef _WIN32
  sigset_t old_sigset;
  if (g_code_env == CODE_ENVIRONMENT_LIBRARY) {
    block_signals(NULL, &old_sigset);
  }
  else {
    int to_block[] = { SIGPIPE , 0 };
    block_signals(to_block, &old_sigset);
  }
  r = pthread_create(&thread_id, thread_attr, _entry_func, (void*)this);
  restore_sigset(&old_sigset);
  #else
  r = pthread_create(&thread_id, thread_attr, _entry_func, (void*)this);
  #endif

  if (thread_attr) {
    pthread_attr_destroy(thread_attr);	
  }

  return r;
}

void Thread::create(const char *name, size_t stacksize)
{
  ceph_assert(strlen(name) < 16);
  thread_name = name;

  int ret = try_create(stacksize);
  if (ret != 0) {
    char buf[256];
    snprintf(buf, sizeof(buf), "Thread::try_create(): pthread_create "
	     "failed with error %d", ret);
    dout_emergency(buf);
    ceph_assert(ret == 0);
  }
}

int Thread::join(void **prval)
{
  if (thread_id == 0) {
    ceph_abort_msg("join on thread that was never started");
    return -EINVAL;
  }

  int status = pthread_join(thread_id, prval);
  if (status != 0) {
    char buf[256];
    snprintf(buf, sizeof(buf), "Thread::join(): pthread_join "
             "failed with error %d\n", status);
    dout_emergency(buf);
    ceph_assert(status == 0);
  }

  thread_id = 0;
  return status;
}

int Thread::detach()
{
  return pthread_detach(thread_id);
}

int Thread::set_affinity(int id)
{
  int r = 0;
  cpuid = id;
  if (pid && ceph_gettid() == pid)
    r = _set_affinity(id);
  return r;
}

// Functions for std::thread
// =========================

void set_thread_name(std::thread& t, const std::string& s) {
  int r = ceph_pthread_setname(t.native_handle(), s.c_str());
  if (r != 0) {
    throw std::system_error(r, std::generic_category());
  }
}
std::string get_thread_name(const std::thread& t) {
  std::string s(256, '\0');

  int r = ceph_pthread_getname(const_cast<std::thread&>(t).native_handle(),
			       s.data(), s.length());
  if (r != 0) {
    throw std::system_error(r, std::generic_category());
  }
  s.resize(std::strlen(s.data()));
  return s;
}

void kill(std::thread& t, int signal)
{
  auto r = pthread_kill(t.native_handle(), signal);
  if (r != 0) {
    throw std::system_error(r, std::generic_category());
  }
}
