// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include "include/atomic.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <pthread.h>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#if defined(__FreeBSD__)
#include <pthread_np.h>
#endif

using std::ostringstream;
using std::string;

static pid_t do_gettid(void)
{
#if defined(__linux__)
  return static_cast < pid_t >(syscall(SYS_gettid));
#else
  return static_cast < pid_t >(pthread_getthreadid_np());
#endif
}

ceph::atomic_t m_highest_id(0);

SysTestRunnable::
SysTestRunnable(int argc, const char **argv)
  : m_argc(0),
    m_argv(NULL),
    m_argv_orig(NULL)
{
  m_started = false;
  m_id = m_highest_id.inc();
  memset(&m_pthread, 0, sizeof(m_pthread));
  update_id_str(false);
  set_argv(argc, argv);
}

SysTestRunnable::
~SysTestRunnable()
{
  set_argv(0, NULL);
}

const char* SysTestRunnable::
get_id_str(void) const
{
  return m_id_str;
}

int SysTestRunnable::
start()
{
  if (m_started) {
    return -EDOM;
  }
  int ret;
  bool use_threads = SysTestSettings::inst().use_threads();
  if (use_threads) {
    ret = pthread_create(&m_pthread, NULL, systest_runnable_pthread_helper,
			     static_cast<void*>(this));
    if (ret)
      return ret;
    m_started = true;
  } else {
    std::string err_msg;
    ret = preforker.prefork(err_msg);
    if (ret < 0)
      preforker.exit(ret);

    if (preforker.is_child()) {
      m_started = true;
      void *retptr = systest_runnable_pthread_helper(static_cast<void*>(this));
      preforker.exit((int)(uintptr_t)retptr);
    } else {
      m_started = true;
    }
  }
  return 0;
}

std::string SysTestRunnable::
join()
{
  if (!m_started) {
    return "SysTestRunnable was never started.";
  }
  int ret;
  bool use_threads = SysTestSettings::inst().use_threads();
  if (use_threads) {
    void *ptrretval;
    ret = pthread_join(m_pthread, &ptrretval);
    if (ret) {
      ostringstream oss;
      oss << "pthread_join failed with error " << ret;
      return oss.str();
    }
    int retval = (int)(uintptr_t)ptrretval;
    if (retval != 0) {
      ostringstream oss;
      oss << "ERROR " << retval;
      return oss.str();
    }
    return "";
  } else {
    std::string err_msg;
    ret = preforker.parent_wait(err_msg);
    return err_msg;
  }
}

std::string SysTestRunnable::
run_until_finished(std::vector < SysTestRunnable * > &runnables)
{
  int index = 0;
  for (std::vector < SysTestRunnable * >::const_iterator r = runnables.begin();
      r != runnables.end(); ++r) {
    int ret = (*r)->start();
    if (ret) {
      ostringstream oss;
      oss << "run_until_finished: got error " << ret
	  << " when starting runnable " << index;
      return oss.str();
    }
    ++index;
  }

  for (std::vector < SysTestRunnable * >::const_iterator r = runnables.begin();
      r != runnables.end(); ++r) {
    std::string rstr = (*r)->join();
    if (!rstr.empty()) {
      ostringstream oss;
      oss << "run_until_finished: runnable " << (*r)->get_id_str() 
	  << ": got error: " << rstr;
      return oss.str();
    }
  }
  printf("*******************************\n");
  return "";
}

void *systest_runnable_pthread_helper(void *arg)
{
  SysTestRunnable *st = static_cast < SysTestRunnable * >(arg);
  st->update_id_str(true);
  printf("%s: starting.\n", st->get_id_str());
  int ret = st->run();
  printf("%s: shutting down.\n", st->get_id_str());
  return (void*)(uintptr_t)ret;
}

void SysTestRunnable::
update_id_str(bool started)
{
  bool use_threads = SysTestSettings::inst().use_threads();
  char extra[128];
  extra[0] = '\0';

  if (started) {
    if (use_threads)
      snprintf(extra, sizeof(extra), "_[%d]", do_gettid());
    else
      snprintf(extra, sizeof(extra), "_[%d]", getpid());
  }
  if (use_threads)
    snprintf(m_id_str, SysTestRunnable::ID_STR_SZ, "thread_%d%s", m_id, extra);
  else
    snprintf(m_id_str, SysTestRunnable::ID_STR_SZ, "process_%d%s", m_id, extra);
}

// Copy argv so that if some fiend decides to modify it, it's ok.
void SysTestRunnable::
set_argv(int argc, const char **argv)
{
  if (m_argv_orig != NULL) {
    for (int i = 0; i < m_argc; ++i)
      free((void*)(m_argv_orig[i]));
    delete[] m_argv_orig;
    m_argv_orig = NULL;
    delete[] m_argv;
    m_argv = NULL;
    m_argc = 0;
  }
  if (argv == NULL)
    return;
  m_argc = argc;
  m_argv_orig = new const char*[m_argc+1];
  for (int i = 0; i < m_argc; ++i)
    m_argv_orig[i] = strdup(argv[i]);
  m_argv_orig[argc] = NULL;
  m_argv = new const char*[m_argc+1];
  for (int i = 0; i <= m_argc; ++i)
    m_argv[i] = m_argv_orig[i];
}
