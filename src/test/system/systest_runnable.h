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

#ifndef CEPH_SYSTEM_TEST_H
#define CEPH_SYSTEM_TEST_H

#include <pthread.h>
#include <stdio.h>
#include <string>
#include <vector>

#include "common/Preforker.h"

#define RETURN1_IF_NOT_VAL(expected, expr) \
  do {\
    int _rinv_ret = expr;\
    if (_rinv_ret != expected) {\
      printf("%s: file %s, line %d: expected %d, got %d\n",\
	     get_id_str(), __FILE__, __LINE__, expected, _rinv_ret);\
      return 1; \
    }\
  } while(0);

#define RETURN1_IF_NONZERO(expr) \
  RETURN1_IF_NOT_VAL(0, expr)

extern void* systest_runnable_pthread_helper(void *arg);
std::string get_temp_pool_name(const char* prefix);
/* Represents a single test thread / process.
 *
 * Inherit from this class and implement the test body in run().
*/
class SysTestRunnable
{
public:
  static const int ID_STR_SZ = 128;

  SysTestRunnable(int argc, const char **argv);
  virtual ~SysTestRunnable();

  /* Returns 0 on success; error code otherwise. */
  virtual int run() = 0;

  /* Return a string identifying the runnable. */ 
  const char* get_id_str(void) const;

  /* Start the Runnable */
  int start();

  /* Wait until the Runnable is finished. Returns an error string on failure. */
  std::string join();

  /* Starts a bunch of SystemTestRunnables and waits until they're done.
   *
   * Returns an error string on failure. */
  static std::string run_until_finished(std::vector < SysTestRunnable * >&
					runnables);

protected:
  int m_argc;
  const char **m_argv;

private:
  explicit SysTestRunnable(const SysTestRunnable &rhs);
  SysTestRunnable& operator=(const SysTestRunnable &rhs);
  void update_id_str(bool started);
  void set_argv(int argc, const char **argv);

  friend void* systest_runnable_pthread_helper(void *arg);

  Preforker preforker;
  const char **m_argv_orig;
  bool m_started;
  int m_id;
  pthread_t m_pthread;
  char m_id_str[ID_STR_SZ];
};

#endif
