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

#ifndef TEST_SYSTEM_ST_RADOS_CREATE_POOL_H
#define TEST_SYSTEM_ST_RADOS_CREATE_POOL_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_create_pool
 *
 * Waits, then posts to setup_sem.
 * Creates a pool and populates it with some objects.
 * Then, calls pool_setup_sem->post()
 */
class StRadosCreatePool : public SysTestRunnable
{
public:
  static std::string get_random_buf(int sz);
  StRadosCreatePool(int argc, const char **argv,
		    CrossProcessSem *setup_sem,
		    CrossProcessSem *pool_setup_sem,
		    CrossProcessSem *close_create_pool_sem,
		    const std::string &pool_name,
		    int num_objects,
		    const std::string &suffix);
  ~StRadosCreatePool();
  virtual int run();
private:
  CrossProcessSem *m_setup_sem;
  CrossProcessSem *m_pool_setup_sem;
  CrossProcessSem *m_close_create_pool;
  std::string m_pool_name;
  int m_num_objects;
  std::string m_suffix;
};

std::string get_temp_pool_name(const char* prefix);

#endif
