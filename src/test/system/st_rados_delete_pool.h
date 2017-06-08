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

#ifndef TEST_SYSTEM_ST_RADOS_DELETE_POOL_H
#define TEST_SYSTEM_ST_RADOS_DELETE_POOL_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_delete_pool
 *
 * Waits on pool_setup_sem, posts to it,
 * deletes a pool, and posts to delete_pool_sem.
 */
class StRadosDeletePool : public SysTestRunnable
{
public:
  StRadosDeletePool(int argc, const char **argv,
		    CrossProcessSem *pool_setup_sem,
		    CrossProcessSem *delete_pool_sem,
		    const std::string &pool_name);
  ~StRadosDeletePool();
  virtual int run();
private:
  CrossProcessSem *m_pool_setup_sem;
  CrossProcessSem *m_delete_pool_sem;
  std::string m_pool_name;
};

#endif
