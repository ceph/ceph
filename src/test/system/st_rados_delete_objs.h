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

#ifndef TEST_SYSTEM_ST_RADOS_DELETE_OBJS_H
#define TEST_SYSTEM_ST_RADOS_DELETE_OBJS_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_delete_objs
 *
 * Waits on setup_sem, posts to it,
 * deletes num_objs objects from the pool,
 * and posts to deleted_sem.
 */
class StRadosDeleteObjs : public SysTestRunnable
{
public:
  StRadosDeleteObjs(int argc, const char **argv,
		    CrossProcessSem *setup_sem,
		    CrossProcessSem *deleted_sem,
		    int num_objs,
		    const std::string &pool_name,
		    const std::string &suffix);
  ~StRadosDeleteObjs();
  virtual int run();
private:
  CrossProcessSem *m_setup_sem;
  CrossProcessSem *m_deleted_sem;
  int m_num_objs;
  std::string m_pool_name;
  std::string m_suffix;
};

#endif
