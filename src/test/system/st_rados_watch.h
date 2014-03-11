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

#ifndef TEST_SYSTEM_ST_RADOS_WATCH_H
#define TEST_SYSTEM_ST_RADOS_WATCH_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_watch
 *
 * 1. waits on setup_sem
 * 2. posts to setup_sem
 * 3. watches an object
 * 4. posts to watch_sem
 * 5. waits on notify_sem
 * 6. posts to notify_sem
 * 7. checks that the correct number of notifies were received
 */
class StRadosWatch : public SysTestRunnable
{
public:
  StRadosWatch(int argc, const char **argv,
	       CrossProcessSem *setup_sem,
	       CrossProcessSem *watch_sem,
	       CrossProcessSem *notify_sem,
	       int num_notifies,
	       int watch_retcode,
	       const std::string &pool_name,
	       const std::string &obj_name);
  ~StRadosWatch();
  virtual int run();
private:
  CrossProcessSem *m_setup_sem;
  CrossProcessSem *m_watch_sem;
  CrossProcessSem *m_notify_sem;
  int m_num_notifies;
  int m_watch_retcode;
  std::string m_pool_name;
  std::string m_obj_name;
};

#endif
