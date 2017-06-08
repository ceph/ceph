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

#ifndef TEST_SYSTEM_ST_RADOS_NOTIFY_H
#define TEST_SYSTEM_ST_RADOS_NOTIFY_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_notify
 *
 * 1. waits on and then posts to setup_sem
 * 2. connects and opens the pool
 * 3. waits on and then posts to notify_sem
 * 4. notifies on the object
 * 5. posts to notified_sem
 */
class StRadosNotify : public SysTestRunnable
{
public:
  StRadosNotify(int argc, const char **argv,
		CrossProcessSem *setup_sem,
		CrossProcessSem *notify_sem,
		CrossProcessSem *notified_sem,
		int notify_retcode,
		const std::string &pool_name,
		const std::string &obj_name);
  ~StRadosNotify();
  virtual int run();
private:
  CrossProcessSem *m_setup_sem;
  CrossProcessSem *m_notify_sem;
  CrossProcessSem *m_notified_sem;
  int m_notify_retcode;
  std::string m_pool_name;
  std::string m_obj_name;
};

#endif
