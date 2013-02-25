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

#ifndef TEST_SYSTEM_ST_RADOS_LIST_OBJECTS_H
#define TEST_SYSTEM_ST_RADOS_LIST_OBJECTS_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_list_objects
 *
 * 1. calls pool_setup_sem->wait()
 * 2. calls pool_setup_sem->post()
 * 3. list some objects
 * 4. modify_sem->wait()
 * 5. list some objects
 */
class StRadosListObjects : public SysTestRunnable
{
public:
  static std::string get_random_buf(int sz);
  StRadosListObjects(int argc, const char **argv,
		     const std::string &pool_name,
		     bool accept_list_errors,
		     int midway_cnt,
		     CrossProcessSem *pool_setup_sem,
		     CrossProcessSem *midway_sem_wait,
		     CrossProcessSem *midway_sem_post);
  ~StRadosListObjects();
  virtual int run();
private:
  std::string m_pool_name;
  bool m_accept_list_errors;
  int m_midway_cnt;
  CrossProcessSem *m_pool_setup_sem;
  CrossProcessSem *m_midway_sem_wait;
  CrossProcessSem *m_midway_sem_post;
};

#endif
