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

#include "cross_process_sem.h"
#include "include/rados/librados.h"
#include "st_rados_watch.h"
#include "systest_runnable.h"

void notify_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  int *notifies = reinterpret_cast<int*>(arg);
  ++(*notifies);
}

StRadosWatch::StRadosWatch(int argc, const char **argv,
			   CrossProcessSem *setup_sem,
			   CrossProcessSem *watch_sem,
			   CrossProcessSem *notify_sem,
			   int num_notifies,
			   int watch_retcode,
			   const std::string &pool_name,
			   const std::string &obj_name)
  : SysTestRunnable(argc, argv),
    m_setup_sem(setup_sem),
    m_watch_sem(watch_sem),
    m_notify_sem(notify_sem),
    m_num_notifies(num_notifies),
    m_watch_retcode(watch_retcode),
    m_pool_name(pool_name),
    m_obj_name(obj_name)
{
}

StRadosWatch::
~StRadosWatch()
{
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

int StRadosWatch::
run()
{
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  rados_conf_parse_env(cl, NULL);

  if (m_setup_sem) {
    m_setup_sem->wait();
    m_setup_sem->post();
  }

  rados_ioctx_t io_ctx;
  uint64_t handle;
  int num_notifies = 0;
  RETURN1_IF_NONZERO(rados_connect(cl));
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));
  printf("%s: watching object %s\n", get_id_str(), m_obj_name.c_str());

  RETURN1_IF_NOT_VAL(
    rados_watch(io_ctx, m_obj_name.c_str(), 0, &handle,
		reinterpret_cast<rados_watchcb_t>(notify_cb),
		reinterpret_cast<void*>(&num_notifies)),
    m_watch_retcode
    );
  if (m_watch_sem) {
    m_watch_sem->post();
  }

  m_notify_sem->wait();
  m_notify_sem->post();

  int r = 0;
  if (num_notifies < m_num_notifies) {
    printf("Received fewer notifies than expected: %d < %d\n",
	   num_notifies, m_num_notifies);
    r = 1;
  }

  if (m_watch_retcode == 0)
    rados_unwatch(io_ctx, m_obj_name.c_str(), handle);
  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cl);

  return r;
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"
