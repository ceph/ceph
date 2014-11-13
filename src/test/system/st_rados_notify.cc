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
#include "st_rados_notify.h"
#include "systest_runnable.h"

StRadosNotify::StRadosNotify(int argc, const char **argv,
			     CrossProcessSem *setup_sem,
			     CrossProcessSem *notify_sem,
			     CrossProcessSem *notified_sem,
			     int notify_retcode,
			     const std::string &pool_name,
			     const std::string &obj_name)
  : SysTestRunnable(argc, argv),
    m_setup_sem(setup_sem),
    m_notify_sem(notify_sem),
    m_notified_sem(notified_sem),
    m_notify_retcode(notify_retcode),
    m_pool_name(pool_name),
    m_obj_name(obj_name)
{
}

StRadosNotify::~StRadosNotify()
{
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

int StRadosNotify::run()
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
  RETURN1_IF_NONZERO(rados_connect(cl));
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));

  if (m_notify_sem) {
    m_notify_sem->wait();
    m_notify_sem->post();
  }

  printf("%s: notifying object %s\n", get_id_str(), m_obj_name.c_str());
  RETURN1_IF_NOT_VAL(m_notify_retcode,
		     rados_notify(io_ctx, m_obj_name.c_str(), 0, NULL, 0));
  if (m_notified_sem) {
    m_notified_sem->post();
  }

  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cl);

  return 0;
}

#pragma GCC diagnostic pop
