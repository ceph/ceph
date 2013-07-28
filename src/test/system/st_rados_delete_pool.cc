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
#include "st_rados_delete_pool.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>

StRadosDeletePool::StRadosDeletePool(int argc, const char **argv,
				     CrossProcessSem *pool_setup_sem,
				     CrossProcessSem *delete_pool_sem,
				     const std::string &pool_name)
    : SysTestRunnable(argc, argv),
      m_pool_setup_sem(pool_setup_sem),
      m_delete_pool_sem(delete_pool_sem),
      m_pool_name(pool_name)
{
}

StRadosDeletePool::~StRadosDeletePool()
{
}

int StRadosDeletePool::run()
{
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  rados_conf_parse_env(cl, NULL);
  RETURN1_IF_NONZERO(rados_connect(cl));
  m_pool_setup_sem->wait();
  m_pool_setup_sem->post();

  rados_ioctx_t io_ctx;
  rados_pool_create(cl, m_pool_name.c_str());
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));
  rados_ioctx_destroy(io_ctx);
  printf("%s: deleting pool %s\n", get_id_str(), m_pool_name.c_str());
  RETURN1_IF_NONZERO(rados_pool_delete(cl, m_pool_name.c_str()));
  if (m_delete_pool_sem)
    m_delete_pool_sem->post();
  rados_shutdown(cl);
  return 0;
}
