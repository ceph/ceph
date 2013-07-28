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
#include "st_rados_delete_objs.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>

StRadosDeleteObjs::StRadosDeleteObjs(int argc, const char **argv,
				     CrossProcessSem *setup_sem,
				     CrossProcessSem *deleted_sem,
				     int num_objs,
				     const std::string &pool_name,
				     const std::string &suffix)
  : SysTestRunnable(argc, argv),
    m_setup_sem(setup_sem),
    m_deleted_sem(deleted_sem),
    m_num_objs(num_objs),
    m_pool_name(pool_name),
    m_suffix(suffix)
{
}

StRadosDeleteObjs::~StRadosDeleteObjs()
{
}

int StRadosDeleteObjs::run()
{
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  rados_conf_parse_env(cl, NULL);
  RETURN1_IF_NONZERO(rados_connect(cl));
  m_setup_sem->wait();
  m_setup_sem->post();

  rados_ioctx_t io_ctx;
  rados_pool_create(cl, m_pool_name.c_str());
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));

  for (int i = 0; i < m_num_objs; ++i) {
    char oid[128];
    snprintf(oid, sizeof(oid), "%d%s", i, m_suffix.c_str());
    RETURN1_IF_NONZERO(rados_remove(io_ctx, oid));
    if (((i % 25) == 0) || (i == m_num_objs - 1)) {
      printf("%s: deleted object %d...\n", get_id_str(), i);
    }
  }

  rados_ioctx_destroy(io_ctx);
  if (m_deleted_sem)
    m_deleted_sem->post();
  rados_shutdown(cl);
  return 0;
}
