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
#include "st_rados_create_pool.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <string>

using std::ostringstream;

std::string StRadosCreatePool::
get_random_buf(int sz)
{
  ostringstream oss;
  int size = rand() % sz; // yep, it's not very random
  for (int i = 0; i < size; ++i) {
    oss << ".";
  }
  return oss.str();
}

StRadosCreatePool::
StRadosCreatePool(int argc, const char **argv,
		  CrossProcessSem *setup_sem,
		  CrossProcessSem *pool_setup_sem,
		  CrossProcessSem *close_create_pool,
		  const std::string &pool_name,
		  int num_objects,
		  const std::string &suffix)
  : SysTestRunnable(argc, argv),
    m_setup_sem(setup_sem),
    m_pool_setup_sem(pool_setup_sem),
    m_close_create_pool(close_create_pool),
    m_pool_name(pool_name),
    m_num_objects(num_objects),
    m_suffix(suffix)
{
}

StRadosCreatePool::
~StRadosCreatePool()
{
}

int StRadosCreatePool::
run()
{
  int ret_val = 0;
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  std::string log_name = SysTestSettings::inst().get_log_name(get_id_str());
  if (!log_name.empty())
    rados_conf_set(cl, "log_file", log_name.c_str());
  rados_conf_parse_env(cl, NULL);

  if (m_setup_sem) {
    m_setup_sem->wait();
    m_setup_sem->post();
  }

  RETURN1_IF_NONZERO(rados_connect(cl));

  printf("%s: creating pool %s\n", get_id_str(), m_pool_name.c_str());
  rados_pool_create(cl, m_pool_name.c_str());
  rados_ioctx_t io_ctx;
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));

  for (int i = 0; i < m_num_objects; ++i) {
    char oid[128];
    snprintf(oid, sizeof(oid), "%d%s", i, m_suffix.c_str());
    std::string buf(get_random_buf(256));
    int ret = rados_write(io_ctx, oid, buf.c_str(), buf.size(), 0);
    if (ret != 0) {
      printf("%s: rados_write(%s) failed with error: %d\n",
	     get_id_str(), oid, ret);
      ret_val = ret;
      goto out;
    }
    if (((i % 25) == 0) || (i == m_num_objects - 1)) {
      printf("%s: created object %d...\n", get_id_str(), i);
    }
  }

out:
  printf("%s: finishing.\n", get_id_str());
  if (m_pool_setup_sem)
    m_pool_setup_sem->post();
  if (m_close_create_pool)
    m_close_create_pool->wait();
  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cl);
  return ret_val;
}
