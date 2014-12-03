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
#include "st_rados_list_objects.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <string>

using std::ostringstream;

StRadosListObjects::
StRadosListObjects(int argc, const char **argv,
		   const std::string &pool_name,
		   bool accept_list_errors,
		   int midway_cnt,
		   CrossProcessSem *pool_setup_sem,
		   CrossProcessSem *midway_sem_wait,
		   CrossProcessSem *midway_sem_post)
  : SysTestRunnable(argc, argv),
    m_accept_list_errors(accept_list_errors),
    m_midway_cnt(midway_cnt),
    m_pool_setup_sem(pool_setup_sem),
    m_midway_sem_wait(midway_sem_wait),
    m_midway_sem_post(midway_sem_post)
{
}

StRadosListObjects::
~StRadosListObjects()
{
}

int StRadosListObjects::
run()
{
  int retval = 0;
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  rados_conf_parse_env(cl, NULL);
  RETURN1_IF_NONZERO(rados_connect(cl));
  m_pool_setup_sem->wait();
  m_pool_setup_sem->post();

  rados_ioctx_t io_ctx;
  rados_pool_create(cl, "foo");
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, "foo", &io_ctx));

  int saw = 0;
  const char *obj_name;
  rados_list_ctx_t h;
  printf("%s: listing objects.\n", get_id_str());
  RETURN1_IF_NONZERO(rados_nobjects_list_open(io_ctx, &h));
  while (true) {
    int ret = rados_nobjects_list_next(h, &obj_name, NULL, NULL);
    if (ret == -ENOENT) {
      break;
    }
    else if (ret != 0) {
      if (m_accept_list_errors && (!m_midway_sem_post || saw > m_midway_cnt))
	break;
      printf("%s: rados_objects_list_next error: %d\n", get_id_str(), ret);
      retval = ret;
      goto out;
    }
    if ((saw % 25) == 0) {
      printf("%s: listed object %d...\n", get_id_str(), saw);
    }
    ++saw;
    if (saw == m_midway_cnt) {
      if (m_midway_sem_wait)
	m_midway_sem_wait->wait();
      if (m_midway_sem_post)
	m_midway_sem_post->post();
    }
  }

  printf("%s: saw %d objects\n", get_id_str(), saw);

out:
  rados_nobjects_list_close(h);
  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cl);

  return retval;
}
