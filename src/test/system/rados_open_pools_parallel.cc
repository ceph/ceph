
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
#include <pthread.h>
#include <semaphore.h>
#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>

using std::ostringstream;
using std::string;
using std::vector;

/*
 * rados_open_pools_parallel
 *
 * This tests creating a pool in one Runnable, and then opening an io context
 * based on that pool in another.
 *
 * EXPECT:            * can't create the same pool twice
 *                    * one Runnable can use the pool after the other one creates it
 *
 * DO NOT EXPECT      * hangs, crashes
 */
class StRadosOpenPool : public SysTestRunnable
{
public:
  StRadosOpenPool(int argc, const char **argv,
                  CrossProcessSem *pool_setup_sem,
                  CrossProcessSem *open_pool_sem,
                  const std::string& pool_name)
    : SysTestRunnable(argc, argv),
      m_pool_setup_sem(pool_setup_sem),
      m_open_pool_sem(open_pool_sem),
      m_pool_name(pool_name)
  {
  }

  ~StRadosOpenPool()
  {
  }

  int run()
  {
    rados_t cl;
    RETURN1_IF_NONZERO(rados_create(&cl, NULL));
    rados_conf_parse_argv(cl, m_argc, m_argv);
    std::string log_name = SysTestSettings::inst().get_log_name(get_id_str());
    if (!log_name.empty())
      rados_conf_set(cl, "log_file", log_name.c_str());
    RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
    rados_conf_parse_env(cl, NULL);
    RETURN1_IF_NONZERO(rados_connect(cl));
    if (m_pool_setup_sem)
      m_pool_setup_sem->wait();

    printf("%s: rados_pool_create.\n", get_id_str());
    rados_pool_create(cl, m_pool_name.c_str());
    rados_ioctx_t io_ctx;
    printf("%s: rados_ioctx_create.\n", get_id_str());
    RETURN1_IF_NOT_VAL(0, rados_ioctx_create(cl, m_pool_name.c_str(), &io_ctx));
    if (m_open_pool_sem)
      m_open_pool_sem->post();
    rados_ioctx_destroy(io_ctx);
    rados_shutdown(cl);
    return 0;
  }

private:
  CrossProcessSem *m_pool_setup_sem;
  CrossProcessSem *m_open_pool_sem;
  std::string m_pool_name;
};

const char *get_id_str()
{
  return "main";
}

int main(int argc, const char **argv)
{
  const std::string pool = get_temp_pool_name(argv[0]);
  // first test: create a pool, shut down the client, access that 
  // pool in a different process.
  CrossProcessSem *pool_setup_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &pool_setup_sem));
  StRadosCreatePool r1(argc, argv, NULL, pool_setup_sem, NULL,
					   pool, 50, ".obj");
  StRadosOpenPool r2(argc, argv, pool_setup_sem, NULL, pool);
  vector < SysTestRunnable* > vec;
  vec.push_back(&r1);
  vec.push_back(&r2);
  std::string error = SysTestRunnable::run_until_finished(vec);
  if (!error.empty()) {
    printf("test1: got error: %s\n", error.c_str());
    return EXIT_FAILURE;
  }

  // second test: create a pool, access that 
  // pool in a different process, THEN shut down the first client.
  CrossProcessSem *pool_setup_sem2 = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &pool_setup_sem2));
  CrossProcessSem *open_pool_sem2 = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &open_pool_sem2));
  StRadosCreatePool r3(argc, argv, NULL, pool_setup_sem2, open_pool_sem2,
					   pool, 50, ".obj");
  StRadosOpenPool r4(argc, argv, pool_setup_sem2, open_pool_sem2, pool);
  vector < SysTestRunnable* > vec2;
  vec2.push_back(&r3);
  vec2.push_back(&r4);
  error = SysTestRunnable::run_until_finished(vec2);
  if (!error.empty()) {
    printf("test2: got error: %s\n", error.c_str());
    return EXIT_FAILURE;
  }

  printf("******* SUCCESS **********\n"); 
  return EXIT_SUCCESS;
}
