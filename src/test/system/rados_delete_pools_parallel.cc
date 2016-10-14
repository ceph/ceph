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
#include "st_rados_delete_pool.h"
#include "st_rados_list_objects.h"
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

static int g_num_objects = 50;

/*
 * rados_delete_pools_parallel
 *
 * This tests creation and deletion races.
 *
 * EXPECT:            * can delete a pool while another user is using it
 *                    * operations on pools return error codes after the pools
 *                      are deleted
 *
 * DO NOT EXPECT      * hangs, crashes
 */

const char *get_id_str()
{
  return "main";
}

int main(int argc, const char **argv)
{
  const char *num_objects = getenv("NUM_OBJECTS");
  const std::string pool = get_temp_pool_name(argv[0]);
  if (num_objects) {
    g_num_objects = atoi(num_objects); 
    if (g_num_objects == 0)
      return 100;
  }

  CrossProcessSem *pool_setup_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &pool_setup_sem));
  CrossProcessSem *delete_pool_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &delete_pool_sem));

  // first test: create a pool, then delete that pool
  {
    StRadosCreatePool r1(argc, argv, NULL, pool_setup_sem, NULL,
			 pool, 50, ".obj");
    StRadosDeletePool r2(argc, argv, pool_setup_sem, NULL, pool);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test1: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // second test: create a pool, the list objects in that pool while it's
  // being deleted.
  RETURN1_IF_NONZERO(pool_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(delete_pool_sem->reinit(0));
  {
    StRadosCreatePool r1(argc, argv, NULL, pool_setup_sem, NULL,
			 pool, g_num_objects, ".obj");
    StRadosDeletePool r2(argc, argv, delete_pool_sem, NULL, pool);
    StRadosListObjects r3(argc, argv, pool, true, g_num_objects / 2,
			  pool_setup_sem, NULL, delete_pool_sem);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test2: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  printf("******* SUCCESS **********\n"); 
  return EXIT_SUCCESS;
}
