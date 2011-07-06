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

static const int RLP_NUM_OBJECTS = 50;

static CrossProcessSem *pool_setup_sem = NULL;
static CrossProcessSem *modify_sem = NULL;

/* Rados doesn't have read-after-write consistency for pool creation events.
 * What this means is that even after the first process has created the pool,
 * we may have to wait a while before we're able to see it. We will be able to
 * see it when a new OSDMap arrives.
 */
static int do_ioctx_create(const char *id_str, rados_t &cl,
			   const char *pool_name, rados_ioctx_t &io_ctx)
{
  int ret = rados_pool_create(cl, "foo");
  if (ret != -EEXIST) {
    return ret;
  }
  ret = rados_ioctx_create(cl, "foo", &io_ctx);
  return ret;
}

class RadosListObjectsR : public SysTestRunnable
{
public:
  RadosListObjectsR(int argc, const char **argv)
    : SysTestRunnable(argc, argv)
  {
  }

  ~RadosListObjectsR()
  {
  }

  int run()
  {
    rados_t cl;
    RETURN_IF_NONZERO(rados_create(&cl, NULL));
    rados_conf_parse_argv(cl, m_argc, m_argv);
    RETURN_IF_NONZERO(rados_conf_read_file(cl, NULL));
    RETURN_IF_NONZERO(rados_connect(cl));
    pool_setup_sem->wait();

    rados_ioctx_t io_ctx;
    printf("%s: do_ioctx_create.\n", get_id_str());
    RETURN_IF_NONZERO(do_ioctx_create(get_id_str(), cl, "foo", io_ctx));

//    int ret, saw = 0;
//    const char *obj_name;
//    char tmp[RLP_OBJECT_SZ_MAX];
    rados_list_ctx_t h;
//    printf("%s: listing objects.\n", get_id_str());
    RETURN_IF_NONZERO(rados_objects_list_open(io_ctx, &h));
//    while (true) {
//      ret = rados_objects_list_next(h, &obj_name);
//      if (ret == -ENOENT) {
//	break;
//      }
//      else if (ret != 0) {
//	printf("%s: rados_objects_list_next error: %d\n", get_id_str(), ret);
//	return ret;
//      }
//      printf("%s: listed an object!\n", get_id_str());
//      int len = strlen(obj_name);
//      if (len > RLP_OBJECT_SZ_MAX)
//	len = RLP_OBJECT_SZ_MAX;
//      memcpy(tmp, obj_name, strlen(obj_name));
//      printf("%s: listing object '%s'\n", get_id_str(), obj_name);
//      ++saw;
////      if (saw == RLP_NUM_OBJECTS / 2)
////	modify_sem->wait();
//    }
    rados_objects_list_close(h);

    //printf("%s: saw %d objects\n", get_id_str(), saw);

    rados_ioctx_destroy(cl);

    return 0;
  }
};

class RadosModifyPoolR : public SysTestRunnable
{
public:
  RadosModifyPoolR(int argc, const char **argv)
    : SysTestRunnable(argc, argv)
  {
  }

  ~RadosModifyPoolR()
  {
  }

  int run(void)
  {
    int ret;
    rados_t cl;
    RETURN_IF_NONZERO(rados_create(&cl, NULL));
    rados_conf_parse_argv(cl, m_argc, m_argv);
    RETURN_IF_NONZERO(rados_conf_read_file(cl, NULL));
    RETURN_IF_NONZERO(rados_connect(cl));
    pool_setup_sem->wait();

    rados_ioctx_t io_ctx;
    RETURN_IF_NONZERO(do_ioctx_create(get_id_str(), cl, "foo", io_ctx));

    std::vector <std::string> to_delete;
    for (int i = 0; i < RLP_NUM_OBJECTS; ++i) {
      char oid[128];
      snprintf(oid, sizeof(oid), "%d.obj", i);
      to_delete.push_back(oid);
    }

    int removed = 0;
    while (true) {
      if (to_delete.empty())
	break;
      int r = rand() % to_delete.size();
      std::string oid(to_delete[r]);
      ret = rados_remove(io_ctx, oid.c_str());
      if (ret != 0) {
	printf("%s: rados_remove(%s) failed with error %d\n",
	       get_id_str(), oid.c_str(), ret);
	return ret;
      }
      ++removed;
      if (removed == RLP_NUM_OBJECTS / 2)
	modify_sem->post();
    }

    printf("%s: removed %d objects\n", get_id_str(), removed);

    rados_ioctx_destroy(cl);

    return 0;
  }
};

const char *get_id_str()
{
  return "main";
}

int main(int argc, const char **argv)
{
  RETURN_IF_NONZERO(CrossProcessSem::create(0, &pool_setup_sem));
  RETURN_IF_NONZERO(CrossProcessSem::create(1, &modify_sem));

  StRadosCreatePool r1(argc, argv, pool_setup_sem, NULL, RLP_NUM_OBJECTS);
  RadosListObjectsR r2(argc, argv);
  RadosModifyPoolR r3(argc, argv);
  vector < SysTestRunnable* > vec;
  vec.push_back(&r1);
  vec.push_back(&r2);
  //vec.push_back(&r3);
  std::string error = SysTestRunnable::run_until_finished(vec);
  if (!error.empty()) {
    printf("got error: %s\n", error.c_str());
    return EXIT_FAILURE;
  }

  printf("******* SUCCESS **********\n"); 
  return EXIT_SUCCESS;
}
