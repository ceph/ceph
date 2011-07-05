// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/

#include "include/rados/librados.h"
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

static const int RLP_NUM_OBJECTS = 50; //16384;
static const int RLP_OBJECT_SZ_MAX = 256;

static std::string get_random_buf(void)
{
  ostringstream oss;
  int size = rand() % RLP_OBJECT_SZ_MAX; // yep, it's not very random
  for (int i = 0; i < size; ++i) {
    oss << ".";
  }
  return oss.str();
}

sem_t pool_setup_sem;
sem_t modify_sem;

class RadosCreateBigPoolR : public SysTestRunnable
{
public:
  RadosCreateBigPoolR()
    : SysTestRunnable()
  {
  }

  ~RadosCreateBigPoolR()
  {
  }

  int run(void)
  {
    rados_t cl;
    RETURN_IF_NONZERO(rados_create(&cl, NULL));
    RETURN_IF_NONZERO(rados_conf_read_file(cl, NULL));
    RETURN_IF_NONZERO(rados_connect(cl));
    int ret = rados_pool_delete(cl, "foo");
    if (!((ret == 0) || (ret == -ENOENT))) {
      printf("%s: rados_pool_delete error %d\n", get_id_str(), ret);
      return ret;
    }
    RETURN_IF_NONZERO(rados_pool_create(cl, "foo"));
    rados_ioctx_t io_ctx;
    RETURN_IF_NONZERO(rados_ioctx_create(cl, "foo", &io_ctx));

    for (int i = 0; i < RLP_NUM_OBJECTS; ++i) {
      char oid[128];
      snprintf(oid, sizeof(oid), "%d.obj", i);
      std::string buf(get_random_buf());
      ret = rados_write(io_ctx, oid, buf.c_str(), buf.size(), 0);
      if (ret < static_cast<int>(buf.size())) {
	printf("%s: rados_write error %d\n", get_id_str(), ret);
	return ret;
      }
    }
    sem_post(&pool_setup_sem);
    sem_post(&pool_setup_sem);
    rados_ioctx_destroy(cl);
    return 0;
  }
};

class RadosListObjectsR : public SysTestRunnable
{
public:
  RadosListObjectsR()
    : SysTestRunnable()
  {
  }

  ~RadosListObjectsR()
  {
  }

  int run(void)
  {
    rados_t cl;
    RETURN_IF_NONZERO(rados_create(&cl, NULL));
    RETURN_IF_NONZERO(rados_conf_read_file(cl, NULL));
    RETURN_IF_NONZERO(rados_connect(cl));
    sem_wait(&pool_setup_sem);

    rados_ioctx_t io_ctx;
    RETURN_IF_NONZERO(rados_ioctx_create(cl, "foo", &io_ctx));

    int ret, saw = 0;
    const char *obj_name;
    char tmp[RLP_OBJECT_SZ_MAX];
    rados_list_ctx_t h;
    RETURN_IF_NONZERO(rados_objects_list_open(io_ctx, &h));
    while (true) {
      ret = rados_objects_list_next(h, &obj_name);
      if (ret == -ENOENT) {
	break;
      }
      else if (ret != 0) {
	printf("%s: rados_objects_list_next error: %d\n", get_id_str(), ret);
	return ret;
      }
      int len = strlen(obj_name);
      if (len > RLP_OBJECT_SZ_MAX)
	len = RLP_OBJECT_SZ_MAX;
      memcpy(tmp, obj_name, strlen(obj_name));
      ++saw;
      if (saw == RLP_NUM_OBJECTS / 2)
	sem_wait(&modify_sem);
    }
    rados_objects_list_close(h);

    printf("%s: saw %d objects\n", get_id_str(), saw);

    rados_ioctx_destroy(cl);

    return 0;
  }
};

class RadosModifyPoolR : public SysTestRunnable
{
public:
  RadosModifyPoolR()
    : SysTestRunnable()
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
    RETURN_IF_NONZERO(rados_conf_read_file(cl, NULL));
    RETURN_IF_NONZERO(rados_connect(cl));
    sem_wait(&pool_setup_sem);

    rados_ioctx_t io_ctx;
    RETURN_IF_NONZERO(rados_ioctx_create(cl, "foo", &io_ctx));

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
	sem_post(&modify_sem);
    }

    printf("%s: removed %d objects\n", get_id_str(), removed);

    rados_ioctx_destroy(cl);

    return 0;
  }
};

int main(int argc, const char **argv)
{
  sem_init(&pool_setup_sem, 1, 0);
  sem_init(&modify_sem, 1, 0);

  RadosCreateBigPoolR r1;
  RadosListObjectsR r2;
  RadosModifyPoolR r3;
  vector < SysTestRunnable* > vec;
  vec.push_back(&r1);
  vec.push_back(&r2);
  vec.push_back(&r3);
  std::string error = SysTestRunnable::run_until_finished(vec);
  if (!error.empty()) {
    printf("got error: %s\n", error.c_str());
    return EXIT_FAILURE;
  }

  printf("******* SUCCESS **********\n"); 
  return EXIT_SUCCESS;
}
