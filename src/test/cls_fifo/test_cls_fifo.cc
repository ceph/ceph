// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "gtest/gtest.h"

using namespace librados;

#include "cls/fifo/cls_fifo_client.h"


using namespace rados::cls::fifo;

static int fifo_create(IoCtx& ioctx,
                       const string& oid,
                       const ClsFIFO::MetaCreateParams& params)
{
  ObjectWriteOperation op;

  int r = ClsFIFO::meta_create(&op, params);
  if (r < 0) {
    return r;
  }

  return ioctx.operate(oid, &op);
}

TEST(ClsFIFO, TestCreate) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";
  string oid = fifo_id;

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid,
                                  ClsFIFO::MetaCreateParams()));

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid,
                     ClsFIFO::MetaCreateParams()
                     .id(fifo_id)
                     .max_part_size(0)));

  ASSERT_EQ(-EINVAL, fifo_create(ioctx, oid,
                     ClsFIFO::MetaCreateParams()
                     .id(fifo_id)
                     .max_entry_size(0)));
  
  /* first successful create */
  ASSERT_EQ(0, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id(fifo_id)));

  uint64_t size;
  struct timespec ts;
  ASSERT_EQ(0, ioctx.stat2(oid, &size, &ts));
  ASSERT_GT(size, 0);

  /* test idempotency */
  ASSERT_EQ(0, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id(fifo_id)));

  uint64_t size2;
  struct timespec ts2;
  ASSERT_EQ(0, ioctx.stat2(oid, &size2, &ts2));
  ASSERT_EQ(size2, size);

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id(fifo_id)
               .exclusive(true)));

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id(fifo_id)
               .oid_prefix("myprefix")
               .exclusive(false)));

  ASSERT_EQ(-EEXIST, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id("foo")
               .exclusive(false)));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsFIFO, TestGetInfo) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string fifo_id = "fifo";
  string oid = fifo_id;

  fifo_info_t info;

  /* first successful create */
  ASSERT_EQ(0, fifo_create(ioctx, oid,
               ClsFIFO::MetaCreateParams()
               .id(fifo_id)));

  ASSERT_EQ(0, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams(), &info));

  ASSERT_TRUE(!info.objv.instance.empty());

  ASSERT_EQ(0, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams()
               .objv(info.objv),
               &info));

  fifo_objv_t objv;
  objv.instance="foo";
  objv.ver = 12;
  ASSERT_EQ(-ECANCELED, ClsFIFO::meta_get(ioctx, oid,
               ClsFIFO::MetaGetParams()
               .objv(objv),
               &info));

}

