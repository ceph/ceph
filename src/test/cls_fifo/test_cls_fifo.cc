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
#include "gtest/gtest.h"

using namespace librados;

#include "cls/fifo/cls_fifo_client.h"

using namespace rados::cls::fifo;

#if 0
void lock_info(IoCtx *ioctx, string& oid, string& name, map<locker_id_t, locker_info_t>& lockers,
	       ClsLockType *assert_type, string *assert_tag)
{
  ClsLockType lock_type = LOCK_NONE;
  string tag;
  lockers.clear();
  ASSERT_EQ(0, get_lock_info(ioctx, oid, name, &lockers, &lock_type, &tag));
  cout << "lock: " << name << std::endl;
  cout << "  lock_type: " << cls_lock_type_str(lock_type) << std::endl;
  cout << "  tag: " << tag << std::endl;
  cout << "  lockers:" << std::endl;

  if (assert_type) {
    ASSERT_EQ(*assert_type, lock_type);
  }

  if (assert_tag) {
    ASSERT_EQ(*assert_tag, tag);
  }

  map<locker_id_t, locker_info_t>::iterator liter;
  for (liter = lockers.begin(); liter != lockers.end(); ++liter) {
    const locker_id_t& locker = liter->first;
    cout << "    " << locker.locker << " expiration=" << liter->second.expiration
         << " addr=" << liter->second.addr << " cookie=" << locker.cookie << std::endl;
  }
}

void lock_info(IoCtx *ioctx, string& oid, string& name, map<locker_id_t, locker_info_t>& lockers)
{
  lock_info(ioctx, oid, name, lockers, NULL, NULL);
}
#endif

TEST(ClsFIFO, TestCreate) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ObjectWriteOperation op;

  ASSERT_EQ(-EINVAL, FIFO::create(&op,
                                  FIFO::CreateParams()));

  string fifo_id = "fifo";

  ASSERT_EQ(-EINVAL, FIFO::create(&op,
                                  FIFO::CreateParams()
                                  .id("fifo")));
  ASSERT_EQ(0, FIFO::create(&op,
               FIFO::CreateParams()
               .id("fifo")
               .pool(pool_name)));

  string oid = fifo_id;
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  uint64_t size;
  struct timespec ts;
  ASSERT_EQ(0, ioctx.stat2(oid, &size, &ts));
  ASSERT_GT(size, 0);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

