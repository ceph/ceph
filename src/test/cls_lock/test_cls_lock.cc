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

#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "msg/msg_types.h"
#include "include/rados/librados.hpp"

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

using namespace librados;

#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_ops.h"

using namespace rados::cls::lock;

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

TEST(ClsLock, TestMultiLocking) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  ClsLockType lock_type_shared = LOCK_SHARED;
  ClsLockType lock_type_exclusive = LOCK_EXCLUSIVE;


  Rados cluster2;
  IoCtx ioctx2;
  ASSERT_EQ("", connect_cluster_pp(cluster2));
  cluster2.ioctx_create(pool_name.c_str(), ioctx2);

  string oid = "foo";
  bufferlist bl;
  string lock_name = "mylock";

  ASSERT_EQ(0, ioctx.write(oid, bl, bl.length(), 0));

  Lock l(lock_name);
  // we set the duration, so the log output contains a locker with a
  // non-zero expiration time
  l.set_duration(utime_t(120, 0));

  /* test lock object */

  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  /* test exclusive lock */
  ASSERT_EQ(-EEXIST, l.lock_exclusive(&ioctx, oid));

  /* test idempotency */
  l.set_may_renew(true);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  l.set_may_renew(false);

  /* test second client */
  Lock l2(lock_name);
  ASSERT_EQ(-EBUSY, l2.lock_exclusive(&ioctx2, oid));
  ASSERT_EQ(-EBUSY, l2.lock_shared(&ioctx2, oid));

  list<string> locks;
  ASSERT_EQ(0, list_locks(&ioctx, oid, &locks));

  ASSERT_EQ(1, (int)locks.size());
  list<string>::iterator iter = locks.begin();
  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid, *iter, lockers, &lock_type_exclusive, NULL);

  ASSERT_EQ(1, (int)lockers.size());

  /* test unlock */
  ASSERT_EQ(0, l.unlock(&ioctx, oid));
  locks.clear();
  ASSERT_EQ(0, list_locks(&ioctx, oid, &locks));

  /* test shared lock */
  ASSERT_EQ(0, l2.lock_shared(&ioctx2, oid));
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid));

  locks.clear();
  ASSERT_EQ(0, list_locks(&ioctx, oid, &locks));
  ASSERT_EQ(1, (int)locks.size());
  iter = locks.begin();
  lock_info(&ioctx, oid, *iter, lockers, &lock_type_shared, NULL);
  ASSERT_EQ(2, (int)lockers.size());

  /* test break locks */
  entity_name_t name = entity_name_t::CLIENT(cluster.get_instance_id());
  entity_name_t name2 = entity_name_t::CLIENT(cluster2.get_instance_id());

  l2.break_lock(&ioctx2, oid, name);
  lock_info(&ioctx, oid, *iter, lockers);
  ASSERT_EQ(1, (int)lockers.size());
  map<locker_id_t, locker_info_t>::iterator liter = lockers.begin();
  const locker_id_t& id = liter->first;
  ASSERT_EQ(name2, id.locker);

  /* test lock tag */
  Lock l_tag(lock_name);
  l_tag.set_tag("non-default tag");
  ASSERT_EQ(-EBUSY, l_tag.lock_shared(&ioctx, oid));


  /* test modify description */
  string description = "new description";
  l.set_description(description);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestMeta) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);


  Rados cluster2;
  IoCtx ioctx2;
  ASSERT_EQ("", connect_cluster_pp(cluster2));
  cluster2.ioctx_create(pool_name.c_str(), ioctx2);

  string oid = "foo";
  bufferlist bl;
  string lock_name = "mylock";

  ASSERT_EQ(0, ioctx.write(oid, bl, bl.length(), 0));

  Lock l(lock_name);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid));

  /* test lock tag */
  Lock l_tag(lock_name);
  l_tag.set_tag("non-default tag");
  ASSERT_EQ(-EBUSY, l_tag.lock_shared(&ioctx2, oid));


  ASSERT_EQ(0, l.unlock(&ioctx, oid));

  /* test description */
  Lock l2(lock_name);
  string description = "new description";
  l2.set_description(description);
  ASSERT_EQ(0, l2.lock_shared(&ioctx2, oid));

  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid, lock_name, lockers, NULL, NULL);
  ASSERT_EQ(1, (int)lockers.size());

  map<locker_id_t, locker_info_t>::iterator iter = lockers.begin();
  locker_info_t locker = iter->second;
  ASSERT_EQ("new description", locker.description);

  ASSERT_EQ(0, l2.unlock(&ioctx2, oid));

  /* check new tag */
  string new_tag = "new_tag";
  l.set_tag(new_tag);
  l.set_may_renew(true);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));
  lock_info(&ioctx, oid, lock_name, lockers, NULL, &new_tag);
  ASSERT_EQ(1, (int)lockers.size());
  l.set_tag("");
  ASSERT_EQ(-EBUSY, l.lock_exclusive(&ioctx, oid));
  l.set_tag(new_tag);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestCookie) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string oid = "foo";
  string lock_name = "mylock";
  Lock l(lock_name);

  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  /* new cookie */
  string cookie = "new cookie";
  l.set_cookie(cookie);
  ASSERT_EQ(-EBUSY, l.lock_exclusive(&ioctx, oid));
  ASSERT_EQ(-ENOENT, l.unlock(&ioctx, oid));
  l.set_cookie("");
  ASSERT_EQ(0, l.unlock(&ioctx, oid));

  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid, lock_name, lockers);
  ASSERT_EQ(0, (int)lockers.size());

  l.set_cookie(cookie);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid));
  l.set_cookie("");
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid));

  lock_info(&ioctx, oid, lock_name, lockers);
  ASSERT_EQ(2, (int)lockers.size());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestMultipleLocks) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string oid = "foo";
  Lock l("lock1");
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  Lock l2("lock2");
  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid));

  list<string> locks;
  ASSERT_EQ(0, list_locks(&ioctx, oid, &locks));

  ASSERT_EQ(2, (int)locks.size());

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestLockDuration) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string oid = "foo";
  Lock l("lock");
  utime_t dur(5, 0);
  l.set_duration(dur);
  utime_t start = ceph_clock_now();
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));
  int r = l.lock_exclusive(&ioctx, oid);
  if (r == 0) {
    // it's possible to get success if we were just really slow...
    ASSERT_TRUE(ceph_clock_now() > start + dur);
  } else {
    ASSERT_EQ(-EEXIST, r);
  }

  sleep(dur.sec());
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestAssertLocked) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string oid = "foo";
  Lock l("lock1");
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid));

  librados::ObjectWriteOperation op1;
  l.assert_locked_exclusive(&op1);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  l.assert_locked_shared(&op2);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op2));

  l.set_tag("tag");
  librados::ObjectWriteOperation op3;
  l.assert_locked_exclusive(&op3);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op3));
  l.set_tag("");

  l.set_cookie("cookie");
  librados::ObjectWriteOperation op4;
  l.assert_locked_exclusive(&op4);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op4));
  l.set_cookie("");

  ASSERT_EQ(0, l.unlock(&ioctx, oid));

  librados::ObjectWriteOperation op5;
  l.assert_locked_exclusive(&op5);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op5));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestSetCookie) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  string oid = "foo";
  string name = "name";
  string tag = "tag";
  string cookie = "cookie";
  string new_cookie = "new cookie";
  librados::ObjectWriteOperation op1;
  set_cookie(&op1, name, LOCK_SHARED, cookie, tag, new_cookie);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  lock(&op2, name, LOCK_SHARED, cookie, tag, "", utime_t{}, 0);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  librados::ObjectWriteOperation op3;
  lock(&op3, name, LOCK_SHARED, "cookie 2", tag, "", utime_t{}, 0);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));

  librados::ObjectWriteOperation op4;
  set_cookie(&op4, name, LOCK_SHARED, cookie, tag, cookie);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op4));

  librados::ObjectWriteOperation op5;
  set_cookie(&op5, name, LOCK_SHARED, cookie, "wrong tag", new_cookie);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op5));

  librados::ObjectWriteOperation op6;
  set_cookie(&op6, name, LOCK_SHARED, "wrong cookie", tag, new_cookie);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op6));

  librados::ObjectWriteOperation op7;
  set_cookie(&op7, name, LOCK_EXCLUSIVE, cookie, tag, new_cookie);
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op7));

  librados::ObjectWriteOperation op8;
  set_cookie(&op8, name, LOCK_SHARED, cookie, tag, "cookie 2");
  ASSERT_EQ(-EBUSY, ioctx.operate(oid, &op8));

  librados::ObjectWriteOperation op9;
  set_cookie(&op9, name, LOCK_SHARED, cookie, tag, new_cookie);
  ASSERT_EQ(0, ioctx.operate(oid, &op9));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestRenew) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist bl;

  string oid1 = "foo1";
  string lock_name1 = "mylock1";

  ASSERT_EQ(0, ioctx.write(oid1, bl, bl.length(), 0));

  Lock l1(lock_name1);
  utime_t lock_duration1(5, 0);
  l1.set_duration(lock_duration1);

  ASSERT_EQ(0, l1.lock_exclusive(&ioctx, oid1));
  l1.set_may_renew(true);
  sleep(2);
  ASSERT_EQ(0, l1.lock_exclusive(&ioctx, oid1));
  sleep(7);
  ASSERT_EQ(0, l1.lock_exclusive(&ioctx, oid1)) <<
    "when a cls_lock is set to may_renew, a relock after expiration "
    "should still work";
  ASSERT_EQ(0, l1.unlock(&ioctx, oid1));

  // ***********************************************

  string oid2 = "foo2";
  string lock_name2 = "mylock2";

  ASSERT_EQ(0, ioctx.write(oid2, bl, bl.length(), 0));

  Lock l2(lock_name2);
  utime_t lock_duration2(5, 0);
  l2.set_duration(lock_duration2);

  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid2));
  l2.set_must_renew(true);
  sleep(2);
  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid2));
  sleep(7);
  ASSERT_EQ(-ENOENT, l2.lock_exclusive(&ioctx, oid2)) <<
    "when a cls_lock is set to must_renew, a relock after expiration "
    "should fail";
  ASSERT_EQ(-ENOENT, l2.unlock(&ioctx, oid2));

  // ***********************************************

  string oid3 = "foo3";
  string lock_name3 = "mylock3";

  ASSERT_EQ(0, ioctx.write(oid3, bl, bl.length(), 0));

  Lock l3(lock_name3);
  l3.set_duration(utime_t(5, 0));
  l3.set_must_renew(true);

  ASSERT_EQ(-ENOENT, l3.lock_exclusive(&ioctx, oid3)) <<
    "unable to create a lock with must_renew";

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsLock, TestExclusiveEphemeralBasic) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist bl;

  string oid1 = "foo1";
  string oid2 = "foo2";
  string lock_name1 = "mylock1";
  string lock_name2 = "mylock2";

  Lock l1(lock_name1);
  l1.set_duration(utime_t(5, 0));

  uint64_t size;
  time_t mod_time;

  l1.set_may_renew(true);
  ASSERT_EQ(0, l1.lock_exclusive_ephemeral(&ioctx, oid1));
  ASSERT_EQ(0, ioctx.stat(oid1, &size, &mod_time));
  sleep(2);
  ASSERT_EQ(0, l1.unlock(&ioctx, oid1));
  ASSERT_EQ(-ENOENT, ioctx.stat(oid1, &size, &mod_time));

  // ***********************************************

  Lock l2(lock_name2);
  utime_t lock_duration2(5, 0);
  l2.set_duration(utime_t(5, 0));

  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid2));
  ASSERT_EQ(0, ioctx.stat(oid2, &size, &mod_time));
  sleep(2);
  ASSERT_EQ(0, l2.unlock(&ioctx, oid2));
  ASSERT_EQ(0, ioctx.stat(oid2, &size, &mod_time));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}


TEST(ClsLock, TestExclusiveEphemeralStealEphemeral) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist bl;

  string oid1 = "foo1";
  string lock_name1 = "mylock1";

  Lock l1(lock_name1);
  l1.set_duration(utime_t(3, 0));

  ASSERT_EQ(0, l1.lock_exclusive_ephemeral(&ioctx, oid1));
  sleep(4);

  // l1 is expired, l2 can take; l2 is also exclusive_ephemeral
  Lock l2(lock_name1);
  l2.set_duration(utime_t(3, 0));
  ASSERT_EQ(0, l2.lock_exclusive_ephemeral(&ioctx, oid1));
  sleep(1);
  ASSERT_EQ(0, l2.unlock(&ioctx, oid1));

  // l2 cannot unlock its expired lock
  ASSERT_EQ(-ENOENT, l1.unlock(&ioctx, oid1));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}


TEST(ClsLock, TestExclusiveEphemeralStealExclusive) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist bl;

  string oid1 = "foo1";
  string lock_name1 = "mylock1";

  Lock l1(lock_name1);
  l1.set_duration(utime_t(3, 0));

  ASSERT_EQ(0, l1.lock_exclusive_ephemeral(&ioctx, oid1));
  sleep(4);

  // l1 is expired, l2 can take; l2 is exclusive (but not ephemeral)
  Lock l2(lock_name1);
  l2.set_duration(utime_t(3, 0));
  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid1));
  sleep(1);
  ASSERT_EQ(0, l2.unlock(&ioctx, oid1));

  // l2 cannot unlock its expired lock
  ASSERT_EQ(-ENOENT, l1.unlock(&ioctx, oid1));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
