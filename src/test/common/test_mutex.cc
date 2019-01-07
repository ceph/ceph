// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 */

#include <common/Mutex.h>
#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "include/coredumpctl.h"

static CephContext* cct;

static void do_init() {
  if (cct == nullptr) {
    cct = new CephContext(0);
    lockdep_register_ceph_context(cct);
  }
}

static void disable_lockdep() {
  if (cct) {
    lockdep_unregister_ceph_context(cct);
    cct->put();
    cct = nullptr;
  }
}

TEST(Mutex, NormalAsserts) {
  Mutex* m = new Mutex("Normal",false);
  m->Lock();
  testing::GTEST_FLAG(death_test_style) = "threadsafe";
  PrCtl unset_dumpable;
  EXPECT_DEATH(m->Lock(), ".*");
}

TEST(Mutex, RecursiveWithLockdep) {
  do_init();
  Mutex* m = new Mutex("Recursive1",true);
  m->Lock();
  m->Lock();
  m->Unlock();
  m->Unlock();
  delete m;
}

TEST(Mutex, RecursiveWithoutLockdep) {
  disable_lockdep();
  Mutex* m = new Mutex("Recursive2",true);
  m->Lock();
  m->Lock();
  m->Unlock();
  m->Unlock();
  delete m;
}

TEST(Mutex, DeleteLocked) {
  Mutex* m = new Mutex("Recursive3",false);
  m->Lock();
  PrCtl unset_dumpable;
  EXPECT_DEATH(delete m,".*");
}
