// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <atomic>

#include <gtest/gtest.h>

#include "include/Context.h"

#include "common/ceph_time.h"
#include "common/Cond.h"
#include "common/Finisher.h"

TEST(UpDown, Finisher) {
  auto cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  Finisher f(cct);
  f.start();
  f.stop();
  cct->put();
}

TEST(Fire, Finisher) {
  auto cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  Finisher f(cct);
  C_SaferCond c;
  f.start();
  f.queue(&c);
  c.wait();
  f.stop();
  cct->put();
}

TEST(Bench, Finisher) {
  using namespace std::literals;
  auto cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  Finisher f(cct);
  std::vector<C_SaferCond> cs(1'000'000);
  f.start();
  auto begin = ceph::coarse_mono_clock::now();
  for (auto& c : cs) f.queue(&c);
  for (auto& c : cs) c.wait();
  auto end = ceph::coarse_mono_clock::now();
  f.stop();
  cct->put();
  std::cout << "Queued and completed 1,000,000 C_SaferCond objects in " << end - begin
	    << std::endl;
}


TEST(FireFuction, Finisher) {
  std::atomic<bool> fired = false;
  auto cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  Finisher f(cct);
  f.start();
  ASSERT_FALSE(fired);
  f.queue([&fired]() { fired = true; });
  std::this_thread::sleep_for(5s);
  ASSERT_TRUE(fired);
  f.stop();
  cct->put();
}
