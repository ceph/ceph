// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/Throttle.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

class ThrottleTest : public ::testing::Test {
protected:

  class Thread_get : public Thread {
  public:
    Throttle &throttle;
    int64_t count;
    bool waited;

    Thread_get(Throttle& _throttle, int64_t _count) :
      throttle(_throttle),
      count(_count),
      waited(false)
    {
    }

    virtual void *entry() {
      waited = throttle.get(count);
      throttle.put(count);
      return NULL;
    }
  };

};

TEST_F(ThrottleTest, Throttle) {
  ASSERT_DEATH({
      Throttle throttle(g_ceph_context, "throttle", -1);
    }, "");

  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_EQ(throttle.get_max(), throttle_max);
  ASSERT_EQ(throttle.get_current(), 0);
}

TEST_F(ThrottleTest, take) {
  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_DEATH(throttle.take(-1), "");
  ASSERT_EQ(throttle.take(throttle_max), throttle_max);
  ASSERT_EQ(throttle.take(throttle_max), throttle_max * 2);
}

TEST_F(ThrottleTest, get) {
  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle");

  // test increasing max from 0 to throttle_max
  {
    ASSERT_FALSE(throttle.get(throttle_max, throttle_max));
    ASSERT_EQ(throttle.get_max(), throttle_max);
    ASSERT_EQ(throttle.put(throttle_max), 0);
  }

  ASSERT_DEATH(throttle.get(-1), "");
  ASSERT_FALSE(throttle.get(5));
  ASSERT_EQ(throttle.put(5), 0);

  ASSERT_FALSE(throttle.get(throttle_max));
  ASSERT_FALSE(throttle.get_or_fail(1));
  ASSERT_FALSE(throttle.get(1, throttle_max + 1));
  ASSERT_EQ(throttle.put(throttle_max + 1), 0);
  ASSERT_FALSE(throttle.get(0, throttle_max));
  ASSERT_FALSE(throttle.get(throttle_max));
  ASSERT_FALSE(throttle.get_or_fail(1));
  ASSERT_EQ(throttle.put(throttle_max), 0);

  useconds_t delay = 1;

  bool waited;

  do {
    cout << "Trying (1) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, 7);
    t.create();
    usleep(delay);
    ASSERT_EQ(throttle.put(throttle_max), 0);
    t.join();

    if (!(waited = t.waited))
      delay *= 2;
  } while(!waited);

  do {
    cout << "Trying (2) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max / 2));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, throttle_max);
    t.create();
    usleep(delay);

    Thread_get u(throttle, 1);
    u.create();
    usleep(delay);

    throttle.put(throttle_max / 2);

    t.join();
    u.join();

    if (!(waited = t.waited && u.waited))
      delay *= 2;
  } while(!waited);

}

TEST_F(ThrottleTest, get_or_fail) {
  {
    Throttle throttle(g_ceph_context, "throttle");

    ASSERT_TRUE(throttle.get_or_fail(5));
    ASSERT_TRUE(throttle.get_or_fail(5));
  }

  {
    int64_t throttle_max = 10;
    Throttle throttle(g_ceph_context, "throttle", throttle_max);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max));
    ASSERT_EQ(throttle.put(throttle_max), 0);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max * 2));
    ASSERT_FALSE(throttle.get_or_fail(1));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max * 2));
    ASSERT_EQ(throttle.put(throttle_max * 2), 0);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max));
    ASSERT_FALSE(throttle.get_or_fail(1));
    ASSERT_EQ(throttle.put(throttle_max), 0);
  }
}

TEST_F(ThrottleTest, wait) {
  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle");

  // test increasing max from 0 to throttle_max
  {
    ASSERT_FALSE(throttle.wait(throttle_max));
    ASSERT_EQ(throttle.get_max(), throttle_max);
  }

  useconds_t delay = 1;

  bool waited;

  do {
    cout << "Trying (3) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max / 2));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, throttle_max);
    t.create();
    usleep(delay);

    //
    // Throttle::_reset_max(int64_t m) used to contain a test
    // that blocked the following statement, only if
    // the argument was greater than throttle_max.
    // Although a value lower than throttle_max would cover
    // the same code in _reset_max, the throttle_max * 100
    // value is left here to demonstrate that the problem
    // has been solved.
    //
    throttle.wait(throttle_max * 100);
    usleep(delay);
    t.join();
    ASSERT_EQ(throttle.get_current(), throttle_max / 2);

    if (!(waited = t.waited)) {
      delay *= 2;
      // undo the changes we made
      throttle.put(throttle_max / 2);
      throttle.wait(throttle_max);
    }
  } while(!waited);
}

TEST_F(ThrottleTest, destructor) {
  Thread_get *t;
  {
    int64_t throttle_max = 10;
    Throttle *throttle = new Throttle(g_ceph_context, "throttle", throttle_max);

    ASSERT_FALSE(throttle->get(5));

    t = new Thread_get(*throttle, 7);
    t->create();
    bool blocked;
    useconds_t delay = 1;
    do {
      usleep(delay);
      if (throttle->get_or_fail(1)) {
	throttle->put(1);
	blocked = false;
      } else {
	blocked = true;
      }
      delay *= 2;
    } while(!blocked);
    delete throttle;
  }

  { //
    // The thread is left hanging, otherwise it will abort().
    // Deleting the Throttle on which it is waiting creates a
    // inconsistency that will be detected: the Throttle object that
    // it references no longer exists.
    //
    pthread_t id = t->get_thread_id();
    ASSERT_EQ(pthread_kill(id, 0), 0);
    delete t;
    ASSERT_EQ(pthread_kill(id, 0), 0);
  }
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_throttle ;
 *   ./unittest_throttle # --gtest_filter=ThrottleTest.destructor \
 *       --log-to-stderr=true --debug-filestore=20
 * "
 * End:
 */

