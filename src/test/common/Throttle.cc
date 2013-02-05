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
#include "test/unit.h"

#define dout_subsys ceph_subsys_throttle

//#define TEST_DEBUG 20
#define TEST_DEBUG 0

class ThrottleTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    g_ceph_context->_conf->subsys.set_log_level(dout_subsys, TEST_DEBUG);
  }

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
  ASSERT_THROW({
      Throttle throttle(g_ceph_context, "throttle", -1);
    }, FailedAssertion);

  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_EQ(throttle.get_max(), throttle_max);
  ASSERT_EQ(throttle.get_current(), 0);
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
      if(throttle->get_or_fail(1)) {
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

TEST_F(ThrottleTest, take) {
  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_THROW(throttle.take(-1), FailedAssertion);
  ASSERT_EQ(throttle.take(throttle_max), throttle_max);
  ASSERT_EQ(throttle.take(throttle_max), throttle_max * 2);
}

TEST_F(ThrottleTest, get) {
  int64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_THROW(throttle.get(-1), FailedAssertion);
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

    if(!(waited = t.waited))
      delay *= 2;
  } while(!waited);
	  
  //
  //
  // When a get() is waiting, the others will be added to a list,
  // first come first served. The desired sequence is:
  //
  // get 5 (0 -> 5)	   
  // get_or_fail 10 failed 
  // get 10 (5 -> 15)	   
  // _wait waiting...	   
  // get 1 (5 -> 6)	   
  // _wait waiting...	   
  // put 5 (5 -> 0)	   
  // _wait finished waiting
  // put 10 (10 -> 0)	   
  // _wait finished waiting
  // put 1 (1 -> 0)        
  //
  // However, there are two threads and if the delay between actions
  // is not long enough, a race condition where get(1) succeeds before
  // get(10) has a chance to add itself to the list would be as
  // follows:
  //
  // get 5 (0 -> 5)	    
  // get_or_fail 10 failed  
  // get 10 (5 -> 15)	    
  // _wait waiting...	    
  // get 1 (5 -> 6)	    
  // put 5 (5 -> 0)	    
  // _wait finished waiting 
  // put 10 (10 -> 0)	    
  // put 1 (1 -> 0)         
  //
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

    if(!(waited = t.waited && u.waited))
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
  Throttle throttle(g_ceph_context, "throttle", throttle_max);

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
    // the pending condition will be signaled because
    // the "throttle_max - 1" is lower than the current
    // maximum. 
    // 
    // the condition tries to set a value that is larger than the
    // maximum and it succeeds because the current value is lower
    // than the maximum
    //
    throttle.wait(throttle_max - 1);
    //
    // this would block:
    // throttle.wait(throttle_max * 100);
    // because the pending condition is only signaled by _reset_max if the 
    // maximum is lowered. What is the rationale ?
    // 
    usleep(delay);
    ASSERT_EQ(throttle.get_current(), throttle_max / 2);


    t.join();

    if(!(waited = t.waited))
      delay *= 2;
  } while(!waited);
	  
}
