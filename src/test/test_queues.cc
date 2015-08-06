// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <iostream>
#include <thread>
#include "gtest/gtest.h"
#include "include/mpmc-bounded-queue.hpp"
#include "include/mpmc-slock-queue.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

namespace {

  static constexpr int32_t COUNT = 100000000;

  template<typename T>
    void consumer_func(T* queue)
    {
      size_t count = COUNT;
      size_t value = 0;

      while (count > 0) {
        if (queue->dequeue(value)) {
	  --count;
        }
      }
    }

  template<typename T>
    void bounded_producer_func(T* queue)
    {
      size_t count = COUNT;
      while (count > 0) {
        if (queue->enqueue(count)) {
	  --count;
        }
      }
    }

  template<typename T>
    void producer_func(T* queue)
    {
      for (int count = 0; count < COUNT; ++count) {
        queue->enqueue(count);
      }
    }

  template<typename T>
    long double run_test(T producer_func,
			 T consumer_func)
    {
      typedef std::chrono::high_resolution_clock clock_t;
      typedef std::chrono::time_point<clock_t> time_t;
      time_t start;
      time_t end;

      start = clock_t::now();
      std::thread producer(producer_func);
      std::thread consumer(consumer_func);

      producer.join();
      consumer.join();
      end = clock_t::now();

      return
        (end - start).count()
        * ((double) std::chrono::high_resolution_clock::period::num
           / std::chrono::high_resolution_clock::period::den);
    } /* run_test */

  /* Objects (pass by address) */
  struct Object {
  public:
    int ix;
  Object(int _ix) : ix(_ix) {}
  };

  std::vector<Object> ovec;

  template<typename T>
    void obj_consumer_func(T* queue)
    {
      size_t count = COUNT;
      Object *obj = nullptr;

      while (count > 0) {
        if (queue->dequeue(obj)) {
	  --count;
        }
      }
    }

  template<typename T>
    void obj_producer_func(T* queue)
    {
      size_t count = COUNT;
      while (count > 0) {
        if (queue->enqueue(&(ovec[count]))) {
	  --count;
        }
      }
    }

  namespace sq = ceph::slock_queue;
  typedef sq::Object<size_t> SObject;
  typedef typename sq::Queue<size_t> slock_queue_t;
  std::vector<SObject> ovec2;

  void slock_consumer_func(slock_queue_t* queue)
  {
    size_t count = COUNT;
    SObject* obj;

    while (count > 0) {
      obj = queue->dequeue();
      if (obj) {
	--count;
      }
    }
  }

  void slock_producer_func(slock_queue_t* queue)
  {
    size_t count = COUNT;
    while (count > 0) {
      if (queue->enqueue(&(ovec2[count]))) {
	--count;
      }
    }
  }

} /* namespace */

TEST(QUEUES, MPMC_BOUNDED_64K)
{
  typedef mpmc_bounded_queue_t<size_t> queue_t;
  queue_t queue(65536);
  long double seconds =
    run_test(std::bind(&bounded_producer_func<queue_t>, &queue),
	     std::bind(&consumer_func<queue_t>, &queue));

  std::cout << "MPMC bound queue completed "
	    << COUNT
	    << " iterations in "
	    << seconds
	    << " seconds. "
	    << ((long double) COUNT / seconds) / 1000000
	    << " million enqueue/dequeue pairs per second."
	    << std::endl;
}

TEST(QUEUES, MPMC_BOUNDED_1024)
{
  typedef mpmc_bounded_queue_t<size_t> queue_t;
  queue_t queue(1024);
  long double seconds =
    run_test(std::bind(&bounded_producer_func<queue_t>, &queue),
	     std::bind(&consumer_func<queue_t>, &queue));

  std::cout << "MPMC bound queue completed "
	    << COUNT
	    << " iterations in "
	    << seconds
	    << " seconds. "
	    << ((long double) COUNT / seconds) / 1000000
	    << " million enqueue/dequeue pairs per second."
	    << std::endl;
}

TEST(QUEUES, MAKE_OBJECTS_1024)
{
  ovec.reserve(COUNT); /* yikes */
  for (int ix = 0; ix < COUNT; ++ix) {
    ovec.emplace_back(Object(ix));
  }
}

TEST(QUEUES, MPMC_OBJECT_1024)
{
  typedef mpmc_bounded_queue_t<Object*> queue_t;
  queue_t queue(1024);
  long double seconds =
    run_test(std::bind(&obj_producer_func<queue_t>, &queue),
	     std::bind(&obj_consumer_func<queue_t>, &queue));

  std::cout << "MPMC bound queue completed "
	    << COUNT
	    << " iterations in "
	    << seconds
	    << " seconds. "
	    << ((long double) COUNT / seconds) / 1000000
	    << " million enqueue/dequeue pairs per second."
	    << std::endl;
}

TEST(QUEUES, OVCLEAR)
{
  ovec.clear();
}

TEST(QUEUES, MAKE_SLOCK_OBJECTS)
{
  ovec2.reserve(COUNT); /* yikes */
  for (int ix = 0; ix < COUNT; ++ix) {
    ovec2.emplace_back(SObject(ix));
  }
}

TEST(QUEUES, MPMC_SLOCK_UNBOUNDED)
{
  slock_queue_t queue;
  long double seconds =
    run_test(std::bind(&slock_producer_func, &queue),
	     std::bind(&slock_consumer_func, &queue));

  std::cout << "MPMC slock unbounded queue completed "
	    << COUNT
	    << " iterations in "
	    << seconds
	    << " seconds. "
	    << ((long double) COUNT / seconds) / 1000000
	    << " million enqueue/dequeue pairs per second."
	    << std::endl;
}

TEST(QUEUES, OV2CLEAR)
{
  ovec2.clear();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
