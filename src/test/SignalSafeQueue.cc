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

#include "common/errno.h"
#include "common/SignalSafeQueue.h"
#include "gtest/gtest.h"

#include <errno.h>
#include <iostream>
#include <pthread.h>

using std::cout;

void *SUCCESS_RET = 0;
void *ERROR_RET = (void*)-1;

static void *dthread1(void *v)
{
  SignalSafeQueue *ssq = (SignalSafeQueue*)v;
  int item;
  int ret = ssq->pop((void*)&item);
  if (ret) {
    cout << "pop returned " << ret << "(" << cpp_strerror(ret) << std::endl;
    pthread_exit(ERROR_RET);
  }
  if (item != 123) {
    cout << "expected 123, got " << item << std::endl;
    pthread_exit(ERROR_RET);
  }
  pthread_exit(SUCCESS_RET);
}

TEST(EnqueueDequeue, Test1) {
  int i, ret;
  void *thread_ret;
  SignalSafeQueue *ssq = SignalSafeQueue::create_queue();
  ret = ssq->init(sizeof(int));
  ASSERT_EQ(ret, 0);

  pthread_t t1;
  pthread_create(&t1, NULL, dthread1, (void*)ssq);

  i = 123;
  ssq->push((void*)&i);

  ret = pthread_join(t1, &thread_ret);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(thread_ret, SUCCESS_RET);

  delete ssq;
}

static int test2_total = 0;

void increment_test2_total(int amount)
{
  static pthread_mutex_t test2_lock = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_lock(&test2_lock);
  test2_total += amount;
  pthread_mutex_unlock(&test2_lock);
}

static void *dthread2(void *v)
{
  SignalSafeQueue *ssq = (SignalSafeQueue*)v;
  int item;
  int ret = ssq->pop((void*)&item);
  if (ret) {
    cout << "pop returned " << ret << "(" << cpp_strerror(ret) << std::endl;
    pthread_exit(ERROR_RET);
  }
  increment_test2_total(item);

  ret = ssq->pop((void*)&item);
  if (ret) {
    cout << "pop returned " << ret << "(" << cpp_strerror(ret) << std::endl;
    pthread_exit(ERROR_RET);
  }
  increment_test2_total(item);

  pthread_exit(SUCCESS_RET);
}

TEST(EnqueueDequeue, Test2) {
  int i, ret;
  void *thread_ret;
  SignalSafeQueue *ssq = SignalSafeQueue::create_queue();
  ret = ssq->init(sizeof(int));
  ASSERT_EQ(ret, 0);

  pthread_t t2_threadA, t2_threadB;
  pthread_create(&t2_threadA, NULL, dthread2, (void*)ssq);
  pthread_create(&t2_threadB, NULL, dthread2, (void*)ssq);

  i = 50;
  ssq->push((void*)&i);
  i = 100;
  ssq->push((void*)&i);
  i = 0;
  ssq->push((void*)&i);
  i = 50;
  ssq->push((void*)&i);

  ret = pthread_join(t2_threadA, &thread_ret);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(thread_ret, SUCCESS_RET);

  ret = pthread_join(t2_threadB, &thread_ret);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(thread_ret, SUCCESS_RET);

  ASSERT_EQ(test2_total, 200);

  delete ssq;
}

static pthread_mutex_t shutdown_test_lock = PTHREAD_MUTEX_INITIALIZER;

void *shutdown_thread(void *v)
{
  int ret, i;
  SignalSafeQueue *ssq = (SignalSafeQueue*)v;

  ret = ssq->pop((void*)&i);
  if (ret != 0) {
    printf("shutdown_thread: failed to pop the first element off the queue. "
	   "Error %d\n", ret);
    pthread_exit(ERROR_RET);
  }
  if (i != 456) {
    printf("shutdown_thread: unexpected value for first element. "
	   "Got %d, Expected %d\n", i, 456);
    pthread_exit(ERROR_RET);
  }

  pthread_mutex_lock(&shutdown_test_lock);
  // block until the parent has finished shutting down the queue
  pthread_mutex_unlock(&shutdown_test_lock);

  ret = ssq->pop((void*)&i);
  if (ret == EPIPE) {
    pthread_exit(SUCCESS_RET);
  }
  printf("shutdown_thread: expected to get EPIPE, but got return code %d\n",
	 ret);
  pthread_exit(ERROR_RET);
}

TEST(ShutdownTest, ShutdownTest1) {
  int i, ret;
  void *thread_ret;
  SignalSafeQueue *ssq = SignalSafeQueue::create_queue();
  ret = ssq->init(sizeof(int));
  ASSERT_EQ(ret, 0);

  pthread_mutex_lock(&shutdown_test_lock);
  pthread_t s_thread;
  pthread_create(&s_thread, NULL, shutdown_thread, (void*)ssq);

  // send 456
  i = 456;
  ssq->push((void*)&i);

  // shutdown
  ssq->wake_readers_and_shutdown();

  pthread_mutex_unlock(&shutdown_test_lock);

  ret = pthread_join(s_thread, &thread_ret);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(thread_ret, SUCCESS_RET);

  delete ssq;
}
