// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

#include <fcntl.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdint.h>
#include <arpa/inet.h>
#include "include/Context.h"
#include "include/atomic.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "msg/async/Event.h"

// We use epoll, kqueue, evport, select in descending order by performance.
#if defined(__linux__)
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

#ifdef HAVE_EPOLL
#include "msg/async/EventEpoll.h"
#endif
#ifdef HAVE_KQUEUE
#include "msg/async/EventKqueue.h"
#endif
#include "msg/async/EventSelect.h"
#include "test/unit.h"

#include <gtest/gtest.h>


#if GTEST_HAS_PARAM_TEST

class EventDriverTest : public ::testing::TestWithParam<const char*> {
 public:
  EventDriver *driver;

  EventDriverTest(): driver(0) {}
  virtual void SetUp() {
    cerr << __func__ << " start set up " << GetParam() << std::endl;
#ifdef HAVE_EPOLL
    if (strcmp(GetParam(), "epoll"))
      driver = new EpollDriver(g_ceph_context);
#endif
#ifdef HAVE_KQUEUE
    if (strcmp(GetParam(), "kqueue"))
      driver = new KqueueDriver(g_ceph_context);
#endif
    if (strcmp(GetParam(), "select"))
      driver = new SelectDriver(g_ceph_context);
    driver->init(100);
  }
  virtual void TearDown() {
    delete driver;
  }
};

int set_nonblock(int sd)
{
  int flags;

  /* Set the socket nonblocking.
   * Note that fcntl(2) for F_GETFL and F_SETFL can't be
   * interrupted by a signal. */
  if ((flags = fcntl(sd, F_GETFL)) < 0 ) {
    return -1;
  }
  if (fcntl(sd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return -1;
  }
  return 0;
}


TEST_P(EventDriverTest, PipeTest) {
  int fds[2];
  vector<FiredFileEvent> fired_events;
  int r;
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 1;

  r = pipe(fds);
  ASSERT_EQ(r, 0);
  r = driver->add_event(fds[0], EVENT_NONE, EVENT_READABLE);
  ASSERT_EQ(r, 0);
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 0);

  char c;
  r = write(fds[1], &c, sizeof(c));
  ASSERT_EQ(r, 1);
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(fired_events[0].fd, fds[0]);


  fired_events.clear();
  r = write(fds[1], &c, sizeof(c));
  ASSERT_EQ(r, 1);
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(fired_events[0].fd, fds[0]);

  fired_events.clear();
  driver->del_event(fds[0], EVENT_READABLE, EVENT_READABLE);
  r = write(fds[1], &c, sizeof(c));
  ASSERT_EQ(r, 1);
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 0);
}

void* echoclient(void *arg)
{
  intptr_t port = (intptr_t)arg;
  struct sockaddr_in sa;
  memset(&sa, 0, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  char addr[] = "127.0.0.1";
  int r = inet_pton(AF_INET, addr, &sa.sin_addr);

  int connect_sd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (connect_sd >= 0) {
    r = connect(connect_sd, (struct sockaddr*)&sa, sizeof(sa));
    int t = 0;
  
    do {
      char c[] = "banner";
      r = write(connect_sd, c, sizeof(c));
      char d[100];
      r = read(connect_sd, d, sizeof(d));
      if (r == 0)
        break;
      if (t++ == 30)
        break;
    } while (1);
    ::close(connect_sd);
  }
  return 0;
}

TEST_P(EventDriverTest, NetworkSocketTest) {
  int listen_sd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_TRUE(listen_sd > 0);
  int on = 1;
  int r = ::setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  ASSERT_EQ(r, 0);
  r = set_nonblock(listen_sd);
  ASSERT_EQ(r, 0);
  struct sockaddr_in sa;
  long port = 0;
  for (port = 38788; port < 40000; port++) {
    memset(&sa,0,sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_ANY);

    r = ::bind(listen_sd, (struct sockaddr *)&sa, sizeof(sa));
    if (r == 0) {
      break;
    }
  }
  ASSERT_EQ(r, 0);
  r = listen(listen_sd, 511);
  ASSERT_EQ(r, 0);

  vector<FiredFileEvent> fired_events;
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 1;
  r = driver->add_event(listen_sd, EVENT_NONE, EVENT_READABLE);
  ASSERT_EQ(r, 0);
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 0);

  fired_events.clear();
  pthread_t thread1;
  r = pthread_create(&thread1, NULL, echoclient, (void*)(intptr_t)port);
  ASSERT_EQ(r, 0);
  tv.tv_sec = 5;
  tv.tv_usec = 0;
  r = driver->event_wait(fired_events, &tv);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(fired_events[0].fd, listen_sd);

  fired_events.clear();
  int client_sd = ::accept(listen_sd, NULL, NULL);
  ASSERT_TRUE(client_sd > 0);
  r = driver->add_event(client_sd, EVENT_NONE, EVENT_READABLE);
  ASSERT_EQ(r, 0);

  do {
    fired_events.clear();
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    r = driver->event_wait(fired_events, &tv);
    ASSERT_EQ(r, 1);
    ASSERT_EQ(fired_events[0].mask, EVENT_READABLE);

    fired_events.clear();
    char data[100];
    r = ::read(client_sd, data, sizeof(data));
    if (r == 0)
      break;
    ASSERT_TRUE(r > 0);
    r = driver->add_event(client_sd, EVENT_READABLE, EVENT_WRITABLE);
    r = driver->event_wait(fired_events, &tv);
    ASSERT_EQ(r, 1);
    ASSERT_EQ(fired_events[0].mask, EVENT_WRITABLE);
    r = write(client_sd, data, strlen(data));
    ASSERT_EQ(r, (int)strlen(data));
    driver->del_event(client_sd, EVENT_READABLE|EVENT_WRITABLE,
                      EVENT_WRITABLE);
  } while (1);

  ::close(client_sd);
  ::close(listen_sd);
}

class FakeEvent : public EventCallback {

 public:
  void do_request(int fd_or_id) {}
};

TEST(EventCenterTest, FileEventExpansion) {
  vector<int> sds;
  EventCenter center(g_ceph_context);
  center.init(100);
  EventCallbackRef e(new FakeEvent());
  for (int i = 0; i < 300; i++) {
    int sd = ::socket(AF_INET, SOCK_STREAM, 0);
    center.create_file_event(sd, EVENT_READABLE, e);
    sds.push_back(sd);
  }

  for (vector<int>::iterator it = sds.begin(); it != sds.end(); ++it)
    center.delete_file_event(*it, EVENT_READABLE);
}


class Worker : public Thread {
  CephContext *cct;
  bool done;

 public:
  EventCenter center;
  explicit Worker(CephContext *c): cct(c), done(false), center(c) {
    center.init(100);
  }
  void stop() {
    done = true; 
    center.wakeup();
  }
  void* entry() {
    center.set_owner();
    while (!done)
      center.process_events(1000000);
    return 0;
  }
};

class CountEvent: public EventCallback {
  atomic_t *count;
  Mutex *lock;
  Cond *cond;

 public:
  CountEvent(atomic_t *atomic, Mutex *l, Cond *c): count(atomic), lock(l), cond(c) {}
  void do_request(int id) {
    lock->Lock();
    count->dec();
    cond->Signal();
    lock->Unlock();
  }
};

TEST(EventCenterTest, DispatchTest) {
  Worker worker1(g_ceph_context), worker2(g_ceph_context);
  atomic_t count(0);
  Mutex lock("DispatchTest::lock");
  Cond cond;
  worker1.create("worker_1");
  worker2.create("worker_2");
  for (int i = 0; i < 10000; ++i) {
    count.inc();
    worker1.center.dispatch_event_external(EventCallbackRef(new CountEvent(&count, &lock, &cond)));
    count.inc();
    worker2.center.dispatch_event_external(EventCallbackRef(new CountEvent(&count, &lock, &cond)));
    Mutex::Locker l(lock);
    while (count.read())
      cond.Wait(lock);
  }
  worker1.stop();
  worker2.stop();
}

INSTANTIATE_TEST_CASE_P(
  AsyncMessenger,
  EventDriverTest,
  ::testing::Values(
#ifdef HAVE_EPOLL
    "epoll",
#endif
#ifdef HAVE_KQUEUE
    "kqueue",
#endif
    "select"
  )
);

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_async_driver && 
 *    ./ceph_test_async_driver
 *
 * End:
 */
