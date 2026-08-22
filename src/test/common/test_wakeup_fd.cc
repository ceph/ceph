// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <poll.h>
#include <thread>
#include <vector>

#include "common/WakeupFd.h"
#include "gtest/gtest.h"

static bool readable(int fd, int timeout_ms)
{
  struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
  int r = TEMP_FAILURE_RETRY(::poll(&pfd, 1, timeout_ms));
  return r > 0 && (pfd.revents & POLLIN);
}

TEST(WakeupFd, Basic)
{
  WakeupFd w;
  ASSERT_GE(w.fd(), 0);
  ASSERT_FALSE(readable(w.fd(), 0));
  ASSERT_FALSE(w.consume());

  w.notify();
  ASSERT_TRUE(readable(w.fd(), 0));
  ASSERT_TRUE(w.consume());
  ASSERT_FALSE(readable(w.fd(), 0));
  ASSERT_FALSE(w.consume());
}

TEST(WakeupFd, Coalescing)
{
  WakeupFd w;
  for (int i = 0; i < 1000; i++) {
    w.notify();
  }
  // any number of notifies is one readable event, drained by one consume
  ASSERT_TRUE(w.consume());
  ASSERT_FALSE(w.consume());
  ASSERT_FALSE(readable(w.fd(), 0));
}

TEST(WakeupFd, StickyBeforeWait)
{
  // a notify issued before anyone waits must still wake the waiter
  WakeupFd w;
  w.notify();
  w.wait_and_consume();  // must not block
  ASSERT_FALSE(w.consume());
}

TEST(WakeupFd, WakesBlockedWaiter)
{
  WakeupFd w;
  std::thread waiter([&] {
    w.wait_and_consume();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  w.notify();
  waiter.join();
}

TEST(WakeupFd, ManyNotifiers)
{
  WakeupFd w;
  constexpr int nthreads = 8, per_thread = 10000;
  std::vector<std::thread> ts;
  for (int i = 0; i < nthreads; i++) {
    ts.emplace_back([&] {
      for (int j = 0; j < per_thread; j++) {
	w.notify();
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }
  // all notifies coalesce; the fd is readable exactly until consumed
  ASSERT_TRUE(w.consume());
  ASSERT_FALSE(w.consume());
}
