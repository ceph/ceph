// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "global/global_context.h"
#include "librbd/asio/AsioContextWQ.h"
#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include <atomic>

namespace librbd {
namespace {

TEST(AsioContextWQ, PostRunsOnIoContext) {
  boost::asio::io_context io_context;
  asio::AsioContextWQ wq(g_ceph_context, io_context);
  std::atomic<bool> done{false};
  wq.post([&]() { done = true; });
  io_context.poll();
  ASSERT_TRUE(done.load());
}

TEST(AsioContextWQ, PostSerialSerializes) {
  boost::asio::io_context io_context;
  asio::AsioContextWQ wq(g_ceph_context, io_context);
  int step = 0;
  wq.post_serial([&]() {
    ASSERT_EQ(0, step);
    step = 1;
  });
  wq.post_serial([&]() {
    ASSERT_EQ(1, step);
    step = 2;
  });
  io_context.poll();
  ASSERT_EQ(2, step);
}

} // namespace
} // namespace librbd
