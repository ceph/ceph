// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

//#define BOOST_ASIO_ENABLE_HANDLER_TRACKING

#include "rgw/rgw_dmclock_sync_scheduler.h"
#include "rgw/rgw_dmclock_async_scheduler.h"

#include <optional>
#include <boost/asio/spawn.hpp>
#include <gtest/gtest.h>
#include "acconfig.h"
#include "global/global_context.h"

namespace rgw::dmclock {

using boost::system::error_code;

// return a lambda that can be used as a callback to capture its arguments
auto capture(std::optional<error_code>& opt_ec,
             std::optional<PhaseType>& opt_phase)
{
  return [&] (error_code ec, PhaseType phase) {
    opt_ec = ec;
    opt_phase = phase;
  };
}

TEST(Queue, SyncRequest)
{
  ClientCounters counters(g_ceph_context);
  auto client_info_f = [] (client_id client) -> ClientInfo* {
                         static ClientInfo clients[] = {
                                                        {1, 1, 1}, //admin: satisfy by reservation
                                                        {0, 1, 1}, //auth: satisfy by priority
                         };
                         return &clients[static_cast<size_t>(client)];
                       };
  std::atomic <bool> ready = false;
  auto server_ready_f = [&ready]() -> bool { return ready.load();};

  SyncScheduler queue(g_ceph_context, std::ref(counters),
		      client_info_f, server_ready_f,
		      std::ref(SyncScheduler::handle_request_cb)
		      );


  auto now = get_time();
  ready = true;
  queue.add_request(client_id::admin, {}, now, 1);
  queue.add_request(client_id::auth, {}, now, 1);

  // We can't see the queue at length 1 as the queue len is decremented as the
  //request is processed
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_cancel));
}

#ifdef HAVE_BOOST_CONTEXT
TEST(Queue, RateLimit)
{
  boost::asio::io_context context;
  ClientCounters counters(g_ceph_context);
  AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                  [] (client_id client) -> ClientInfo* {
      static ClientInfo clients[] = {
        {1, 1, 1}, // admin
        {0, 1, 1}, // auth
      };
      return &clients[static_cast<size_t>(client)];
    }, AtLimit::Reject);

  std::optional<error_code> ec1, ec2, ec3, ec4;
  std::optional<PhaseType> p1, p2, p3, p4;

  auto now = get_time();
  queue.async_request(client_id::admin, {}, now, 1, capture(ec1, p1));
  queue.async_request(client_id::admin, {}, now, 1, capture(ec2, p2));
  queue.async_request(client_id::auth, {}, now, 1, capture(ec3, p3));
  queue.async_request(client_id::auth, {}, now, 1, capture(ec4, p4));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);
  EXPECT_FALSE(ec4);

  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(p1);
  EXPECT_EQ(PhaseType::reservation, *p1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::resource_unavailable_try_again, *ec2);

  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::system::errc::success, *ec3);
  ASSERT_TRUE(p3);
  EXPECT_EQ(PhaseType::priority, *p3);

  ASSERT_TRUE(ec4);
  EXPECT_EQ(boost::system::errc::resource_unavailable_try_again, *ec4);

  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_cancel));
}

TEST(Queue, AsyncRequest)
{
  boost::asio::io_context context;
  ClientCounters counters(g_ceph_context);
  AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                  [] (client_id client) -> ClientInfo* {
      static ClientInfo clients[] = {
        {1, 1, 1}, // admin: satisfy by reservation
        {0, 1, 1}, // auth: satisfy by priority
      };
      return &clients[static_cast<size_t>(client)];
		  }, AtLimit::Reject
		  );

  std::optional<error_code> ec1, ec2;
  std::optional<PhaseType> p1, p2;

  auto now = get_time();
  queue.async_request(client_id::admin, {}, now, 1, capture(ec1, p1));
  queue.async_request(client_id::auth, {}, now, 1, capture(ec2, p2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(p1);
  EXPECT_EQ(PhaseType::reservation, *p1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(PhaseType::priority, *p2);

  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_cancel));
}


TEST(Queue, Cancel)
{
  boost::asio::io_context context;
  ClientCounters counters(g_ceph_context);
  AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                  [] (client_id client) -> ClientInfo* {
      static ClientInfo info{0, 1, 1};
      return &info;
    });

  std::optional<error_code> ec1, ec2;
  std::optional<PhaseType> p1, p2;

  auto now = get_time();
  queue.async_request(client_id::admin, {}, now, 1, capture(ec1, p1));
  queue.async_request(client_id::auth, {}, now, 1, capture(ec2, p2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));

  queue.cancel();

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);

  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_cancel));
}

TEST(Queue, CancelClient)
{
  boost::asio::io_context context;
  ClientCounters counters(g_ceph_context);
  AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                  [] (client_id client) -> ClientInfo* {
      static ClientInfo info{0, 1, 1};
      return &info;
    });

  std::optional<error_code> ec1, ec2;
  std::optional<PhaseType> p1, p2;

  auto now = get_time();
  queue.async_request(client_id::admin, {}, now, 1, capture(ec1, p1));
  queue.async_request(client_id::auth, {}, now, 1, capture(ec2, p2));
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));

  queue.cancel(client_id::admin);

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(PhaseType::priority, *p2);

  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_cancel));
}

TEST(Queue, CancelOnDestructor)
{
  boost::asio::io_context context;

  std::optional<error_code> ec1, ec2;
  std::optional<PhaseType> p1, p2;

  ClientCounters counters(g_ceph_context);
  {
    AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                    [] (client_id client) -> ClientInfo* {
        static ClientInfo info{0, 1, 1};
        return &info;
      });

    auto now = get_time();
    queue.async_request(client_id::admin, {}, now, 1, capture(ec1, p1));
    queue.async_request(client_id::auth, {}, now, 1, capture(ec2, p2));

    EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
    EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));
  }

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);

  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::admin)->get(queue_counters::l_limit));
  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_cancel));

  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_qlen));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_res));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_prio));
  EXPECT_EQ(0u, counters(client_id::auth)->get(queue_counters::l_limit));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_cancel));
}

// return a lambda from capture() that's bound to run on the given executor
template <typename Executor>
auto capture(const Executor& ex, std::optional<error_code>& opt_ec,
             std::optional<PhaseType>& opt_res)
{
  return boost::asio::bind_executor(ex, capture(opt_ec, opt_res));
}

TEST(Queue, CrossExecutorRequest)
{
  boost::asio::io_context queue_context;
  ClientCounters counters(g_ceph_context);
  AsyncScheduler queue(g_ceph_context, queue_context, std::ref(counters), nullptr,
                  [] (client_id client) -> ClientInfo* {
      static ClientInfo info{0, 1, 1};
      return &info;
    });

  // create a separate execution context to use for all callbacks to test that
  // pending requests maintain executor work guards on both executors
  boost::asio::io_context callback_context;
  auto ex2 = callback_context.get_executor();

  std::optional<error_code> ec1, ec2;
  std::optional<PhaseType> p1, p2;

  auto now = get_time();
  queue.async_request(client_id::admin, {}, now, 1, capture(ex2, ec1, p1));
  queue.async_request(client_id::auth, {}, now, 1, capture(ex2, ec2, p2));

  EXPECT_EQ(1u, counters(client_id::admin)->get(queue_counters::l_qlen));
  EXPECT_EQ(1u, counters(client_id::auth)->get(queue_counters::l_qlen));

  callback_context.poll();
  // maintains work on callback executor while in queue
  EXPECT_FALSE(callback_context.stopped());

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);

  queue_context.poll();
  EXPECT_TRUE(queue_context.stopped());

  EXPECT_FALSE(ec1); // no callbacks until callback executor runs
  EXPECT_FALSE(ec2);

  callback_context.poll();
  EXPECT_TRUE(callback_context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(p1);
  EXPECT_EQ(PhaseType::priority, *p1);

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(p2);
  EXPECT_EQ(PhaseType::priority, *p2);
}

TEST(Queue, SpawnAsyncRequest)
{
  boost::asio::io_context context;

  boost::asio::spawn(context, [&] (boost::asio::yield_context yield) {
    ClientCounters counters(g_ceph_context);
    AsyncScheduler queue(g_ceph_context, context, std::ref(counters), nullptr,
                    [] (client_id client) -> ClientInfo* {
        static ClientInfo clients[] = {
          {1, 1, 1}, // admin: satisfy by reservation
          {0, 1, 1}, // auth: satisfy by priority
        };
        return &clients[static_cast<size_t>(client)];
      });

    error_code ec1, ec2;
    auto p1 = queue.async_request(client_id::admin, {}, get_time(), 1, yield[ec1]);
    EXPECT_EQ(boost::system::errc::success, ec1);
    EXPECT_EQ(PhaseType::reservation, p1);

    auto p2 = queue.async_request(client_id::auth, {}, get_time(), 1, yield[ec2]);
    EXPECT_EQ(boost::system::errc::success, ec2);
    EXPECT_EQ(PhaseType::priority, p2);
  });

  context.poll();
  EXPECT_TRUE(context.stopped());
}

#endif

} // namespace rgw::dmclock
