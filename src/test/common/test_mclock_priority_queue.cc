// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <thread>
#include <chrono>
#include <iostream>
#include "gtest/gtest.h"
#include "common/mClockPriorityQueue.h"


struct Request {
  int value;
  Request() :
    value(0)
  {}
  Request(const Request& o) = default;
  explicit Request(int value) :
    value(value)
  {}
};


struct Client {
  int client_num;
  Client() :
    Client(-1)
  {}
  Client(int client_num) :
    client_num(client_num)
  {}
  friend bool operator<(const Client& r1, const Client& r2) {
    return r1.client_num < r2.client_num;
  }
  friend bool operator==(const Client& r1, const Client& r2) {
    return r1.client_num == r2.client_num;
  }
};


const crimson::dmclock::ClientInfo* client_info_func(const Client& c) {
  static const crimson::dmclock::ClientInfo
    the_info(10.0, 10.0, 10.0);
  return &the_info;
}


TEST(mClockPriorityQueue, Create)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);
}


TEST(mClockPriorityQueue, Sizes)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.get_size_slow());

  Client c1(1);
  Client c2(2);

  q.enqueue_strict(c1, 1, Request(1));
  q.enqueue_strict(c2, 2, Request(2));
  q.enqueue_strict(c1, 2, Request(3));
  q.enqueue(c2, 1, 1u, Request(4));
  q.enqueue(c1, 2, 1u, Request(5));
  q.enqueue_strict(c2, 1, Request(6));

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(6u, q.get_size_slow());


  for (int i = 0; i < 6; ++i) {
    (void) q.dequeue();
  }

  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.get_size_slow());
}


TEST(mClockPriorityQueue, JustStrict)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);

  q.enqueue_strict(c1, 1, Request(1));
  q.enqueue_strict(c2, 2, Request(2));
  q.enqueue_strict(c1, 2, Request(3));
  q.enqueue_strict(c2, 1, Request(4));

  Request r;

  r = q.dequeue();
  ASSERT_EQ(2, r.value);
  r = q.dequeue();
  ASSERT_EQ(3, r.value);
  r = q.dequeue();
  ASSERT_EQ(1, r.value);
  r = q.dequeue();
  ASSERT_EQ(4, r.value);
}


TEST(mClockPriorityQueue, StrictPriorities)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);

  q.enqueue_strict(c1, 1, Request(1));
  q.enqueue_strict(c2, 2, Request(2));
  q.enqueue_strict(c1, 3, Request(3));
  q.enqueue_strict(c2, 4, Request(4));

  Request r;

  r = q.dequeue();
  ASSERT_EQ(4, r.value);
  r = q.dequeue();
  ASSERT_EQ(3, r.value);
  r = q.dequeue();
  ASSERT_EQ(2, r.value);
  r = q.dequeue();
  ASSERT_EQ(1, r.value);
}


TEST(mClockPriorityQueue, JustNotStrict)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);

  // non-strict queue ignores priorites, but will divide between
  // clients evenly and maintain orders between clients
  q.enqueue(c1, 1, 1u, Request(1));
  q.enqueue(c1, 2, 1u, Request(2));
  q.enqueue(c2, 3, 1u, Request(3));
  q.enqueue(c2, 4, 1u, Request(4));

  Request r1, r2;

  r1 = q.dequeue();
  ASSERT_TRUE(1 == r1.value || 3 == r1.value);

  r2 = q.dequeue();
  ASSERT_TRUE(1 == r2.value || 3 == r2.value);

  ASSERT_NE(r1.value, r2.value);

  r1 = q.dequeue();
  ASSERT_TRUE(2 == r1.value || 4 == r1.value);

  r2 = q.dequeue();
  ASSERT_TRUE(2 == r2.value || 4 == r2.value);

  ASSERT_NE(r1.value, r2.value);
}


TEST(mClockPriorityQueue, EnqueuFront)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);

  // non-strict queue ignores priorites, but will divide between
  // clients evenly and maintain orders between clients
  q.enqueue(c1, 1, 1u, Request(1));
  q.enqueue(c1, 2, 1u, Request(2));
  q.enqueue(c2, 3, 1u, Request(3));
  q.enqueue(c2, 4, 1u, Request(4));
  q.enqueue_strict(c2, 6, Request(6));
  q.enqueue_strict(c1, 7, Request(7));

  std::list<Request> reqs;

  for (uint i = 0; i < 4; ++i) {
    reqs.emplace_back(q.dequeue());
  }

  for (uint i = 0; i < 4; ++i) {
    Request& r = reqs.front();
    if (r.value > 5) {
      q.enqueue_strict_front(r.value == 6 ? c2 : 1, r.value, std::move(r));
    } else {
      q.enqueue_front(r.value <= 2 ? c1 : c2, r.value, 0, std::move(r));
    }
    reqs.pop_front();
  }

  Request r;

  r = q.dequeue();
  ASSERT_EQ(7, r.value);

  r = q.dequeue();
  ASSERT_EQ(6, r.value);

  r = q.dequeue();
  ASSERT_TRUE(1 == r.value || 3 == r.value);

  r = q.dequeue();
  ASSERT_TRUE(1 == r.value || 3 == r.value);

  r = q.dequeue();
  ASSERT_TRUE(2 == r.value || 4 == r.value);

  r = q.dequeue();
  ASSERT_TRUE(2 == r.value || 4 == r.value);
}


TEST(mClockPriorityQueue, RemoveByClass)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);
  Client c3(3);

  q.enqueue(c1, 1, 1u, Request(1));
  q.enqueue(c2, 1, 1u, Request(2));
  q.enqueue(c3, 1, 1u, Request(4));
  q.enqueue_strict(c1, 2, Request(8));
  q.enqueue_strict(c2, 1, Request(16));
  q.enqueue_strict(c3, 3, Request(32));
  q.enqueue(c3, 1, 1u, Request(64));
  q.enqueue(c2, 1, 1u, Request(128));
  q.enqueue(c1, 1, 1u, Request(256));

  int out_mask = 2 | 16 | 128;
  int in_mask = 1 | 8 | 256;

  std::list<Request> out;
  q.remove_by_class(c2, &out);

  ASSERT_EQ(3u, out.size());
  while (!out.empty()) {
    ASSERT_TRUE((out.front().value & out_mask) > 0) <<
      "had value that was not expected after first removal";
    out.pop_front();
  }

  ASSERT_EQ(6u, q.get_size_slow()) << "after removal of three from client c2";

  q.remove_by_class(c3);

  ASSERT_EQ(3u, q.get_size_slow()) << "after removal of three from client c3";
  while (!q.empty()) {
    Request r = q.dequeue();
    ASSERT_TRUE((r.value & in_mask) > 0) <<
      "had value that was not expected after two removals";
  }
}


TEST(mClockPriorityQueue, RemoveByFilter)
{
  ceph::mClockQueue<Request,Client> q(&client_info_func);

  Client c1(1);
  Client c2(2);
  Client c3(3);

  q.enqueue(c1, 1, 1u, Request(1));
  q.enqueue(c2, 1, 1u, Request(2));
  q.enqueue(c3, 1, 1u, Request(3));
  q.enqueue_strict(c1, 2, Request(4));
  q.enqueue_strict(c2, 1, Request(5));
  q.enqueue_strict(c3, 3, Request(6));
  q.enqueue(c3, 1, 1u, Request(7));
  q.enqueue(c2, 1, 1u, Request(8));
  q.enqueue(c1, 1, 1u, Request(9));

  std::list<Request> filtered;

  q.remove_by_filter([&](const Request& r) -> bool {
    if (r.value & 2) {
      filtered.push_back(r);
      return true;
    } else {
      return false;
    }
  });

  ASSERT_EQ(4u, filtered.size()) <<
    "filter should have removed four elements";
  while (!filtered.empty()) {
    ASSERT_TRUE((filtered.front().value & 2) > 0) <<
      "expect this value to have been filtered out";
    filtered.pop_front();
  }

  ASSERT_EQ(5u, q.get_size_slow()) <<
    "filter should have left five remaining elements";
  while (!q.empty()) {
    Request r = q.dequeue();
    ASSERT_TRUE((r.value & 2) == 0) <<
      "expect this value to have been left in";
   }
}
