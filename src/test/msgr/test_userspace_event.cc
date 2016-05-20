// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <map>
#include <random>
#include <gtest/gtest.h>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "msg/async/dpdk/UserspaceEvent.h"
#include "global/global_context.h"
#include "global/global_init.h"

class UserspaceManagerTest : public ::testing::Test {
 public:
  UserspaceEventManager *manager;

  UserspaceManagerTest() {}
  virtual void SetUp() {
    manager = new UserspaceEventManager(g_ceph_context);
  }
  virtual void TearDown() {
    delete manager;
  }
};

TEST_F(UserspaceManagerTest, BasicTest) {
  int events[10];
  int masks[10];
  int fd = manager->get_eventfd();
  ASSERT_EQ(fd, 1);
  ASSERT_EQ(0, manager->listen(fd, 1));
  ASSERT_EQ(0, manager->notify(fd, 1));
  ASSERT_EQ(1, manager->poll(events, masks, 10, nullptr));
  ASSERT_EQ(fd, events[0]);
  ASSERT_EQ(1, masks[0]);
  ASSERT_EQ(0, manager->notify(fd, 2));
  ASSERT_EQ(0, manager->poll(events, masks, 10, nullptr));
  ASSERT_EQ(0, manager->unlisten(fd, 1));
  ASSERT_EQ(0, manager->notify(fd, 1));
  ASSERT_EQ(0, manager->poll(events, masks, 10, nullptr));
  manager->close(fd);
  fd = manager->get_eventfd();
  ASSERT_EQ(fd, 1);
  ASSERT_EQ(0, manager->poll(events, masks, 10, nullptr));
}

TEST_F(UserspaceManagerTest, FailTest) {
  int events[10];
  int masks[10];
  int fd = manager->get_eventfd();
  ASSERT_EQ(fd, 1);
  ASSERT_EQ(-ENOENT, manager->listen(fd+1, 1));
  ASSERT_EQ(-ENOENT, manager->notify(fd+1, 1));
  ASSERT_EQ(0, manager->poll(events, masks, 10, nullptr));
  ASSERT_EQ(-ENOENT, manager->unlisten(fd+1, 1));
  manager->close(fd);
}

TEST_F(UserspaceManagerTest, StressTest) {
  std::vector<std::pair<int, int> > mappings;
  int events[10];
  int masks[10];
  std::random_device rd;
  std::default_random_engine rng(rd());
  std::uniform_int_distribution<> dist(0, 100);

  mappings.resize(1001);
  mappings[0] = make_pair(-1, -1);
  for (int i = 0; i < 1000; ++i) {
    int fd = manager->get_eventfd();
    ASSERT_TRUE(fd > 0);
    mappings[fd] = make_pair(0, 0);
  }
  int r = 0;
  int fd = manager->get_eventfd();
  auto get_activate_count = [](std::vector<std::pair<int, int> > &m) {
    std::vector<int> fds;
    int mask = 0;
    size_t idx = 0;
    for (auto &&p : m) {
      mask = p.first & p.second;
      if (p.first != -1 && mask) {
        p.second &= (~mask);
        fds.push_back(idx);
        std::cerr << " activate " << idx << " mask " << mask << std::endl;
      }
      ++idx;
    }
    return fds;
  };
  for (int i = 0; i < 10000; ++i) {
    int value = dist(rng);
    fd = dist(rng) % mappings.size();
    auto &p = mappings[fd];
    int mask = dist(rng) % 2 + 1;
    if (value > 55) {
      r = manager->notify(fd, mask);
      if (p.first == -1) {
        ASSERT_EQ(p.second, -1);
        ASSERT_EQ(r, -ENOENT);
      } else {
        p.second |= mask;
        ASSERT_EQ(r, 0);
      }
      std::cerr << " notify fd " << fd << " mask " << mask << " r " << r << std::endl;
    } else if (value > 45) {
      r = manager->listen(fd, mask);
      std::cerr << " listen fd " << fd << " mask " << mask << " r " << r << std::endl;
      if (p.first == -1) {
        ASSERT_EQ(p.second, -1);
        ASSERT_EQ(r, -ENOENT);
      } else {
        p.first |= mask;
        ASSERT_EQ(r, 0);
      }
    } else if (value > 35) {
      r = manager->unlisten(fd, mask);
      std::cerr << " unlisten fd " << fd << " mask " << mask << " r " << r << std::endl;
      if (p.first == -1) {
        ASSERT_EQ(p.second, -1);
        ASSERT_EQ(r, -ENOENT);
      } else {
        p.first &= ~mask;
        ASSERT_EQ(r, 0);
      }
    } else if (value > 20) {
      std::set<int> actual, expected;
      do {
        r = manager->poll(events, masks, 3, nullptr);
        std::cerr << " poll " << r;
        for (int k = 0; k < r; ++k) {
          std::cerr << events[k] << " ";
          actual.insert(events[k]);
        }
      } while (r == 3);
      std::cerr << std::endl;
      auto fds = get_activate_count(mappings);
      for (auto &&d : fds)
        expected.insert(d);
      ASSERT_EQ(expected, actual);
    } else if (value > 10) {
      r = manager->get_eventfd();
      std::cerr << " open fd " << r << std::endl;
      ASSERT_TRUE(r > 0);
      if ((size_t)r >= mappings.size())
        mappings.resize(r+1);
      mappings[r] = make_pair(0, 0);
    } else {
      manager->close(fd);
      std::cerr << " close fd " << fd << std::endl;
      mappings[fd] = make_pair(-1, -1);
    }
    ASSERT_TRUE(manager->check());
  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_userspace_event &&
 *    ./ceph_test_userspace_event.cc
 *
 * End:
 */
