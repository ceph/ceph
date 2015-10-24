// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <iostream>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <map>

#include "include/Context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "msg/async/SlabPool.h"
#include <boost/random/uniform_int.hpp>

#include <gtest/gtest.h>

typedef boost::mt11213b gen_type;

static const size_t max_object_size = 1024*1024;

static void free_map(SlabAllocator &slab, std::map<uint32_t, void*> &items) {
  for (std::map<uint32_t, void*>::iterator it = items.begin();
       it != items.end(); ++it) {
    slab.free(it->first, it->second);
  }
}

TEST(SlabPool, test_allocation) {
  uint64_t resident = 1024*1024*30;
  SlabAllocator slab(NULL, "", 1.25, resident, 1*1024*1024);
  size_t size = max_object_size;

  slab.print_slab_classes();

  std::map<uint32_t, void*> datas;

  assert(resident % size == 0);
  uint32_t idx;
  void *data;
  for (unsigned i = 0u; i < (resident / size); i++) {
    int r = slab.create(size, &idx, &data);
    ASSERT_EQ(r, 0);
    datas[idx] = data;
  }
  free_map(slab, datas);
}

TEST(SlabPool, test_reclaim) {
  uint64_t resident = 1024*1024*30;
  SlabAllocator slab(NULL, "", 2, resident, 1024*1024*4);
  uint32_t idx;
  void *data;
  std::map<uint32_t, void*> datas;
  for (size_t i = 0; i < 2*resident;) {
    size_t size = rand() % slab.max_size();
    int r = slab.create(size, &idx, &data);
    ASSERT_EQ(r, 0);
    datas[idx] = data;
    i += size;
  }

  free_map(slab, datas);
  ASSERT_TRUE(slab.size() > resident);
  while (!slab.reclaim());
  ASSERT_TRUE(slab.size() <= resident);
}

class Worker : public Thread {
  bool stop;
  SlabAllocator *slab;
  gen_type *rng;
  map<uint32_t, void*> data;
  uint64_t count;

 public:
  Worker(SlabAllocator *slab, gen_type *r, uint64_t c): stop(false), slab(slab), rng(r), count(c) {}
  void *entry() {
    while (--count) {
      if (rand() % 2 && data.size() < 30) {
        boost::uniform_int<> choose(0, slab->max_size());
        uint64_t size = choose(*rng);
        uint32_t idx;
        void *d;
        int r = slab->create(size, &idx, &d);
        assert(r == 0);
        data[idx] = d;
      } else {
        boost::uniform_int<> choose(0, data.size() - 1);
        int index = choose(*rng);
        map<uint32_t, void*>::iterator it = data.begin();
        for ( ; index > 0; --index, ++it) ;
        slab->free(it->first, it->second);
        data.erase(it);
      }
    }
  }
};

TEST(SlabPool, test_reclaim) {
  gen_type rng(time(NULL));
  std::vector<Worker*> workers;
  SlabAllocator slab(NULL, "", 2, 1024*1024*100, 4*1024*1024, 100000);
  for (int i = 0; i < 10; ++i) {
    Worker *w = new Worker(&slab, &rng)
    w->create();
    workers.push_back(w);
  }
  size_t freed = 0;
  uint64_t passed_time = 0;
  struct timespec spec;
  spec.tv_sec = 0;
  spec.tv_nsec = 1000*1000*10;
  int i = 0;
  do {
    nanosleep(&spec);
    if (i % 10000) {
      std::cerr << "reclaim" << std::endl;
    }
    i++;
  } while (slab.reclaim());
}

int main(int argc, char **argv) {
  std::vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make unittest_slab_pool && 
 *    ./unittest_slab_pool
 *
 * End:
 */
