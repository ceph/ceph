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
#include "msg/async/SlabPool.h"

#include <gtest/gtest.h>

static const size_t max_object_size = 1024*1024;

static void free_vector(slab_allocator &slab, std::map<uint32_t, void*> &items) {
  for (std::map<uint32_t, void*>::iterator it = items.begin();
       it != items.end(); ++it {
    slab.free(it->first, it->second);
  }
}

static void test_allocation_1(const double growth_factor, const unsigned slab_limit_size) {
    slab_allocator slab(growth_factor, slab_limit_size, max_object_size);
    size_t size = max_object_size;

    slab.print_slab_classes();

    std::map<uint32_t, void*> datas;

    assert(slab_limit_size % size == 0);
    uint32_t idx;
    void *data;
    for (unsigned i = 0u; i < (slab_limit_size / size); i++) {
        int r = slab.create(size, &idx, &data);
        assert(r == 0);
        datas[idx] = data;
    }
    assert(slab.create(size, &idx, &data) < 0);
    free_vector(slab, datas);
}

static void test_allocation_2(const double growth_factor, const unsigned slab_limit_size) {
    slab_allocator slab(growth_factor, slab_limit_size, max_object_size);
    size_t size = 1024;

    std::map<uint32_t, void*> datas;

    size_t allocations = 0u;
    uint32_t idx;
    void *data;
    for (;;) {
      int r = slab.create(size, &idx, &data);
      if (r < 0) {
        break;
      }
      datas[idx] = data;
      allocations++;
    }

    size_t class_size = slab.class_size(size);
    size_t per_slab_page = max_object_size / class_size;
    unsigned available_slab_pages = slab_limit_size / max_object_size;
    assert(allocations == (per_slab_page * available_slab_pages));
    free_vector(slab, datas);
}

TEST(SlabPool, test_allocation) {
  test_allocation_1(1.25, 5*1024*1024);
  test_allocation_2(1.07, 5*1024*1024); // 1.07 is the growth factor used by facebook.
}

int main(int argc, char **argv) {
  vector<const char*> args;
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
 *
