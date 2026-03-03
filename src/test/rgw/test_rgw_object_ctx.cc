// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>

#include "rgw_rados.h"

TEST(TestRGWObjectCtx, create_and_destroy_many)
{
  // Create and destroy many RGWObjectCtx instances and
  // for each one exercise its API from multiple threads.
  //
  // If there are lifetime or concurrency bugs in use of RGWObjectCtx,
  // this might help surfacing them (perhaps under ASan/TSan/valgrind runs).

  constexpr int test_duration_secs = 10;
  constexpr int num_concurrent_requests = 100;

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // Pre-generate objects handled by the object contexts
  std::vector<rgw_obj> objs;
  objs.reserve(num_concurrent_requests);
  for (int i = 0; i < num_concurrent_requests; ++i) {
    objs.emplace_back(
        rgw_bucket("", "dummy_bucket" + std::to_string(i)),
        rgw_obj_key("dummy_key" + std::to_string(i)));
  }

  // Spawn threads that repeatedly construct/destroy RGWObjectCtx objects
  // and exercise its methods on random objects in random order
  for (int t = 0; t < num_concurrent_requests; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng{
        static_cast<std::mt19937_64::result_type>(
          std::chrono::steady_clock::now().time_since_epoch().count() + t
        )
      };
      std::uniform_int_distribution<int> obj_dist(0, num_concurrent_requests - 1);
      std::uniform_int_distribution<int> coin(0, 1);

      while (!stop.load(std::memory_order_relaxed)) {
        RGWObjectCtx ctx(nullptr);
        const auto& o = objs[obj_dist(rng)];

        // randomly choose which operations to do and in which order
        // to simulate a real-life pattern.
        if (coin(rng)) {
          ctx.set_atomic(o, true);
        }
        if (coin(rng)) {
          ctx.set_compressed(o);
        }
        if (coin(rng)) {
          ctx.set_prefetch_data(o);
        }
        if (coin(rng)) {
          (void)ctx.get_state(o);
        }
        if (coin(rng)) {
          ctx.invalidate(o);
        }
        if (coin(rng)) {
          (void) ctx.get_driver();
        }
      }
    });
  }

  // Let the threads run for some time
  std::this_thread::sleep_for(std::chrono::seconds(test_duration_secs));
  stop.store(true);
  for (auto& th : threads) th.join();
}
