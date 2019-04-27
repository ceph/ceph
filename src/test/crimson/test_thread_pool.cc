#include <chrono>
#include <iostream>
#include <numeric>
#include <seastar/core/app-template.hh>
#include "crimson/thread/ThreadPool.h"

using namespace std::chrono_literals;
using ThreadPool = ceph::thread::ThreadPool;

seastar::future<> test_accumulate(ThreadPool& tp) {
  static constexpr auto N = 5;
  static constexpr auto M = 1;
  auto slow_plus = [&tp](int i) {
    return tp.submit([=] {
      std::this_thread::sleep_for(10ns);
      return i + M;
    });
  };
  return seastar::map_reduce(
    boost::irange(0, N), slow_plus, 0, std::plus{}).then([] (int sum) {
    auto r = boost::irange(0 + M, N + M);
    if (sum != std::accumulate(r.begin(), r.end(), 0)) {
      throw std::runtime_error("test_accumulate failed");
    }
  });
}

int main(int argc, char** argv)
{
  ThreadPool tp{2, 128, 0};
  seastar::app_template app;
  return app.run(argc, argv, [&tp] {
      return tp.start().then([&tp] {
          return test_accumulate(tp);
        }).handle_exception([](auto e) {
          std::cerr << "Error: " << e << std::endl;
          seastar::engine().exit(1);
        }).finally([&tp] {
          return tp.stop();
        });
      });
}

/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * unittest_seastar_thread_pool"
 * End:
 */
