#include <chrono>
#include <iostream>
#include <numeric>
#include <seastar/core/app-template.hh>
#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/alienstore/thread_pool.h"
#include "include/msgr.h"
#include "test/crimson/ctest_utils.h"

using namespace std::chrono_literals;
using ThreadPool = crimson::os::ThreadPool;
using crimson::common::local_conf;

seastar::future<> test_accumulate(ThreadPool& tp) {
  static constexpr auto N = 5;
  static constexpr auto M = 1;
  auto slow_plus = [&tp](int i) {
    return tp.submit(::rand() % 2, [=] {
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

seastar::future<> test_void_return(ThreadPool& tp) {
  return tp.submit(::rand() % 2, [=] {
    std::this_thread::sleep_for(10ns);
  });
}

int main(int argc, char** argv)
{
  seastar::app_template app{get_smp_opts_from_ctest()};
  return app.run(argc, argv, [] {
    std::vector<const char*> args;
    std::string cluster;
    std::string conf_file_list;
    auto init_params = ceph_argparse_early_args(args,
                                                CEPH_ENTITY_TYPE_CLIENT,
                                                &cluster,
                                                &conf_file_list);
    return crimson::common::sharded_conf().start(init_params.name, cluster)
    .then([conf_file_list] {
      return local_conf().parse_config_files(conf_file_list);
    }).then([] {
      return seastar::do_with(std::make_unique<crimson::os::ThreadPool>(2, 128, seastar::resource::cpuset{0}),
                              [](auto& tp) {
        return tp->start().then([&tp] {
          return test_accumulate(*tp);
        }).then([&tp] {
          return test_void_return(*tp);
        }).finally([&tp] {
          return tp->stop();
        });
      });
    }).finally([] {
      return crimson::common::sharded_conf().stop();
    }).handle_exception([](auto e) {
      std::cerr << "Error: " << e << std::endl;
      seastar::engine().exit(1);
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
