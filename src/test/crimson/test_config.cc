#include <chrono>
#include <numeric>
#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include "crimson/common/config_proxy.h"

using Config = ceph::common::ConfigProxy;

static seastar::future<> test_config()
{
  return ceph::common::sharded_conf().start().then([] {
    return ceph::common::sharded_conf().invoke_on(0, &Config::start);
  }).then([] {
    return ceph::common::sharded_conf().invoke_on_all([](auto& config) {
      return config.set_val("osd_tracing", "true");
    });
  }).then([] {
    return ceph::common::local_conf().get_val<bool>("osd_tracing");
  }).then([](bool osd_tracing) {
    if (osd_tracing) {
      return seastar::make_ready_future<>();
    } else {
      throw std::runtime_error("run osd_tracing");
    }
  }).then([] {
    return ceph::common::sharded_conf().stop();
  });
}

int main(int argc, char** argv)
{
  seastar::app_template app;
  return app.run(argc, argv, [&] {
    return test_config().then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}


/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * unittest_seastar_config"
 * End:
 */
