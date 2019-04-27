#include <chrono>
#include <string>
#include <numeric>
#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include "common/ceph_argparse.h"
#include "common/config_obs.h"
#include "crimson/common/config_proxy.h"

using Config = ceph::common::ConfigProxy;
const std::string test_uint_option = "osd_max_pgls";
const uint64_t INVALID_VALUE = (uint64_t)(-1);

class ConfigObs : public ceph::md_config_obs_impl<Config> {
  uint64_t last_change = INVALID_VALUE;
  uint64_t num_changes = 0;

  const char** get_tracked_conf_keys() const override {
    static const char* keys[] = {
      test_uint_option.c_str(),
      nullptr,
    };
    return keys;
  }
  void handle_conf_change(const Config& conf,
                          const std::set <std::string> &changes) override{
    if (changes.count(test_uint_option)) {
      last_change = conf.get_val<uint64_t>(test_uint_option);
      num_changes += 1;
    }
  }
public:
  ConfigObs() {
    ceph::common::local_conf().add_observer(this);
  }

  uint64_t get_last_change() const { return last_change; }
  uint64_t get_num_changes() const { return num_changes; }
  seastar::future<> stop() {
    ceph::common::local_conf().remove_observer(this);
    return seastar::now();
  }
};

seastar::sharded<ConfigObs> sharded_cobs;

static seastar::future<> test_config()
{
  return ceph::common::sharded_conf().start(EntityName{}, string_view{"ceph"}).then([] {
    std::vector<const char*> args;
    std::string cluster;
    std::string conf_file_list;
    auto init_params = ceph_argparse_early_args(args,
                                                CEPH_ENTITY_TYPE_CLIENT,
                                                &cluster,
                                                &conf_file_list);
    auto& conf = ceph::common::local_conf();
    conf->name = init_params.name;
    conf->cluster = cluster;
    return conf.parse_config_files(conf_file_list);
  }).then([] {
    return ceph::common::sharded_conf().invoke_on(0, &Config::start);
  }).then([] {
    return sharded_cobs.start();
  }).then([] {
    return ceph::common::sharded_conf().invoke_on_all([](Config& config) {
      return config.set_val(test_uint_option,
                            std::to_string(seastar::engine().cpu_id()));
    });
  }).then([] {
    auto expected = ceph::common::local_conf().get_val<uint64_t>(test_uint_option);
    return ceph::common::sharded_conf().invoke_on_all([expected](Config& config) {
      if (expected != config.get_val<uint64_t>(test_uint_option)) {
        throw std::runtime_error("configurations don't match");
      }
      if (expected != sharded_cobs.local().get_last_change()) {
        throw std::runtime_error("last applied changes don't match the latest config");
      }
      if (seastar::smp::count != sharded_cobs.local().get_num_changes()) {
        throw std::runtime_error("num changes don't match actual changes");
      }
    });
  }).finally([] {
    return sharded_cobs.stop();
  }).finally([] {
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
