#include <seastar/core/app-template.hh>
#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/SocketMessenger.h"

using Config = ceph::common::ConfigProxy;
using MonClient = ceph::mon::Client;

static seastar::future<> test_monc()
{
  return ceph::common::sharded_conf().start().then([] {
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
    return seastar::do_with(ceph::net::SocketMessenger{entity_name_t::OSD(0)},
                            [](ceph::net::Messenger& msgr) {
      auto& conf = ceph::common::local_conf();
      if (conf->ms_crc_data) {
        msgr.set_crc_data();
      }
      if (conf->ms_crc_header) {
        msgr.set_crc_header();
      }
      return seastar::do_with(MonClient{conf->name, msgr},
                              [&msgr](auto& monc) {
        return monc.build_initial_map().then([&monc] {
          return monc.load_keyring();
        }).then([&msgr, &monc] {
          return msgr.start(&monc);
        }).then([&monc] {
          return seastar::with_timeout(
            seastar::lowres_clock::now() + std::chrono::seconds{10},
            monc.authenticate());
        }).finally([&monc] {
          return monc.stop();
        });
      }).finally([&msgr] {
        return msgr.shutdown();
      });
    });
  }).finally([] {
    return ceph::common::sharded_conf().stop();
  });
}

int main(int argc, char** argv)
{
  seastar::app_template app;
  return app.run(argc, argv, [&] {
    return test_monc().then([] {
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
 * unittest_seastar_monc"
 * End:
 */
