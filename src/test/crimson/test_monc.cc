#include <seastar/core/app-template.hh>
#include "common/ceph_argparse.h"
#include "crimson/common/auth_handler.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"

using Config = ceph::common::ConfigProxy;
using MonClient = ceph::mon::Client;

namespace {

class DummyAuthHandler : public ceph::common::AuthHandler {
public:
  void handle_authentication(const EntityName& name,
                             uint64_t global_id,
                             const AuthCapsInfo& caps) override {}
};

DummyAuthHandler dummy_handler;

}

static seastar::future<> test_monc()
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
    return ceph::common::sharded_perf_coll().start();
  }).then([] {
    return ceph::net::Messenger::create(entity_name_t::OSD(0), "monc", 0,
                                        seastar::engine().cpu_id())
        .then([] (ceph::net::Messenger *msgr) {
      auto& conf = ceph::common::local_conf();
      if (conf->ms_crc_data) {
        msgr->set_crc_data();
      }
      if (conf->ms_crc_header) {
        msgr->set_crc_header();
      }
      auto monc = std::make_unique<MonClient>(*msgr, dummy_handler);
      auto monc_ptr = monc.get();
      return msgr->start(monc_ptr).then([monc_ptr] {
        return seastar::with_timeout(
          seastar::lowres_clock::now() + std::chrono::seconds{10},
          monc_ptr->start());
      }).then([monc_ptr] {
        return monc_ptr->stop();
      }).finally([msgr, monc=std::move(monc)] {
        return msgr->shutdown();
      });
    });
  }).finally([] {
    return ceph::common::sharded_perf_coll().stop().then([] {
      return ceph::common::sharded_conf().stop();
    });
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
