#include <seastar/core/app-template.hh>
#include "common/ceph_argparse.h"
#include "crimson/common/auth_handler.h"
#include "crimson/common/config_proxy.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"

using Config = crimson::common::ConfigProxy;
using MonClient = crimson::mon::Client;

namespace {

class DummyAuthHandler : public crimson::common::AuthHandler {
public:
  void handle_authentication(const EntityName& name,
                             const AuthCapsInfo& caps) final
  {}
};

DummyAuthHandler dummy_handler;

}

static seastar::future<> test_monc()
{
  auto chained_dispatchers = seastar::make_lw_shared<ChainedDispatchers>();
  return crimson::common::sharded_conf().start(EntityName{}, string_view{"ceph"}).then([] {
    std::vector<const char*> args;
    std::string cluster;
    std::string conf_file_list;
    auto init_params = ceph_argparse_early_args(args,
                                                CEPH_ENTITY_TYPE_CLIENT,
                                                &cluster,
                                                &conf_file_list);
    auto& conf = crimson::common::local_conf();
    conf->name = init_params.name;
    conf->cluster = cluster;
    return conf.parse_config_files(conf_file_list);
  }).then([] {
    return crimson::common::sharded_perf_coll().start();
  }).then([chained_dispatchers]() mutable {
    auto msgr = crimson::net::Messenger::create(entity_name_t::OSD(0), "monc", 0);
    auto& conf = crimson::common::local_conf();
    if (conf->ms_crc_data) {
      msgr->set_crc_data();
    }
    if (conf->ms_crc_header) {
      msgr->set_crc_header();
    }
    msgr->set_require_authorizer(false);
    return seastar::do_with(MonClient{*msgr, dummy_handler},
                            [msgr, chained_dispatchers](auto& monc) mutable {
      chained_dispatchers->push_back(monc);
      return msgr->start(chained_dispatchers).then([&monc] {
        return seastar::with_timeout(
          seastar::lowres_clock::now() + std::chrono::seconds{10},
          monc.start());
      }).then([&monc] {
        return monc.stop();
      });
    }).finally([msgr] {
      return msgr->shutdown();
    });
  }).finally([] {
    return crimson::common::sharded_perf_coll().stop().then([] {
      return crimson::common::sharded_conf().stop();
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
