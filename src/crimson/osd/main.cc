// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <random>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/std-compat.hh>

#include "auth/KeyRing.h"
#include "common/ceph_argparse.h"
#include "common/config_tracker.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/fatal_signal.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "global/pidfile.h"
#include "osd.h"

using namespace std::literals;
namespace bpo = boost::program_options;

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_osd);
}

void usage(const char* prog) {
  std::cout << "usage: " << prog << std::endl;
  generic_server_usage();
}

auto partition_args(int argc, const char *argv[])
{
  auto seastar_n_early_args = [=] {
    class DummyTracker : public ConfigTracker {
      bool is_tracking(const std::string& name) const override {
        return false;
      }
    };
    DummyTracker dt;
    ConfigValues config_values;
    md_config_t config(config_values, dt, true);
    std::vector<const char *> seastar_n_early_args{argv, argv + argc};
    // pull off the stuff from seastar_n_early_args that we'll give to local_conf
    config.parse_argv(config_values, dt, seastar_n_early_args, CONF_CMDLINE);
    return seastar_n_early_args;
  }();
  
  // Now that we have a filtered seastar_n_early_args, populate config_proxy_args without them
  std::vector<const char*> config_proxy_args;
  std::set_difference(argv, argv + argc,
                      std::begin(seastar_n_early_args), std::end(seastar_n_early_args),
                      std::back_inserter(config_proxy_args));
  return make_pair(std::move(config_proxy_args), std::move(seastar_n_early_args));
}

using crimson::common::local_conf;

seastar::future<> make_keyring()
{
  const auto path = local_conf().get_val<std::string>("keyring");
  return seastar::file_exists(path).then([path](bool exists) {
    KeyRing keyring;
    EntityName name{local_conf()->name};
    EntityAuth auth;
    if (exists &&
        keyring.load(nullptr, path) == 0 &&
        keyring.get_auth(name, auth)) {
      fmt::print(std::cerr, "already have key in keyring: {}\n", path);
      return seastar::now();
    } else {
      CephContext temp_cct{};
      auth.key.create(&temp_cct, CEPH_CRYPTO_AES);
      keyring.add(name, auth);
      bufferlist bl;
      keyring.encode_plaintext(bl);
      const auto permissions = (seastar::file_permissions::user_read |
                              seastar::file_permissions::user_write);
      return crimson::write_file(std::move(bl), path, permissions);
    }
  }).handle_exception_type([path](const std::filesystem::filesystem_error& e) {
    fmt::print(std::cerr, "FATAL: writing new keyring to {}: {}\n", path, e.what());
    throw e;
  });
}

static uint64_t get_nonce()
{
  if (auto pid = getpid(); pid == 1 || std::getenv("CEPH_USE_RANDOM_NONCE")) {
    // we're running in a container; use a random number instead!
    std::random_device rd;
    std::default_random_engine rng{rd()};
    return std::uniform_int_distribution<uint64_t>{}(rng);
  } else {
    return pid;
  }
}

seastar::future<> fetch_config()
{
  // i don't have any client before joining the cluster, so no need to have
  // a proper auth handler
  class DummyAuthHandler : public crimson::common::AuthHandler {
  public:
    void handle_authentication(const EntityName& name,
                               const AuthCapsInfo& caps)
    {}
  };
  return seastar::async([] {
    auto auth_handler = std::make_unique<DummyAuthHandler>();
    auto msgr = crimson::net::Messenger::create(entity_name_t::CLIENT(),
                                                "temp_mon_client",
                                                get_nonce());
    crimson::mon::Client monc{*msgr, *auth_handler};
    msgr->set_auth_client(&monc);
    msgr->start({&monc}).get();
    auto stop_msgr = seastar::defer([&] {
      // stop msgr here also, in case monc fails to start.
      msgr->stop();
      msgr->shutdown().get();
    });
    monc.start().handle_exception([] (auto ep) {
      fmt::print(std::cerr, "FATAL: unable to connect to cluster: {}\n", ep);
      return seastar::make_exception_future<>(ep);
    }).get();
    auto stop_monc = seastar::defer([&] {
      // unregister me from msgr first.
      msgr->stop();
      monc.stop().get();
    });
    monc.sub_want("config", 0, 0);
    monc.renew_subs().get();
    // wait for monmap and config
    monc.wait_for_config().get();
    local_conf().set_val("fsid", monc.get_fsid().to_string()).get();
  });
}

static void override_seastar_opts(std::vector<const char*>& args)
{
  if (auto found = std::find_if(std::begin(args), std::end(args),
                                [] (auto* arg) { return "--smp"sv == arg; });
      found == std::end(args)) {
    // TODO: we don't have a way to communicate the resource requirements
    // with the deployment tools, like cephadm and rook, which don't set, for
    // instance, aio-max-nr for us. but we should fix this, once crimson is able
    // to run on a multi-core system, i.e., once m-to-n problem is resolved.
    std::cout << "warn: added seastar option --smp 1" << std::endl;
    args.emplace_back("--smp");
    args.emplace_back("1");
  }
}

static std::ofstream maybe_set_logger()
{
  std::ofstream log_file_stream;
  if (auto log_file = local_conf()->log_file; !log_file.empty()) {
    log_file_stream.open(log_file, std::ios::app | std::ios::out);
    try {
      seastar::throw_system_error_on(log_file_stream.fail());
    } catch (const std::system_error& e) {
      ceph_abort_msg(fmt::format("unable to open log file: {}", e.what()));
    }
    logger().set_ostream(log_file_stream);
  }
  return log_file_stream;
}

int main(int argc, const char* argv[])
{
  seastar::app_template::config app_cfg;
  app_cfg.name = "Crimson";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));
  app.add_options()
    ("mkkey", "generate a new secret key. "
              "This is normally used in combination with --mkfs")
    ("mkfs", "create a [new] data directory")
    ("debug", "enable debug output on all loggers")
    ("trace", "enable trace output on all loggers")
    ("osdspec-affinity", bpo::value<std::string>()->default_value(std::string{}),
     "set affinity to an osdspec")
    ("prometheus_port", bpo::value<uint16_t>()->default_value(0),
     "Prometheus port. Set to zero to disable")
    ("prometheus_address", bpo::value<std::string>()->default_value("0.0.0.0"),
     "Prometheus listening address")
    ("prometheus_prefix", bpo::value<std::string>()->default_value("osd"),
     "Prometheus metrics prefix");

  auto [config_proxy_args, seastar_n_early_args] = partition_args(argc, argv);
  if (ceph_argparse_need_usage(seastar_n_early_args)) {
    usage(argv[0]);
  }
  override_seastar_opts(seastar_n_early_args);
  std::string cluster_name{"ceph"};
  std::string conf_file_list;
  // ceph_argparse_early_args() could _exit(), while local_conf() won't ready
  // until it's started. so do the boilerplate-settings parsing here.
  auto init_params = ceph_argparse_early_args(seastar_n_early_args,
                                              CEPH_ENTITY_TYPE_OSD,
                                              &cluster_name,
                                              &conf_file_list);
  using crimson::common::sharded_conf;
  using crimson::common::sharded_perf_coll;
  try {
    return app.run(seastar_n_early_args.size(), const_cast<char**>(seastar_n_early_args.data()),
      [&, &config_proxy_args=config_proxy_args] {
      auto& config = app.configuration();
      return seastar::async([&] {
        try {
          FatalSignal fatal_signal;
          seastar_apps_lib::stop_signal should_stop;
          if (config.count("debug")) {
            seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::debug
            );
          }
          if (config.count("trace")) {
            seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::trace
            );
          }
          sharded_conf().start(init_params.name, cluster_name).get();
          local_conf().start().get();
          auto stop_conf = seastar::deferred_stop(sharded_conf());
          sharded_perf_coll().start().get();
          auto stop_perf_coll = seastar::deferred_stop(sharded_perf_coll());
          local_conf().parse_config_files(conf_file_list).get();
          local_conf().parse_env().get();
          local_conf().parse_argv(config_proxy_args).get();
          auto log_file_stream = maybe_set_logger();
          auto reset_logger = seastar::defer([] {
            logger().set_ostream(std::cerr);
          });
          if (const auto ret = pidfile_write(local_conf()->pid_file);
              ret == -EACCES || ret == -EAGAIN) {
            ceph_abort_msg(
              "likely there is another crimson-osd instance with the same id");
          } else if (ret < 0) {
            ceph_abort_msg(fmt::format("pidfile_write failed with {} {}",
                                       ret, cpp_strerror(-ret)));
          }
          // just ignore SIGHUP, we don't reread settings. keep in mind signals
          // handled by S* must be blocked for alien threads (see AlienStore).
          seastar::engine().handle_signal(SIGHUP, [] {});

          // start prometheus API server
          seastar::httpd::http_server_control prom_server;
          std::any stop_prometheus;
          if (uint16_t prom_port = config["prometheus_port"].as<uint16_t>();
              prom_port != 0) {
            prom_server.start("prometheus").get();
            stop_prometheus = seastar::make_shared(seastar::deferred_stop(prom_server));

            seastar::prometheus::config prom_config;
            prom_config.prefix = config["prometheus_prefix"].as<std::string>();
            seastar::prometheus::start(prom_server, prom_config).get();
            seastar::net::inet_address prom_addr(config["prometheus_address"].as<std::string>());
            prom_server.listen(seastar::socket_address{prom_addr, prom_port})
              .handle_exception([=] (auto ep) {
              std::cerr << seastar::format("Could not start Prometheus API server on {}:{}: {}\n",
                                           prom_addr, prom_port, ep);
              return seastar::make_exception_future(ep);
            }).get();
          }

          const int whoami = std::stoi(local_conf()->name.get_id());
          const auto nonce = get_nonce();
          crimson::net::MessengerRef cluster_msgr, client_msgr;
          crimson::net::MessengerRef hb_front_msgr, hb_back_msgr;
          for (auto [msgr, name] : {make_pair(std::ref(cluster_msgr), "cluster"s),
                                    make_pair(std::ref(client_msgr), "client"s),
                                    make_pair(std::ref(hb_front_msgr), "hb_front"s),
                                    make_pair(std::ref(hb_back_msgr), "hb_back"s)}) {
            msgr = crimson::net::Messenger::create(entity_name_t::OSD(whoami),
                                                   name,
                                                   nonce);
          }
          auto store = crimson::os::FuturizedStore::create(
            local_conf().get_val<std::string>("osd_objectstore"),
            local_conf().get_val<std::string>("osd_data"),
            local_conf().get_config_values()).get();

          crimson::osd::OSD osd(
	    whoami, nonce, std::ref(*store), cluster_msgr, client_msgr,
	    hb_front_msgr, hb_back_msgr);

          if (config.count("mkkey")) {
            make_keyring().get();
          }
          if (local_conf()->no_mon_config) {
            logger().info("bypassing the config fetch due to --no-mon-config");
          } else {
            fetch_config().get();
          }
          if (config.count("mkfs")) {
            auto osd_uuid = local_conf().get_val<uuid_d>("osd_uuid");
            if (osd_uuid.is_zero()) {
              // use a random osd uuid if not specified
              osd_uuid.generate_random();
            }
            osd.mkfs(
	      *store,
	      whoami,
              osd_uuid,
              local_conf().get_val<uuid_d>("fsid"),
              config["osdspec-affinity"].as<std::string>()).get();
          }
          if (config.count("mkkey") || config.count("mkfs")) {
            return EXIT_SUCCESS;
          } else {
            osd.start().get();
          }
          logger().info("crimson startup completed");
          should_stop.wait().get();
          logger().info("crimson shutting down");
          osd.stop().get();
          // stop()s registered using defer() are called here
        } catch (...) {
          logger().error("startup failed: {}", std::current_exception());
          return EXIT_FAILURE;
        }
        logger().info("crimson shutdown complete");
        return EXIT_SUCCESS;
      });
    });
  } catch (...) {
    fmt::print(std::cerr, "FATAL: Exception during startup, aborting: {}\n", std::current_exception());
    return EXIT_FAILURE;
  }
}

/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * crimson-osd"
 * End:
 */
