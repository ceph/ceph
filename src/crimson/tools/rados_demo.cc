// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * Minimal RADOS client demo using Crimson Objecter and RadosClient.
 * Connects to cluster, creates IoCtx, and exercises: write, read,
 * discard, write_zeroes, compare_and_write.
 *
 * Usage: crimson-rados-demo -c /etc/ceph/ceph.conf -n client.admin --pool rbd
 */

#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>

#include "auth/KeyRing.h"
#include "crimson/client/io_context.h"
#include "crimson/client/rados_client.h"
#include "crimson/osdc/objecter.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/fatal_signal.h"
#include "crimson/common/log.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/main_config_bootstrap_helpers.h"
#include "msg/msg_types.h"

namespace bpo = boost::program_options;

static seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_client);
}

int main(int argc, const char* argv[])
{
  auto early_result = crimson::osd::get_early_config_client(argc, argv);
  if (!early_result.has_value()) {
    std::cerr << "get_early_config_client failed: " << early_result.error()
              << std::endl;
    return early_result.error();
  }
  auto& early_config = early_result.value();

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-rados-demo";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));
  app.add_options()
    ("pool", bpo::value<std::string>()->default_value("rbd"),
     "pool name for I/O demo")
    ("obj", bpo::value<std::string>()->default_value("crimson_rados_demo_obj"),
     "object name for write/read")
    ("debug", "enable debug logging");

  try {
    return app.run(
      early_config.get_early_args().size(),
      const_cast<char**>(early_config.get_early_args().data()),
      [&] {
        auto& config = app.configuration();
        auto config_proxy_args = early_config.ceph_args;
        return seastar::async([config_proxy_args, &config, &early_config] {
          try {
            FatalSignal fatal_signal;
            if (config.count("debug")) {
              seastar::global_logger_registry().set_all_loggers_level(
                seastar::log_level::debug);
            }

            crimson::common::sharded_conf().start(
              early_config.init_params.name,
              early_config.cluster_name).get();
            crimson::common::local_conf().start().get();
            auto stop_conf = seastar::deferred_stop(
              crimson::common::sharded_conf());
            crimson::common::sharded_perf_coll().start().get();
            auto stop_perf = seastar::deferred_stop(
              crimson::common::sharded_perf_coll());

            crimson::common::local_conf().parse_config_files(
              early_config.conf_file_list).get();
            crimson::common::local_conf().parse_env().get();
            crimson::common::local_conf().parse_argv(
              config_proxy_args).get();

            crimson::osd::populate_config_from_mon().get();

            const auto pool_name = config["pool"].as<std::string>();
            const auto obj_name = config["obj"].as<std::string>();

            class DemoAuthHandler : public crimson::common::AuthHandler {
            public:
              void handle_authentication(const EntityName& name,
                                        const AuthCapsInfo& caps) override {}
            };
            auto auth_handler = std::make_unique<DemoAuthHandler>();
            auto msgr = crimson::net::Messenger::create(
              entity_name_t(early_config.init_params.name.get_type(), -1),
              "rados_demo",
              crimson::osd::get_nonce(),
              true);
            crimson::mon::Client monc(*msgr, *auth_handler);
            msgr->set_auth_client(&monc);
            msgr->set_auth_server(&monc);

            crimson::client::RadosClient rados(*msgr, monc);
            crimson::net::dispatchers_t dispatchers;
            dispatchers.push_back(&monc);
            dispatchers.push_back(&rados.get_objecter());
            msgr->start(dispatchers).get();
            auto stop_msgr = seastar::defer([&] {
              msgr->stop();
              msgr->shutdown().get();
            });

            monc.start().get();
            auto stop_monc = seastar::defer([&] { monc.stop().get(); });

            rados.get_objecter().set_client_incarnation(
              static_cast<int>(crimson::osd::get_nonce() & 0x7fffffff));
            rados.connect().get();
            auto ioctx = rados.create_ioctx(pool_name).get();

            ceph::bufferlist bl;
            bl.append_zero(4096);
            ioctx.write(obj_name, 0, std::move(bl)).get();
            logger().info("wrote {} bytes to {}/{}", 4096, pool_name, obj_name);

            auto data = ioctx.read(obj_name, 0, 4096).get();
            logger().info("read {} bytes from {}/{}", data.length(),
                         pool_name, obj_name);

            // Test discard (zero range) and write_zeroes
            ioctx.discard(obj_name, 512, 1024).get();
            logger().info("discarded bytes 512-1536");
            ioctx.write_zeroes(obj_name, 2048, 512).get();
            logger().info("write_zeroes bytes 2048-2560");

            data = ioctx.read(obj_name, 0, 4096).get();
            logger().info("read {} bytes after discard/write_zeroes",
                         data.length());

            // Test compare_and_write: write known content, then compare-and-write
            ceph::bufferlist init_bl;
            init_bl.append("ABCD", 4);
            ioctx.write(obj_name, 0, std::move(init_bl)).get();
            logger().info("wrote ABCD at 0 for compare_and_write test");
            ceph::bufferlist cmp_bl;
            cmp_bl.append("ABCD", 4);
            ceph::bufferlist write_bl;
            write_bl.append("WXYZ", 4);
            ioctx.compare_and_write(obj_name, 0, std::move(cmp_bl), 0,
                                   std::move(write_bl)).get();
            logger().info("compare_and_write succeeded (ABCD -> WXYZ)");
            data = ioctx.read(obj_name, 0, 4).get();
            logger().info("read {} bytes after compare_and_write", data.length());

            // Test exec: cls hello say_hello (returns "Hello, world!" with empty in)
            ceph::bufferlist exec_in;
            auto exec_out = ioctx.exec(obj_name, "hello", "say_hello",
                                      std::move(exec_in)).get();
            std::string exec_str(exec_out.c_str(), exec_out.length());
            logger().info("exec hello say_hello returned: {}", exec_str);

            rados.shutdown().get();
            logger().info("crimson-rados-demo completed successfully");
            return EXIT_SUCCESS;
          } catch (const std::exception& e) {
            logger().error("crimson-rados-demo failed: {}", e.what());
            return EXIT_FAILURE;
          }
        });
      });
  } catch (const std::exception& e) {
    std::cerr << "FATAL: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
}
