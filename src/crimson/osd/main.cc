// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <sys/types.h>
#include <unistd.h>

#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>

#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"
#include "crimson/net/SocketMessenger.h"

#include "osd.h"

using config_t = ceph::common::ConfigProxy;

void usage(const char* prog) {
  std::cout << "usage: " << prog << " -i <ID>" << std::endl;
  generic_server_usage();
}

auto partition_args(seastar::app_template& app, char** argv_begin, char** argv_end)
{
  namespace bpo = boost::program_options;
  // collect all options consumed by seastar::app_template
  auto parsed = bpo::command_line_parser(std::distance(argv_begin, argv_end),
                                         argv_begin)
    .options(app.get_options_description()).allow_unregistered().run();
  auto unknown_args = bpo::collect_unrecognized(parsed.options,
                                                bpo::include_positional);
  std::vector<const char*> ceph_args, app_args;
  // ceph_argparse_early_args() and
  // seastar::smp::get_options_description() use "-c" for different
  // options. and ceph wins
  auto consume_conf_arg = [&](char** argv) {
    if (std::strcmp(*argv, "-c") == 0) {
      ceph_args.push_back(*argv++);
      if (argv != argv_end) {
        ceph_args.push_back(*argv++);
      }
    }
    return argv;
  };
  auto unknown = unknown_args.begin();
  auto consume_ceph_arg = [&](char** argv) {
    while (unknown != unknown_args.end() && argv != argv_end &&
           *unknown == *argv) {
      ++unknown;
      ceph_args.push_back(*argv++);
    }
    return argv;
  };
  for (auto argv = argv_begin; argv != argv_end;) {
    if (auto next_arg = consume_conf_arg(argv); next_arg != argv) {
      argv = next_arg;
    } else if (auto next_arg = consume_ceph_arg(argv); next_arg != argv) {
      argv = next_arg;
    } else {
      app_args.push_back(*argv++);
    }
  }
  return make_pair(std::move(ceph_args), std::move(app_args));
}

int main(int argc, char* argv[])
{
  seastar::app_template app;
  app.add_options()
    ("mkfs", "create a [new] data directory");

  auto [ceph_args, app_args] = partition_args(app, argv, argv + argc);
  if (ceph_argparse_need_usage(ceph_args)) {
    usage(argv[0]);
    return EXIT_SUCCESS;
  }
  std::string cluster_name;
  std::string conf_file_list;
  // ceph_argparse_early_args() could _exit(), while local_conf() won't ready
  // until it's started. so do the boilerplate-settings parsing here.
  auto init_params = ceph_argparse_early_args(ceph_args,
                                              CEPH_ENTITY_TYPE_OSD,
                                              &cluster_name,
                                              &conf_file_list);
  seastar::sharded<OSD> osd;
  seastar::sharded<ceph::net::SocketMessenger> cluster_msgr, client_msgr;
  seastar::sharded<ceph::net::SocketMessenger> hb_front_msgr, hb_back_msgr;
  using ceph::common::sharded_conf;
  using ceph::common::sharded_perf_coll;
  using ceph::common::local_conf;
  try {
    return app.run_deprecated(app_args.size(), const_cast<char**>(app_args.data()), [&] {
      auto& config = app.configuration();
      return seastar::async([&] {
        sharded_conf().start(init_params.name, cluster_name).get();
        sharded_perf_coll().start().get();
        local_conf().parse_config_files(conf_file_list).get();
        local_conf().parse_argv(ceph_args).get();
        const int whoami = std::stoi(local_conf()->name.get_id());
        const auto nonce = static_cast<uint32_t>(getpid());
        const auto shard = seastar::engine().cpu_id();
        cluster_msgr.start(entity_name_t::OSD(whoami), "cluster"s, nonce, shard).get();
        client_msgr.start(entity_name_t::OSD(whoami), "client"s, nonce, shard).get();
        hb_front_msgr.start(entity_name_t::OSD(whoami), "hb_front"s, nonce, shard).get();
        hb_back_msgr.start(entity_name_t::OSD(whoami), "hb_back"s, nonce, shard).get();
        osd.start_single(whoami, nonce,
          reference_wrapper<ceph::net::Messenger>(cluster_msgr.local()),
          reference_wrapper<ceph::net::Messenger>(client_msgr.local()),
          reference_wrapper<ceph::net::Messenger>(hb_front_msgr.local()),
          reference_wrapper<ceph::net::Messenger>(hb_back_msgr.local())).get();
        seastar::engine().at_exit([&] {
          return osd.stop();
        });
        seastar::engine().at_exit([&] {
          return seastar::when_all_succeed(cluster_msgr.stop(),
                                           client_msgr.stop(),
                                           hb_front_msgr.stop(),
                                           hb_back_msgr.stop());
        });
        seastar::engine().at_exit([] {
          return sharded_perf_coll().stop();
        });
        seastar::engine().at_exit([] {
          return sharded_conf().stop();
        });

        if (config.count("mkfs")) {
          osd.invoke_on(0, &OSD::mkfs,
                        local_conf().get_val<uuid_d>("fsid"))
            .then([] { seastar::engine().exit(0); }).get();
        } else {
          osd.invoke_on(0, &OSD::start).get();
        }
      });
    });
  } catch (...) {
    seastar::fprint(std::cerr, "FATAL: Exception during startup, aborting: %s\n", std::current_exception());
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
