// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <sys/types.h>
#include <unistd.h>

#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "common/ceph_argparse.h"
#include "crimson/common/config_proxy.h"

#include "osd.h"

using config_t = ceph::common::ConfigProxy;

void usage(const char* prog) {
  std::cout << "usage: " << prog << " -i <ID>" << std::endl;
  generic_server_usage();
}

auto partition_args(seastar::app_template& app, int argc, char* argv[])
{
  namespace bpo = boost::program_options;
  // collect all options consumed by seastar::app_template
  auto parsed = bpo::command_line_parser(argc, argv)
    .options(app.get_options_description()).allow_unregistered().run();
  const auto unknown_args = bpo::collect_unrecognized(parsed.options,
                                                      bpo::include_positional);
  std::vector<const char*> ceph_args, app_args;
  bool consume_next_arg = false;
  std::partition_copy(
    argv, argv + argc,
    std::back_inserter(ceph_args),
    std::back_inserter(app_args),
    [begin=unknown_args.begin(),
     end=unknown_args.end(),
     &consume_next_arg](const char* arg) {
      if (std::find(begin, end, arg) != end) {
        return true;
      } else if (std::strcmp(arg, "-c") == 0) {
        // ceph_argparse_early_args() and
        // seastar::smp::get_options_description() use "-c" for different
        // options. and ceph wins
        consume_next_arg = true;
        return true;
      } else if (consume_next_arg) {
        consume_next_arg = false;
        return true;
      } else {
        return false;
      }
    });
  return make_pair(std::move(ceph_args), std::move(app_args));
}

int main(int argc, char* argv[])
{
  seastar::app_template app;
  app.add_options()
    ("mkfs", "create a [new] data directory");

  auto [ceph_args, app_args] = partition_args(app, argc, argv);
  if (ceph_argparse_need_usage(ceph_args)) {
    usage(argv[0]);
    return EXIT_SUCCESS;
  }
  std::string cluster;
  std::string conf_file_list;
  // ceph_argparse_early_args() could _exit(), while local_conf() won't ready
  // until it's started. so do the boilerplate-settings parsing here.
  auto init_params = ceph_argparse_early_args(ceph_args,
                                              CEPH_ENTITY_TYPE_OSD,
                                              &cluster,
                                              &conf_file_list);
  seastar::sharded<OSD> osd;
  using ceph::common::sharded_conf;
  using ceph::common::sharded_perf_coll;
  using ceph::common::local_conf;
  try {
    return app.run_deprecated(app_args.size(), const_cast<char**>(app_args.data()), [&] {
      auto& config = app.configuration();
      seastar::engine().at_exit([] {
        return sharded_conf().stop();
      });
      seastar::engine().at_exit([] {
        return sharded_perf_coll().stop();
      });
      seastar::engine().at_exit([&] {
       return osd.stop();
      });
      return sharded_conf().start(init_params.name, cluster).then([] {
        return sharded_perf_coll().start();
      }).then([&conf_file_list] {
        return local_conf().parse_config_files(conf_file_list);
      }).then([&] {
        return local_conf().parse_argv(ceph_args);
      }).then([&] {
        return osd.start_single(std::stoi(local_conf()->name.get_id()),
                                static_cast<uint32_t>(getpid()));
      }).then([&osd, mkfs = config.count("mkfs")] {
        if (mkfs) {
          return osd.invoke_on(0, &OSD::mkfs,
                               local_conf().get_val<uuid_d>("fsid"))
            .then([] { seastar::engine().exit(0); });
        } else {
          return osd.invoke_on(0, &OSD::start);
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
