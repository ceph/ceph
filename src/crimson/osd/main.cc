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

int main(int argc, char* argv[])
{
  std::vector<const char*> args{argv + 1, argv + argc};
  if (ceph_argparse_need_usage(args)) {
    usage(argv[0]);
    return EXIT_SUCCESS;
  }
  std::string cluster;
  std::string conf_file_list;
  // ceph_argparse_early_args() could _exit(), while local_conf() won't ready
  // until it's started. so do the boilerplate-settings parsing here.
  auto init_params = ceph_argparse_early_args(args,
                                              CEPH_ENTITY_TYPE_OSD,
                                              &cluster,
                                              &conf_file_list);
  seastar::app_template app;
  app.add_options()
    ("mkfs", "create a [new] data directory");
  seastar::sharded<OSD> osd;

  using ceph::common::sharded_conf;
  using ceph::common::sharded_perf_coll;
  using ceph::common::local_conf;

  args.insert(begin(args), argv[0]);
  try {
    return app.run_deprecated(args.size(), const_cast<char**>(args.data()), [&] {
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
        return osd.start_single(std::stoi(local_conf()->name.get_id()),
                                static_cast<uint32_t>(getpid()));
      }).then([&osd, mkfs = config.count("mkfs")] {
        if (mkfs) {
          return osd.invoke_on(0, &OSD::mkfs,
                               local_conf().get_val<uuid_d>("fsid"));
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
