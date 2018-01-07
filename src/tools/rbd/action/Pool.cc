// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "osd/osd_types.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace pool {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_init(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options);
  options->add_options()
      ("force", po::bool_switch(),
       "force initialize pool for RBD use if registered by another application");
}

int execute_init(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = io_ctx.application_enable(pg_pool_t::APPLICATION_NAME_RBD,
                                vm["force"].as<bool>());
  if (r == -EOPNOTSUPP) {
    std::cerr << "rbd: luminous or later release required." << std::endl;
  } else if (r == -EPERM) {
    std::cerr << "rbd: pool already registered to a different application."
              << std::endl;
  } else if (r < 0) {
    std::cerr << "rbd: error registered application: " << cpp_strerror(r)
              << std::endl;
  }

  return 0;
}

Shell::Action action(
  {"pool", "init"}, {}, "Initialize pool for use by RBD.", "",
    &get_arguments_init, &execute_init);

} // namespace pool
} // namespace action
} // namespace rbd
