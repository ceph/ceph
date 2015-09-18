// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace list {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  positional->add_options()
    ("pool-name", "pool name");
  options->add_options()
    ("long,l", po::bool_switch(), "long listing format")
    ("pool,p", po::value<std::string>(), "pool name");
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  std::string pool_name = utils::get_positional_argument(vm, 0);
  if (pool_name.empty() && vm.count("pool")) {
    pool_name = vm["pool"].as<std::string>();
  }

  if (pool_name.empty()) {
    pool_name = at::DEFAULT_POOL_NAME;
  }

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::SwitchArguments switched_arguments({"long", "l"});
Shell::Action action(
  {"list"}, {"ls"}, "List rbd images.", "", &get_arguments, &execute);

} // namespace list
} // namespace action
} // namespace rbd
