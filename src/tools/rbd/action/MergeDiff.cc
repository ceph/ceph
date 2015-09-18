// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

#define dout_subsys ceph_subsys_rbd

namespace rbd {
namespace action {
namespace merge_diff {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  positional->add_options()
    ("diff1-path", "path to first diff (or '-' for stdin)")
    ("diff2-path", "path to second diff");
  at::add_path_options(positional, options,
                       "path to merged diff (or '-' for stdout)");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  std::string first_diff = utils::get_positional_argument(vm, 0);
  if (first_diff.empty()) {
    std::cerr << "rbd: first diff was not specified" << std::endl;
    return -EINVAL;
  }

  std::string second_diff = utils::get_positional_argument(vm, 1);
  if (second_diff.empty()) {
    std::cerr << "rbd: second diff was not specified" << std::endl;
    return -EINVAL;
  }

  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 2),
                          &path);
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::Action action(
  {"merge-diff"}, {}, "Merge two diff exports together.", "",
  &get_arguments, &execute);

} // namespace merge_diff
} // namespace action
} // namespace rbd
