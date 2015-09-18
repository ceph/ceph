// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace export_diff {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_SOURCE);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    (at::WHOLE_OBJECT.c_str(), po::bool_switch(), "compare whole object");
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::SwitchArguments switched_arguments({at::WHOLE_OBJECT});
Shell::Action action(
  {"export-diff"}, {}, "Export incremental diff to file.", "",
  &get_arguments, &execute);

} // namespace export_diff
} // namespace action
} // namespace rbd
