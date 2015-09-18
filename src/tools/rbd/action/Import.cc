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
namespace import {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
  at::add_create_image_options(options, true);
  at::add_no_progress_option(options);

  // TODO legacy rbd allowed import to accept both 'image'/'dest' and
  //      'pool'/'dest-pool'
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE, " (deprecated)");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE, " (deprecated)");
}

int execute(const po::variables_map &vm) {
  std::string path;
  int r = utils::get_path(vm, utils::get_positional_argument(vm, 0), &path);
  if (r < 0) {
    return r;
  }

  // odd check to support legacy / deprecated behavior of import
  std::string deprecated_pool_name;
  if (vm.count(at::POOL_NAME)) {
    deprecated_pool_name = vm[at::POOL_NAME].as<std::string>();
    std::cerr << "rbd: --pool is deprecated for import, use --dest-pool"
              << std::endl;
  }

  std::string deprecated_image_name;
  if (vm.count(at::IMAGE_NAME)) {
    utils::extract_spec(vm[at::IMAGE_NAME].as<std::string>(),
                        &deprecated_pool_name, &deprecated_image_name, nullptr);
    std::cerr << "rbd: --image is deprecated for import, use --dest"
              << std::endl;
  } else {
    deprecated_image_name = path.substr(path.find_last_of("/") + 1);
  }

  size_t arg_index = 1;
  std::string pool_name = deprecated_pool_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, false);
  if (r < 0) {
    return r;
  }

  if (image_name.empty()) {
    image_name = deprecated_image_name;
  }

  int order;
  uint32_t format;
  uint64_t features;
  uint32_t stripe_unit;
  uint32_t stripe_count;
  r = utils::get_image_options(vm, &order, &format, &features, &stripe_unit,
                               &stripe_count);
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

Shell::Action action(
  {"import"}, {}, "Import image from file.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace import
} // namespace action
} // namespace rbd
