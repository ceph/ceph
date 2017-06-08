// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <iostream>
#include <map>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace feature {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options, bool enabled) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("features", po::value<at::ImageFeatures>()->multitoken(),
     ("image features\n" + at::get_short_features_help(false)).c_str());
  if (enabled) {
    at::add_create_journal_options(options);
  }
}

void get_arguments_disable(po::options_description *positional,
			   po::options_description *options) {
  get_arguments(positional, options, false);
}

void get_arguments_enable(po::options_description *positional,
			  po::options_description *options) {
  get_arguments(positional, options, true);
}

int execute(const po::variables_map &vm, bool enabled) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_journal_options(vm, &opts);
  if (r < 0) {
    return r;
  }

  const std::vector<std::string> &args = vm[at::POSITIONAL_ARGUMENTS]
    .as<std::vector<std::string> >();
  std::vector<std::string> feature_names(args.begin() + 1, args.end());
  if (feature_names.empty()) {
    std::cerr << "rbd: at least one feature name must be specified"
              << std::endl;
    return -EINVAL;
  }

  boost::any features_any(static_cast<uint64_t>(0));
  at::ImageFeatures image_features;
  at::validate(features_any, feature_names, &image_features, 0);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.update_features(boost::any_cast<uint64_t>(features_any), enabled);
  if (r < 0) {
    std::cerr << "rbd: failed to update image features: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

int execute_disable(const po::variables_map &vm) {
  return execute(vm, false);
}

int execute_enable(const po::variables_map &vm) {
  return execute(vm, true);
}

Shell::Action action_disable(
  {"feature", "disable"}, {}, "Disable the specified image feature.", "",
  &get_arguments_disable, &execute_disable);
Shell::Action action_enable(
  {"feature", "enable"}, {}, "Enable the specified image feature.", "",
  &get_arguments_enable, &execute_enable);

} // namespace feature
} // namespace action
} // namespace rbd
