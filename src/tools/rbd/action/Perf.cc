// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace perf {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_perf_dump(librbd::Image& image, const std::string &format)
{
  bufferlist outbl;
  int r = image.perf_dump(format, &outbl);
  if (r < 0) {
    std::cerr << "failed to dump image perf counters" << std::endl;
    return r;
  }

  std::cout << outbl.c_str();
  return 0;
}

static int do_perf_reset(librbd::Image& image)
{
  int r = image.perf_reset();
  if (r < 0) {
    std::cerr << "failed to reset image perf counters" << std::endl;
    return r;
  }
  return 0;
}

void get_dump_arguments(po::options_description *positional,
                       po::options_description *options)
{
  at::add_image_spec_options(positional, options,
                             at::ARGUMENT_MODIFIER_NONE);
  // TODO: TypedValue
  options->add_options()
    (at::FORMAT.c_str(), po::value<std::string>(),
     "output format [json, json-pretty, xml, or xml-pretty]");
}

int execute_dump(const po::variables_map &vm)
{
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    nullptr, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string format = "json-pretty";
  if (vm.count(at::FORMAT)) {
    format = vm[at::FORMAT].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_perf_dump(image, format);
  if (r < 0) {
    std::cerr << "rbd: dump image perf counters error: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_reset_arguments(po::options_description *positional,
                         po::options_description *options)
{
  at::add_image_spec_options(positional, options,
                             at::ARGUMENT_MODIFIER_NONE);
}

int execute_reset(const po::variables_map &vm)
{
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    nullptr, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_perf_reset(image);
  if (r < 0) {
    std::cerr << "rbd: reset perf counter error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_dump(
  {"perf", "dump"}, {}, "Dump image perf counters.", "",
  &get_dump_arguments, &execute_dump);

Shell::Action action_reset(
  {"perf", "reset"}, {}, "Reset image perf counters.", "",
  &get_reset_arguments, &execute_reset);

} // namespace perf
} // namespace action
} // namespace rbd
