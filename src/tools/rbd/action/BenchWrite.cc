// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/strtol.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace bench_write {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

struct Size {};
struct IOPattern {};

void validate(boost::any& v, const std::vector<std::string>& values,
              Size *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_sistrtoll(s.c_str(), &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(size);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              IOPattern *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "rand") {
    v = boost::any(true);
  } else if (s == "seq") {
    v = boost::any(false);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }

}

} // anonymous namespace

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  // TODO
  options->add_options()
    ("io-size", po::value<Size>(), "write size (in B/K/M/G/T)")
    ("io-threads", po::value<uint32_t>(), "ios in flight")
    ("io-total", po::value<Size>(), "total size to write (in B/K/M/G/T)")
    ("io-pattern", po::value<IOPattern>(), "write pattern (rand or seq)");
}

int execute(const po::variables_map &vm) {
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

  uint64_t bench_io_size;
  if (vm.count("io-size")) {
    bench_io_size = vm["io-size"].as<uint64_t>();
  } else {
    bench_io_size = 4096;
  }

  uint32_t bench_io_threads;
  if (vm.count("io-threads")) {
    bench_io_threads = vm["io-threads"].as<uint32_t>();
  } else {
    bench_io_threads = 16;
  }

  uint64_t bench_bytes;
  if (vm.count("io-total")) {
    bench_bytes = vm["io-total"].as<uint64_t>();
  } else {
    bench_bytes = 1 << 30;
  }

  bool bench_random;
  if (vm.count("io-pattern")) {
    bench_random = vm["io-pattern"].as<bool>();
  } else {
    bench_random = false;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false, &rados,
                                 &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  return 0;
}

Shell::Action action(
  {"bench-write"}, {}, "Simple write benchmark.", "", &get_arguments, &execute);

} // namespace bench_write
} // namespace action
} // namespace rbd
