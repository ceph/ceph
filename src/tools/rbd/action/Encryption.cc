// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "include/scope_guard.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <fstream>
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace encryption {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("format", "encryption format [possible values: luks1, luks2]")
    ("passphrase-file",
      "path of file containing passphrase for unlocking the image");
  options->add_options()
    ("cipher-alg", po::value<at::EncryptionAlgorithm>(),
     "encryption algorithm [possible values: aes-128, aes-256 (default)]");
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string format_str = utils::get_positional_argument(vm, arg_index++);
  if (format_str.empty()) {
    std::cerr << "rbd: must specify format." << std::endl;
    return -EINVAL;
  }

  std::string passphrase_file =
          utils::get_positional_argument(vm, arg_index++);
  if (passphrase_file.empty()) {
    std::cerr << "rbd: must specify passphrase-file." << std::endl;
    return -EINVAL;
  }

  std::ifstream file(passphrase_file, std::ios::in | std::ios::binary);
  if (file.fail()) {
    std::cerr << "rbd: unable to open passphrase file " << passphrase_file
              << ": " << cpp_strerror(errno) << std::endl;
    return -errno;
  }
  std::string passphrase((std::istreambuf_iterator<char>(file)),
                         (std::istreambuf_iterator<char>()));
  auto sg = make_scope_guard([&] {
      ceph_memzero_s(&passphrase[0], passphrase.size(), passphrase.size()); });
  file.close();

  auto alg = RBD_ENCRYPTION_ALGORITHM_AES256;
  if (vm.count("cipher-alg")) {
    alg = vm["cipher-alg"].as<librbd::encryption_algorithm_t>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  if (format_str == "luks1") {
    librbd::encryption_luks1_format_options_t opts = {};
    opts.alg = alg;
    opts.passphrase = passphrase;
    r = image.encryption_format(
            RBD_ENCRYPTION_FORMAT_LUKS1, &opts, sizeof(opts));
  } else if (format_str == "luks2") {
    librbd::encryption_luks2_format_options_t opts = {};
    opts.alg = alg;
    opts.passphrase = passphrase;
    r = image.encryption_format(
            RBD_ENCRYPTION_FORMAT_LUKS2, &opts, sizeof(opts));
  } else {
    std::cerr << "rbd: unsupported encryption format" << std::endl;
    return -ENOTSUP;
  }

  if (r < 0) {
    std::cerr << "rbd: encryption format error: " << cpp_strerror(r)
              << std::endl;
  }
  return r;
}

Shell::Action action(
  {"encryption", "format"}, {}, "Format image to an encrypted format.", "",
  &get_arguments, &execute);

} // namespace encryption
} // namespace action
} // namespace rbd
