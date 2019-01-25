// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace image_meta {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

void add_key_option(po::options_description *positional) {
  positional->add_options()
    ("key", "image meta key");
}

int get_key(const po::variables_map &vm, size_t *arg_index,
            std::string *key) {
  *key = utils::get_positional_argument(vm, *arg_index);
  if (key->empty()) {
    std::cerr << "rbd: metadata key was not specified" << std::endl;
    return -EINVAL;
  } else {
    ++(*arg_index);
  }
  return 0;
}

const uint32_t MAX_KEYS = 64;

} // anonymous namespace

static int do_metadata_list(librbd::Image& image, Formatter *f)
{
  int r;
  TextTable tbl;

  size_t count = 0;
  std::string last_key;
  bool more_results = true;
  while (more_results) {
    std::map<std::string, bufferlist> pairs;
    r = image.metadata_list(last_key, MAX_KEYS, &pairs);
    if (r < 0) {
      std::cerr << "failed to list metadata of image : " << cpp_strerror(r)
                << std::endl;
      return r;
    }

    more_results = (pairs.size() == MAX_KEYS);
    if (!pairs.empty()) {
      if (count == 0) {
        if (f) {
          f->open_object_section("metadatas");
        } else {
          tbl.define_column("Key", TextTable::LEFT, TextTable::LEFT);
          tbl.define_column("Value", TextTable::LEFT, TextTable::LEFT);
        }
      }

      last_key = pairs.rbegin()->first;
      count += pairs.size();

      for (auto kv : pairs) {
        std::string val(kv.second.c_str(), kv.second.length());
        if (f) {
          f->dump_string(kv.first.c_str(), val.c_str());
        } else {
          tbl << kv.first << val << TextTable::endrow;
        }
      }
    }
  }

  if (f == nullptr) {
    bool single = (count == 1);
    std::cout << "There " << (single ? "is" : "are") << " " << count << " "
              << (single ? "metadatum" : "metadata") << " on this image"
              << (count == 0 ? "." : ":") << std::endl;
  }

  if (count > 0) {
    if (f) {
      f->close_section();
      f->flush(std::cout);
    } else {
      std::cout << std::endl << tbl;
    }
  }
  return 0;
}

static int do_metadata_set(librbd::Image& image, std::string &key,
                          std::string &value)
{
  int r = image.metadata_set(key, value);
  if (r < 0) {
    std::cerr << "failed to set metadata " << key << " of image : "
              << cpp_strerror(r) << std::endl;
  }
  return r;
}

static int do_metadata_remove(librbd::Image& image, std::string &key)
{
  int r = image.metadata_remove(key);
  if (r == -ENOENT) {
      std::cerr << "rbd: no existing metadata key " << key << " of image : "
                << cpp_strerror(r) << std::endl;
  } else if(r < 0) {
      std::cerr << "failed to remove metadata " << key << " of image : "
                << cpp_strerror(r) << std::endl;
  }
  return r;
}

static int do_metadata_get(librbd::Image& image, std::string &key)
{
  std::string s;
  int r = image.metadata_get(key, &s);
  if (r < 0) {
    std::cerr << "failed to get metadata " << key << " of image : "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  std::cout << s << std::endl;
  return r;
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm,
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

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_metadata_list(image, formatter.get());
  if (r < 0) {
    std::cerr << "rbd: listing metadata failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void get_get_arguments(po::options_description *positional,
                       po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
}

int execute_get(const po::variables_map &vm,
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

  std::string key;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_metadata_get(image, key);
  if (r < 0) {
    std::cerr << "rbd: getting metadata failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void get_set_arguments(po::options_description *positional,
                       po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
  positional->add_options()
    ("value", "image meta value");
}

int execute_set(const po::variables_map &vm,
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

  std::string key;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  std::string value = utils::get_positional_argument(vm, 2);
  if (value.empty()) {
    std::cerr << "rbd: metadata value was not specified" << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_metadata_set(image, key, value);
  if (r < 0) {
    std::cerr << "rbd: setting metadata failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
}

int execute_remove(const po::variables_map &vm,
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

  std::string key;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_metadata_remove(image, key);
  if (r < 0) {
    std::cerr << "rbd: removing metadata failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_list(
  {"image-meta", "list"}, {"image-meta", "ls"}, "Image metadata list keys with values.", "",
  &get_list_arguments, &execute_list);
Shell::Action action_get(
  {"image-meta", "get"}, {},
  "Image metadata get the value associated with the key.", "",
  &get_get_arguments, &execute_get);
Shell::Action action_set(
  {"image-meta", "set"}, {}, "Image metadata set key with value.", "",
  &get_set_arguments, &execute_set);
Shell::Action action_remove(
  {"image-meta", "remove"}, {"image-meta", "rm"},
  "Image metadata remove the key and value associated.", "",
  &get_remove_arguments, &execute_remove);

} // namespace image_meta
} // namespace action
} // namespace rbd
