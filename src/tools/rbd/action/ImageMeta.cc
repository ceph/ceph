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

int get_key(const po::variables_map &vm, std::string *key) {
  *key = utils::get_positional_argument(vm, 1);
  if (key->empty()) {
    std::cerr << "rbd: metadata key was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

} // anonymous namespace

static int do_metadata_list(librbd::Image& image, Formatter *f)
{
  std::map<std::string, bufferlist> pairs;
  int r;
  TextTable tbl;

  r = image.metadata_list("", 0, &pairs);
  if (r < 0) {
    std::cerr << "failed to list metadata of image : " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  if (f) {
    f->open_object_section("metadatas");
  } else {
    tbl.define_column("Key", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Value", TextTable::LEFT, TextTable::LEFT);
  }

  if (!pairs.empty()) {
    bool one = (pairs.size() == 1);

    if (!f) {
      std::cout << "There " << (one ? "is " : "are ") << pairs.size()
           << " metadata" << (one ? "" : "s") << " on this image.\n";
    }

    for (std::map<std::string, bufferlist>::iterator it = pairs.begin();
         it != pairs.end(); ++it) {
      std::string val(it->second.c_str(), it->second.length());
      if (f) {
        f->dump_string(it->first.c_str(), val.c_str());
      } else {
        tbl << it->first << val.c_str() << TextTable::endrow;
      }
    }
    if (!f)
      std::cout << tbl;
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }
  return 0;
}

static int do_metadata_set(librbd::Image& image, const char *key,
                          const char *value)
{
  int r = image.metadata_set(key, value);
  if (r < 0) {
    std::cerr << "failed to set metadata " << key << " of image : "
              << cpp_strerror(r) << std::endl;
  }
  return r;
}

static int do_metadata_remove(librbd::Image& image, const char *key)
{
  int r = image.metadata_remove(key);
  if (r < 0) {
    std::cerr << "failed to remove metadata " << key << " of image : "
              << cpp_strerror(r) << std::endl;
  }
  return r;
}

static int do_metadata_get(librbd::Image& image, const char *key)
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

int execute_list(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
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
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
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

int execute_get(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string key;
  r = get_key(vm, &key);
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

  r = do_metadata_get(image, key.c_str());
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

int execute_set(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string key;
  r = get_key(vm, &key);
  if (r < 0) {
    return r;
  }

  std::string value = utils::get_positional_argument(vm, 2);
  if (value.empty()) {
    std::cerr << "rbd: metadata value was not specified" << std::endl;
    return -EINVAL;
  }

  if ((key.compare("rbd_cache") == 0) || (key.compare("rbd_cache_writethrough_until_flush") == 0)) {
    if ((value.compare("true") != 0) && (key.compare("false") != 0)) {
      std::cerr << "rbd: rbd_cache or rbd_cache_writethrough_until_flush must be true or false" << std::endl;
      return -EINVAL;
    }
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_metadata_set(image, key.c_str(), value.c_str());
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

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string key;
  r = get_key(vm, &key);
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

  r = do_metadata_remove(image, key.c_str());
  if (r < 0) {
    std::cerr << "rbd: removing metadata failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_list(
  {"image-meta", "list"}, {}, "Image metadata list keys with values.", "",
  &get_list_arguments, &execute_list);
Shell::Action action_get(
  {"image-meta", "get"}, {},
  "Image metadata get the value associated with the key.", "",
  &get_get_arguments, &execute_get);
Shell::Action action_set(
  {"image-meta", "set"}, {}, "Image metadata set key with value.", "",
  &get_set_arguments, &execute_set);
Shell::Action action_remove(
  {"image-meta", "remove"}, {},
  "Image metadata remove the key and value associated.", "",
  &get_remove_arguments, &execute_remove);

} // namespace image_meta
} // namespace action
} // namespace rbd
