// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/config_proxy.h"
#include "common/errno.h"

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"

#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace config {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

const std::string METADATA_CONF_PREFIX = "conf_";
const uint32_t MAX_KEYS = 64;

void add_pool_option(boost::program_options::options_description *positional) {
  positional->add_options()
    ("pool-name", "pool name");
}

void add_key_option(po::options_description *positional) {
  positional->add_options()
    ("key", "config key");
}

int get_pool(const po::variables_map &vm, std::string *pool_name) {
  *pool_name = utils::get_positional_argument(vm, 0);
  if (pool_name->empty()) {
    std::cerr << "rbd: pool name was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_key(const po::variables_map &vm, std::string *key) {
  *key = utils::get_positional_argument(vm, 1);
  if (key->empty()) {
    std::cerr << "rbd: config key was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

std::ostream& operator<<(std::ostream& os,
                         const librbd::config_source_t& source) {
  switch (source) {
  case RBD_CONFIG_SOURCE_CONFIG:
    os << "config";
    break;
  case RBD_CONFIG_SOURCE_POOL:
    os << "pool";
    break;
  case RBD_CONFIG_SOURCE_IMAGE:
    os << "image";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(source) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

void get_pool_get_arguments(po::options_description *positional,
                            po::options_description *options) {
  add_pool_option(positional);
  add_key_option(positional);
}

int execute_pool_get(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  int r = get_pool(vm, &pool_name);
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
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::string value;

  r = rbd.pool_metadata_get(io_ctx, METADATA_CONF_PREFIX + key, &value);
  if (r < 0) {
    if (r == -ENOENT) {
      std::cerr << "rbd: " << key << " is not set" << std::endl;
    } else {
      std::cerr << "rbd: failed to get " << key << ": " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }

  std::cout << value << std::endl;
  return 0;
}

void get_pool_set_arguments(po::options_description *positional,
                            po::options_description *options) {
  add_pool_option(positional);
  add_key_option(positional);
  positional->add_options()
    ("value", "config value");
}

int execute_pool_set(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  int r = get_pool(vm, &pool_name);
  if (r < 0) {
    return r;
  }

  std::string key;
  r = get_key(vm, &key);
  if (r < 0) {
    return r;
  }

  std::string value = utils::get_positional_argument(vm, 2);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.pool_metadata_set(io_ctx, METADATA_CONF_PREFIX + key, value);
  if (r < 0) {
    std::cerr << "rbd: failed to set " << key << ": " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_pool_remove_arguments(po::options_description *positional,
                               po::options_description *options) {
  add_pool_option(positional);
  add_key_option(positional);
}

int execute_pool_remove(const po::variables_map &vm,
                        const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  int r = get_pool(vm, &pool_name);
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
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.pool_metadata_remove(io_ctx, METADATA_CONF_PREFIX + key);
  if (r < 0) {
    std::cerr << "rbd: failed to remove " << key << ": " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_pool_list_arguments(po::options_description *positional,
                             po::options_description *options) {
  add_pool_option(positional);
  at::add_format_options(options);
}

int execute_pool_list(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  int r = get_pool(vm, &pool_name);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter f;
  r = utils::get_formatter(vm, &f);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, "", &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  TextTable tbl;
  librbd::RBD rbd;
  std::vector<librbd::config_option_t> options;

  r = rbd.config_list(io_ctx, &options);
  if (r < 0) {
    std::cerr << "rbd: failed to list config: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (f) {
    f->open_array_section("config");
  } else {
    tbl.define_column("Name", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Value", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Source", TextTable::LEFT, TextTable::LEFT);
  }

  for (auto &option : options) {
    if (f) {
      f->open_object_section("option");
      f->dump_string("name", option.name);
      f->dump_string("value", option.value);
      f->dump_stream("source") << option.source;
      f->close_section();
    } else {
      std::ostringstream source;
      source << option.source;
      tbl << option.name << option.value << source.str() << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << std::endl << tbl;
  }

  return 0;
}

void get_image_get_arguments(po::options_description *positional,
                             po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
}

int execute_image_get(const po::variables_map &vm,
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
  r = get_key(vm, &key);
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

  std::string value;

  r = image.metadata_get(METADATA_CONF_PREFIX + key, &value);
  if (r < 0) {
    if (r == -ENOENT) {
      std::cerr << "rbd: " << key << " is not set" << std::endl;
    } else {
      std::cerr << "rbd: failed to get " << key << ": " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }

  std::cout << value << std::endl;
  return 0;
}

void get_image_set_arguments(po::options_description *positional,
                             po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
  positional->add_options()
    ("value", "config value");
}

int execute_image_set(const po::variables_map &vm,
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
  r = get_key(vm, &key);
  if (r < 0) {
    return r;
  }

  std::string value = utils::get_positional_argument(vm, 2);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = image.metadata_set(METADATA_CONF_PREFIX + key, value);
  if (r < 0) {
    std::cerr << "rbd: failed to set " << key << ": " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_image_remove_arguments(po::options_description *positional,
                                po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  add_key_option(positional);
}

int execute_image_remove(
    const po::variables_map &vm,
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
  r = get_key(vm, &key);
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

  r = image.metadata_remove(METADATA_CONF_PREFIX + key);
  if (r < 0) {
    std::cerr << "rbd: failed to remove " << key << ": " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void get_image_list_arguments(po::options_description *positional,
                              po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_image_list(const po::variables_map &vm,
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

  at::Format::Formatter f;
  r = utils::get_formatter(vm, &f);
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

  TextTable tbl;
  std::vector<librbd::config_option_t> options;

  r = image.config_list(&options);
  if (r < 0) {
    std::cerr << "rbd: failed to list config: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (options.empty()) {
    if (f == nullptr) {
      std::cout << "There are no values" << std::endl;
    }
    return 0;
  }

  if (f) {
    f->open_array_section("config");
  } else {
    tbl.define_column("Name", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Value", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Source", TextTable::LEFT, TextTable::LEFT);
  }

  for (auto &option : options) {
    if (f) {
      f->open_object_section("option");
      f->dump_string("name", option.name);
      f->dump_string("value", option.value);
      f->dump_stream("source") << option.source;
      f->close_section();
    } else {
      std::ostringstream source;
      source << option.source;
      tbl << option.name << option.value << source.str() << TextTable::endrow;
    }
  }

  if (f == nullptr) {
    bool single = (options.size() == 1);
    std::cout << "There " << (single ? "is" : "are") << " " << options.size()
              << " " << (single ? "value" : "values") << ":" << std::endl;
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << std::endl << tbl;
  }

  return 0;
}

Shell::Action action_pool_get(
  {"config", "pool", "get"}, {}, "Get a pool-level configuration override.", "",
   &get_pool_get_arguments, &execute_pool_get);
Shell::Action action_pool_set(
  {"config", "pool", "set"}, {}, "Set a pool-level configuration override.", "",
   &get_pool_set_arguments, &execute_pool_set);
Shell::Action action_pool_remove(
  {"config", "pool", "remove"}, {"config", "pool", "rm"},
   "Remove a pool-level configuration override.", "",
   &get_pool_remove_arguments, &execute_pool_remove);
Shell::Action action_pool_list(
  {"config", "pool", "list"}, {"config", "pool", "ls"},
   "List pool-level configuration overrides.", "",
   &get_pool_list_arguments, &execute_pool_list);

Shell::Action action_image_get(
  {"config", "image", "get"}, {}, "Get an image-level configuration override.",
   "", &get_image_get_arguments, &execute_image_get);
Shell::Action action_image_set(
  {"config", "image", "set"}, {}, "Set an image-level configuration override.",
   "", &get_image_set_arguments, &execute_image_set);
Shell::Action action_image_remove(
  {"config", "image", "remove"}, {"config", "image", "rm"},
   "Remove an image-level configuration override.", "",
   &get_image_remove_arguments, &execute_image_remove);
Shell::Action action_image_list(
  {"config", "image", "list"}, {"config", "image", "ls"},
   "List image-level configuration overrides.", "",
   &get_image_list_arguments, &execute_image_list);

} // namespace config
} // namespace action
} // namespace rbd
