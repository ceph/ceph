// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/escape.h"
#include "common/errno.h"
#include "common/options.h"
#include "global/global_context.h"
#include "include/stringify.h"

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"

#include <iostream>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace config {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

const std::string METADATA_CONF_PREFIX = "conf_";

void add_config_entity_option(
    boost::program_options::options_description *positional) {
  positional->add_options()
    ("config-entity", "config entity (global, client, client.<id>)");
}

void add_pool_option(boost::program_options::options_description *positional) {
  positional->add_options()
    ("pool-name", "pool name");
}

void add_key_option(po::options_description *positional) {
  positional->add_options()
    ("key", "config key");
}

int get_config_entity(const po::variables_map &vm, std::string *config_entity) {
  *config_entity = utils::get_positional_argument(vm, 0);

  if (*config_entity != "global" && *config_entity != "client" &&
      !boost::starts_with(*config_entity, ("client."))) {
    std::cerr << "rbd: invalid config entity: " << *config_entity
              << " (must be global, client or client.<id>)" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_pool(const po::variables_map &vm, std::string *pool_name) {
  *pool_name = utils::get_positional_argument(vm, 0);
  if (pool_name->empty()) {
    std::cerr << "rbd: pool name was not specified" << std::endl;
    return -EINVAL;
  }

  return 0;
}

int get_key(const po::variables_map &vm, size_t *arg_index,
            std::string *key) {
  *key = utils::get_positional_argument(vm, *arg_index);
  if (key->empty()) {
    std::cerr << "rbd: config key was not specified" << std::endl;
    return -EINVAL;
  } else {
    ++(*arg_index);
  }

  if (!boost::starts_with(*key, "rbd_")) {
    std::cerr << "rbd: not rbd option: " << *key << std::endl;
    return -EINVAL;
  }

  std::string value;
  int r = g_ceph_context->_conf.get_val(key->c_str(), &value);
  if (r < 0) {
    std::cerr << "rbd: invalid config key: " << *key << std::endl;
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

int config_global_list(
    librados::Rados &rados, const std::string &config_entity,
    std::map<std::string, std::pair<std::string, std::string>> *options) {
  bool client_id_config_entity =
    boost::starts_with(config_entity, ("client."));
  std::string cmd =
    "{"
      "\"prefix\": \"config dump\", "
      "\"format\": \"json\" "
    "}";
  bufferlist in_bl;
  bufferlist out_bl;
  std::string ss;
  int r = rados.mon_command(cmd, in_bl, &out_bl, &ss);
  if (r < 0) {
    std::cerr << "rbd: error reading config: " << ss << std::endl;
    return r;
  }

  json_spirit::mValue json_root;
  if (!json_spirit::read(out_bl.to_str(), json_root)) {
    std::cerr << "rbd: error parsing config dump" << std::endl;
    return -EINVAL;
  }

  try {
    auto &json_array = json_root.get_array();
    for (auto& e : json_array) {
      auto &json_obj = e.get_obj();
      std::string section;
      std::string name;
      std::string value;

      for (auto &pairs : json_obj) {
        if (pairs.first == "section") {
          section = pairs.second.get_str();
        } else if (pairs.first == "name") {
          name = pairs.second.get_str();
        } else if (pairs.first == "value") {
          value = pairs.second.get_str();
        }
      }

      if (!boost::starts_with(name, "rbd_")) {
        continue;
      }
      if (section != "global" && section != "client" &&
          (!client_id_config_entity || section != config_entity)) {
        continue;
      }
      if (config_entity == "global" && section != "global") {
        continue;
      }
      auto it = options->find(name);
      if (it == options->end()) {
        (*options)[name] = {value, section};
        continue;
      }
      if (section == "client") {
        if (it->second.second == "global") {
          it->second = {value, section};
        }
      } else if (client_id_config_entity) {
        it->second = {value, section};
      }
    }
  } catch (std::runtime_error &e) {
    std::cerr << "rbd: error parsing config dump: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0;
}

} // anonymous namespace

void get_global_get_arguments(po::options_description *positional,
                              po::options_description *options) {
  add_config_entity_option(positional);
  add_key_option(positional);
}

int execute_global_get(const po::variables_map &vm,
                       const std::vector<std::string> &ceph_global_init_args) {
  std::string config_entity;
  int r = get_config_entity(vm, &config_entity);
  if (r < 0) {
    return r;
  }

  std::string key;
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  std::map<std::string, std::pair<std::string, std::string>> options;
  r = config_global_list(rados, config_entity, &options);
  if (r < 0) {
    return r;
  }

  auto it = options.find(key);

  if (it == options.end() || it->second.second != config_entity) {
    std::cerr << "rbd: " << key << " is not set" << std::endl;
    return -ENOENT;
  }

  std::cout << it->second.first << std::endl;
  return 0;
}

void get_global_set_arguments(po::options_description *positional,
                              po::options_description *options) {
  add_config_entity_option(positional);
  add_key_option(positional);
  positional->add_options()
    ("value", "config value");
}

int execute_global_set(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  std::string config_entity;
  int r = get_config_entity(vm, &config_entity);
  if (r < 0) {
    return r;
  }

  std::string key;
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  std::string value = utils::get_positional_argument(vm, 2);
  std::string cmd =
    "{"
      "\"prefix\": \"config set\", "
      "\"who\": \"" + stringify(json_stream_escaper(config_entity)) + "\", "
      "\"name\": \"" + key + "\", "
      "\"value\": \"" + stringify(json_stream_escaper(value)) + "\""
    "}";
  bufferlist in_bl;
  std::string ss;
  r = rados.mon_command(cmd, in_bl, nullptr, &ss);
  if (r < 0) {
    std::cerr << "rbd: error setting " << key << ": " << ss << std::endl;
    return r;
  }

  return 0;
}

void get_global_remove_arguments(po::options_description *positional,
                                 po::options_description *options) {
  add_config_entity_option(positional);
  add_key_option(positional);
}

int execute_global_remove(
    const po::variables_map &vm,
    const std::vector<std::string> &ceph_global_init_args) {
  std::string config_entity;
  int r = get_config_entity(vm, &config_entity);
  if (r < 0) {
    return r;
  }

  std::string key;
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  std::string cmd =
    "{"
      "\"prefix\": \"config rm\", "
      "\"who\": \"" + stringify(json_stream_escaper(config_entity)) + "\", "
      "\"name\": \"" + key + "\""
    "}";
  bufferlist in_bl;
  std::string ss;
  r = rados.mon_command(cmd, in_bl, nullptr, &ss);
  if (r < 0) {
    std::cerr << "rbd: error removing " << key << ": " << ss << std::endl;
    return r;
  }

  return 0;
}

void get_global_list_arguments(po::options_description *positional,
                             po::options_description *options) {
  add_config_entity_option(positional);
  at::add_format_options(options);
}

int execute_global_list(const po::variables_map &vm,
                        const std::vector<std::string> &ceph_global_init_args) {
  std::string config_entity;
  int r = get_config_entity(vm, &config_entity);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter f;
  r = utils::get_formatter(vm, &f);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  r = utils::init_rados(&rados);
  if (r < 0) {
    return r;
  }

  std::map<std::string, std::pair<std::string, std::string>> options;
  r = config_global_list(rados, config_entity, &options);
  if (r < 0) {
    return r;
  }

  if (options.empty() && !f) {
    return 0;
  }

  TextTable tbl;

  if (f) {
    f->open_array_section("config");
  } else {
    tbl.define_column("Name", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Value", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Section", TextTable::LEFT, TextTable::LEFT);
  }

  for (const auto &it : options) {
    if (f) {
      f->open_object_section("option");
      f->dump_string("name", it.first);
      f->dump_string("value", it.second.first);
      f->dump_string("section", it.second.second);
      f->close_section();
    } else {
      tbl << it.first << it.second.first << it.second.second
          << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << tbl;
  }

  return 0;
}

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
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
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
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
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
  size_t arg_index = 1;
  r = get_key(vm, &arg_index, &key);
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
    std::cout << tbl;
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
  r = get_key(vm, &arg_index, &key);
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
    std::cout << tbl;
  }

  return 0;
}

Shell::Action action_global_get(
  {"config", "global", "get"}, {},
   "Get a global-level configuration override.", "",
   &get_global_get_arguments, &execute_global_get);
Shell::Action action_global_set(
  {"config", "global", "set"}, {},
   "Set a global-level configuration override.", "",
   &get_global_set_arguments, &execute_global_set);
Shell::Action action_global_remove(
  {"config", "global", "remove"}, {"config", "global", "rm"},
   "Remove a global-level configuration override.", "",
   &get_global_remove_arguments, &execute_global_remove);
Shell::Action action_global_list(
  {"config", "global", "list"}, {"config", "global", "ls"},
   "List global-level configuration overrides.", "",
   &get_global_list_arguments, &execute_global_list);

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
