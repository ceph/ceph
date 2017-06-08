// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "global/global_context.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>

namespace rbd {
namespace action {
namespace mirror_pool {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

int init_remote(const std::string &config_path, const std::string &client_name,
                const std::string &cluster_name, const std::string &pool_name,
                librados::Rados *rados, librados::IoCtx *io_ctx) {
  int r = rados->init2(client_name.c_str(), cluster_name.c_str(), 0);
  if (r < 0) {
    std::cerr << "rbd: couldn't initialize remote rados!" << std::endl;
    return r;
  }

  r = rados->conf_read_file(config_path.empty() ? nullptr :
                                                  config_path.c_str());
  if (r < 0) {
    std::cerr << "rbd: couldn't read remote configuration" << std::endl;
    return r;
  }

  r = rados->connect();
  if (r < 0) {
    std::cerr << "rbd: couldn't connect to the remote cluster!" << std::endl;
    return r;
  }

  if (io_ctx != nullptr) {
    r = utils::init_io_ctx(*rados, pool_name, io_ctx);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int validate_uuid(const std::string &uuid) {
  boost::regex pattern("^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$",
                       boost::regex::icase);
  boost::smatch match;
  if (!boost::regex_match(uuid, match, pattern)) {
    std::cerr << "rbd: invalid uuid '" << uuid << "'" << std::endl;
    return -EINVAL;
  }
  return 0;
}

void add_cluster_uuid_option(po::options_description *positional) {
  positional->add_options()
    ("cluster-uuid", po::value<std::string>(), "cluster UUID");
}

int get_cluster_uuid(const po::variables_map &vm, size_t arg_index,
                     std::string *cluster_uuid) {
  *cluster_uuid = utils::get_positional_argument(vm, arg_index);
  if (cluster_uuid->empty()) {
    std::cerr << "rbd: must specify cluster uuid" << std::endl;
    return -EINVAL;
  }
  return validate_uuid(*cluster_uuid);
}

int get_remote_cluster_spec(const po::variables_map &vm,
                            const std::string &spec,
                            std::string *remote_client_name,
                            std::string *remote_cluster,
                            std::string *remote_cluster_uuid) {
  if (vm.count("remote-client-name")) {
    *remote_client_name = vm["remote-client-name"].as<std::string>();
  }
  if (vm.count("remote-cluster")) {
    *remote_cluster = vm["remote-cluster"].as<std::string>();
  }
  if (vm.count("remote-cluster-uuid")) {
    *remote_cluster_uuid = vm["remote-cluster-uuid"].as<std::string>();
    int r = validate_uuid(*remote_cluster_uuid);
    if (r < 0) {
      return r;
    }
  }

  if (!spec.empty()) {
    boost::regex pattern("^(?:(client\\.[^@]+)@)?([^/@]+)$");
    boost::smatch match;
    if (!boost::regex_match(spec, match, pattern)) {
      std::cerr << "rbd: invalid spec '" << spec << "'" << std::endl;
      return -EINVAL;
    }
    if (match[1].matched) {
      *remote_client_name = match[1];
    }
    *remote_cluster = match[2];
  }

  if (remote_cluster->empty()) {
    std::cerr << "rbd: remote cluster was not specified" << std::endl;
    return -EINVAL;
  }
  return 0;
}

void format_mirror_peers(const std::string &config_path,
                         at::Format::Formatter formatter,
                         const std::vector<librbd::mirror_peer_t> &peers) {
  if (formatter != nullptr) {
    formatter->open_array_section("peers");
    for (auto &peer : peers) {
      formatter->open_object_section("peer");
      formatter->dump_string("cluster_uuid", peer.cluster_uuid);
      formatter->dump_string("cluster_name", peer.cluster_name);
      formatter->dump_string("client_name", peer.client_name);
      formatter->close_section();
    }
    formatter->close_section();
  } else {
    std::cout << "Peers: ";
    if (peers.empty()) {
      std::cout << "none" << std::endl;
    } else {
      TextTable tbl;
      tbl.define_column("", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("UUID", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("CLIENT", TextTable::LEFT, TextTable::LEFT);
      for (auto &peer : peers) {
        tbl << " "
            << peer.cluster_uuid
            << peer.cluster_name
            << peer.client_name
            << TextTable::endrow;
      }
      std::cout << std::endl << tbl;
    }
  }
}

} // anonymous namespace

void get_peer_add_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options);
  positional->add_options()
    ("remote-cluster-spec", "remote cluster spec\n"
     "(example: [<client name>@]<cluster name>");
  options->add_options()
    ("remote-client-name", po::value<std::string>(), "remote client name")
    ("remote-cluster", po::value<std::string>(), "remote cluster name")
    ("remote-cluster-uuid", po::value<std::string>(), "remote cluster uuid");
}

int execute_peer_add(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string remote_client_name = g_ceph_context->_conf->name.to_str();
  std::string remote_cluster;
  std::string remote_cluster_uuid;
  int r = get_remote_cluster_spec(
    vm, utils::get_positional_argument(vm, arg_index),
    &remote_client_name, &remote_cluster, &remote_cluster_uuid);
  if (r < 0) {
    return r;
  }

  std::string config_path;
  if (vm.count(at::CONFIG_PATH)) {
    config_path = vm[at::CONFIG_PATH].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  if (remote_cluster_uuid.empty()) {
    librados::Rados remote_rados;
    librados::IoCtx remote_io_ctx;
    r = init_remote(config_path, remote_client_name, remote_cluster,
                    pool_name, &remote_rados, &remote_io_ctx);
    if (r < 0) {
      return r;
    }

    r = remote_rados.cluster_fsid(&remote_cluster_uuid);
    if (r < 0) {
      std::cerr << "rbd: error retrieving remote cluster id" << std::endl;
      return r;
    }
  }

  librbd::RBD rbd;
  r = rbd.mirror_peer_add(io_ctx, remote_cluster_uuid, remote_cluster,
                          remote_client_name);
  if (r < 0) {
    std::cerr << "rbd: error adding mirror peer" << std::endl;
    return r;
  }
  return 0;
}

void get_peer_remove_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_pool_options(positional, options);
  add_cluster_uuid_option(positional);
}

int execute_peer_remove(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string cluster_uuid;
  int r = get_cluster_uuid(vm, arg_index, &cluster_uuid);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_peer_remove(io_ctx, cluster_uuid);
  if (r < 0) {
    std::cerr << "rbd: error removing mirror peer" << std::endl;
    return r;
  }
  return 0;
}

void get_peer_set_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options);
  add_cluster_uuid_option(positional);
  positional->add_options()
    ("key", "peer parameter [client or cluster]")
    ("value", "new client or cluster name");
}

int execute_peer_set(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string cluster_uuid;
  int r = get_cluster_uuid(vm, arg_index++, &cluster_uuid);
  if (r < 0) {
    return r;
  }

  std::string key = utils::get_positional_argument(vm, arg_index++);
  if (key != "client" && key != "cluster") {
    std::cerr << "rbd: must specify 'client' or 'cluster' key." << std::endl;
    return -EINVAL;
  }

  std::string value = utils::get_positional_argument(vm, arg_index++);
  if (value.empty()) {
    std::cerr << "rbd: must specify new " << key << " value." << std::endl;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  if (key == "client") {
    r = rbd.mirror_peer_set_client(io_ctx, cluster_uuid.c_str(), value.c_str());
  } else {
    r = rbd.mirror_peer_set_cluster(io_ctx, cluster_uuid.c_str(),
                                    value.c_str());
  }
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_enable_disable_arguments(po::options_description *positional,
                                  po::options_description *options) {
  at::add_pool_options(positional, options);
}

int execute_enable_disable(const po::variables_map &vm, bool enabled) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  librados::Rados rados;
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_set_enabled(io_ctx, enabled);
  if (r < 0) {
    return r;
  }
  return 0;
}

int execute_disable(const po::variables_map &vm) {
  return execute_enable_disable(vm, false);
}

int execute_enable(const po::variables_map &vm) {
  return execute_enable_disable(vm, true);
}

void get_info_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
}

int execute_info(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  std::string config_path;
  if (vm.count(at::CONFIG_PATH)) {
    config_path = vm[at::CONFIG_PATH].as<std::string>();
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  bool enabled;
  r = rbd.mirror_is_enabled(io_ctx, &enabled);
  if (r < 0) {
    return r;
  }

  std::vector<librbd::mirror_peer_t> mirror_peers;
  r = rbd.mirror_peer_list(io_ctx, &mirror_peers);
  if (r < 0) {
    return r;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("mirror");
    formatter->dump_bool("enabled", enabled);
  } else {
    std::cout << "Enabled: " << (enabled ? "true" : "false") << std::endl;
  }

  format_mirror_peers(config_path, formatter, mirror_peers);
  if (formatter != nullptr) {
    formatter->close_section();
    formatter->flush(std::cout);
  }
  return 0;
}

Shell::Action action_add(
  {"mirror", "pool", "peer", "add"}, {},
  "Add a mirroring peer to a pool.", "",
  &get_peer_add_arguments, &execute_peer_add);
Shell::Action action_remove(
  {"mirror", "pool", "peer", "remove"}, {},
  "Remove a mirroring peer from a pool.", "",
  &get_peer_remove_arguments, &execute_peer_remove);
Shell::Action action_set(
  {"mirror", "pool", "peer", "set"}, {},
  "Update mirroring peer settings.", "",
  &get_peer_set_arguments, &execute_peer_set);

Shell::Action action_disable(
  {"mirror", "pool", "disable"}, {},
  "Disable RBD mirroring by default within a pool.", "",
  &get_enable_disable_arguments, &execute_disable);
Shell::Action action_enable(
  {"mirror", "pool", "enable"}, {},
  "Enable RBD mirroring by default within a pool.", "",
  &get_enable_disable_arguments, &execute_enable);
Shell::Action action_info(
  {"mirror", "pool", "info"}, {},
  "Show information about the pool mirroring configuration.", {},
  &get_info_arguments, &execute_info);

} // namespace mirror_pool
} // namespace action
} // namespace rbd
