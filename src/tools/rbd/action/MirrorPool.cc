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

void add_uuid_option(po::options_description *positional) {
  positional->add_options()
    ("uuid", po::value<std::string>(), "peer uuid");
}

int get_uuid(const po::variables_map &vm, size_t arg_index,
             std::string *uuid) {
  *uuid = utils::get_positional_argument(vm, arg_index);
  if (uuid->empty()) {
    std::cerr << "rbd: must specify peer uuid" << std::endl;
    return -EINVAL;
  }
  return validate_uuid(*uuid);
}

int get_remote_cluster_spec(const po::variables_map &vm,
                            const std::string &spec,
                            std::string *remote_client_name,
                            std::string *remote_cluster) {
  if (vm.count("remote-client-name")) {
    *remote_client_name = vm["remote-client-name"].as<std::string>();
  }
  if (vm.count("remote-cluster")) {
    *remote_cluster = vm["remote-cluster"].as<std::string>();
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
      formatter->dump_string("uuid", peer.uuid);
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
            << peer.uuid
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
    ("remote-cluster", po::value<std::string>(), "remote cluster name");
}

int execute_peer_add(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string remote_client_name = g_ceph_context->_conf->name.to_str();
  std::string remote_cluster;
  int r = get_remote_cluster_spec(
    vm, utils::get_positional_argument(vm, arg_index),
    &remote_client_name, &remote_cluster);
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
  std::string uuid;
  r = rbd.mirror_peer_add(io_ctx, &uuid, remote_cluster, remote_client_name);
  if (r < 0) {
    std::cerr << "rbd: error adding mirror peer" << std::endl;
    return r;
  }

  std::cout << uuid << std::endl;
  return 0;
}

void get_peer_remove_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_pool_options(positional, options);
  add_uuid_option(positional);
}

int execute_peer_remove(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string uuid;
  int r = get_uuid(vm, arg_index, &uuid);
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
  r = rbd.mirror_peer_remove(io_ctx, uuid);
  if (r < 0) {
    std::cerr << "rbd: error removing mirror peer" << std::endl;
    return r;
  }
  return 0;
}

void get_peer_set_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_pool_options(positional, options);
  add_uuid_option(positional);
  positional->add_options()
    ("key", "peer parameter [client or cluster]")
    ("value", "new client or cluster name");
}

int execute_peer_set(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  std::string uuid;
  int r = get_uuid(vm, arg_index++, &uuid);
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
    r = rbd.mirror_peer_set_client(io_ctx, uuid.c_str(), value.c_str());
  } else {
    r = rbd.mirror_peer_set_cluster(io_ctx, uuid.c_str(), value.c_str());
  }
  if (r < 0) {
    return r;
  }
  return 0;
}

void get_disable_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_pool_options(positional, options);
}

void get_enable_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_pool_options(positional, options);
  positional->add_options()
    ("mode", "mirror mode [image or pool]");
}

int execute_enable_disable(const std::string &pool_name,
                           rbd_mirror_mode_t mirror_mode) {
  librados::Rados rados;
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.mirror_mode_set(io_ctx, mirror_mode);
  if (r < 0) {
    return r;
  }
  return 0;
}

int execute_disable(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  return execute_enable_disable(pool_name, RBD_MIRROR_MODE_DISABLED);
}

int execute_enable(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  rbd_mirror_mode_t mirror_mode;
  std::string mode = utils::get_positional_argument(vm, arg_index++);
  if (mode == "image") {
    mirror_mode = RBD_MIRROR_MODE_IMAGE;
  } else if (mode == "pool") {
    mirror_mode = RBD_MIRROR_MODE_POOL;
  } else {
    std::cerr << "rbd: must specify 'image' or 'pool' mode." << std::endl;
    return -EINVAL;
  }

  return execute_enable_disable(pool_name, mirror_mode);
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
  rbd_mirror_mode_t mirror_mode;
  r = rbd.mirror_mode_get(io_ctx, &mirror_mode);
  if (r < 0) {
    return r;
  }

  std::vector<librbd::mirror_peer_t> mirror_peers;
  r = rbd.mirror_peer_list(io_ctx, &mirror_peers);
  if (r < 0) {
    return r;
  }

  std::string mirror_mode_desc;
  switch (mirror_mode) {
  case RBD_MIRROR_MODE_DISABLED:
    mirror_mode_desc = "disabled";
    break;
  case RBD_MIRROR_MODE_IMAGE:
    mirror_mode_desc = "image";
    break;
  case RBD_MIRROR_MODE_POOL:
    mirror_mode_desc = "pool";
    break;
  default:
    mirror_mode_desc = "unknown";
    break;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("mirror");
    formatter->dump_string("mode", mirror_mode_desc);
  } else {
    std::cout << "Mode: " << mirror_mode_desc << std::endl;
  }

  if (mirror_mode != RBD_MIRROR_MODE_DISABLED) {
    format_mirror_peers(config_path, formatter, mirror_peers);
  }
  if (formatter != nullptr) {
    formatter->close_section();
    formatter->flush(std::cout);
  }
  return 0;
}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
  at::add_verbose_option(options);
}

int execute_status(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  bool verbose = vm[at::VERBOSE].as<bool>();

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

  std::map<librbd::mirror_image_status_state_t, int> states;
  r = rbd.mirror_image_status_summary(io_ctx, &states);
  if (r < 0) {
    std::cerr << "rbd: failed to get status summary for mirrored images: "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("status");
  }

  enum Health {Ok = 0, Warning = 1, Error = 2} health = Ok;
  const char *names[] = {"OK", "WARNING", "ERROR"};
  int total = 0;

  for (auto &it : states) {
    auto &state = it.first;
    if (health < Warning &&
	(state != MIRROR_IMAGE_STATUS_STATE_REPLAYING &&
	 state != MIRROR_IMAGE_STATUS_STATE_STOPPED)) {
      health = Warning;
    }
    if (health < Error &&
	state == MIRROR_IMAGE_STATUS_STATE_ERROR) {
      health = Error;
    }
    total += it.second;
  }

  if (formatter != nullptr) {
    formatter->open_object_section("summary");
    formatter->dump_string("health", names[health]);
    formatter->open_object_section("states");
    for (auto &it : states) {
      std::string state_name = utils::mirror_image_status_state(it.first);
      formatter->dump_int(state_name.c_str(), it.second);
    }
    formatter->close_section(); // states
    formatter->close_section(); // summary
  } else {
    std::cout << "health: " << names[health] << std::endl;
    std::cout << "images: " << total << " total" << std::endl;
    for (auto &it : states) {
      std::cout << "    " << it.second << " "
		<< utils::mirror_image_status_state(it.first) << std::endl;
    }
  }

  int ret = 0;

  if (verbose) {
    if (formatter != nullptr) {
      formatter->open_array_section("images");
    }

    std::string last_read = "";
    int max_read = 1024;
    do {
      map<std::string, librbd::mirror_image_info_t> mirror_images;
      map<std::string, librbd::mirror_image_status_t> statuses;
      r = rbd.mirror_image_status_list(io_ctx, last_read, max_read,
				       &mirror_images, &statuses);
      if (r < 0) {
	std::cerr << "rbd: failed to list mirrored image directory: "
		  << cpp_strerror(r) << std::endl;
	return r;
      }
      for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {
	const std::string &image_name = it->first;
	std::string &global_image_id = it->second.global_id;
	librbd::mirror_image_status_t &status = statuses[image_name];
	std::string state = utils::mirror_image_status_state(status);
	std::string last_update = utils::timestr(status.last_update);

	if (formatter != nullptr) {
	  formatter->open_object_section("image");
	  formatter->dump_string("name", image_name);
	  formatter->dump_string("global_id", global_image_id);
	  formatter->dump_string("state", state);
	  formatter->dump_string("description", status.description);
	  formatter->dump_string("last_update", last_update);
	  formatter->close_section(); // image
	} else {
	  std::cout << "\n" << image_name << ":\n"
		    << "  global_id:   " << global_image_id << "\n"
		    << "  state:       " << state << "\n"
		    << "  description: " << status.description << "\n"
		    << "  last_update: " << last_update << std::endl;
	}
      }
      if (!mirror_images.empty()) {
	last_read = mirror_images.rbegin()->first;
      }
      r = mirror_images.size();
    } while (r == max_read);

    if (formatter != nullptr) {
      formatter->close_section(); // images
    }
  }

  if (formatter != nullptr) {
    formatter->close_section(); // status
    formatter->flush(std::cout);
  }

  return ret;
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
  &get_disable_arguments, &execute_disable);
Shell::Action action_enable(
  {"mirror", "pool", "enable"}, {},
  "Enable RBD mirroring by default within a pool.", "",
  &get_enable_arguments, &execute_enable);
Shell::Action action_info(
  {"mirror", "pool", "info"}, {},
  "Show information about the pool mirroring configuration.", {},
  &get_info_arguments, &execute_info);
Shell::Action action_status(
  {"mirror", "pool", "status"}, {},
  "Show status for all mirrored images in the pool.", {},
  &get_status_arguments, &execute_status);

} // namespace mirror_pool
} // namespace action
} // namespace rbd
