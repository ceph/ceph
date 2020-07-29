// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/MirrorDaemonServiceInfo.h"
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

namespace rbd {
namespace action {
namespace mirror_image {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

int validate_mirroring_enabled(librbd::Image &image, bool snapshot = false) {
  librbd::mirror_image_info_t mirror_image;
  int r = image.mirror_image_get_info(&mirror_image, sizeof(mirror_image));
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve mirror info: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  if (mirror_image.state != RBD_MIRROR_IMAGE_ENABLED) {
    std::cerr << "rbd: mirroring not enabled on the image" << std::endl;
    return -EINVAL;
  }

  if (snapshot) {
    librbd::mirror_image_mode_t mode;
    r = image.mirror_image_get_mode(&mode);
    if (r < 0) {
      std::cerr << "rbd: failed to retrieve mirror mode: "
                << cpp_strerror(r) << std::endl;
      return r;
    }

    if (mode != RBD_MIRROR_IMAGE_MODE_SNAPSHOT) {
      std::cerr << "rbd: snapshot based mirroring not enabled on the image"
                << std::endl;
      return -EINVAL;
    }
  }

  return 0;
}

} // anonymous namespace

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_arguments_enable(po::options_description *positional,
                          po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  positional->add_options()
    ("mode", "mirror image mode (journal or snapshot) [default: journal]");
}

void get_arguments_disable(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "disable even if not primary");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_enable_disable(const po::variables_map &vm, bool enable,
                           bool force) {
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  if (enable) {
    librbd::mirror_image_mode_t mode = RBD_MIRROR_IMAGE_MODE_JOURNAL;
    std::string mode_arg = utils::get_positional_argument(vm, arg_index++);
    if (mode_arg == "journal") {
      mode = RBD_MIRROR_IMAGE_MODE_JOURNAL;
    } else if (mode_arg == "snapshot") {
      mode = RBD_MIRROR_IMAGE_MODE_SNAPSHOT;
    } else if (!mode_arg.empty()) {
      std::cerr << "rbd: invalid mode name: " << mode_arg << std::endl;
      return -EINVAL;
    }
    r = image.mirror_image_enable2(mode);
  } else {
    r = image.mirror_image_disable(force);
  }
  if (r < 0) {
    return r;
  }

  std::cout << (enable ? "Mirroring enabled" : "Mirroring disabled")
    << std::endl;

  return 0;
}

int execute_disable(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  return execute_enable_disable(vm, false, vm["force"].as<bool>());
}

int execute_enable(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  return execute_enable_disable(vm, true, false);
}

void get_arguments_promote(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "promote even if not cleanly demoted by remote cluster");
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_promote(const po::variables_map &vm,
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

  bool force = vm["force"].as<bool>();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_promote(force);
  if (r < 0) {
    std::cerr << "rbd: error promoting image to primary" << std::endl;
    return r;
  }

  std::cout << "Image promoted to primary" << std::endl;
  return 0;
}

int execute_demote(const po::variables_map &vm,
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_demote();
  if (r < 0) {
    std::cerr << "rbd: error demoting image to non-primary" << std::endl;
    return r;
  }

  std::cout << "Image demoted to non-primary" << std::endl;
  return 0;
}

int execute_resync(const po::variables_map &vm,
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

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(image);
  if (r < 0) {
    return r;
  }

  r = image.mirror_image_resync();
  if (r < 0) {
    std::cerr << "rbd: error flagging image resync" << std::endl;
    return r;
  }

  std::cout << "Flagged image for resync from primary" << std::endl;
  return 0;
}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

int execute_status(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
      &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
      utils::SPEC_VALIDATION_NONE);
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

  r = validate_mirroring_enabled(image);
  if (r < 0) {
    return r;
  }

  librados::IoCtx default_ns_io_ctx;
  default_ns_io_ctx.dup(io_ctx);
  default_ns_io_ctx.set_namespace("");

  std::vector<librbd::mirror_peer_site_t> mirror_peers;
  utils::get_mirror_peer_sites(default_ns_io_ctx, &mirror_peers);

  std::map<std::string, std::string> peer_mirror_uuids_to_name;
  utils::get_mirror_peer_mirror_uuids_to_names(mirror_peers,
                                               &peer_mirror_uuids_to_name);

  librbd::mirror_image_global_status_t status;
  r = image.mirror_image_get_global_status(&status, sizeof(status));
  if (r < 0) {
    std::cerr << "rbd: failed to get status for image " << image_name << ": "
	      << cpp_strerror(r) << std::endl;
    return r;
  }

  utils::populate_unknown_mirror_image_site_statuses(mirror_peers, &status);

  std::string instance_id;
  MirrorDaemonServiceInfo daemon_service_info(io_ctx);

  librbd::mirror_image_site_status_t local_status;
  int local_site_r = utils::get_local_mirror_image_status(
    status, &local_status);
  status.site_statuses.erase(
    std::remove_if(status.site_statuses.begin(),
                   status.site_statuses.end(),
                   [](auto& status) {
        return (status.mirror_uuid ==
                  RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID);
      }),
    status.site_statuses.end());

  if (local_site_r >= 0 && local_status.up) {
    r = image.mirror_image_get_instance_id(&instance_id);
    if (r == -EOPNOTSUPP) {
      std::cerr << "rbd: newer release of Ceph OSDs required to map image "
                << "to rbd-mirror daemon instance" << std::endl;
      // not fatal
    } else if (r < 0 && r != -ENOENT) {
      std::cerr << "rbd: failed to get service id for image "
                << image_name << ": " << cpp_strerror(r) << std::endl;
      // not fatal
    } else if (!instance_id.empty()) {
      daemon_service_info.init();
    }
  }

  std::vector<librbd::snap_info_t> snaps;
  if (status.info.primary && status.info.state == RBD_MIRROR_IMAGE_ENABLED) {
    librbd::mirror_image_mode_t mode = RBD_MIRROR_IMAGE_MODE_JOURNAL;
    r = image.mirror_image_get_mode(&mode);
    if (r < 0) {
      std::cerr << "rbd: failed to retrieve mirror mode: "
                << cpp_strerror(r) << std::endl;
      // not fatal
    }

    if (mode == RBD_MIRROR_IMAGE_MODE_SNAPSHOT) {
      image.snap_list(snaps);
      snaps.erase(
        remove_if(snaps.begin(),
                  snaps.end(),
                  [&image](const librbd::snap_info_t &snap) {
                    librbd::snap_namespace_type_t type;
                    int r = image.snap_get_namespace_type(snap.id, &type);
                    if (r < 0) {
                      return false;
                    }
                    return type != RBD_SNAP_NAMESPACE_TYPE_MIRROR;
                  }),
        snaps.end());
    }
  }

  auto mirror_service = daemon_service_info.get_by_instance_id(instance_id);

  if (formatter != nullptr) {
    formatter->open_object_section("image");
    formatter->dump_string("name", image_name);
    formatter->dump_string("global_id", status.info.global_id);
    if (local_site_r >= 0) {
      formatter->dump_string("state", utils::mirror_image_site_status_state(
        local_status));
      formatter->dump_string("description", local_status.description);
      if (mirror_service != nullptr) {
        mirror_service->dump_image(formatter);
      }
      formatter->dump_string("last_update", utils::timestr(
        local_status.last_update));
    }
    if (!status.site_statuses.empty()) {
      formatter->open_array_section("peer_sites");
      for (auto& status : status.site_statuses) {
        formatter->open_object_section("peer_site");

        auto name_it = peer_mirror_uuids_to_name.find(status.mirror_uuid);
        formatter->dump_string("site_name",
          (name_it != peer_mirror_uuids_to_name.end() ? name_it->second : ""));
        formatter->dump_string("mirror_uuids", status.mirror_uuid);

        formatter->dump_string("state", utils::mirror_image_site_status_state(
          status));
        formatter->dump_string("description", status.description);
        formatter->dump_string("last_update", utils::timestr(
          status.last_update));
        formatter->close_section(); // peer_site
      }
      formatter->close_section(); // peer_sites
    }
    if (!snaps.empty()) {
      formatter->open_array_section("snapshots");
      for (auto &snap : snaps) {
        librbd::snap_mirror_namespace_t info;
        r = image.snap_get_mirror_namespace(snap.id, &info, sizeof(info));
        if (r < 0 ||
            (info.state != RBD_SNAP_MIRROR_STATE_PRIMARY &&
             info.state != RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED)) {
          continue;
        }
        formatter->open_object_section("snapshot");
        formatter->dump_unsigned("id", snap.id);
        formatter->dump_string("name", snap.name);
        formatter->dump_bool("demoted",
                             info.state == RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED);
        formatter->open_array_section("mirror_peer_uuids");
        for (auto &peer : info.mirror_peer_uuids) {
          formatter->dump_string("peer_uuid", peer);
        }
        formatter->close_section(); // mirror_peer_uuids
        formatter->close_section(); // snapshot
      }
      formatter->close_section(); // snapshots
    }
    formatter->close_section(); // image
    formatter->flush(std::cout);
  } else {
    std::cout << image_name << ":\n"
	      << "  global_id:   " << status.info.global_id << "\n";
    if (local_site_r >= 0) {
      std::cout << "  state:       " << utils::mirror_image_site_status_state(
                  local_status) << "\n"
                << "  description: " << local_status.description << "\n";
      if (mirror_service != nullptr) {
        std::cout << "  service:     " <<
          mirror_service->get_image_description() << "\n";
      }
      std::cout << "  last_update: " << utils::timestr(
        local_status.last_update) << std::endl;
    }
    if (!status.site_statuses.empty()) {
      std::cout << "  peer_sites:" << std::endl;

      bool first_site = true;
      for (auto& site : status.site_statuses) {
        if (!first_site) {
          std::cout << std::endl;
        }
        first_site = false;

        auto name_it = peer_mirror_uuids_to_name.find(site.mirror_uuid);
        std::cout << "    name: "
                  << (name_it != peer_mirror_uuids_to_name.end() ?
                        name_it->second : site.mirror_uuid)
                  << std::endl
                  << "    state: " << utils::mirror_image_site_status_state(
                    site) << std::endl
                  << "    description: " << site.description << std::endl
                  << "    last_update: " << utils::timestr(
                    site.last_update) << std::endl;
      }
    }
    if (!snaps.empty()) {
      std::cout << "  snapshots:" << std::endl;

      bool first_site = true;
      for (auto &snap : snaps) {
        librbd::snap_mirror_namespace_t info;
        r = image.snap_get_mirror_namespace(snap.id, &info, sizeof(info));
        if (r < 0 ||
            (info.state != RBD_SNAP_MIRROR_STATE_PRIMARY &&
             info.state != RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED)) {
          continue;
        }

        if (!first_site) {
          std::cout << std::endl;
        }

        first_site = false;
        std::cout << "    " << snap.id << " " << snap.name << " ("
                  << (info.state == RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED ?
                        "demoted " : "")
                  << "peer_uuids:[" << info.mirror_peer_uuids << "])";
      }
      std::cout << std::endl;
    }
  }

  return 0;
}

void get_snapshot_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_create_options(options);
}

int execute_snapshot(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  int r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
      &image_name, nullptr, true, utils::SNAPSHOT_PRESENCE_NONE,
      utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  uint32_t flags;
  r = utils::get_snap_create_flags(vm, &flags);
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

  r = validate_mirroring_enabled(image, true);
  if (r < 0) {
    return r;
  }

  uint64_t snap_id;
  r = image.mirror_image_create_snapshot2(flags, &snap_id);
  if (r < 0) {
    std::cerr << "rbd: error creating snapshot: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  std::cout << "Snapshot ID: " << snap_id << std::endl;
  return 0;
}

Shell::Action action_enable(
  {"mirror", "image", "enable"}, {},
  "Enable RBD mirroring for an image.", "",
  &get_arguments_enable, &execute_enable);
Shell::Action action_disable(
  {"mirror", "image", "disable"}, {},
  "Disable RBD mirroring for an image.", "",
  &get_arguments_disable, &execute_disable);
Shell::Action action_promote(
  {"mirror", "image", "promote"}, {},
  "Promote an image to primary for RBD mirroring.", "",
  &get_arguments_promote, &execute_promote);
Shell::Action action_demote(
  {"mirror", "image", "demote"}, {},
  "Demote an image to non-primary for RBD mirroring.", "",
  &get_arguments, &execute_demote);
Shell::Action action_resync(
  {"mirror", "image", "resync"}, {},
  "Force resync to primary image for RBD mirroring.", "",
  &get_arguments, &execute_resync);
Shell::Action action_status(
  {"mirror", "image", "status"}, {},
  "Show RBD mirroring status for an image.", "",
  &get_status_arguments, &execute_status);
Shell::Action action_snapshot(
  {"mirror", "image", "snapshot"}, {},
  "Create RBD mirroring image snapshot.", "",
  &get_snapshot_arguments, &execute_snapshot);

} // namespace mirror_image
} // namespace action
} // namespace rbd
