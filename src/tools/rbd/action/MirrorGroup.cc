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
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace mirror_group {

namespace at = argument_types;
namespace po = boost::program_options;

// TODO: move code common with Group.cc to shared ArgumentTypes.cc

static const std::string GROUP_SPEC("group-spec");

static const std::string GROUP_NAME("group");

static const std::string GROUP_POOL_NAME("group-" + at::POOL_NAME);

void add_group_option(po::options_description *opt) {
  opt->add_options()
    (GROUP_NAME.c_str(), po::value<std::string>(), "group name");
}

void add_group_spec_options(po::options_description *pos,
			    po::options_description *opt) {
  at::add_pool_option(opt, at::ARGUMENT_MODIFIER_NONE);
  at::add_namespace_option(opt, at::ARGUMENT_MODIFIER_NONE);
  add_group_option(opt);
  pos->add_options()
    (GROUP_SPEC.c_str(),
     ("group specification\n"
      "(example: [<pool-name>/[<namespace>/]]<group-name>)"));
}

namespace {

int validate_mirroring_enabled(librados::IoCtx io_ctx,
                               const std::string group_name) {
  librbd::RBD rbd;
  librbd::mirror_group_info_t info;
  int r = rbd.mirror_group_get_info(io_ctx, group_name.c_str(), &info,
                                sizeof(info));
  if (r < 0 && r != -ENOENT) {
    std::cerr << "rbd: failed to get mirror info for group " << group_name
              << ": " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (r == -ENOENT || info.state != RBD_MIRROR_GROUP_ENABLED) {
    std::cerr << "rbd: mirroring not enabled on the group" << std::endl;
    return -EINVAL;
  }

  if (info.mirror_image_mode != RBD_MIRROR_IMAGE_MODE_SNAPSHOT) {
    std::cerr << "rbd: snapshot based mirroring not enabled on the group"
              << std::endl;
    return -EINVAL;
  }

  return 0;
}

} // anonymous namespace

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  add_group_spec_options(positional, options);
  at::add_snap_create_options(options);
}

void get_arguments_enable(po::options_description *positional,
                          po::options_description *options) {
  add_group_spec_options(positional, options);
  positional->add_options()
    ("mode", "mirror group mode [default: snapshot]");
  at::add_snap_create_options(options);
}

void get_arguments_disable(po::options_description *positional,
                           po::options_description *options) {
  options->add_options()
    ("force", po::bool_switch(), "disable even if not primary");
  add_group_spec_options(positional, options);
}

int execute_enable_disable(const po::variables_map &vm, bool enable,
                           bool force) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  uint32_t flags;
  if (enable) {
    r = utils::get_snap_create_flags(vm, &flags);
    if (r < 0) {
      return r;
    }
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  if (enable) {
    librbd::mirror_image_mode_t mode = RBD_MIRROR_IMAGE_MODE_SNAPSHOT;
    std::string mode_arg = utils::get_positional_argument(vm, arg_index++);
    if (mode_arg == "journal") {
      mode = RBD_MIRROR_IMAGE_MODE_JOURNAL;
      std::cerr << "rbd: journal mode not supported with group mirroring"
                << std::endl;
      return -EINVAL;
    } else if (mode_arg == "snapshot") {
      mode = RBD_MIRROR_IMAGE_MODE_SNAPSHOT;
    } else if (!mode_arg.empty()) {
      std::cerr << "rbd: invalid mode name: " << mode_arg << std::endl;
      return -EINVAL;
    }
    r = rbd.mirror_group_enable(io_ctx, group_name.c_str(), mode, flags);
  } else {
    r = rbd.mirror_group_disable(io_ctx, group_name.c_str(), force);
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
  add_group_spec_options(positional, options);
  at::add_snap_create_options(options);
}

int execute_promote(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  uint32_t flags;
  r = utils::get_snap_create_flags(vm, &flags);
  if (r < 0) {
    return r;
  }

  bool force = vm["force"].as<bool>();

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx, group_name);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  r = rbd.mirror_group_promote(io_ctx, group_name.c_str(), flags, force);
  if (r < 0) {
    std::cerr << "rbd: error promoting group to primary" << std::endl;
    return r;
  }

  std::cout << "Group promoted to primary" << std::endl;
  return 0;
}

int execute_demote(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
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

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx, group_name);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  r = rbd.mirror_group_demote(io_ctx, group_name.c_str(), flags);
  if (r < 0) {
    std::cerr << "rbd: error demoting group to non-primary" << std::endl;
    return r;
  }

  std::cout << "Group demoted to non-primary" << std::endl;
  return 0;
}

int execute_resync(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx, group_name);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;

  r = rbd.mirror_group_resync(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: error flagging group resync" << std::endl;
    return r;
  }

  std::cout << "Flagged group for resync from primary" << std::endl;
  return 0;
}

void get_status_arguments(po::options_description *positional,
			  po::options_description *options) {
  add_group_spec_options(positional, options);
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
  std::string group_name;

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx, group_name);
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
  librbd::RBD rbd;
  librbd::mirror_group_global_status_t status;
  r = rbd.mirror_group_get_status(io_ctx, group_name.c_str(), &status,
                                  sizeof(status));
  if (r < 0) {
    std::cerr << "rbd: failed to get status for group " << group_name << ": "
	      << cpp_strerror(r) << std::endl;
    return r;
  }


  utils::populate_unknown_mirror_group_site_statuses(mirror_peers, &status);

  std::string instance_id;
  MirrorDaemonServiceInfo daemon_service_info(io_ctx);

  librbd::mirror_group_site_status_t local_status;
  int local_site_r = utils::get_local_mirror_group_status(
    status, &local_status);
  status.site_statuses.erase(
    std::remove_if(status.site_statuses.begin(),
                   status.site_statuses.end(),
                   [](auto& status) {
        return (status.mirror_uuid ==
                  RBD_MIRROR_GROUP_STATUS_LOCAL_MIRROR_UUID);
      }),
    status.site_statuses.end());

  if (local_site_r >= 0 && local_status.up) {
    r = rbd.mirror_group_get_instance_id(io_ctx, group_name.c_str(),
                                         &instance_id);
    if (r < 0 && r != -ENOENT) {
      std::cerr << "rbd: failed to get service id for group "
                << group_name << ": " << cpp_strerror(r) << std::endl;
      // not fatal
    } else if (!instance_id.empty()) {
      daemon_service_info.init();
    }
  }

  std::vector<librbd::group_snap_info_t> snaps;
  if (status.info.primary && status.info.state == RBD_MIRROR_GROUP_ENABLED) {
    if (status.info.mirror_image_mode == RBD_MIRROR_IMAGE_MODE_SNAPSHOT) {
      rbd.group_snap_list(io_ctx, group_name.c_str(), &snaps,
                          sizeof(librbd::group_snap_info_t));
      snaps.erase(
        remove_if(snaps.begin(),
                  snaps.end(),
                  [](const librbd::group_snap_info_t &snap) {
                    // TODO: a more reliable way to filter mirror snapshots
                    return !boost::starts_with(snap.name, ".mirror.");
                  }),
        snaps.end());
    }
  }

  auto mirror_service = daemon_service_info.get_by_instance_id(instance_id);

  if (formatter != nullptr) {
    formatter->open_object_section("group");
    formatter->dump_string("name", group_name);
    formatter->dump_string("global_id", status.info.global_id);
    if (local_site_r >= 0) {
      formatter->dump_string("state", utils::mirror_group_site_status_state(
        local_status));
      formatter->dump_string("description", local_status.description);
      if (mirror_service != nullptr) {
        mirror_service->dump(formatter);
      }
      formatter->dump_string("last_update", utils::timestr(
        local_status.last_update));
      formatter->open_array_section("images");
      for (auto &[p, image_status] : local_status.mirror_images) {
        formatter->open_object_section("image");
        formatter->dump_int("pool_id", p.first);
        formatter->dump_string("global_image_id", p.second);
        formatter->dump_string(
            "status", utils::mirror_image_site_status_state(image_status));
        formatter->dump_string("description", image_status.description);
        formatter->close_section(); // image
      }
      formatter->close_section(); // images
    }
    if (!status.site_statuses.empty()) {
      formatter->open_array_section("peer_sites");
      for (auto& status : status.site_statuses) {
        formatter->open_object_section("peer_site");

        auto name_it = peer_mirror_uuids_to_name.find(status.mirror_uuid);
        formatter->dump_string("site_name",
          (name_it != peer_mirror_uuids_to_name.end() ? name_it->second : ""));
        formatter->dump_string("mirror_uuids", status.mirror_uuid);

        formatter->dump_string("state", utils::mirror_group_site_status_state(
          status));
        formatter->dump_string("description", status.description);
        formatter->dump_string("last_update", utils::timestr(
          status.last_update));
        formatter->open_array_section("images");
        for (auto &[p, image_status] : status.mirror_images) {
          formatter->open_object_section("image");
          formatter->dump_int("pool_id", p.first);
          formatter->dump_string("global_image_id", p.second);
          formatter->dump_string(
              "status", utils::mirror_image_site_status_state(image_status));
          formatter->dump_string("description", image_status.description);
          formatter->close_section(); // image
        }
        formatter->close_section(); // images
        formatter->close_section(); // peer_site
      }
      formatter->close_section(); // peer_sites
    }
    if (!snaps.empty()) {
      formatter->open_array_section("snapshots");
      for (auto &snap : snaps) {
        std::string state_string;
        if (snap.state == RBD_GROUP_SNAP_STATE_INCOMPLETE) {
          state_string = "incomplete";
        } else {
          state_string = "ok";
        }
        formatter->open_object_section("snapshot");
        formatter->dump_string("name", snap.name);
        formatter->dump_string("state", state_string);
        formatter->close_section(); // snapshot
      }
      formatter->close_section(); // snapshots
    }
    formatter->close_section(); // group
    formatter->flush(std::cout);
  } else {
    std::cout << group_name << ":\n"
	      << "  global_id:   " << status.info.global_id << "\n";
    if (local_site_r >= 0) {
      std::cout << "  state:       " << utils::mirror_group_site_status_state(
                  local_status) << "\n"
                << "  description: " << local_status.description << "\n";
      if (mirror_service != nullptr) {
        std::cout << "  service:     " <<
          mirror_service->get_description() << "\n";
      }
      std::cout << "  last_update: " << utils::timestr(
        local_status.last_update) << std::endl;
      std::cout << "  images:" << std::endl;
      bool first_image = true;
      for (auto &[p, image_status] : local_status.mirror_images) {
        if (!first_image) {
          std::cout << std::endl;
        }
        first_image = false;
        // TODO: resolve pool_id/global_image_id into pool_name/image_name?
        std::cout << "    image:       " << p.first << "/" << p.second << "\n"
                  << "    state:       " << utils::mirror_image_site_status_state(
                                              image_status) << "\n"
                  << "    description: " << image_status.description << "\n";
      }
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
                  << "    state: " << utils::mirror_group_site_status_state(
                    site) << std::endl
                  << "    description: " << site.description << std::endl
                  << "    last_update: " << utils::timestr(
                    site.last_update) << std::endl;
        std::cout << "    images:" << std::endl;
        bool first_image = true;
        for (auto &[p, image_status] : site.mirror_images) {
          if (!first_image) {
            std::cout << std::endl;
          }
          first_image = false;
          // TODO: resolve pool_id/global_image_id into pool_name/image_name?
          std::cout << "      image:       " << p.first << "/" << p.second << "\n"
                    << "      state:       " << utils::mirror_image_site_status_state(
                                                  image_status) << "\n"
                    << "      description: " << image_status.description << "\n";
        }
      }
    }
    if (!snaps.empty()) {
      std::cout << "  snapshots:" << std::endl;

      bool first_snap = true;
      for (auto &snap : snaps) {
        if (!first_snap) {
          std::cout << std::endl;
        }
        first_snap = false;
        std::cout << "    " << snap.name;
      }
      std::cout << std::endl;
    }
  }

  return 0;
}

void get_snapshot_arguments(po::options_description *positional,
                            po::options_description *options) {
  add_group_spec_options(positional, options);
  at::add_snap_create_options(options);
}

int execute_snapshot(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, nullptr, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
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

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = validate_mirroring_enabled(io_ctx, group_name);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::string snap_id;
  r = rbd.mirror_group_create_snapshot(io_ctx, group_name.c_str(), flags,
                                       &snap_id);
  if (r < 0) {
    std::cerr << "rbd: error creating snapshot: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  std::cout << "Snapshot ID: " << snap_id << std::endl;
  return 0;
}

Shell::Action action_enable(
  {"mirror", "group", "enable"}, {},
  "Enable RBD mirroring for an group.", "",
  &get_arguments_enable, &execute_enable);
Shell::Action action_disable(
  {"mirror", "group", "disable"}, {},
  "Disable RBD mirroring for an group.", "",
  &get_arguments_disable, &execute_disable);
Shell::Action action_promote(
  {"mirror", "group", "promote"}, {},
  "Promote an group to primary for RBD mirroring.", "",
  &get_arguments_promote, &execute_promote);
Shell::Action action_demote(
  {"mirror", "group", "demote"}, {},
  "Demote an group to non-primary for RBD mirroring.", "",
  &get_arguments, &execute_demote);
Shell::Action action_resync(
  {"mirror", "group", "resync"}, {},
  "Force resync to primary group for RBD mirroring.", "",
  &get_arguments, &execute_resync);
Shell::Action action_status(
  {"mirror", "group", "status"}, {},
  "Show RBD mirroring status for an group.", "",
  &get_status_arguments, &execute_status);
Shell::Action action_snapshot(
  {"mirror", "group", "snapshot"}, {},
  "Create RBD mirroring group snapshot.", "",
  &get_snapshot_arguments, &execute_snapshot);

} // namespace mirror_image
} // namespace action
} // namespace rbd
