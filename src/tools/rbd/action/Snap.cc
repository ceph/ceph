// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/bind.hpp>

namespace rbd {
namespace action {
namespace snap {

static const std::string ALL_NAME("all");

namespace at = argument_types;
namespace po = boost::program_options;

int do_list_snaps(librbd::Image& image, Formatter *f, bool all_snaps, librados::Rados& rados)
{
  std::vector<librbd::snap_info_t> snaps;
  TextTable t;
  int r;

  r = image.snap_list(snaps);
  if (r < 0) {
    std::cerr << "rbd: unable to list snapshots" << std::endl;
    return r;
  }

  librbd::image_info_t info;
  if (!all_snaps) {
    snaps.erase(remove_if(snaps.begin(),
                          snaps.end(),
                          boost::bind(utils::is_not_user_snap_namespace, &image, _1)),
                snaps.end());
  } else if (!f) {
    r = image.stat(info, sizeof(info));
    if (r < 0) {
      std::cerr << "rbd: unable to get image info" << std::endl;
      return r;
    }
  }

  if (f) {
    f->open_array_section("snapshots");
  } else {
    t.define_column("SNAPID", TextTable::LEFT, TextTable::RIGHT);
    t.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    t.define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    t.define_column("PROTECTED", TextTable::LEFT, TextTable::LEFT);
    t.define_column("TIMESTAMP", TextTable::LEFT, TextTable::RIGHT);
    if (all_snaps) {
      t.define_column("NAMESPACE", TextTable::LEFT, TextTable::LEFT);
    }
  }

  std::list<std::pair<int64_t, std::string>> pool_list;
  rados.pool_list2(pool_list);
  std::map<int64_t, std::string> pool_map(pool_list.begin(), pool_list.end());

  for (std::vector<librbd::snap_info_t>::iterator s = snaps.begin();
       s != snaps.end(); ++s) {
    struct timespec timestamp;
    bool snap_protected = false;
    image.snap_get_timestamp(s->id, &timestamp);
    string tt_str = "";
    if(timestamp.tv_sec != 0) {
      time_t tt = timestamp.tv_sec;
      tt_str = ctime(&tt);
      tt_str = tt_str.substr(0, tt_str.length() - 1);
    }

    librbd::snap_namespace_type_t snap_namespace;
    r = image.snap_get_namespace_type(s->id, &snap_namespace);
    if (r < 0) {
      std::cerr << "rbd: unable to retrieve snap namespace" << std::endl;
      return r;
    }

    std::string snap_namespace_name = "Unknown";
    switch (snap_namespace) {
    case RBD_SNAP_NAMESPACE_TYPE_USER:
      snap_namespace_name = "user";
      break;
    case RBD_SNAP_NAMESPACE_TYPE_GROUP:
      snap_namespace_name = "group";
      break;
    case RBD_SNAP_NAMESPACE_TYPE_TRASH:
      snap_namespace_name = "trash";
      break;
    case RBD_SNAP_NAMESPACE_TYPE_MIRROR:
      snap_namespace_name = "mirror";
      break;
    }

    int get_trash_res = -ENOENT;
    std::string trash_original_name;
    int get_group_res = -ENOENT;
    librbd::snap_group_namespace_t group_snap;
    int get_mirror_res = -ENOENT;
    librbd::snap_mirror_namespace_t mirror_snap;
    std::string mirror_snap_state = "unknown";
    if (snap_namespace == RBD_SNAP_NAMESPACE_TYPE_GROUP) {
      get_group_res = image.snap_get_group_namespace(s->id, &group_snap,
                                                     sizeof(group_snap));
    } else if (snap_namespace == RBD_SNAP_NAMESPACE_TYPE_TRASH) {
      get_trash_res = image.snap_get_trash_namespace(
        s->id, &trash_original_name);
    } else if (snap_namespace == RBD_SNAP_NAMESPACE_TYPE_MIRROR) {
      get_mirror_res = image.snap_get_mirror_namespace(
        s->id, &mirror_snap, sizeof(mirror_snap));

      switch (mirror_snap.state) {
      case RBD_SNAP_MIRROR_STATE_PRIMARY:
        mirror_snap_state = "primary";
        break;
      case RBD_SNAP_MIRROR_STATE_NON_PRIMARY:
        mirror_snap_state = "non-primary";
        break;
      case RBD_SNAP_MIRROR_STATE_PRIMARY_DEMOTED:
      case RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED:
        mirror_snap_state = "demoted";
        break;
      }
    }

    std::string protected_str = "";
    if (snap_namespace == RBD_SNAP_NAMESPACE_TYPE_USER) {
      r = image.snap_is_protected(s->name.c_str(), &snap_protected);
      if (r < 0) {
        std::cerr << "rbd: unable to retrieve snap protection" << std::endl;
        return r;
      }
    }

    if (f) {
      protected_str = snap_protected ? "true" : "false";
      f->open_object_section("snapshot");
      f->dump_unsigned("id", s->id);
      f->dump_string("name", s->name);
      f->dump_unsigned("size", s->size);
      f->dump_string("protected", protected_str);
      f->dump_string("timestamp", tt_str);
      if (all_snaps) {
        f->open_object_section("namespace");
        f->dump_string("type", snap_namespace_name);
        if (get_group_res == 0) {
          std::string pool_name = pool_map[group_snap.group_pool];
          f->dump_string("pool", pool_name);
          f->dump_string("group", group_snap.group_name);
          f->dump_string("group snap", group_snap.group_snap_name);
        } else if (get_trash_res == 0) {
          f->dump_string("original_name", trash_original_name);
        } else if (get_mirror_res == 0) {
          f->dump_string("state", mirror_snap_state);
          f->open_array_section("mirror_peer_uuids");
          for (auto &uuid : mirror_snap.mirror_peer_uuids) {
            f->dump_string("peer_uuid", uuid);
          }
          f->close_section();
          f->dump_bool("complete", mirror_snap.complete);
          if (mirror_snap.state == RBD_SNAP_MIRROR_STATE_NON_PRIMARY ||
              mirror_snap.state == RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED) {
            f->dump_string("primary_mirror_uuid",
                           mirror_snap.primary_mirror_uuid);
            f->dump_unsigned("primary_snap_id",
                             mirror_snap.primary_snap_id);
            f->dump_unsigned("last_copied_object_number",
                             mirror_snap.last_copied_object_number);
          }
        }
        f->close_section();
      }
      f->close_section();
    } else {
      protected_str = snap_protected ? "yes" : "";
      t << s->id << s->name << stringify(byte_u_t(s->size)) << protected_str << tt_str;

      if (all_snaps) {
        ostringstream oss;
        oss << snap_namespace_name;

        if (get_group_res == 0) {
          std::string pool_name = pool_map[group_snap.group_pool];
          oss << " (" << pool_name << "/"
                      << group_snap.group_name << "@"
                      << group_snap.group_snap_name << ")";
        } else if (get_trash_res == 0) {
          oss << " (" << trash_original_name << ")";
        } else if (get_mirror_res == 0) {
          oss << " (" << mirror_snap_state << " "
                      << "peer_uuids:[" << mirror_snap.mirror_peer_uuids << "]";
          if (mirror_snap.state == RBD_SNAP_MIRROR_STATE_NON_PRIMARY ||
              mirror_snap.state == RBD_SNAP_MIRROR_STATE_NON_PRIMARY_DEMOTED) {
            oss << " " << mirror_snap.primary_mirror_uuid << ":"
                << mirror_snap.primary_snap_id << " ";
            if (!mirror_snap.complete) {
              if (info.num_objs > 0) {
                auto progress = std::min<uint64_t>(
                  100, 100 * mirror_snap.last_copied_object_number /
                             info.num_objs);
                oss << progress << "% ";
              } else {
                oss << "not ";
              }
            }
            oss << "copied";
          }
          oss << ")";
        }

        t << oss.str();
      }
      t << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else if (snaps.size()) {
    std::cout << t;
  }

  return 0;
}

int do_add_snap(librbd::Image& image, const char *snapname,
                uint32_t flags, bool no_progress)
{
  utils::ProgressContext pc("Creating snap", no_progress);
  
  int r = image.snap_create2(snapname, flags, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }

  pc.finish();
  return 0;
}

int do_remove_snap(librbd::Image& image, const char *snapname, bool force,
		   bool no_progress)
{
  uint32_t flags = force? RBD_SNAP_REMOVE_FORCE : 0;
  int r = 0;
  utils::ProgressContext pc("Removing snap", no_progress);

  r = image.snap_remove2(snapname, flags, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }

  pc.finish();
  return 0;
}

int do_rollback_snap(librbd::Image& image, const char *snapname,
                     bool no_progress)
{
  utils::ProgressContext pc("Rolling back to snapshot", no_progress);
  int r = image.snap_rollback_with_progress(snapname, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

int do_purge_snaps(librbd::Image& image, bool no_progress)
{
  utils::ProgressContext pc("Removing all snapshots", no_progress);
  std::vector<librbd::snap_info_t> snaps;
  bool is_protected = false;
  int r = image.snap_list(snaps);
  if (r < 0) {
    pc.fail();
    return r;
  } else if (0 == snaps.size()) {
    return 0;
  } else {
    list<std::string> protect;
    snaps.erase(remove_if(snaps.begin(),
                          snaps.end(),
                          boost::bind(utils::is_not_user_snap_namespace, &image, _1)),
                snaps.end());
    for (auto it = snaps.begin(); it != snaps.end();) {
      r = image.snap_is_protected(it->name.c_str(), &is_protected);
      if (r < 0) {
        pc.fail();
        return r;
      } else if (is_protected == true) {
        protect.push_back(it->name.c_str());
        snaps.erase(it);
      } else {
        ++it;
      }
    }

    if (!protect.empty()) {
      std::cout << "rbd: error removing snapshot(s) '" << protect << "', which "
                << (1 == protect.size() ? "is" : "are")
                << " protected - these must be unprotected with "
                << "`rbd snap unprotect`."
                << std::endl;
    }
    for (size_t i = 0; i < snaps.size(); ++i) {
      r = image.snap_remove(snaps[i].name.c_str());
      if (r < 0) {
        pc.fail();
        return r;
      }
      pc.update_progress(i + 1, snaps.size() + protect.size());
    }

    if (!protect.empty()) {
      pc.fail();
    } else if (snaps.size() > 0) {
      pc.finish();
    }

    return 0;
  }
}

int do_protect_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_protect(snapname);
  if (r < 0)
    return r;

  return 0;
}

int do_unprotect_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_unprotect(snapname);
  if (r < 0)
    return r;

  return 0;
}

int do_set_limit(librbd::Image& image, uint64_t limit)
{
  return image.snap_set_limit(limit);
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_format_options(options);

  std::string name = ALL_NAME + ",a";

  options->add_options()
    (name.c_str(), po::bool_switch(), "list snapshots from all namespaces");
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name,
                                 image_id, "", true, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  bool all_snaps = vm[ALL_NAME].as<bool>();
  r = do_list_snaps(image, formatter.get(), all_snaps, rados);
  if (r < 0) {
    cerr << "rbd: failed to list snapshots: " << cpp_strerror(r)
         << std::endl;
    return r;
  }
  return 0;
}

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_create_options(options);
  at::add_no_progress_option(options);
}

int execute_create(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
    utils::SPEC_VALIDATION_SNAP);
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

  r = do_add_snap(image, snap_name.c_str(), flags,
                  vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    cerr << "rbd: failed to create snapshot: " << cpp_strerror(r)
         << std::endl;
    return r;
  }
  return 0;
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_snap_id_option(options);
  at::add_no_progress_option(options);

  options->add_options()
    ("force", po::bool_switch(), "flatten children and unprotect snapshot if needed.");
}

int execute_remove(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  uint64_t snap_id = CEPH_NOSNAP;
  bool force = vm["force"].as<bool>();
  bool no_progress = vm[at::NO_PROGRESS].as<bool>();

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }
  if (vm.count(at::SNAPSHOT_ID)) {
    snap_id = vm[at::SNAPSHOT_ID].as<uint64_t>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    (snap_id == CEPH_NOSNAP ? utils::SNAPSHOT_PRESENCE_REQUIRED :
                              utils::SNAPSHOT_PRESENCE_PERMITTED),
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id."
              << std::endl;
    return -EINVAL;
  } else if (!snap_name.empty() && snap_id != CEPH_NOSNAP) {
    std::cerr << "rbd: trying to access snapshot using both name and id."
              << std::endl;
    return -EINVAL;
  } else if ((force || no_progress) && snap_id != CEPH_NOSNAP) {
    std::cerr << "rbd: force and no-progress options not permitted when "
              << "removing by id." << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_pool_full_try();
  if (image_id.empty()) {
    r = utils::open_image(io_ctx, image_name, false, &image);
  } else {
    r = utils::open_image_by_id(io_ctx, image_id, false, &image);
  }
  if (r < 0) {
    return r;
  }

  if (!snap_name.empty()) {
    r = do_remove_snap(image, snap_name.c_str(), force, no_progress);
  } else {
    r = image.snap_remove_by_id(snap_id);
  }

  if (r < 0) {
    if (r == -EBUSY) {
      std::cerr << "rbd: snapshot "
                << (snap_name.empty() ? std::string("id ") + stringify(snap_id) :
                                        std::string("'") + snap_name + "'")
                << " is protected from removal." << std::endl;
    } else {
      std::cerr << "rbd: failed to remove snapshot: " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }
  return 0;
}

void get_purge_arguments(po::options_description *positional,
                         po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_no_progress_option(options);
}

int execute_purge(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_pool_full_try();
  if (image_id.empty()) {
    r = utils::open_image(io_ctx, image_name, false, &image);
  } else {
    r = utils::open_image_by_id(io_ctx, image_id, false, &image);
  }
  if (r < 0) {
    return r;
  }

  r = do_purge_snaps(image, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    if (r != -EBUSY) {
      std::cerr << "rbd: removing snaps failed: " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }
  return 0;
}

void get_rollback_arguments(po::options_description *positional,
                            po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_no_progress_option(options);
}

int execute_rollback(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
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

  r = do_rollback_snap(image, snap_name.c_str(),
                       vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: rollback failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_protect_arguments(po::options_description *positional,
                           po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

int execute_protect(const po::variables_map &vm,
                    const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
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

  bool is_protected = false;
  r = image.snap_is_protected(snap_name.c_str(), &is_protected);
  if (r < 0) {
    std::cerr << "rbd: protecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  } else if (is_protected) {
    std::cerr << "rbd: snap is already protected" << std::endl;
    return -EBUSY;
  }

  r = do_protect_snap(image, snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: protecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void get_unprotect_arguments(po::options_description *positional,
                             po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
}

int execute_unprotect(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_pool_full_try();
  if (image_id.empty()) {
    r = utils::open_image(io_ctx, image_name, false, &image);
  } else {
    r = utils::open_image_by_id(io_ctx, image_id, false, &image);
  }
  if (r < 0) {
    return r;
  }

  bool is_protected = false;
  r = image.snap_is_protected(snap_name.c_str(), &is_protected);
  if (r < 0) {
    std::cerr << "rbd: unprotecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  } else if (!is_protected) {
    std::cerr << "rbd: snap is already unprotected" << std::endl;
    return -EINVAL;
  }

  r = do_unprotect_snap(image, snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: unprotecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

void get_set_limit_arguments(po::options_description *pos,
			     po::options_description *opt) {
  at::add_image_spec_options(pos, opt, at::ARGUMENT_MODIFIER_NONE);
  at::add_limit_option(opt);
}

int execute_set_limit(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  uint64_t limit;

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (vm.count(at::LIMIT)) {
    limit = vm[at::LIMIT].as<uint64_t>();
  } else {
    std::cerr << "rbd: must specify --limit <num>" << std::endl;
    return -ERANGE;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
      return r;
  }

  r = do_set_limit(image, limit);
  if (r < 0) {
    std::cerr << "rbd: setting snapshot limit failed: " << cpp_strerror(r)
	      << std::endl;
    return r;
  }
  return 0;
}

void get_clear_limit_arguments(po::options_description *pos,
			       po::options_description *opt) {
  at::add_image_spec_options(pos, opt, at::ARGUMENT_MODIFIER_NONE);
}

int execute_clear_limit(const po::variables_map &vm,
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

  r = do_set_limit(image, UINT64_MAX);
  if (r < 0) {
    std::cerr << "rbd: clearing snapshot limit failed: " << cpp_strerror(r)
	      << std::endl;
    return r;
  }
  return 0;
}

void get_rename_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE);
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST);
}

int execute_rename(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string src_snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_SOURCE, &arg_index, &pool_name, &namespace_name,
    &image_name, &src_snap_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return -r;
  }

  std::string dest_pool_name(pool_name);
  std::string dest_namespace_name(namespace_name);
  std::string dest_image_name;
  std::string dest_snap_name;
  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, &dest_pool_name,
    &dest_namespace_name, &dest_image_name, &dest_snap_name, true,
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_SNAP);
  if (r < 0) {
    return -r;
  }

  if (pool_name != dest_pool_name) {
    std::cerr << "rbd: source and destination pool must be the same"
              << std::endl;
    return -EINVAL;
  } else if (namespace_name != dest_namespace_name) {
    std::cerr << "rbd: source and destination namespace must be the same"
              << std::endl;
    return -EINVAL;
  } else if (image_name != dest_image_name) {
    std::cerr << "rbd: source and destination image name must be the same"
              << std::endl;
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

  r = image.snap_rename(src_snap_name.c_str(), dest_snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: renaming snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_list(
  {"snap", "list"}, {"snap", "ls"}, "Dump list of image snapshots.", "",
  &get_list_arguments, &execute_list);
Shell::Action action_create(
  {"snap", "create"}, {"snap", "add"}, "Create a snapshot.", "",
  &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"snap", "remove"}, {"snap", "rm"}, "Delete a snapshot.", "",
  &get_remove_arguments, &execute_remove);
Shell::Action action_purge(
  {"snap", "purge"}, {}, "Delete all unprotected snapshots.", "",
  &get_purge_arguments, &execute_purge);
Shell::Action action_rollback(
  {"snap", "rollback"}, {"snap", "revert"}, "Rollback image to snapshot.", "",
  &get_rollback_arguments, &execute_rollback);
Shell::Action action_protect(
  {"snap", "protect"}, {}, "Prevent a snapshot from being deleted.", "",
  &get_protect_arguments, &execute_protect);
Shell::Action action_unprotect(
  {"snap", "unprotect"}, {}, "Allow a snapshot to be deleted.", "",
  &get_unprotect_arguments, &execute_unprotect);
Shell::Action action_set_limit(
  {"snap", "limit", "set"}, {}, "Limit the number of snapshots.", "",
  &get_set_limit_arguments, &execute_set_limit);
Shell::Action action_clear_limit(
  {"snap", "limit", "clear"}, {}, "Remove snapshot limit.", "",
  &get_clear_limit_arguments, &execute_clear_limit);
Shell::Action action_rename(
  {"snap", "rename"}, {}, "Rename a snapshot.", "",
  &get_rename_arguments, &execute_rename);

} // namespace snap
} // namespace action
} // namespace rbd
