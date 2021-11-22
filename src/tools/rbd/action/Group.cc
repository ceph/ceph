// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "tools/rbd/ExportImport.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "librbd/Utils.h"

namespace rbd {
namespace action {
namespace group {

namespace at = argument_types;
namespace po = boost::program_options;

static const std::string GROUP_SPEC("group-spec");
static const std::string GROUP_SNAP_SPEC("group-snap-spec");
static const std::string GROUP_OR_SNAP_SPEC("group-or-snap-spec");

static const std::string GROUP_NAME("group");
static const std::string DEST_GROUP_NAME("dest-group");

static const std::string GROUP_POOL_NAME("group-" + at::POOL_NAME);
static const std::string IMAGE_POOL_NAME("image-" + at::POOL_NAME);

static const std::string RBD_GROUP_BANNER ("rbd group\n");
static const std::string RBD_GROUP_BANNER_SNAP ("rbd group snap\n");
static const std::string RBD_GROUP_BANNER_DIFF ("rbd group diff\n");
static const std::string RBD_GROUP_BANNER_IMAGE_INFO ("rbd group image info\n");

#define RBD_EXPORT_GROUP_IMAGE_POOL_NAME  'L'
#define RBD_EXPORT_GROUP_IMAGE_NAME       'N'
#define RBD_EXPORT_GROUP_CREATE_IMAGE     'G'

void add_group_option(po::options_description *opt,
		      at::ArgumentModifier modifier) {
  std::string name = GROUP_NAME;
  std::string description = at::get_description_prefix(modifier) + "group name";
  switch (modifier) {
  case at::ARGUMENT_MODIFIER_NONE:
  case at::ARGUMENT_MODIFIER_SOURCE:
    break;
  case at::ARGUMENT_MODIFIER_DEST:
    name = DEST_GROUP_NAME;
    break;
  }

  // TODO add validator
  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_prefixed_pool_option(po::options_description *opt,
                              const std::string &prefix) {
  std::string name = prefix + "-" + at::POOL_NAME;
  std::string description = prefix + " pool name";

  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_prefixed_namespace_option(po::options_description *opt,
                                   const std::string &prefix) {
  std::string name = prefix + "-" + at::NAMESPACE_NAME;
  std::string description = prefix + " namespace name";

  opt->add_options()
    (name.c_str(), po::value<std::string>(), description.c_str());
}

void add_group_spec_options(po::options_description *pos,
			    po::options_description *opt,
			    at::ArgumentModifier modifier,
                            bool snap) {
  at::add_pool_option(opt, modifier);
  at::add_namespace_option(opt, modifier);
  add_group_option(opt, modifier);
  if (!snap) {
    pos->add_options()
      ((get_name_prefix(modifier) + GROUP_SPEC).c_str(),
       (get_description_prefix(modifier) + "group specification\n" +
         "(example: [<pool-name>/[<namespace>/]]<group-name>)").c_str());
  } else {
    add_snap_option(opt, modifier);
    pos->add_options()
      ((get_name_prefix(modifier) + GROUP_SNAP_SPEC).c_str(),
       (get_description_prefix(modifier) + "group specification\n" +
         "(example: [<pool-name>/[<namespace>/]]<group-name>@<snap-name>)").c_str());
  }
}

void add_group_or_snap_spec_options(po::options_description *pos,
			    po::options_description *opt,
			    at::ArgumentModifier modifier, bool snap) {
  at::add_pool_option(opt, modifier);
  at::add_namespace_option(opt, modifier);
  add_group_option(opt, modifier);
  if (!snap) {
    pos->add_options()
      ((get_name_prefix(modifier) + GROUP_SPEC).c_str(),
       (get_description_prefix(modifier) + "group specification\n" +
         "(example: [<pool-name>/[<namespace>/]]<group-name>)").c_str());
  } else {
    add_snap_option(opt, modifier);
    pos->add_options()
      ((get_name_prefix(modifier) + GROUP_OR_SNAP_SPEC).c_str(),
       (get_description_prefix(modifier) + "group or snapshot specification\n" +
         "(example: [<pool-name>/[<namespace>/]]<group-name>[@<snap-name>])").c_str());
  }
}

int execute_create(const po::variables_map &vm,
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
  librbd::RBD rbd;
  r = rbd.group_create(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  std::string pool_name;
  std::string namespace_name;
  size_t arg_index = 0;
  int r = utils::get_pool_and_namespace_names(vm, true, false, &pool_name,
                                              &namespace_name, &arg_index);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<std::string> names;
  r = rbd.group_list(io_ctx, &names);
  if (r < 0)
    return r;

  if (f)
    f->open_array_section("groups");
  for (auto i : names) {
    if (f)
      f->dump_string("name", i);
    else
      std::cout << i << std::endl;
  }
  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

int execute_remove(const po::variables_map &vm,
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
  librbd::RBD rbd;

  r = rbd.group_remove(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_rename(const po::variables_map &vm,
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

  std::string dest_pool_name;
  std::string dest_namespace_name;
  std::string dest_group_name;

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_DEST, &arg_index, at::DEST_POOL_NAME,
    &dest_pool_name, &dest_namespace_name, DEST_GROUP_NAME, "group",
    &dest_group_name, nullptr, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  if (pool_name != dest_pool_name) {
    std::cerr << "rbd: group rename across pools not supported" << std::endl
              << "source pool: " << pool_name << ", dest pool: "
              << dest_pool_name << std::endl;
    return -EINVAL;
  } else if (namespace_name != dest_namespace_name) {
    std::cerr << "rbd: group rename across namespaces not supported"
              << std::endl
              << "source namespace: " << namespace_name << ", dest namespace: "
              << dest_namespace_name << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_rename(io_ctx, group_name.c_str(),
                       dest_group_name.c_str());

  if (r < 0) {
    std::cerr << "rbd: failed to rename group: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_add(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  // Parse group data.
  std::string group_pool_name;
  std::string group_namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, GROUP_POOL_NAME,
    &group_pool_name, &group_namespace_name, GROUP_NAME, "group", &group_name,
    nullptr, true, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  std::string image_pool_name;
  std::string image_namespace_name;
  std::string image_name;

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, IMAGE_POOL_NAME,
    &image_pool_name, &image_namespace_name, at::IMAGE_NAME, "image",
    &image_name, nullptr, true, utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  if (group_namespace_name != image_namespace_name) {
    std::cerr << "rbd: group and image namespace must match." << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx cg_io_ctx;
  r = utils::init(group_pool_name, group_namespace_name, &rados, &cg_io_ctx);
  if (r < 0) {
    return r;
  }

  librados::IoCtx image_io_ctx;
  r = utils::init(image_pool_name, group_namespace_name, &rados, &image_io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_image_add(cg_io_ctx, group_name.c_str(),
			  image_io_ctx, image_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: add image error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_remove_image(const po::variables_map &vm,
                         const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;

  std::string group_pool_name;
  std::string group_namespace_name;
  std::string group_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, GROUP_POOL_NAME,
    &group_pool_name, &group_namespace_name, GROUP_NAME, "group", &group_name,
    nullptr, true, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  std::string image_pool_name;
  std::string image_namespace_name;
  std::string image_name;
  std::string image_id;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, IMAGE_POOL_NAME,
    &image_pool_name, &image_namespace_name, at::IMAGE_NAME, "image",
    &image_name, nullptr, image_id.empty(), utils::SNAPSHOT_PRESENCE_NONE,
    utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  if (group_namespace_name != image_namespace_name) {
    std::cerr << "rbd: group and image namespace must match." << std::endl;
    return -EINVAL;
  } else if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx cg_io_ctx;
  r = utils::init(group_pool_name, group_namespace_name, &rados, &cg_io_ctx);
  if (r < 0) {
    return r;
  }

  librados::IoCtx image_io_ctx;
  r = utils::init(image_pool_name, group_namespace_name, &rados, &image_io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  if (image_id.empty()) {
    r = rbd.group_image_remove(cg_io_ctx, group_name.c_str(),
                               image_io_ctx, image_name.c_str());
  } else {
    r = rbd.group_image_remove_by_id(cg_io_ctx, group_name.c_str(),
                                     image_io_ctx, image_id.c_str());
  }
  if (r < 0) {
    std::cerr << "rbd: remove image error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_list_images(const po::variables_map &vm,
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

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<librbd::group_image_info_t> images;

  r = rbd.group_image_list(io_ctx, group_name.c_str(), &images,
                           sizeof(librbd::group_image_info_t));

  if (r == -ENOENT)
    r = 0;

  if (r < 0)
    return r;

  std::sort(images.begin(), images.end(),
    [](const librbd::group_image_info_t &lhs,
       const librbd::group_image_info_t &rhs) {
      if (lhs.pool != rhs.pool) {
        return lhs.pool < rhs.pool;
      }
      return lhs.name < rhs.name;
    }
  );

  if (f)
    f->open_array_section("images");

  for (auto image : images) {
    std::string image_name = image.name;
    int state = image.state;
    std::string state_string;
    if (RBD_GROUP_IMAGE_STATE_INCOMPLETE == state) {
      state_string = "incomplete";
    }

    std::string pool_name = "";

    librados::Rados rados(io_ctx);
    librados::IoCtx pool_io_ctx;
    r = rados.ioctx_create2(image.pool, pool_io_ctx);
    if (r < 0) {
      pool_name = "<missing image pool " + stringify(image.pool) + ">";
    } else {
      pool_name = pool_io_ctx.get_pool_name();
    }

    if (f) {
      f->open_object_section("image");
      f->dump_string("image", image_name);
      f->dump_string("pool", pool_name);
      f->dump_string("namespace", io_ctx.get_namespace());
      f->dump_int("state", state);
      f->close_section();
    } else {
      std::cout << pool_name << "/";
      if (!io_ctx.get_namespace().empty()) {
        std::cout << io_ctx.get_namespace() << "/";
      }
      std::cout << image_name << " " << state_string << std::endl;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

int execute_group_snap_create(const po::variables_map &vm,
                              const std::vector<std::string> &global_args) {
  size_t arg_index = 0;

  std::string pool_name;
  std::string namespace_name;
  std::string group_name;
  std::string snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  uint32_t flags;
  r = utils::get_snap_create_flags(vm, &flags);
  if (r < 0) {
    return r;
  }

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_snap_create2(io_ctx, group_name.c_str(), snap_name.c_str(),
                             flags);
  if (r < 0) {
    return r;
  }

  return 0;
}

int execute_group_snap_remove(const po::variables_map &vm,
                              const std::vector<std::string> &global_args) {
  size_t arg_index = 0;

  std::string pool_name;
  std::string namespace_name;
  std::string group_name;
  std::string snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_snap_remove(io_ctx, group_name.c_str(), snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: failed to remove group snapshot: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_group_snap_rename(const po::variables_map &vm,
                              const std::vector<std::string> &global_args) {
  size_t arg_index = 0;

  std::string pool_name;
  std::string namespace_name;
  std::string group_name;
  std::string source_snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &source_snap_name, true,
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  std::string dest_snap_name;
  if (vm.count(at::DEST_SNAPSHOT_NAME)) {
    dest_snap_name = vm[at::DEST_SNAPSHOT_NAME].as<std::string>();
  }

  if (dest_snap_name.empty()) {
    dest_snap_name = utils::get_positional_argument(vm, arg_index++);
  }

  if (dest_snap_name.empty()) {
    std::cerr << "rbd: destination snapshot name was not specified"
              << std::endl;
    return -EINVAL;
  }

  r = utils::validate_snapshot_name(at::ARGUMENT_MODIFIER_DEST, dest_snap_name,
                                    utils::SNAPSHOT_PRESENCE_REQUIRED,
                                    utils::SPEC_VALIDATION_SNAP);
  if (r < 0) {
    return r;
  }
  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_snap_rename(io_ctx, group_name.c_str(),
                            source_snap_name.c_str(), dest_snap_name.c_str());

  if (r < 0) {
    std::cerr << "rbd: failed to rename group snapshot: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_group_snap_list(const po::variables_map &vm,
                            const std::vector<std::string> &ceph_global_args) {
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

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<librbd::group_snap_info_t> snaps;

  r = rbd.group_snap_list(io_ctx, group_name.c_str(), &snaps,
                          sizeof(librbd::group_snap_info_t));

  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    return r;
  }

  TextTable t;
  if (f) {
    f->open_array_section("group_snaps");
  } else {
    t.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    t.define_column("STATUS", TextTable::LEFT, TextTable::RIGHT);
  }

  for (auto i : snaps) {
    std::string snap_name = i.name;
    int state = i.state;
    std::string state_string;
    if (RBD_GROUP_SNAP_STATE_INCOMPLETE == state) {
      state_string = "incomplete";
    } else {
      state_string = "ok";
    }
    if (r < 0) {
      return r;
    }
    if (f) {
      f->open_object_section("group_snap");
      f->dump_string("snapshot", snap_name);
      f->dump_string("state", state_string);
      f->close_section();
    } else {
      t << snap_name << state_string << TextTable::endrow;
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

int execute_group_snap_rollback(const po::variables_map &vm,
                                const std::vector<std::string> &global_args) {
  size_t arg_index = 0;

  std::string group_name;
  std::string namespace_name;
  std::string pool_name;
  std::string snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_REQUIRED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  utils::ProgressContext pc("Rolling back to group snapshot",
                            vm[at::NO_PROGRESS].as<bool>());
  r = rbd.group_snap_rollback_with_progress(io_ctx, group_name.c_str(),
                                            snap_name.c_str(), pc);
  if (r < 0) {
    pc.fail();
    std::cerr << "rbd: rollback group to snapshot failed: "
              << cpp_strerror(r) << std::endl;
    return r;
  }

  pc.finish();
  return 0;
}

typedef struct {
  std::string snap_name;
  std::vector<librbd::group_image_snap_info_t> images_snap;
}group_snap_info;

typedef struct {
  std::string image_pool_name;
  librbd::Image image;
}image_export_info;

static int create_ioctx(librados::IoCtx& src_io_ctx, const std::string& pool_desc,
                 int64_t pool_id,
                 const std::optional<std::string>& pool_namespace,
                 librados::IoCtx* dst_io_ctx) {

  librados::Rados rados(src_io_ctx);
  int r = rados.ioctx_create2(pool_id, *dst_io_ctx);
  if (r == -ENOENT) {
    std::cerr  << pool_desc << " pool " << pool_id << " no longer exists"
                  << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "error accessing " << pool_desc << " pool " << pool_id
               << std::endl;
    return r;
  }

  dst_io_ctx->set_namespace(
    pool_namespace ? *pool_namespace : src_io_ctx.get_namespace());
  return 0;
}

static int get_image_key(const int64_t pool_id, const char *image_name,
                        std::string *image_key)
{
  std::stringstream sstream;
  sstream << pool_id;

  std::string &key = *image_key;
  sstream >> key;
  key.append("_").append(image_name);

  return 0;
}

static int open_export_image(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                            const int64_t pool_id, const char *image_name,
                            std::map<std::string, image_export_info> *images,
                            std::vector<librbd::RBD::AioCompletion*> &on_finishes)
{
  std::string key;
  get_image_key(pool_id, image_name, &key);

  auto it = images->find(key);
  if (it != images->end()) {
    return 0;
  }

  librados::IoCtx image_io_ctx;
  int r = create_ioctx(group_ioctx, "image", pool_id,
                              {}, &image_io_ctx);
  if (r < 0) {
    std::cerr << "Create io ctx failed." << std::endl;
    return r;
  }

  image_export_info &image_info = (*images)[key];
  image_info.image_pool_name = image_io_ctx.get_pool_name();

  librbd::RBD::AioCompletion *open_comp =
    new librbd::RBD::AioCompletion(NULL, NULL);

  r = rbd.aio_open_read_only(image_io_ctx, image_info.image, image_name,
                            NULL, open_comp);
  if (r < 0) {
    std::cerr << "Open image failed. image name:" << image_name << std::endl;
    return r;
  }

  on_finishes.push_back(open_comp);

  return r;
}

static int open_group_image(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                     const char *group_name,
                     const std::vector<group_snap_info> &group_snaps,
                     const std::vector<librbd::group_image_info_t> &image_list,
                     std::map<std::string, image_export_info> *images)
{
  int open_ret = 0, r = 0;
  std::vector<librbd::RBD::AioCompletion*> on_finishes;
  for (auto &group_snap: group_snaps) {
    for (auto &image_snap: group_snap.images_snap) {
      r = open_export_image(rbd, group_ioctx, image_snap.pool_id,
                            image_snap.image_name.c_str(), images, on_finishes);
      if (r < 0) {
        open_ret = r;
        goto err;
      }
    }
  }

  for (auto &image_info: image_list) {
    r = open_export_image(rbd, group_ioctx, image_info.pool,
                    image_info.name.c_str(), images, on_finishes);
    if (r < 0) {
      open_ret = r;
      goto err;
    }
  }

  err:
  for (uint32_t i = 0; i < on_finishes.size(); ++i) {
    on_finishes[i]->wait_for_complete();
    r = on_finishes[i]->get_return_value();
    if (r < 0) {
      open_ret = r;
    }
    on_finishes[i]->release();
  }

  return open_ret;
}

static int group_export_header(const std::string &flag, const uint64_t num, int fd)
{
  bufferlist bl;
  bl.append(flag);

  encode(num, bl);

  return bl.write_fd(fd);
}

static int group_export_snap_info(const std::string &fromsnapname,
                      const std::string &endsnapname,
                      int fd, const int export_format)
{
  /* snap header */
  bufferlist bl;
  bl.append(RBD_GROUP_BANNER_SNAP);

  __u8 tag;
  uint64_t len = 0;

  if (!fromsnapname.empty()) {
    tag = RBD_DIFF_FROM_SNAP;
    encode(tag, bl);
    if (export_format == 2) {
      len = fromsnapname.length() + 4;
      encode(len, bl);
    }
    encode(fromsnapname, bl);
  }

  if (!endsnapname.empty()) {
    tag = RBD_DIFF_TO_SNAP;
    encode(tag, bl);
    if (export_format == 2) {
      len = endsnapname.length() + 4;
      encode(len, bl);
    }
    encode(endsnapname, bl);
  }

  // encode end tag
  tag = RBD_EXPORT_IMAGE_END;
  encode(tag, bl);

  int r = bl.write_fd(fd);
  if (r < 0) {
    std::cerr << "group export snap info failed." << std::endl;
    return r;
  }

  return r;
}

static int group_export_image_diff_fd(librbd::Image &image,
                const std::string &fromsnapname, const librados::snap_t endsnapid,
                const int64_t group_pool,
                const std::string &group_id,
                const bool whole_object, int fd,
                const int export_format, const bool no_progress)
{
  int r;
  librbd::image_info_t info;

  r = image.stat(info, sizeof(info));
  if (r < 0) {
    return r;
  }

  r = utils::image_export_diff_header(image, info, nullptr, nullptr,
                                      fd, export_format);
  if (r < 0) {
    return r;
  }

  utils::ExportDiffContext edc(&image, fd, info.size,
                  g_conf().get_val<uint64_t>("rbd_concurrent_management_ops"),
                  no_progress, export_format);
  r = image.diff_iterate_group_image_snap(
              fromsnapname.empty() ? nullptr : fromsnapname.c_str(),
              0, info.size, true, whole_object,
              &utils::C_ExportDiff::export_diff_cb, (void *)&edc);
  if (r < 0) {
    goto out;
  }

  r = edc.throttle.wait_for_ret();
  if (r < 0) {
    goto out;
  }

  {
    __u8 tag = RBD_DIFF_END;
    bufferlist bl;
    encode(tag, bl);
    r = bl.write_fd(fd);
  }

out:
  if (r < 0)
    edc.pc.fail();
  else
    edc.pc.finish();

  return r;
}

static void group_encode_string(bufferlist &bl, const std::string &str, const __u8 tag,
                                const int export_format)
{
  if (!str.empty()) {
    encode(tag, bl);
    if (export_format == 2) {
      uint64_t length = str.length() + 4;
      encode(length, bl);
    }
    encode(str, bl);
  }
}

static void group_encode_uint64(bufferlist &bl, const uint64_t num,
                                const __u8 tag, const int export_format)
{
  encode(tag, bl);
  if (export_format == 2) {
    uint64_t len = 8;
    encode(len, bl);
  }
  encode(num, bl);
}

static int group_export_image_info(const std::string &image_pool_name,
                librbd::Image& image, const librados::snap_t from_snap_id,
                int fd, const int export_format)
{
  __u8 tag;
  bufferlist bl;

  /* image banner*/
  bl.append(RBD_GROUP_BANNER_IMAGE_INFO);

  /* record image pool name */
  group_encode_string(bl, image_pool_name,
                      RBD_EXPORT_GROUP_IMAGE_POOL_NAME, export_format);

  /* record image name */
  std::string src_image_name;
  image.get_name(&src_image_name);
  group_encode_string(bl, src_image_name, RBD_EXPORT_GROUP_IMAGE_NAME, export_format);

  /* encode is create image */
  const uint64_t is_create = (from_snap_id == CEPH_NOSNAP ? 1 : 0);
  group_encode_uint64(bl, is_create, RBD_EXPORT_GROUP_CREATE_IMAGE, export_format);

  // encode end tag
  tag = RBD_EXPORT_IMAGE_END;
  encode(tag, bl);

  // write bl to fd.
  return bl.write_fd(fd);
}

static int group_export_image_diff(librados::IoCtx& group_ioctx,
                      const std::string& group_id,
                      const group_snap_info* group_from_snap,
                      const int64_t end_pool_id, const std::string &end_image_name,
                      const snapid_t end_snap_id,
                      const std::map<std::string, image_export_info> &images,
                      const bool whole_object, int fd,
                      const int export_format, const bool no_progress)
{
  int64_t r = 0;
  librados::snap_t from_snap_id = CEPH_NOSNAP;
  /* find image from snap */
  if (group_from_snap) {
    for (auto &image_snap : group_from_snap->images_snap) {
      if (end_pool_id == image_snap.pool_id &&
          end_image_name == image_snap.image_name) {
        from_snap_id = image_snap.snap_id;
        break;
      }
    }
  }

  std::string key;
  get_image_key(end_pool_id, end_image_name.c_str(), &key);
  auto image_it = images.find(key);

  if (image_it == images.end()) {
    std::cerr << "Can't find image by key:" << key << std::endl;
    return -1;
  }

  image_export_info &image_info = const_cast<image_export_info&>(image_it->second);

  /*export header*/
  {
    r = group_export_image_info(image_info.image_pool_name, image_info.image,
                                from_snap_id, fd, export_format);
    if (r < 0) {
      return r;
    }

    /* have new image add */
    if (from_snap_id == CEPH_NOSNAP){
      librbd::image_info_t info;
      int r = image_info.image.stat(info, sizeof(info));
      if (r < 0) {
        return r;
      }

      r = utils::image_export_metadata(image_info.image, info, fd);
      if (r < 0) {
        return r;
      }
    }
  }

  std::string from_snap_name;
  if (from_snap_id != CEPH_NOSNAP) {
    r = image_info.image.snap_get_name(from_snap_id, &from_snap_name);
    if (r < 0) {
      return r;
    }
  }

  /* image diff export */
  image_info.image.snap_set_by_id(end_snap_id);
  r = group_export_image_diff_fd(image_info.image, from_snap_name, end_snap_id,
                  group_ioctx.get_id(), group_id,
                  whole_object, fd, export_format, no_progress);

  return r;
}

static int group_export_diff(librados::IoCtx& group_ioctx,
                      const char *group_name,
                      const std::string &group_id,
                      const group_snap_info* group_from_snap,
                      const group_snap_info* group_end_snap,
                      const std::vector<librbd::group_image_info_t> &image_list,
                      const std::map<std::string, image_export_info> &images,
                      const bool whole_object, int fd,
                      const int export_format, const bool no_progress)
{
  int r = group_export_snap_info(group_from_snap ? group_from_snap->snap_name : "",
                        group_end_snap ? group_end_snap->snap_name : "",
                        fd, export_format);
  if (r < 0) {
    return r;
  }

  int pc_cnt = 1;
  utils::ProgressContext pc("Exporting group diff", no_progress);
  if (group_end_snap) {
    /* group diff header */
    r = group_export_header(RBD_GROUP_BANNER_DIFF,
                            group_end_snap->images_snap.size(), fd);
    if (r < 0) {
      goto err;
    }

    for (auto &end_snap : group_end_snap->images_snap) {
      r = group_export_image_diff(group_ioctx,group_id, group_from_snap,
                              end_snap.pool_id, end_snap.image_name, end_snap.snap_id,
                              images, whole_object, fd, export_format, true);
      if (r < 0) {
        goto err;
      }

      pc.update_progress(pc_cnt++, group_end_snap->images_snap.size());
    }
  }
  else
  {
    /* group diff header */
    r = group_export_header(RBD_GROUP_BANNER_DIFF, image_list.size(), fd);
    if (r < 0) {
      goto err;
    }

    for (auto &image_info: image_list) {
      r = group_export_image_diff(group_ioctx, group_id, group_from_snap,
                              image_info.pool,
                              image_info.name, CEPH_NOSNAP,
                              images, whole_object, fd, export_format, true);
      if (r < 0) {
        goto err;
      }
      pc.update_progress(pc_cnt++, image_list.size());
    }
  }

  err:
  if (r < 0)
    pc.fail();
  else
    pc.finish();

  return r;
}

static int group_export(librados::IoCtx& group_ioctx, const char *group_name,
                     int fd, std::map<std::string, image_export_info> &images,
                     const bool no_progress, librbd::ProgressContext& pc)
{
  librbd::RBD rbd;

  /* snap list */
  std::vector<group_snap_info> snaps;
  int r = 0;
  {
    std::vector<librbd::group_snap_info_t> snaps_info;
    r = rbd.group_snap_list(group_ioctx, group_name, &snaps_info,
                              sizeof(librbd::group_snap_info_t));
    if (r < 0) {
      return r;
    }

    for (auto snap: snaps_info) {
      auto it = snaps.insert(snaps.end(), group_snap_info{snap.name, {}});
      r = rbd.group_image_snap_list(group_ioctx, group_name, snap.name.c_str(),
                      &(it->images_snap), sizeof(librbd::group_image_snap_info_t));
      if (r < 0) {
        return r;
      }
    }
  }

  /* image list */
  std::vector<librbd::group_image_info_t> image_list;
  {
    r = rbd.group_image_list(group_ioctx, group_name, &image_list,
                            sizeof(librbd::group_image_info_t));
    if (r < 0) {
      return r;
    }
  }

  /* open all image */
  r = open_group_image(rbd, group_ioctx, group_name, snaps, image_list, &images);
  if (r < 0) {
    return r;
  }

  /* group diff header */
  r = group_export_header(RBD_GROUP_BANNER, snaps.size() + 1, fd);
  if (r < 0) {
    return r;
  }

  std::string group_id;
  r = librbd::cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
                                 group_name, &group_id);
  if (r < 0) {
    std::cerr << "error reading group id object: " << cpp_strerror(r) << std::endl;
    return r;
  }

  group_snap_info *last_snap = NULL;
  for (size_t i = 0; i < snaps.size(); ++i)
  {
    r = group_export_diff(group_ioctx, group_name, group_id, last_snap, &snaps[i],
                          image_list, images, false, fd, 2, no_progress);
    if (r < 0) {
      return r;
    }

    pc.update_progress(i + 1, snaps.size() + 1);
    last_snap = &snaps[i];
  }

  r = group_export_diff(group_ioctx, group_name, group_id, last_snap, nullptr,
                        image_list, images, false, fd, 2, no_progress);
  if (r < 0) {
    return r;
  }
  pc.update_progress(snaps.size() + 1, snaps.size() + 1);

  return r;
}

static int do_group_export(librados::IoCtx& group_ioctx, const char *group_name,
                     const char *path, const bool no_progress)
{
  int r = 0;
  int fd;
  bool to_stdout;

  /* open file */
  r = utils::open_export_file(path, fd, to_stdout);
  if (r < 0) {
    return r;
  }

  std::map<std::string, image_export_info> images;
  utils::ProgressContext pc("Exporting group", no_progress);
  r = group_export(group_ioctx, group_name, fd, images, true, pc);

  if (!to_stdout)
    close(fd);

  if (r < 0)
    pc.fail();
  else
    pc.finish();

  return r;
}

int execute_group_export(const po::variables_map &vm,
                        const std::vector<std::string> &global_args) {
  size_t arg_index = 0;

  std::string group_name;
  std::string namespace_name;
  std::string pool_name;
  std::string snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_group_export(io_ctx, group_name.c_str(),
                      path.c_str(), vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: group export error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return r;
}

static int do_group_export_diff(librados::IoCtx& group_ioctx,
                const char *group_name, const std::string &fromsnapname,
                const std::string &endsnapname, const bool whole_object,
                const char *path, const bool no_progress)
{
  librbd::RBD rbd;
  std::vector<group_snap_info> snaps;
  int r = 0;
  group_snap_info *group_from_snap = nullptr;
  if (!fromsnapname.empty()) {
    auto it = snaps.insert(snaps.end(), group_snap_info{fromsnapname, {}});
    r = rbd.group_image_snap_list(group_ioctx, group_name, fromsnapname.c_str(),
                      &(it->images_snap), sizeof(librbd::group_image_snap_info_t));
    if (r < 0) {
      return r;
    }
  }

  group_snap_info *group_end_snap = nullptr;
  if (!endsnapname.empty()) {
    auto it = snaps.insert(snaps.end(), group_snap_info{endsnapname, {}});
    r = rbd.group_image_snap_list(group_ioctx, group_name, endsnapname.c_str(),
                      &(it->images_snap), sizeof(librbd::group_image_snap_info_t));
    if (r < 0) {
      return r;
    }
    group_end_snap = &(*it);
  }

  if (!fromsnapname.empty()) {
    group_from_snap = &snaps[0];
  }

  std::vector<librbd::group_image_info_t> image_list;
  if (group_end_snap == nullptr) {
    /* image list */
    r =rbd.group_image_list(group_ioctx, group_name, &image_list,
                            sizeof(librbd::group_image_info_t));
    if (r < 0) {
      return r;
    }
  }

  std::map<std::string, image_export_info> images;
  std::string group_id;
  r = librbd::cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
                                 group_name, &group_id);
  if (r < 0) {
    std::cerr << "error reading group id object: " << cpp_strerror(r) << std::endl;
    return r;
  }

  /* open file */
  int fd;
  bool to_stdout;
  r = utils::open_export_file(path, fd, to_stdout);
  if (r < 0) {
    return r;
  }

  std::vector<group_snap_info> group_snap;
  if (group_end_snap) {
    group_snap.push_back(*group_end_snap);
  }

  /* open image */
  r = open_group_image(rbd, group_ioctx, group_name, group_snap, image_list, &images);
  if (r < 0) {
    goto err;
  }

  r = group_export_diff(group_ioctx, group_name, group_id, group_from_snap,
                        group_end_snap, image_list, images,
                        whole_object, fd, 2, no_progress);

  err:
  if (!to_stdout)
    close(fd);

  return r;
}

int execute_group_export_diff(const po::variables_map &vm,
                        const std::vector<std::string> &global_args)
{
  size_t arg_index = 0;

  std::string group_name;
  std::string namespace_name;
  std::string pool_name;
  std::string snap_name;

  int r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_group_export_diff(io_ctx, group_name.c_str(),
                     from_snap_name,
                     snap_name,
                     vm[at::WHOLE_OBJECT].as<bool>(), path.c_str(),
                     vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: group export diff error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return r;
}

static int group_decode_uint64(int fd, uint64_t *val)
{
  char buf[sizeof(uint64_t)];

  int64_t r = safe_read_exact(fd, buf, sizeof(buf));
  if (r < 0) {
    std::cerr << "rbd: failed to decode image option" << std::endl;
    return r;
  }

  bufferlist bl;
  bl.append(buf, sizeof(buf));
  auto it = bl.cbegin();

  decode(*val, it);

  return r;
}

int group_read_string(int fd, unsigned max, std::string *out) {
  char buf[4];

  int r = safe_read_exact(fd, buf, 4);
  if (r < 0)
    return r;

  bufferlist bl;
  bl.append(buf, 4);
  auto p = bl.cbegin();
  uint32_t len;
  decode(len, p);
  if (len > max)
    return -EINVAL;

  char sbuf[len];
  r = safe_read_exact(fd, sbuf, len);
  if (r < 0)
    return r;
  out->assign(sbuf, len);

  return r;
}

static int group_snap_is_exist(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                              const char *group_name,
                               const std::string &group_snap_name, bool &is_exist)
{
  std::vector<librbd::group_snap_info_t> snaps;

  int r = rbd.group_snap_list(group_ioctx, group_name, &snaps,
                              sizeof(librbd::group_snap_info_t));
  if (r < 0) {
    return r;
  }

  is_exist = false;
  for (auto snap : snaps) {
    if (snap.name == group_snap_name) {
      is_exist = true;
      break;
    }
  }

  return r;
}

static int do_group_snap_from(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                              const char *group_name, int fd)
{
  int r;
  std::string from;

  // 4k limit to make sure we don't get a garbage string
  r = group_read_string(fd, 4096, &from);
  if (r < 0) {
    std::cerr << "rbd: failed to decode start snap name" << std::endl;
    return r;
  }

  bool exist = false;
  r = group_snap_is_exist(rbd, group_ioctx, group_name, from, exist);
  if (r < 0) {
    return r;
  }

  if (!exist) {
    std::cerr << "start snapshot '" << from
              << "' does not exist in the image, aborting" << std::endl;
    return -EINVAL;
  }

  return r;
}

static int do_group_image_snap_to(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                                  const char *group_name,
                                  int fd, std::string &tosnap)
{
  int r;
  std::string to;
  // 4k limit to make sure we don't get a garbage string
  r = group_read_string(fd, 4096, &to);
  if (r < 0) {
    std::cerr << "rbd: failed to decode end snap name" << std::endl;
    return r;
  }

  bool exists;
  r = group_snap_is_exist(rbd, group_ioctx, group_name, to, exists);
  if (r < 0) {
    return r;
  }

  if (exists) {
    std::cerr << "end snapshot '" << to << "' already exists, aborting" << std::endl;
    return -EEXIST;
  }

  tosnap = to;

  return r;
}

static int group_import_snap_info(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                        const char *group_name, int fd,
                        std::string &to_snap, const int import_format)
{
  int r = utils::validate_banner(fd, RBD_GROUP_BANNER_SNAP);
  if (r < 0) {
    return r;
  }

  while (r == 0) {
    __u8 tag;
    uint64_t length = 0;
    r = utils::read_tag(fd, RBD_EXPORT_IMAGE_END, import_format, &tag, &length);
    if (r < 0 || tag == RBD_EXPORT_IMAGE_END) {
      break;
    }

    if (RBD_DIFF_FROM_SNAP == tag) {
      r = do_group_snap_from(rbd, group_ioctx, group_name, fd);
    } else if (RBD_DIFF_TO_SNAP == tag) {
      r = do_group_image_snap_to(rbd, group_ioctx, group_name, fd, to_snap);
    } else {
      std::cerr << "rbd: invalid tag in image properties zone: " << tag
                << ", Skip it." << std::endl;
      r = utils::skip_tag(fd, length);
    }
  }

  return r;
}

static int group_import_image_info(int fd,
                        std::string *src_pool_name, std::string *src_image_name,
                        uint64_t *is_create_image, const int import_format)
{
  int r = utils::validate_banner(fd, RBD_GROUP_BANNER_IMAGE_INFO);
  if (r < 0) {
    return r;
  }

  while (r == 0) {
    __u8 tag;
    uint64_t length = 0;
    r = utils::read_tag(fd, RBD_EXPORT_IMAGE_END, import_format, &tag, &length);
    if (r < 0 || tag == RBD_EXPORT_IMAGE_END) {
      break;
    }

    if (RBD_EXPORT_GROUP_IMAGE_POOL_NAME == tag) {
      r = group_read_string(fd, 4096, src_pool_name);
    } else if (RBD_EXPORT_GROUP_IMAGE_NAME == tag) {
      r = group_read_string(fd, 4096, src_image_name);
    } else if (RBD_EXPORT_GROUP_CREATE_IMAGE == tag) {
      r = group_decode_uint64(fd, is_create_image);
    } else {
      std::cerr << "rbd: invalid tag in image properties zone: "
                << tag << ", Skip it." << std::endl;
      r = utils::skip_tag(fd, length);
    }
  }

  return r;
}

static int group_image_import_diff(librbd::RBD &rbd,
                        librados::Rados &rados, librados::IoCtx& group_ioctx,
                        const char *group_name, const std::string &to_snap, int fd,
                        librbd::ImageOptions& opts, const size_t sparse_size,
                        std::map<std::string, librbd::Image> *images,
                        const std::vector<librbd::group_image_info_t> &images_list,
                        const int import_format, bool no_progress)
{
  std::string src_pool_name;
  std::string src_image_name;
  uint64_t is_create_image;
  /* import image info */
  int r = group_import_image_info(fd, &src_pool_name, &src_image_name,
                              &is_create_image, import_format);
  if (r < 0) {
    return r;
  }

  librados::IoCtx image_io_ctx;
  int64_t image_pool_id = 0;
  {
    image_pool_id = rados.pool_lookup(src_pool_name.c_str());
    if (image_pool_id < 0) {
      std::cerr << "get image pool id err. image pool name:"
                << src_pool_name << std::endl;
      return r;
    }

    r = create_ioctx(group_ioctx, "image", image_pool_id,
                                  {}, &image_io_ctx);
    if (r < 0) {
      return r;
    }

    std::string group_id;
    r = librbd::cls_client::dir_get_id(&group_ioctx, RBD_GROUP_DIRECTORY,
                              group_name, &group_id);
    if (r < 0) {
      std::cerr << "error reading group id object: " << cpp_strerror(r) << std::endl;
      return r;
    }
  }

  librbd::Image* image = nullptr;
  std::string image_key(src_pool_name);
  image_key.append("_").append(src_image_name);
  auto image_it = images->find(image_key);
  if (image_it != images->end()) {
    image = &image_it->second;
  } else {
    std::map<std::string, std::string> imagemetas;
    if (1 == is_create_image) {
      librbd::ImageOptions image_opts(opts);
      /* import metadata */
      r = utils::do_import_header(fd, import_format, image_opts, &imagemetas);
      if (r < 0) {
        std::cerr << "rbd: import header failed." << std::endl;
        return r;
      }

      /* create image */
      r = rbd.create4(image_io_ctx, src_image_name.c_str(), (uint64_t)1, image_opts);
      if (r < 0) {
        return r;
      }

      /* add image to group */
      r = rbd.group_image_add(group_ioctx, group_name,
                              image_io_ctx, src_image_name.c_str());
      if (r < 0) {
        int rm_ret = rbd.remove(image_io_ctx, src_image_name.c_str());
        if (rm_ret < 0) {
          std::cerr << "remove image err, image:" << src_image_name << std::endl;
        }

        return r;
      }
    } else {
      /* check group image */
      bool is_find = false;
      for (auto &image_info : images_list) {
        if(image_info.name == src_image_name && image_info.pool == image_pool_id) {
          is_find = true;
          break;
        }
      }
      if(!is_find) {
        std::cerr << "Can't find image in group, image:" << src_pool_name << "/" <<
                      src_image_name << ", group:" << group_name << std::endl;
        return -1;
      }
    }

    image = &((*images)[image_key]);

    r = rbd.open(image_io_ctx, *image, src_image_name.c_str());
    if (r < 0) {
      std::cerr << "rbd: failed to open image" << std::endl;
      return r;
    }

    if (1 == is_create_image) {
      r = utils::do_import_metadata(import_format, *image, imagemetas);
      if (r < 0) {
        std::cerr << "rbd: failed to import image-meta" << std::endl;
        return r;
      }
    }
  }

  /* import image diff */
  return utils::do_import_diff_fd(rados, *image, fd, true,
                          import_format, sparse_size);
}

static int group_import_diff(librbd::RBD &rbd,
                        librados::Rados &rados, librados::IoCtx& group_ioctx,
                        const char *group_name, int fd,
                        librbd::ImageOptions& opts, const size_t sparse_size,
                        std::map<std::string, librbd::Image> *images,
                        const std::vector<librbd::group_image_info_t> &images_list,
                        const int import_format, const bool no_progress)
{
  std::string to_snap;
  int r = group_import_snap_info(rbd, group_ioctx, group_name,
                                fd, to_snap, import_format);
  if (r < 0) {
    return r;
  }

  r = utils::validate_banner(fd, RBD_GROUP_BANNER_DIFF);
  if (r < 0) {
    return r;
  }

  uint64_t image_num = 0;
  r = group_decode_uint64(fd, &image_num);
  if (r < 0) {
    return r;
  }

  utils::ProgressContext pc("Importing group", no_progress);
  int pc_cnt = 1;
  for (size_t i = 0; i < image_num; i++) {
    r = group_image_import_diff(rbd, rados, group_ioctx, group_name, to_snap, fd,
                opts, sparse_size, images, images_list, import_format, no_progress);
    if (r < 0) {
      goto err;
    }

    pc.update_progress(pc_cnt++, image_num);
  }

  if (!to_snap.empty()) {
    /* create group snap */
    r = rbd.group_snap_create(group_ioctx, group_name, to_snap.c_str());
  }

  err:
  if (r < 0)
    pc.fail();
  else
    pc.finish();

  return r;
}

static int group_import_recover(librbd::RBD &rbd, librados::IoCtx& group_ioctx,
                                 const char *group_name)
{
  int r = 0;
  std::vector<librbd::group_snap_info_t> snaps;
  rbd.group_snap_list(group_ioctx, group_name, &snaps,
                      sizeof(librbd::group_snap_info_t));
  for (auto snap: snaps) {
    r = rbd.group_snap_remove(group_ioctx, group_name, snap.name.c_str());
    if (r < 0) {
      return r;
    }
  }

  std::vector<librbd::group_image_info_t> images_list;
  /* get image list */
  r = rbd.group_image_list(group_ioctx, group_name, &images_list,
                              sizeof(librbd::group_image_info_t));
  if (r < 0) {
    return r;
  }

  /* remove images of group */
  for(auto image : images_list) {
    librados::IoCtx image_io_ctx;
    r = create_ioctx(group_ioctx, "image", image.pool, {}, &image_io_ctx);
    if (r < 0) {
      return r;
    }

    r = rbd.group_image_remove(group_ioctx, group_name,
                                           image_io_ctx, image.name.c_str());
    if (r < 0) {
      return r;
    }

    r = rbd.remove(image_io_ctx, image.name.c_str());
    if (r < 0) {
      return r;
    }
  }

  /* remove group */
  return rbd.group_remove(group_ioctx, group_name);
}

static int group_import(librados::Rados &rados, librados::IoCtx& group_ioctx,
                        const char *group_name, int fd,
                        librbd::ImageOptions& opts, const size_t sparse_size,
                        const bool no_progress, librbd::ProgressContext& pc)
{
  int r = utils::validate_banner(fd, RBD_GROUP_BANNER);
  if (r < 0) {
    return r;
  }

  uint64_t diff_num = 0;
  r = group_decode_uint64(fd, &diff_num);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  /* create group */
  r = rbd.group_create(group_ioctx, group_name);
  if (r < 0) {
    return r;
  }

  std::map<std::string, librbd::Image> images;
  for (size_t i = 0; i < diff_num; i++) {
    /* get image list */
    std::vector<librbd::group_image_info_t> images_list;
    r = rbd.group_image_list(group_ioctx, group_name, &images_list,
                            sizeof(librbd::group_image_info_t));
    if (r < 0) {
      goto err;
    }

    /* import group diff */
    r = group_import_diff(rbd, rados, group_ioctx, group_name, fd, opts, sparse_size,
                          &images, images_list, 2, true);
    if (r < 0) {
      goto err;
    }

    pc.update_progress(i+1, diff_num);
  }

  err:
  if (r < 0) {
    /* recover */
    group_import_recover(rbd, group_ioctx, group_name);
  }

  return r;
}

static int open_import_file(const char *path, int &fd, bool &from_stdin)
{
  from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    fd = STDIN_FILENO;
  } else {
    if ((fd = open(path, O_RDONLY)) < 0) {
      std::cerr << "rbd: error opening " << path << std::endl;
      return -errno;
    }

    struct stat stat_buf;
    if ((fstat(fd, &stat_buf)) < 0) {
      std::cerr << "rbd: stat error " << path << std::endl;
      return -errno;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
      std::cerr << "rbd: cannot import a directory" << std::endl;
      return -EISDIR;
    }

#ifdef HAVE_POSIX_FADVISE
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
  }

  return 0;
}

static int do_group_import(librados::Rados &rados, librados::IoCtx& group_ioctx,
                        const char *group_name,
                        const char *path, librbd::ImageOptions& opts,
                        const size_t sparse_size, const bool no_progress)
{
  int r = 0;
  if (!group_name) {
    std::cerr << "rbd: group name is null." << path << std::endl;
    return -1;
  }

  int fd = -1;
  bool from_stdin = false;
  r = open_import_file(path, fd, from_stdin);
  if(r < 0) {
    return r;
  }

  utils::ProgressContext pc("Importing group", no_progress);
  r = group_import(rados, group_ioctx, group_name, fd, opts,
                  sparse_size, no_progress, pc);

  if (!from_stdin)
    close(fd);

  if (r < 0)
    pc.fail();
  else
    pc.finish();

  return r;
}

int execute_group_import(const po::variables_map &vm,
                        const std::vector<std::string> &global_args) {
  std::string path;
  size_t arg_index = 0;

  std::string group_name;
  std::string namespace_name;
  std::string pool_name;
  std::string snap_name;

  int r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_group_import(rados, io_ctx, group_name.c_str(), path.c_str(),
                opts, sparse_size, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: import failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return r;
}

static int do_group_import_diff(librados::Rados &rados, librados::IoCtx& group_ioctx,
                        const char *group_name,
                        const char *path, librbd::ImageOptions& opts,
                        const size_t sparse_size, const bool no_progress)
{
  int fd = -1;
  bool from_stdin = false;
  int r = open_import_file(path, fd, from_stdin);
  if(r < 0) {
    return r;
  }

  std::map<std::string, librbd::Image> images;
  librbd::RBD rbd;
  /* get image list */
  std::vector<librbd::group_image_info_t> images_list;
  r = rbd.group_image_list(group_ioctx, group_name, &images_list,
                          sizeof(librbd::group_image_info_t));
  if (r < 0) {
    goto err;
  }

  r = group_import_diff(rbd, rados, group_ioctx, group_name, fd, opts, sparse_size,
                          &images, images_list, 2, no_progress);

  err:
  if (!from_stdin)
    close(fd);

  return r;
}

int execute_group_import_diff(const po::variables_map &vm,
                        const std::vector<std::string> &global_args)
{
  std::string path;
  size_t arg_index = 0;

  std::string group_name;
  std::string namespace_name;
  std::string pool_name;
  std::string snap_name;

  int r = utils::get_path(vm, &arg_index, &path);
  if (r < 0) {
    return r;
  }

  r = utils::get_pool_generic_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, at::POOL_NAME, &pool_name,
    &namespace_name, GROUP_NAME, "group", &group_name, &snap_name, true,
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  size_t sparse_size = utils::RBD_DEFAULT_SPARSE_SIZE;
  if (vm.count(at::IMAGE_SPARSE_SIZE)) {
    sparse_size = vm[at::IMAGE_SPARSE_SIZE].as<size_t>();
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  r = do_group_import_diff(rados, io_ctx, group_name.c_str(), path.c_str(),
                opts, sparse_size, vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: import failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return r;
}

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         false);
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         false);
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  at::add_pool_options(positional, options, true);
  at::add_format_options(options);
}

void get_rename_arguments(po::options_description *positional,
                          po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE,
                         false);
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST,
                         false);
}

void get_add_arguments(po::options_description *positional,
                       po::options_description *options) {
  positional->add_options()
    (GROUP_SPEC.c_str(),
     "group specification\n"
     "(example: [<pool-name>/[<namespace>/]]<group-name>)");

  add_prefixed_pool_option(options, "group");
  add_prefixed_namespace_option(options, "group");
  add_group_option(options, at::ARGUMENT_MODIFIER_NONE);

  positional->add_options()
    (at::IMAGE_SPEC.c_str(),
     "image specification\n"
     "(example: [<pool-name>/[<namespace>/]]<image-name>)");

  add_prefixed_pool_option(options, "image");
  add_prefixed_namespace_option(options, "image");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);

  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE,
	       " unless overridden");
}

void get_remove_image_arguments(po::options_description *positional,
                                po::options_description *options) {
  positional->add_options()
    (GROUP_SPEC.c_str(),
     "group specification\n"
     "(example: [<pool-name>/[<namespace>/]]<group-name>)");

  add_prefixed_pool_option(options, "group");
  add_prefixed_namespace_option(options, "group");
  add_group_option(options, at::ARGUMENT_MODIFIER_NONE);

  positional->add_options()
    (at::IMAGE_SPEC.c_str(),
     "image specification\n"
     "(example: [<pool-name>/[<namespace>/]]<image-name>)");

  add_prefixed_pool_option(options, "image");
  add_prefixed_namespace_option(options, "image");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);

  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE,
	       " unless overridden");
  at::add_image_id_option(options);
}

void get_list_images_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_format_options(options);
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         false);
}

void get_group_snap_create_arguments(po::options_description *positional,
                                     po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         true);
  at::add_snap_create_options(options);
}

void get_group_snap_remove_arguments(po::options_description *positional,
				  po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         true);
}

void get_group_snap_rename_arguments(po::options_description *positional,
				     po::options_description *options) {
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         true);

  positional->add_options()
    (at::DEST_SNAPSHOT_NAME.c_str(),
     "destination snapshot name\n(example: <snap-name>)");
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_DEST);
}

void get_group_snap_list_arguments(po::options_description *positional,
                             po::options_description *options) {
  at::add_format_options(options);
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         false);
}

void get_group_snap_rollback_arguments(po::options_description *positional,
                                       po::options_description *options) {
  at::add_no_progress_option(options);
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE,
                         true);
}

void get_group_export_arguments(po::options_description *positional,
                                       po::options_description *options) {

  add_group_or_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE,
                         false);
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_no_progress_option(options);
}

void get_group_export_diff_arguments(po::options_description *positional,
                                       po::options_description *options) {

  add_group_or_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_SOURCE,
                         true);
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    (at::WHOLE_OBJECT.c_str(), po::bool_switch(), "compare whole object");
  at::add_path_options(positional, options,
                       "export file (or '-' for stdout)");
  at::add_no_progress_option(options);
}

void get_group_import_arguments(po::options_description *positional,
                                       po::options_description *options) {
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST,
                         false);
  at::add_create_image_options(options, true);
  at::add_sparse_size_option(options);
  at::add_no_progress_option(options);
}

void get_group_import_diff_arguments(po::options_description *positional,
                                       po::options_description *options)
{
  at::add_path_options(positional, options,
                       "import file (or '-' for stdin)");
  add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_DEST,
                         false);
  at::add_create_image_options(options, true);
  at::add_sparse_size_option(options);
  at::add_no_progress_option(options);
}

Shell::Action action_create(
  {"group", "create"}, {}, "Create a group.",
  "", &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"group", "remove"}, {"group", "rm"}, "Delete a group.",
  "", &get_remove_arguments, &execute_remove);
Shell::Action action_list(
  {"group", "list"}, {"group", "ls"}, "List rbd groups.",
  "", &get_list_arguments, &execute_list);
Shell::Action action_rename(
  {"group", "rename"}, {}, "Rename a group within pool.",
  "", &get_rename_arguments, &execute_rename);
Shell::Action action_add(
  {"group", "image", "add"}, {}, "Add an image to a group.",
  "", &get_add_arguments, &execute_add);
Shell::Action action_remove_image(
  {"group", "image", "remove"}, {"group", "image", "rm"},
  "Remove an image from a group.", "",
  &get_remove_image_arguments, &execute_remove_image);
Shell::Action action_list_images(
  {"group", "image", "list"}, {"group", "image", "ls"},
  "List images in a group.", "",
  &get_list_images_arguments, &execute_list_images);
Shell::Action action_group_snap_create(
  {"group", "snap", "create"}, {}, "Make a snapshot of a group.",
  "", &get_group_snap_create_arguments, &execute_group_snap_create);
Shell::Action action_group_snap_remove(
  {"group", "snap", "remove"}, {"group", "snap", "rm"},
  "Remove a snapshot from a group.",
  "", &get_group_snap_remove_arguments, &execute_group_snap_remove);
Shell::Action action_group_snap_rename(
  {"group", "snap", "rename"}, {}, "Rename group's snapshot.",
  "", &get_group_snap_rename_arguments, &execute_group_snap_rename);
Shell::Action action_group_snap_list(
  {"group", "snap", "list"}, {"group", "snap", "ls"},
  "List snapshots of a group.",
  "", &get_group_snap_list_arguments, &execute_group_snap_list);
Shell::Action action_group_snap_rollback(
  {"group", "snap", "rollback"}, {},
  "Rollback group to snapshot.",
  "", &get_group_snap_rollback_arguments, &execute_group_snap_rollback);
Shell::Action action_group_export(
  {"group", "export"}, {},
  "Export group to file.",
  "", &get_group_export_arguments, &execute_group_export);
Shell::Action action_group_export_diff(
  {"group", "export-diff"}, {},
  "Export group incremental diff to file.",
  "", &get_group_export_diff_arguments, &execute_group_export_diff);
Shell::Action action_group_import(
  {"group", "import"}, {},
  "Import group from file.",
  "", &get_group_import_arguments, &execute_group_import);
Shell::Action action_group_import_diff(
  {"group", "import-diff"}, {},
  "Import group incremental diff from file.",
  "", &get_group_import_diff_arguments, &execute_group_import_diff);

} // namespace group
} // namespace action
} // namespace rbd
