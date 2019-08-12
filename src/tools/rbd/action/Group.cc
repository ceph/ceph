// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"

namespace rbd {
namespace action {
namespace group {

namespace at = argument_types;
namespace po = boost::program_options;

static const std::string GROUP_SPEC("group-spec");
static const std::string GROUP_SNAP_SPEC("group-snap-spec");

static const std::string GROUP_NAME("group");
static const std::string DEST_GROUP_NAME("dest-group");

static const std::string GROUP_POOL_NAME("group-" + at::POOL_NAME);
static const std::string IMAGE_POOL_NAME("image-" + at::POOL_NAME);

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

  librados::IoCtx io_ctx;
  librados::Rados rados;

  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.group_snap_create(io_ctx, group_name.c_str(), snap_name.c_str());
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
     "destination snapshot name\n(example: <snapshot-name>)");
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

} // namespace group
} // namespace action
} // namespace rbd
