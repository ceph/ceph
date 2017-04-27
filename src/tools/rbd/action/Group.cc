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

namespace rbd {
namespace action {
namespace consgrp {

namespace at = argument_types;
namespace po = boost::program_options;

int execute_create(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string group_name;
  std::string pool_name;

  int r = utils::get_pool_group_names(vm, at::ARGUMENT_MODIFIER_NONE,
                                      &arg_index, &pool_name, &group_name);
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
  r = rbd.group_create(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_list(const po::variables_map &vm) {

  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<std::string> names;
  r = rbd.group_list(io_ctx, &names);

  if (r == -ENOENT)
    r = 0;
  if (r < 0)
    return r;

  if (f)
    f->open_array_section("consistency_groups");
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

int execute_remove(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string group_name;
  std::string pool_name;

  int r = utils::get_pool_group_names(vm, at::ARGUMENT_MODIFIER_NONE,
                                      &arg_index, &pool_name, &group_name);
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

  r = rbd.group_remove(io_ctx, group_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

int execute_add(const po::variables_map &vm) {
  size_t arg_index = 0;
  // Parse group data.
  std::string group_name;
  std::string group_pool_name;

  int r = utils::get_special_pool_group_names(vm, &arg_index,
					      &group_pool_name,
					      &group_name);
  if (r < 0) {
    std::cerr << "rbd: image add error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  std::string image_name;
  std::string image_pool_name;

  r = utils::get_special_pool_image_names(vm, &arg_index,
					  &image_pool_name,
					  &image_name);

  if (r < 0) {
    std::cerr << "rbd: image add error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  librados::Rados rados;

  librados::IoCtx cg_io_ctx;
  r = utils::init(group_pool_name, &rados, &cg_io_ctx);
  if (r < 0) {
    return r;
  }

  librados::IoCtx image_io_ctx;
  r = utils::init(image_pool_name, &rados, &image_io_ctx);
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

int execute_remove_image(const po::variables_map &vm) {
  size_t arg_index = 0;

  std::string group_name;
  std::string group_pool_name;

  int r = utils::get_special_pool_group_names(vm, &arg_index,
					      &group_pool_name,
					      &group_name);
  if (r < 0) {
    std::cerr << "rbd: image remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  std::string image_name;
  std::string image_pool_name;
  std::string image_id;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  bool has_image_spec = utils::check_if_image_spec_present(
      vm, at::ARGUMENT_MODIFIER_NONE, arg_index);

  if (!image_id.empty() && has_image_spec) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }

  if (image_id.empty()) {
    r = utils::get_special_pool_image_names(vm, &arg_index, &image_pool_name,
                                            &image_name);
  } else {
    image_pool_name = utils::get_pool_name(vm, &arg_index);
  }

  if (r < 0) {
    std::cerr << "rbd: image remove error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  librados::Rados rados;

  librados::IoCtx cg_io_ctx;
  r = utils::init(group_pool_name, &rados, &cg_io_ctx);
  if (r < 0) {
    return r;
  }

  librados::IoCtx image_io_ctx;
  r = utils::init(image_pool_name, &rados, &image_io_ctx);
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

int execute_list_images(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string group_name;
  std::string pool_name;

  int r = utils::get_pool_group_names(vm, at::ARGUMENT_MODIFIER_NONE,
                                      &arg_index, &pool_name, &group_name);
  if (r < 0) {
    return r;
  }

  if (group_name.empty()) {
    std::cerr << "rbd: "
              << "consistency group name was not specified" << std::endl;
    return -EINVAL;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }
  Formatter *f = formatter.get();

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  std::vector<librbd::group_image_status_t> images;

  r = rbd.group_image_list(io_ctx, group_name.c_str(), &images);

  if (r == -ENOENT)
    r = 0;

  if (r < 0)
    return r;

  if (f)
    f->open_array_section("consistency_groups");

  for (auto i : images) {
    std::string image_name = i.name;
    int64_t pool_id = i.pool;
    int state = i.state;
    std::string state_string;
    if (cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE == state) {
      state_string = "incomplete";
    }
    if (f) {
      f->dump_string("image name", image_name);
      f->dump_int("pool id", pool_id);
      f->dump_int("state", state);
    } else
      std::cout << pool_id << "." << image_name << " " << state_string << std::endl;
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
}

void get_add_arguments(po::options_description *positional,
                       po::options_description *options) {
  positional->add_options()
    (at::GROUP_SPEC.c_str(),
     "group specification\n"
     "(example: [<pool-name>/]<group-name>)");

  at::add_special_pool_option(options, "group");
  at::add_group_option(options, at::ARGUMENT_MODIFIER_NONE);

  positional->add_options()
    (at::IMAGE_SPEC.c_str(),
     "image specification\n"
     "(example: [<pool-name>/]<image-name>)");

  at::add_special_pool_option(options, "image");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);

  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE,
	       " unless overridden");
}

void get_remove_image_arguments(po::options_description *positional,
                                po::options_description *options) {
  positional->add_options()
    (at::GROUP_SPEC.c_str(),
     "group specification\n"
     "(example: [<pool-name>/]<group-name>)");

  at::add_special_pool_option(options, "group");
  at::add_group_option(options, at::ARGUMENT_MODIFIER_NONE);

  positional->add_options()
    (at::IMAGE_SPEC.c_str(),
     "image specification\n"
     "(example: [<pool-name>/]<image-name>)");

  at::add_special_pool_option(options, "image");
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);

  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE,
	       " unless overridden");
  at::add_image_id_option(options);
}

void get_list_images_arguments(po::options_description *positional,
                               po::options_description *options) {
  at::add_format_options(options);
  at::add_group_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
}

Shell::Action action_create(
  {"group", "create"}, {}, "Create a consistency group.",
  "", &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"group", "remove"}, {"group", "rm"}, "Delete a consistency group.",
  "", &get_remove_arguments, &execute_remove);
Shell::Action action_list(
  {"group", "list"}, {"group", "ls"}, "List rbd consistency groups.",
  "", &get_list_arguments, &execute_list);
Shell::Action action_add(
  {"group", "image", "add"}, {}, "Add an image to a consistency group.",
  "", &get_add_arguments, &execute_add);
Shell::Action action_remove_image(
  {"group", "image", "remove"}, {}, "Remove an image from a consistency group.",
  "", &get_remove_image_arguments, &execute_remove_image);
Shell::Action action_list_images(
  {"group", "image", "list"}, {}, "List images in a consistency group.",
  "", &get_list_images_arguments, &execute_list_images);
} // namespace group
} // namespace action
} // namespace rbd
