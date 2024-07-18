// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace children {

namespace at = argument_types;
namespace po = boost::program_options;

int do_list_children(librados::IoCtx &io_ctx, librbd::Image &image,
                     bool all_flag, bool descendants_flag, Formatter *f)
{
  std::vector<librbd::linked_image_spec_t> children;
  librbd::RBD rbd;
  int r;
  if (descendants_flag) {
    r = image.list_descendants(&children);
  } else {
    r = image.list_children3(&children);
  }
  if (r < 0)
    return r;

  if (f)
    f->open_array_section("children");

  for (auto& child : children) {
    bool trash = child.trash;
    if (f) {
      if (all_flag) {
        f->open_object_section("child");
        f->dump_string("pool", child.pool_name);
        f->dump_string("pool_namespace", child.pool_namespace);
        f->dump_string("image", child.image_name);
        f->dump_string("id", child.image_id);
        f->dump_bool("trash", child.trash);
        f->close_section();
      } else if (!trash) {
        f->open_object_section("child");
        f->dump_string("pool", child.pool_name);
        f->dump_string("pool_namespace", child.pool_namespace);
        f->dump_string("image", child.image_name);
        f->close_section();
      }
    } else if (all_flag || !trash) {
      if (child.pool_name.empty()) {
        std::cout << "(child missing " << child.pool_id << "/";
      } else {
        std::cout << child.pool_name << "/";
      }
      if (!child.pool_namespace.empty()) {
        std::cout << child.pool_namespace << "/";
      }
      if (child.image_name.empty()) {
        std::cout << child.image_id << ")";
      } else {
        std::cout << child.image_name;
        if (trash) {
          std::cout << " (trash " << child.image_id << ")";
	}
      }
      std::cout << std::endl;
    }
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_snap_id_option(options);
  options->add_options()
    ("all,a", po::bool_switch(), "list all children (include trash)");
  options->add_options()
    ("descendants", po::bool_switch(), "include all descendants");
  at::add_format_options(options);
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  uint64_t snap_id = LIBRADOS_SNAP_HEAD;
  if (vm.count(at::SNAPSHOT_ID)) {
    snap_id = vm[at::SNAPSHOT_ID].as<uint64_t>();
  }

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
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id."
              << std::endl;
    return -EINVAL;
  }

  if (snap_id != LIBRADOS_SNAP_HEAD && !snap_name.empty()) {
    std::cerr << "rbd: trying to access snapshot using both name and id."
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

  if (!snap_name.empty()) {
    r = image.snap_set(snap_name.c_str());
  } else if (snap_id != LIBRADOS_SNAP_HEAD) {
    r = image.snap_set_by_id(snap_id);
  }
  if (r == -ENOENT) {
    std::cerr << "rbd: snapshot does not exist." << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: error setting snapshot: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  r = do_list_children(io_ctx, image, vm["all"].as<bool>(),
                       vm["descendants"].as<bool>(), formatter.get());
  if (r < 0) {
    std::cerr << "rbd: listing children failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({"all", "a", "descendants"});
Shell::Action action(
  {"children"}, {}, "Display children of an image or its snapshot.", "",
  &get_arguments, &execute);

} // namespace children
} // namespace action
} // namespace rbd
