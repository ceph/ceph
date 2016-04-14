// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace info {

namespace at = argument_types;
namespace po = boost::program_options;

static void format_bitmask(Formatter *f, const std::string &name,
                           const std::map<uint64_t, std::string>& mapping,
                           uint64_t bitmask)
{
  int count = 0;
  std::string group_name(name + "s");
  if (f == NULL) {
    std::cout << "\t" << group_name << ": ";
  } else {
    f->open_array_section(group_name.c_str());
  }
  for (std::map<uint64_t, std::string>::const_iterator it = mapping.begin();
       it != mapping.end(); ++it) {
    if ((it->first & bitmask) == 0) {
      continue;
    }

    if (f == NULL) {
      if (count++ > 0) {
        std::cout << ", ";
      }
      std::cout << it->second;
    } else {
      f->dump_string(name.c_str(), it->second);
    }
  }
  if (f == NULL) {
    std::cout << std::endl;
  } else {
    f->close_section();
  }
}

static void format_features(Formatter *f, uint64_t features)
{
  format_bitmask(f, "feature", at::ImageFeatures::FEATURE_MAPPING, features);
}

static void format_flags(Formatter *f, uint64_t flags)
{
  std::map<uint64_t, std::string> mapping = {
    {RBD_FLAG_OBJECT_MAP_INVALID, "object map invalid"},
    {RBD_FLAG_FAST_DIFF_INVALID, "fast diff invalid"}};
  format_bitmask(f, "flag", mapping, flags);
}

static int do_show_info(const char *imgname, librbd::Image& image,
                        const char *snapname, Formatter *f)
{
  librbd::image_info_t info;
  std::string parent_pool, parent_name, parent_snapname;
  uint8_t old_format;
  uint64_t overlap, features, flags;
  bool snap_protected = false;
  librbd::mirror_image_info_t mirror_image;
  int r;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  r = image.old_format(&old_format);
  if (r < 0)
    return r;

  r = image.overlap(&overlap);
  if (r < 0)
    return r;

  r = image.features(&features);
  if (r < 0)
    return r;

  r = image.get_flags(&flags);
  if (r < 0) {
    return r;
  }

  if (snapname) {
    r = image.snap_is_protected(snapname, &snap_protected);
    if (r < 0)
      return r;
  }

  if (features & RBD_FEATURE_JOURNALING) {
    r = image.mirror_image_get_info(&mirror_image, sizeof(mirror_image));
    if (r < 0) {
      return r;
    }
  }

  char prefix[RBD_MAX_BLOCK_NAME_SIZE + 1];
  strncpy(prefix, info.block_name_prefix, RBD_MAX_BLOCK_NAME_SIZE);
  prefix[RBD_MAX_BLOCK_NAME_SIZE] = '\0';

  if (f) {
    f->open_object_section("image");
    f->dump_string("name", imgname);
    f->dump_unsigned("size", info.size);
    f->dump_unsigned("objects", info.num_objs);
    f->dump_int("order", info.order);
    f->dump_unsigned("object_size", info.obj_size);
    f->dump_string("block_name_prefix", prefix);
    f->dump_int("format", (old_format ? 1 : 2));
  } else {
    std::cout << "rbd image '" << imgname << "':\n"
              << "\tsize " << prettybyte_t(info.size) << " in "
              << info.num_objs << " objects"
              << std::endl
              << "\torder " << info.order
              << " (" << prettybyte_t(info.obj_size) << " objects)"
              << std::endl
              << "\tblock_name_prefix: " << prefix
              << std::endl
              << "\tformat: " << (old_format ? "1" : "2")
              << std::endl;
  }

  if (!old_format) {
    format_features(f, features);
    format_flags(f, flags);
  }

  // snapshot info, if present
  if (snapname) {
    if (f) {
      f->dump_string("protected", snap_protected ? "true" : "false");
    } else {
      std::cout << "\tprotected: " << (snap_protected ? "True" : "False")
                << std::endl;
    }
  }

  // parent info, if present
  if ((image.parent_info(&parent_pool, &parent_name, &parent_snapname) == 0) &&
      parent_name.length() > 0) {
    if (f) {
      f->open_object_section("parent");
      f->dump_string("pool", parent_pool);
      f->dump_string("image", parent_name);
      f->dump_string("snapshot", parent_snapname);
      f->dump_unsigned("overlap", overlap);
      f->close_section();
    } else {
      std::cout << "\tparent: " << parent_pool << "/" << parent_name
                << "@" << parent_snapname << std::endl;
      std::cout << "\toverlap: " << prettybyte_t(overlap) << std::endl;
    }
  }

  // striping info, if feature is set
  if (features & RBD_FEATURE_STRIPINGV2) {
    if (f) {
      f->dump_unsigned("stripe_unit", image.get_stripe_unit());
      f->dump_unsigned("stripe_count", image.get_stripe_count());
    } else {
      std::cout << "\tstripe unit: " << prettybyte_t(image.get_stripe_unit())
                << std::endl
                << "\tstripe count: " << image.get_stripe_count() << std::endl;
    }
  }

  if (features & RBD_FEATURE_JOURNALING) {
    if (f) {
      f->dump_string("journal", utils::image_id(image));
    } else {
      std::cout << "\tjournal: " << utils::image_id(image) << std::endl;
    }
  }

  if (features & RBD_FEATURE_JOURNALING) {
    if (f) {
      f->open_object_section("mirroring");
      f->dump_string("state",
          utils::mirror_image_state(mirror_image.state));
      if (mirror_image.state != RBD_MIRROR_IMAGE_DISABLED) {
        f->dump_string("global_id", mirror_image.global_id);
        f->dump_bool("primary", mirror_image.primary);
      }
      f->close_section();
    } else {
      std::cout << "\tmirroring state: "
                << utils::mirror_image_state(mirror_image.state) << std::endl;
      if (mirror_image.state != RBD_MIRROR_IMAGE_DISABLED) {
        std::cout << "\tmirroring global id: " << mirror_image.global_id
                  << std::endl
                  << "\tmirroring primary: "
                  << (mirror_image.primary ? "true" : "false") <<std::endl;
      }
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
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, snap_name, true,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_show_info(image_name.c_str(), image,
                   snap_name.empty() ? nullptr : snap_name.c_str(),
                   formatter.get());
  if (r < 0) {
    std::cerr << "rbd: info: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"info"}, {}, "Show information about image size, striping, etc.", "",
  &get_arguments, &execute);

} // namespace info
} // namespace action
} // namespace rbd
