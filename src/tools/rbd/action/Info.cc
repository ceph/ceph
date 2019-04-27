// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

#include "common/Clock.h"

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

static void format_op_features(Formatter *f, uint64_t op_features)
{
  static std::map<uint64_t, std::string> mapping = {
    {RBD_OPERATION_FEATURE_CLONE_PARENT, RBD_OPERATION_FEATURE_NAME_CLONE_PARENT},
    {RBD_OPERATION_FEATURE_CLONE_CHILD, RBD_OPERATION_FEATURE_NAME_CLONE_CHILD},
    {RBD_OPERATION_FEATURE_GROUP, RBD_OPERATION_FEATURE_NAME_GROUP},
    {RBD_OPERATION_FEATURE_SNAP_TRASH, RBD_OPERATION_FEATURE_NAME_SNAP_TRASH}};
  format_bitmask(f, "op_feature", mapping, op_features);
}

static void format_flags(Formatter *f, uint64_t flags)
{
  std::map<uint64_t, std::string> mapping = {
    {RBD_FLAG_OBJECT_MAP_INVALID, "object map invalid"},
    {RBD_FLAG_FAST_DIFF_INVALID, "fast diff invalid"}};
  format_bitmask(f, "flag", mapping, flags);
}

void format_timestamp(struct timespec timestamp, std::string &timestamp_str) {
  if(timestamp.tv_sec != 0) {
    time_t ts = timestamp.tv_sec;
    timestamp_str = ctime(&ts);
    timestamp_str = timestamp_str.substr(0, timestamp_str.length() - 1);
  }
}

static int do_show_info(librados::IoCtx &io_ctx, librbd::Image& image,
                        const std::string &snapname, Formatter *f)
{
  librbd::image_info_t info;
  uint8_t old_format;
  uint64_t overlap, features, flags, snap_limit;
  bool snap_protected = false;
  librbd::mirror_image_info_t mirror_image;
  std::vector<librbd::snap_info_t> snaps;
  int r;

  std::string imgname;
  r = image.get_name(&imgname);
  if (r < 0)
    return r;

  r = image.snap_list(snaps);
  if (r < 0)
    return r;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  r = image.old_format(&old_format);
  if (r < 0)
    return r;

  std::string imgid;
  if (!old_format) {
    r = image.get_id(&imgid);
    if (r < 0)
      return r;
  }

  std::string data_pool;
  if (!old_format) {
    int64_t data_pool_id = image.get_data_pool_id();
    if (data_pool_id != io_ctx.get_id()) {
      librados::Rados rados(io_ctx);
      librados::IoCtx data_io_ctx;
      r = rados.ioctx_create2(data_pool_id, data_io_ctx);
      if (r < 0) {
        data_pool = "<missing data pool " + stringify(data_pool_id) + ">";
      } else {
        data_pool = data_io_ctx.get_pool_name();
      }
    }
  }

  r = image.overlap(&overlap);
  if (r < 0)
    return r;

  r = image.features(&features);
  if (r < 0)
    return r;

  uint64_t op_features;
  r = image.get_op_features(&op_features);
  if (r < 0) {
    return r;
  }

  r = image.get_flags(&flags);
  if (r < 0) {
    return r;
  }

  if (!snapname.empty()) {
    r = image.snap_is_protected(snapname.c_str(), &snap_protected);
    if (r < 0)
      return r;
  }

  if (features & RBD_FEATURE_JOURNALING) {
    r = image.mirror_image_get_info(&mirror_image, sizeof(mirror_image));
    if (r < 0) {
      return r;
    }
  }

  r = image.snap_get_limit(&snap_limit);
  if (r < 0)
    return r;

  std::string prefix = image.get_block_name_prefix();

  librbd::group_info_t group_info;
  r = image.get_group(&group_info, sizeof(group_info));
  if (r < 0) {
    return r;
  }

  std::string group_string = "";
  if (RBD_GROUP_INVALID_POOL != group_info.pool) {
    std::string group_pool;
    librados::Rados rados(io_ctx);
    librados::IoCtx group_io_ctx;
    r = rados.ioctx_create2(group_info.pool, group_io_ctx);
    if (r < 0) {
      group_pool = "<missing group pool " + stringify(group_info.pool) + ">";
    } else {
      group_pool = group_io_ctx.get_pool_name();
    }

    group_string = group_pool + "/";
    if (!io_ctx.get_namespace().empty()) {
      group_string += io_ctx.get_namespace() + "/";
    }
    group_string += group_info.name;
  }

  struct timespec create_timestamp;
  image.get_create_timestamp(&create_timestamp);

  std::string create_timestamp_str = "";
  format_timestamp(create_timestamp, create_timestamp_str);

  struct timespec access_timestamp;
  image.get_access_timestamp(&access_timestamp);

  std::string access_timestamp_str = "";
  format_timestamp(access_timestamp, access_timestamp_str);

  struct timespec modify_timestamp;
  image.get_modify_timestamp(&modify_timestamp);

  std::string modify_timestamp_str = "";
  format_timestamp(modify_timestamp, modify_timestamp_str);

  if (f) {
    f->open_object_section("image");
    f->dump_string("name", imgname);
    f->dump_string("id", imgid);
    f->dump_unsigned("size", info.size);
    f->dump_unsigned("objects", info.num_objs);
    f->dump_int("order", info.order);
    f->dump_unsigned("object_size", info.obj_size);
    f->dump_int("snapshot_count", snaps.size());
    if (!data_pool.empty()) {
      f->dump_string("data_pool", data_pool);
    }
    f->dump_string("block_name_prefix", prefix);
    f->dump_int("format", (old_format ? 1 : 2));
  } else {
    std::cout << "rbd image '" << imgname << "':\n"
              << "\tsize " << byte_u_t(info.size) << " in "
              << info.num_objs << " objects"
              << std::endl
              << "\torder " << info.order
              << " (" << byte_u_t(info.obj_size) << " objects)"
              << std::endl
              << "\tsnapshot_count: " << snaps.size()
              << std::endl;
    if (!imgid.empty()) {
      std::cout << "\tid: " << imgid << std::endl;
    }
    if (!data_pool.empty()) {
      std::cout << "\tdata_pool: " << data_pool << std::endl;
    }
    std::cout << "\tblock_name_prefix: " << prefix
              << std::endl
              << "\tformat: " << (old_format ? "1" : "2")
	      << std::endl;
  }

  if (!old_format) {
    format_features(f, features);
    format_op_features(f, op_features);
    format_flags(f, flags);
  }

  if (!group_string.empty()) {
    if (f) {
      f->dump_string("group", group_string);
    } else {
      std::cout << "\tgroup: " << group_string
		<< std::endl;
    }
  }

  if (!create_timestamp_str.empty()) {
    if (f) {
      f->dump_string("create_timestamp", create_timestamp_str);
    } else {
      std::cout << "\tcreate_timestamp: " << create_timestamp_str
                << std::endl;
    }
  }

  if (!access_timestamp_str.empty()) {
    if (f) {
      f->dump_string("access_timestamp", access_timestamp_str);
    } else {
      std::cout << "\taccess_timestamp: " << access_timestamp_str
                << std::endl;
    }
  }

  if (!modify_timestamp_str.empty()) {
    if (f) {
      f->dump_string("modify_timestamp", modify_timestamp_str);
    } else {
      std::cout << "\tmodify_timestamp: " << modify_timestamp_str
                << std::endl;
    }
  }

  // snapshot info, if present
  if (!snapname.empty()) {
    if (f) {
      f->dump_string("protected", snap_protected ? "true" : "false");
    } else {
      std::cout << "\tprotected: " << (snap_protected ? "True" : "False")
                << std::endl;
    }
  }

  if (snap_limit < UINT64_MAX) {
    if (f) {
      f->dump_unsigned("snapshot_limit", snap_limit);
    } else {
      std::cout << "\tsnapshot_limit: " << snap_limit << std::endl;
    }
  }

  // parent info, if present
  librbd::linked_image_spec_t parent_image_spec;
  librbd::snap_spec_t parent_snap_spec;
  if ((image.get_parent(&parent_image_spec, &parent_snap_spec) == 0) &&
      (parent_image_spec.image_name.length() > 0)) {
    if (f) {
      f->open_object_section("parent");
      f->dump_string("pool", parent_image_spec.pool_name);
      f->dump_string("pool_namespace", parent_image_spec.pool_namespace);
      f->dump_string("image", parent_image_spec.image_name);
      f->dump_string("id", parent_image_spec.image_id);
      f->dump_string("snapshot", parent_snap_spec.name);
      f->dump_bool("trash", parent_image_spec.trash);
      f->dump_unsigned("overlap", overlap);
      f->close_section();
    } else {
      std::cout << "\tparent: " << parent_image_spec.pool_name << "/";
      if (!parent_image_spec.pool_namespace.empty()) {
        std::cout << parent_image_spec.pool_namespace << "/";
      }
      std::cout << parent_image_spec.image_name << "@"
                << parent_snap_spec.name;
      if (parent_image_spec.trash) {
        std::cout << " (trash " << parent_image_spec.image_id << ")";
      }
      std::cout << std::endl;
      std::cout << "\toverlap: " << byte_u_t(overlap) << std::endl;
    }
  }

  // striping info, if feature is set
  if (features & RBD_FEATURE_STRIPINGV2) {
    if (f) {
      f->dump_unsigned("stripe_unit", image.get_stripe_unit());
      f->dump_unsigned("stripe_count", image.get_stripe_count());
    } else {
      std::cout << "\tstripe unit: " << byte_u_t(image.get_stripe_unit())
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
  at::add_image_id_option(options);
  at::add_format_options(options);
}

int execute(const po::variables_map &vm,
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
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
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
                                 image_id, snap_name, true, &rados, &io_ctx,
                                 &image);
  if (r < 0) {
    return r;
  }

  r = do_show_info(io_ctx, image, snap_name, formatter.get());
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
