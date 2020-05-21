// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"

#include "common/Clock.h"

namespace rbd {
namespace action {
namespace image_cache {

namespace at = argument_types;
namespace po = boost::program_options;

const std::string METADATA_CONF_PREFIX = ".librbd/";

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
  if (r < 0) {
    return r;
  }

  r = image.snap_list(snaps);
  if (r < 0) {
    return r;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0) {
    return r;
  }

  r = image.old_format(&old_format);
  if (r < 0) {
    return r;
  }

  std::string imgid;
  if (!old_format) {
    r = image.get_id(&imgid);
    if (r < 0) {
      return r;
    }
  }

  std::string header_oid;
  if (old_format != 0) {
    header_oid = imgname + RBD_SUFFIX;
  } else {
    header_oid = RBD_HEADER_PREFIX + imgid;
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
  if (r < 0) {
    return r;
  }

  r = image.features(&features);
  if (r < 0) {
    return r;
  }

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
    if (r < 0) {
      return r;
    }
  }

  if (features & RBD_FEATURE_JOURNALING) {
    r = image.mirror_image_get_info(&mirror_image, sizeof(mirror_image));
    if (r < 0) {
      return r;
    }
  }

  r = image.snap_get_limit(&snap_limit);
  if (r < 0) {
    return r;
  }

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

  if ((features & RBD_FEATURE_IMAGE_CACHE) != 0) {
    std::string image_cache_str;
    std::string key = "image_cache_state";
    r = image.metadata_get(METADATA_CONF_PREFIX + key, &image_cache_str);
    if (r < 0) {
      std::cout << "rbd: No image cache info." << std::endl;
    }
    else if (f) {
      f->dump_string(key, image_cache_str);
    } else {
      std::cout << "\timage_cache_state: " << image_cache_str
                << std::endl;
    }
  }
  image.close();

  if (f) {
    f->close_section();
    f->flush(std::cout);
  }

  return 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options,
                             at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  /* We'd need encoders for each image cache spec to do this */
  at::add_format_options(options);
}

int execute(const po::variables_map &vm) {
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

int execute_show(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  return execute(vm);
}

Shell::Action action_show(
  {"image-cache", "show"}, {}, "Show image cache config", "",
  &get_arguments, &execute_show);

} // namespace image_cache
} // namespace action
} // namespace rbd

