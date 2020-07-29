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
#include <algorithm>
#include <iostream>
#include <boost/bind/bind.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace disk_usage {

namespace at = argument_types;
namespace po = boost::program_options;
using namespace boost::placeholders;

static int disk_usage_callback(uint64_t offset, size_t len, int exists,
                               void *arg) {
  uint64_t *used_size = reinterpret_cast<uint64_t *>(arg);
  if (exists) {
    (*used_size) += len;
  }
  return 0;
}

static int get_image_disk_usage(const std::string& name,
                                const std::string& snap_name,
                                const std::string& from_snap_name,
                                librbd::Image &image,
                                bool exact,
                                uint64_t size,
                                uint64_t *used_size){

  const char* from = NULL;
  if (!from_snap_name.empty()) {
    from = from_snap_name.c_str();
  }

  uint64_t flags;
  int r = image.get_flags(&flags);
  if (r < 0) {
    std::cerr << "rbd: failed to retrieve image flags: " << cpp_strerror(r)
         << std::endl;
    return r;
  }
  if ((flags & RBD_FLAG_FAST_DIFF_INVALID) != 0) {
    std::cerr << "warning: fast-diff map is invalid for " << name
         << (snap_name.empty() ? "" : "@" + snap_name) << ". "
         << "operation may be slow." << std::endl;
  }

  *used_size = 0;
  r = image.diff_iterate2(from, 0, size, false, !exact,
                          &disk_usage_callback, used_size);
  if (r < 0) {
    std::cerr << "rbd: failed to iterate diffs: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  return 0;
}

void format_image_disk_usage(const std::string& name,
                              const std::string& id,
                              const std::string& snap_name,
                              uint64_t snap_id,
                              uint64_t size,
                              uint64_t used_size,
                              TextTable& tbl, Formatter *f) {
  if (f) {
    f->open_object_section("image");
    f->dump_string("name", name);
    f->dump_string("id", id);
    if (!snap_name.empty()) {
      f->dump_string("snapshot", snap_name);
      f->dump_unsigned("snapshot_id", snap_id);
    }
    f->dump_unsigned("provisioned_size", size);
    f->dump_unsigned("used_size" , used_size);
    f->close_section();
  } else {
    std::string full_name = name;
    if (!snap_name.empty()) {
      full_name += "@" + snap_name;
    }
    tbl << full_name
        << stringify(byte_u_t(size))
        << stringify(byte_u_t(used_size))
        << TextTable::endrow;
  }
}

static int do_disk_usage(librbd::RBD &rbd, librados::IoCtx &io_ctx,
                         const char *imgname, const char *snapname,
                         const char *from_snapname, bool exact, Formatter *f,
                         bool merge_snap) {
  std::vector<librbd::image_spec_t> images;
  int r = rbd.list2(io_ctx, &images);
  if (r == -ENOENT) {
    r = 0;
  } else if (r < 0) {
    return r;
  }

  TextTable tbl;
  if (f) {
    f->open_object_section("stats");
    f->open_array_section("images");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("PROVISIONED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
  }

  uint32_t count = 0;
  uint64_t used_size = 0;
  uint64_t total_prov = 0;
  uint64_t total_used = 0;
  uint64_t snap_id = CEPH_NOSNAP;
  uint64_t from_id = CEPH_NOSNAP;
  bool found = false;
  for (auto& image_spec : images) {
    if (imgname != NULL && image_spec.name != imgname) {
      continue;
    }
    found = true;

    librbd::Image image;
    r = rbd.open_read_only(io_ctx, image, image_spec.name.c_str(), NULL);
    if (r < 0) {
      if (r != -ENOENT) {
        std::cerr << "rbd: error opening " << image_spec.name << ": "
                  << cpp_strerror(r) << std::endl;
      }
      continue;
    }

    uint64_t features;
    r = image.features(&features);
    if (r < 0) {
      std::cerr << "rbd: failed to retrieve image features: " << cpp_strerror(r)
                << std::endl;
      goto out;
    }
    if ((features & RBD_FEATURE_FAST_DIFF) == 0) {
      std::cerr << "warning: fast-diff map is not enabled for "
                << image_spec.name << ". " << "operation may be slow."
                << std::endl;
    }

    librbd::image_info_t info;
    if (image.stat(info, sizeof(info)) < 0) {
      r = -EINVAL;
      goto out;
    }

    std::vector<librbd::snap_info_t> snap_list;
    r = image.snap_list(snap_list);
    if (r < 0) {
      std::cerr << "rbd: error opening " << image_spec.name << " snapshots: "
                << cpp_strerror(r) << std::endl;
      continue;
    }

    snap_list.erase(remove_if(snap_list.begin(),
                              snap_list.end(),
                              boost::bind(utils::is_not_user_snap_namespace, &image, _1)),
                    snap_list.end());

    bool found_from_snap = (from_snapname == nullptr);
    bool found_snap = (snapname == nullptr);
    bool found_from = (from_snapname == nullptr);
    std::string last_snap_name;
    std::sort(snap_list.begin(), snap_list.end(),
              boost::bind(&librbd::snap_info_t::id, _1) <
                boost::bind(&librbd::snap_info_t::id, _2));
    if (!found_snap || !found_from) {
      for (auto &snap_info : snap_list) {
        if (!found_snap && snap_info.name == snapname) {
          snap_id = snap_info.id;
          found_snap = true;
        }
        if (!found_from && snap_info.name == from_snapname) {
          from_id = snap_info.id;
          found_from = true;
        }
        if (found_snap && found_from) {
          break;
        }
      }
    }
    if ((snapname != nullptr && snap_id == CEPH_NOSNAP) ||
        (from_snapname != nullptr && from_id == CEPH_NOSNAP)) {
      std::cerr << "specified snapshot is not found." << std::endl;
      return -ENOENT;
    }
    if (snap_id != CEPH_NOSNAP && from_id != CEPH_NOSNAP) {
      if (from_id == snap_id) {
        // no diskusage.
        return 0;
      }
      if (from_id >= snap_id) {
        return -EINVAL;
      }
    }

    uint64_t image_full_used_size = 0;

    for (std::vector<librbd::snap_info_t>::const_iterator snap =
         snap_list.begin(); snap != snap_list.end(); ++snap) {
      librbd::Image snap_image;
      r = rbd.open_read_only(io_ctx, snap_image, image_spec.name.c_str(),
                             snap->name.c_str());
      if (r < 0) {
        std::cerr << "rbd: error opening snapshot " << image_spec.name << "@"
                  << snap->name << ": " << cpp_strerror(r) << std::endl;
        goto out;
      }

      if (imgname == nullptr || found_from_snap ||
         (found_from_snap && snapname != nullptr && snap->name == snapname)) {

        r = get_image_disk_usage(image_spec.name, snap->name, last_snap_name, snap_image, exact, snap->size, &used_size);
        if (r < 0) {
          goto out;
        }
        if (!merge_snap) {
          format_image_disk_usage(image_spec.name, image_spec.id, snap->name,
                                   snap->id, snap->size, used_size, tbl, f);
        }

        image_full_used_size += used_size;

        if (snapname != NULL) {
          total_prov += snap->size;
        }
        total_used += used_size;
        ++count;
      }

      if (!found_from_snap && from_snapname != nullptr &&
          snap->name == from_snapname) {
        found_from_snap = true;
      }
      if (snapname != nullptr && snap->name == snapname) {
        break;
      }
      last_snap_name = snap->name;
    }

    if (snapname == NULL) {
      r = get_image_disk_usage(image_spec.name, "", last_snap_name, image, exact, info.size, &used_size);
      if (r < 0) {
        goto out;
      }

      image_full_used_size += used_size;

      if (!merge_snap) {
        format_image_disk_usage(image_spec.name, image_spec.id, "", CEPH_NOSNAP,
                                 info.size, used_size, tbl, f);
      } else {
        format_image_disk_usage(image_spec.name, image_spec.id, "", CEPH_NOSNAP,
                                 info.size, image_full_used_size, tbl, f);
      }

      total_prov += info.size;
      total_used += used_size;
      ++count;
    }
  }
  if (imgname != nullptr && !found) {
    std::cerr << "specified image " << imgname << " is not found." << std::endl;
    return -ENOENT;
  }

out:
  if (f) {
    f->close_section();
    if (imgname == NULL) {
      f->dump_unsigned("total_provisioned_size", total_prov);
      f->dump_unsigned("total_used_size", total_used);
    }
    f->close_section();
    f->flush(std::cout);
  } else if (!images.empty()) {
    if (count > 1) {
      tbl << "<TOTAL>"
          << stringify(byte_u_t(total_prov))
          << stringify(byte_u_t(total_used))
          << TextTable::endrow;
    }
    std::cout << tbl;
  }

  return r < 0 ? r : 0;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  at::add_format_options(options);
  options->add_options()
    (at::FROM_SNAPSHOT_NAME.c_str(), po::value<std::string>(),
     "snapshot starting point")
    ("exact", po::bool_switch(), "compute exact disk usage (slow)")
    ("merge-snapshots", po::bool_switch(),
     "merge snapshot sizes with its image");
}

int execute(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, vm.count(at::FROM_SNAPSHOT_NAME),
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string from_snap_name;
  if (vm.count(at::FROM_SNAPSHOT_NAME)) {
    from_snap_name = vm[at::FROM_SNAPSHOT_NAME].as<std::string>();
  }

  at::Format::Formatter formatter;
  r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  utils::disable_cache();

  librbd::RBD rbd;
  r = do_disk_usage(rbd, io_ctx,
                    image_name.empty() ? nullptr: image_name.c_str(),
                    snap_name.empty() ? nullptr : snap_name.c_str(),
                    from_snap_name.empty() ? nullptr : from_snap_name.c_str(),
                    vm["exact"].as<bool>(), formatter.get(),
                    vm["merge-snapshots"].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: du failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({"exact", "merge-snapshots"});
Shell::Action action(
  {"disk-usage"}, {"du"}, "Show disk usage stats for pool, image or snapshot.",
  "", &get_arguments, &execute);

} // namespace disk_usage
} // namespace action
} // namespace rbd
