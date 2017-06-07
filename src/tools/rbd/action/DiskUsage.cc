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
#include <boost/bind.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace disk_usage {

namespace at = argument_types;
namespace po = boost::program_options;

static int disk_usage_callback(uint64_t offset, size_t len, int exists,
                               void *arg) {
  uint64_t *used_size = reinterpret_cast<uint64_t *>(arg);
  if (exists) {
    (*used_size) += len;
  }
  return 0;
}

static int compute_image_disk_usage(const std::string& name,
                                    const std::string& snap_name,
                                    const std::string& from_snap_name,
                                    librbd::Image &image, uint64_t size,
                                    TextTable& tbl, Formatter *f,
                                    uint64_t *used_size) {
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
  r = image.diff_iterate2(from, 0, size, false, true,
                          &disk_usage_callback, used_size);
  if (r < 0) {
    std::cerr << "rbd: failed to iterate diffs: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  if (f) {
    f->open_object_section("image");
    f->dump_string("name", name);
    if (!snap_name.empty()) {
      f->dump_string("snapshot", snap_name);
    }
    f->dump_unsigned("provisioned_size", size);
    f->dump_unsigned("used_size" , *used_size);
    f->close_section();
  } else {
    std::string full_name = name;
    if (!snap_name.empty()) {
      full_name += "@" + snap_name;
    }
    tbl << full_name
        << stringify(si_t(size))
        << stringify(si_t(*used_size))
        << TextTable::endrow;
  }
  return 0;
}

static int do_disk_usage(librbd::RBD &rbd, librados::IoCtx &io_ctx,
                         const char *imgname, const char *snapname,
                         const char *from_snapname, Formatter *f) {
  std::vector<std::string> names;
  int r = rbd.list(io_ctx, names);
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
    tbl.define_column("PROVISIONED", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("USED", TextTable::RIGHT, TextTable::RIGHT);
  }

  uint32_t count = 0;
  uint64_t used_size = 0;
  uint64_t total_prov = 0;
  uint64_t total_used = 0;
  bool found = false;
  std::sort(names.begin(), names.end());
  for (std::vector<string>::const_iterator name = names.begin();
       name != names.end(); ++name) {
    if (imgname != NULL && *name != imgname) {
      continue;
    }
    found = true;

    librbd::Image image;
    r = rbd.open_read_only(io_ctx, image, name->c_str(), NULL);
    if (r < 0) {
      if (r != -ENOENT) {
        std::cerr << "rbd: error opening " << *name << ": " << cpp_strerror(r)
                  << std::endl;
      }
      continue;
    }

    uint64_t features;
    int r = image.features(&features);
    if (r < 0) {
      std::cerr << "rbd: failed to retrieve image features: " << cpp_strerror(r)
                << std::endl;
      goto out;
    }
    if ((features & RBD_FEATURE_FAST_DIFF) == 0) {
      std::cerr << "warning: fast-diff map is not enabled for " << *name << ". "
                << "operation may be slow." << std::endl;
    }

    librbd::image_info_t info;
    if (image.stat(info, sizeof(info)) < 0) {
      r = -EINVAL;
      goto out;
    }

    std::vector<librbd::snap_info_t> snap_list;
    r = image.snap_list(snap_list);
    if (r < 0) {
      std::cerr << "rbd: error opening " << *name << " snapshots: "
                << cpp_strerror(r) << std::endl;
      continue;
    }

    bool found_from_snap = (from_snapname == nullptr);
    std::string last_snap_name;
    std::sort(snap_list.begin(), snap_list.end(),
              boost::bind(&librbd::snap_info_t::id, _1) <
                boost::bind(&librbd::snap_info_t::id, _2));
    for (std::vector<librbd::snap_info_t>::const_iterator snap =
         snap_list.begin(); snap != snap_list.end(); ++snap) {
      librbd::Image snap_image;
      r = rbd.open_read_only(io_ctx, snap_image, name->c_str(),
                             snap->name.c_str());
      if (r < 0) {
        std::cerr << "rbd: error opening snapshot " << *name << "@"
                  << snap->name << ": " << cpp_strerror(r) << std::endl;
        goto out;
      }

      if (imgname == nullptr || found_from_snap ||
         (found_from_snap && snapname != nullptr && snap->name == snapname)) {
        r = compute_image_disk_usage(*name, snap->name, last_snap_name,
                                     snap_image, snap->size, tbl, f,
                                     &used_size);
        if (r < 0) {
          goto out;
        }

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
      r = compute_image_disk_usage(*name, "", last_snap_name, image, info.size,
                                   tbl, f, &used_size);
      if (r < 0) {
        goto out;
      }
      total_prov += info.size;
      total_used += used_size;
      ++count;
    }
  }
  if (!found) {
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
  } else {
    if (count > 1) {
      tbl << "<TOTAL>"
          << stringify(si_t(total_prov))
          << stringify(si_t(total_used))
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
     "snapshot starting point");
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE, vm.count(at::FROM_SNAPSHOT_NAME));
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
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_disk_usage(rbd, io_ctx,
                    image_name.empty() ? nullptr: image_name.c_str() ,
                    snap_name.empty() ? nullptr : snap_name.c_str(),
                    from_snap_name.empty() ? nullptr : from_snap_name.c_str(),
                    formatter.get());
  if (r < 0) {
    std::cerr << "du failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action(
  {"disk-usage"}, {"du"}, "Show disk usage stats for pool, image or snapshot",
  "", &get_arguments, &execute);

} // namespace disk_usage
} // namespace action
} // namespace rbd
