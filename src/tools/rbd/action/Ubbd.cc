// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/scope_exit.hpp>
#include <iostream>
#include <sys/stat.h>

#if defined(WITH_RBD_UBBD)
#include <libubbd.h>
#endif

namespace rbd {
namespace action {
namespace ubbd {

namespace at = argument_types;
namespace po = boost::program_options;

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
#if defined(WITH_RBD_UBBD)
  ubbdd_mgmt_rsp list_rsp;
  ubbd_list_options list_opts = { .type = UBBD_DEV_TYPE_RBD };

  int r = ubbd_list(&list_opts, &list_rsp);
  if (r < 0) {
    std::cerr << "rbd: ubbd_list failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  at::Format::Formatter f;
  r = utils::get_formatter(vm, &f);
  if (r < 0) {
    return r;
  }

  TextTable tbl;

  if (f) {
    f->open_array_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("namespace", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("snap", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
  }

  for (int i = 0; i < list_rsp.list.dev_num; i++) {
    ubbd_info_options info_opts = { .ubbdid = list_rsp.list.dev_list[i] };
    ubbdd_mgmt_rsp info_rsp;
    ubbdd_mgmt_rsp_dev_info *mgmt_dev_info = &info_rsp.dev_info;
    ubbd_dev_info *dev_info = &mgmt_dev_info->dev_info;

    r = ubbd_device_info(&info_opts, &info_rsp);
    if (r < 0) {
      std::cerr << "ubbd_device_info for /dev/ubbd" << info_opts.ubbdid
                << " failed: " << cpp_strerror(r) << std::endl;
      return r;
    }

    if (f) {
      f->open_object_section("device");
      f->dump_int("id", mgmt_dev_info->devid);
      f->dump_string("pool", dev_info->generic_dev.info.rbd.pool);
      f->dump_string("namespace", dev_info->generic_dev.info.rbd.ns);
      f->dump_string("image", dev_info->generic_dev.info.rbd.image);
      f->dump_string("snap", dev_info->generic_dev.info.rbd.snap);
      f->dump_string("device", "/dev/ubbd" + std::to_string(mgmt_dev_info->devid));
      f->close_section();
    } else {
      tbl << mgmt_dev_info->devid << dev_info->generic_dev.info.rbd.pool
          << dev_info->generic_dev.info.rbd.ns << dev_info->generic_dev.info.rbd.image
          << dev_info->generic_dev.info.rbd.snap
          << "/dev/ubbd" + std::to_string(mgmt_dev_info->devid)
          << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section(); // devices
    f->flush(std::cout);
  } else {
    std::cout << tbl;
  }

  return 0;
#else
  std::cerr << "rbd: ubbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#endif
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
#if defined(WITH_RBD_UBBD)
  size_t arg_index = 0;
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;
  ubbdd_mgmt_rsp rsp;
  ubbd_map_options opts = { 0 };

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &nspace_name,
    &image_name, &snap_name, true, utils::SNAPSHOT_PRESENCE_PERMITTED,
    utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }
  opts.type = opts.generic_dev.opts.type = "rbd";
  opts.generic_dev.opts.rbd.pool = pool_name.c_str();
  opts.generic_dev.opts.rbd.ns = nspace_name.c_str();
  opts.generic_dev.opts.rbd.image = image_name.c_str();
  opts.generic_dev.opts.rbd.snap = snap_name.c_str();

  if (vm["read-only"].as<bool>()) {
    opts.read_only = true;
  }
  if (vm["exclusive"].as<bool>()) {
    opts.generic_dev.opts.rbd.exclusive = true;
  }
  if (vm["quiesce"].as<bool>()) {
    opts.generic_dev.opts.rbd.quiesce = true;
  }
  if (vm.count("quiesce-hook")) {
    opts.generic_dev.opts.rbd.quiesce_hook =
      vm["quiesce-hook"].as<std::string>().c_str();
  }

  r = ubbd_map(&opts, &rsp);
  if (r < 0) {
    std::cerr << "rbd: ubbd_map failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  std::cout << rsp.add.path << std::endl;

  return 0;
#else
  std::cerr << "rbd: ubbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#endif
}

#if defined(WITH_RBD_UBBD)
static int parse_unmap_options(const std::string &options_string,
                               ubbd_unmap_options *unmap_opts)
{
  char *options = strdup(options_string.c_str());
  BOOST_SCOPE_EXIT(options) {
    free(options);
  } BOOST_SCOPE_EXIT_END;

  for (char *this_char = strtok(options, ", ");
       this_char != NULL;
       this_char = strtok(NULL, ",")) {
    char *value_char;

    if ((value_char = strchr(this_char, '=')) != NULL)
      *value_char++ = '\0';

    if (!strcmp(this_char, "force")) {
      unmap_opts->force = true;
    } else {
      std::cerr << "rbd: unknown ubbd unmap option '" << this_char << "'"
                << std::endl;
      return -EINVAL;
    }
  }

  return 0;
}
#endif

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
#if defined(WITH_RBD_UBBD)
  std::string device_name = utils::get_positional_argument(vm, 0);
  ubbdd_mgmt_rsp rsp;
  ubbd_unmap_options opts = { 0 };
  struct stat sb;
  int r;

  if (!boost::starts_with(device_name, "/dev/ubbd")) {
    std::cerr << "rbd: ubbd unmap requires device path (/dev/ubbdX)" << std::endl;
    return -EINVAL;
  }

  if (stat(device_name.c_str(), &sb) < 0 || !S_ISBLK(sb.st_mode)) {
    std::cerr << "rbd: '" << device_name << "' is not a block device" << std::endl;
    return -EINVAL;
  }

  device_name.erase(0, 9);
  opts.ubbdid = stoi(device_name);

  if (vm.count("options")) {
    for (auto &options : vm["options"].as<std::vector<std::string>>()) {
      r = parse_unmap_options(options, &opts);
      if (r < 0) {
        std::cerr << "rbd: couldn't parse ubbd unmap options" << std::endl;
        return r;
      }
    }
  }

  r = ubbd_unmap(&opts, &rsp);
  if (r < 0) {
    std::cerr << "rbd: ubbd_unmap failed: " << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
#else
  std::cerr << "rbd: ubbd device is not supported" << std::endl;
  return -EOPNOTSUPP;
#endif
}

int execute_attach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if defined(WITH_RBD_UBBD)
  std::cerr << "rbd: ubbd does not support attach" << std::endl;
#else
  std::cerr << "rbd: ubbd device is not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

int execute_detach(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
#if defined(WITH_RBD_UBBD)
  std::cerr << "rbd: ubbd does not support detach" << std::endl;
#else
  std::cerr << "rbd: ubbd device is not supported" << std::endl;
#endif
  return -EOPNOTSUPP;
}

} // namespace ubbd
} // namespace action
} // namespace rbd
