// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 * Based on the file ceph/src/common/blkdev.cc
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "ExtBlkDevVdo.h"
#include "common/blkdev.h"
#include "include/stringify.h"
#include "common/debug.h"

#include <dirent.h> // for opendir()
#include <errno.h>

#define dout_subsys ceph_subsys_bdev
#define dout_context cct
#undef dout_prefix
#define dout_prefix *_dout << "vdo(" << this << ") "


int ExtBlkDevVdo::_get_vdo_stats_handle(const std::string& devname)
{
  int rc = -ENOENT;
  dout(10) << __func__ << " VDO init checking device: " << devname << dendl;

  // we need to go from the raw devname (e.g., dm-4) to the VDO volume name.
  // currently the best way seems to be to look at /dev/mapper/* ...
  std::string expect = std::string("../") + devname;  // expected symlink target
  DIR *dir = ::opendir("/dev/mapper");
  if (!dir) {
    return -errno;
  }
  struct dirent *de = nullptr;
  while ((de = ::readdir(dir))) {
    if (de->d_name[0] == '.')
      continue;
    char fn[4096], target[4096];
    snprintf(fn, sizeof(fn), "/dev/mapper/%s", de->d_name);
    int r = readlink(fn, target, sizeof(target));
    if (r < 0 || r >= (int)sizeof(target))
      continue;
    target[r] = 0;
    if (expect == target) {
      snprintf(fn, sizeof(fn), "/sys/kvdo/%s/statistics", de->d_name);
      int vdo_fd = ::open(fn, O_RDONLY|O_CLOEXEC);
      if (vdo_fd >= 0) {
	name = de->d_name;
	vdo_dir_fd = vdo_fd;
	rc = 0;
	break;
      }
    }
  }
  closedir(dir);
  return rc;
}

int ExtBlkDevVdo::get_vdo_stats_handle()
{
  std::set<std::string> devs = { logdevname };
  while (!devs.empty()) {
    std::string dev = *devs.begin();
    devs.erase(devs.begin());
    int rc = _get_vdo_stats_handle(dev);
    if (rc == 0) {
      // yay, it's vdo
      return rc;
    }
    // ok, see if there are constituent devices
    if (dev.find("dm-") == 0) {
      get_dm_parents(dev, &devs);
    }
  }
  return -ENOENT;
}

int64_t ExtBlkDevVdo::get_vdo_stat(const char *property)
{
  int64_t ret = 0;
  int fd = ::openat(vdo_dir_fd, property, O_RDONLY|O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  char buf[1024];
  int r = ::read(fd, buf, sizeof(buf) - 1);
  if (r > 0) {
    buf[r] = 0;
    ret = atoll(buf);
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}


int ExtBlkDevVdo::init(const std::string& alogdevname)
{
  logdevname = alogdevname;
  // get directory handle for VDO metadata
  return get_vdo_stats_handle();
}


int ExtBlkDevVdo::get_state(ceph::ExtBlkDevState& state)
{
  int64_t block_size = get_vdo_stat("block_size");
  int64_t physical_blocks = get_vdo_stat("physical_blocks");
  int64_t overhead_blocks_used = get_vdo_stat("overhead_blocks_used");
  int64_t data_blocks_used = get_vdo_stat("data_blocks_used");
  int64_t logical_blocks = get_vdo_stat("logical_blocks");
  int64_t logical_blocks_used = get_vdo_stat("logical_blocks_used");
  if (!block_size
      || !physical_blocks
      || !overhead_blocks_used
      || !data_blocks_used
      || !logical_blocks) {
    dout(1) << __func__ << " VDO sysfs provided zero value for at least one statistic: " << dendl;
    dout(1) << __func__ << " VDO block_size: " << block_size << dendl;
    dout(1) << __func__ << " VDO physical_blocks: " << physical_blocks << dendl;
    dout(1) << __func__ << " VDO overhead_blocks_used: " << overhead_blocks_used << dendl;
    dout(1) << __func__ << " VDO data_blocks_used: " << data_blocks_used << dendl;
    dout(1) << __func__ << " VDO logical_blocks: " << logical_blocks << dendl;
    return -1;
  }
  int64_t avail_blocks =
    physical_blocks - overhead_blocks_used - data_blocks_used;
  int64_t logical_avail_blocks =
    logical_blocks - logical_blocks_used;
  state.set_logical_total(block_size * logical_blocks);
  state.set_logical_avail(block_size * logical_avail_blocks);
  state.set_physical_total(block_size * physical_blocks);
  state.set_physical_avail(block_size * avail_blocks);
  return 0;
}

int ExtBlkDevVdo::collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm)
{
  ceph::ExtBlkDevState state;
  int rc = get_state(state);
  if(rc != 0){
    return rc;
  }
  (*pm)[prefix + "vdo"] = "true";
  (*pm)[prefix + "vdo_physical_size"] = stringify(state.get_physical_total());
  return 0;
}
