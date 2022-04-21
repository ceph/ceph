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

int ExtBlkDevVdo::_get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  int vdo_fd = -1;

  // we need to go from the raw devname (e.g., dm-4) to the VDO volume name.
  // currently the best way seems to be to look at /dev/mapper/* ...
  std::string expect = std::string("../") + devname;  // expected symlink target
  DIR *dir = ::opendir("/dev/mapper");
  if (!dir) {
    return -1;
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
      vdo_fd = ::open(fn, O_RDONLY|O_CLOEXEC); //DIRECTORY);
      if (vdo_fd >= 0) {
	*vdo_name = de->d_name;
	break;
      }
    }
  }
  closedir(dir);
  return vdo_fd;
}

int ExtBlkDevVdo::get_vdo_stats_handle(std::string devname, std::string *vdo_name)
{
  std::set<std::string> devs = { devname };
  while (!devs.empty()) {
    std::string dev = *devs.begin();
    devs.erase(devs.begin());
    int fd = _get_vdo_stats_handle(dev.c_str(), vdo_name);
    if (fd >= 0) {
      // yay, it's vdo
      return fd;
    }
    // ok, see if there are constituent devices
    if (dev.find("dm-") == 0) {
      get_dm_parents(dev, &devs);
    }
  }
  return -1;
}

int64_t ExtBlkDevVdo::get_vdo_stat(int vdo_fd, const char *property)
{
  int64_t ret = 0;
  int fd = ::openat(vdo_fd, property, O_RDONLY|O_CLOEXEC);
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


int ExtBlkDevVdo::init(ceph::ExtBlkDevProfile &profile_a, std::ostream *ss){
  profile=profile_a;
  // get handle for VDO metadata
  int fda=get_vdo_stats_handle(profile.devname, &name);
  if(fda<0){
    return fda;
  }
  fd=fda;
  return 0;
}


int ExtBlkDevVdo::get_thin_utilization(uint64_t *total, uint64_t *avail, std::ostream *ss)
{
  int64_t block_size = get_vdo_stat(fd, "block_size");
  int64_t physical_blocks = get_vdo_stat(fd, "physical_blocks");
  int64_t overhead_blocks_used = get_vdo_stat(fd, "overhead_blocks_used");
  int64_t data_blocks_used = get_vdo_stat(fd, "data_blocks_used");
  if (!block_size
      || !physical_blocks
      || !overhead_blocks_used
      || !data_blocks_used) {
    return -1;
  }
  int64_t avail_blocks =
    physical_blocks - overhead_blocks_used - data_blocks_used;
  *total = block_size * physical_blocks;
  *avail = block_size * avail_blocks;
  return 0;
}

int ExtBlkDevVdo::collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm)
{
  uint64_t total, avail;
  get_thin_utilization(&total, &avail, 0);
  (*pm)[prefix + "vdo"] = "true";
  (*pm)[prefix + "vdo_physical_size"] = stringify(total);
  return 0;
}
