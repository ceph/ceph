// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include "blkdev.h"

int get_device_by_path(const char *path, char* partition, char* device,
               size_t max)
{
  return -EOPNOTSUPP;
}


BlkDev::BlkDev(int f)
  : fd(f)
{}

BlkDev::BlkDev(const std::string& devname)
  : devname(devname)
{}

int BlkDev::get_devid(dev_t *id) const
{
  return -EOPNOTSUPP;
}

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on Windows
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::get_size(int64_t *psize) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::support_discard() const
{
  return false;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_rotational() const
{
  return false;
}

int BlkDev::model(char *model, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::serial(char *serial, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::partition(char *partition, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::wholedisk(char *wd, size_t max) const
{
  return -EOPNOTSUPP;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
}

void get_raw_devices(const std::string& in,
             std::set<std::string> *ls)
{
}

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  return -1;
}

int64_t get_vdo_stat(int fd, const char *property)
{
  return 0;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  return false;
}

std::string get_device_id(const std::string& devname,
              std::string *err)
{
  if (err) {
    *err = "not implemented";
  }
  return std::string();
}

int block_device_run_smartctl(const char *device, int timeout,
                  std::string *result)
{
  return -EOPNOTSUPP;
}

int block_device_get_metrics(const std::string& devname, int timeout,
                             json_spirit::mValue *result)
{
  return -EOPNOTSUPP;
}

int block_device_run_nvme(const char *device, const char *vendor, int timeout,
            std::string *result)
{
  return -EOPNOTSUPP;
}
