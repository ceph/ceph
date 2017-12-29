// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_COMMON_BLKDEV_H
#define CEPH_COMMON_BLKDEV_H

#include <set>
#include <string>

/* for testing purposes */
void set_block_device_sandbox_dir(const char *dir);

// from a path (e.g., "/dev/sdb")
int get_block_device_base(const char *path, char *devname, size_t len);

// from an fd
int block_device_discard(int fd, int64_t offset, int64_t len);
int get_block_device_size(int fd, int64_t *psize);
int get_device_by_fd(int fd, char* partition, char* device, size_t max);

// from a device (e.g., "sdb")
int64_t get_block_device_int_property(const char *devname,
  const char *property);
int64_t get_block_device_string_property(const char *devname,
  const char *property, char *val, size_t maxlen);
bool block_device_support_discard(const char *devname);
bool block_device_is_rotational(const char *devname);
int block_device_model(const char *devname, char *model, size_t max);

void get_dm_parents(const std::string& dev, std::set<std::string> *ls);
#endif
