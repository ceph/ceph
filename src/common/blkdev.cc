/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#include <errno.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <stdlib.h>
#include "include/int_types.h"
#include "include/uuid.h"

#ifdef __linux__
#include <linux/fs.h>
#include <blkid/blkid.h>

#define UUID_LEN 36

static const char *sandbox_dir = "";

void set_block_device_sandbox_dir(const char *dir)
{
  if (dir)
    sandbox_dir = dir;
  else
    sandbox_dir = "";
}

int get_block_device_size(int fd, int64_t *psize)
{
#ifdef BLKGETSIZE64
  int ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif defined(BLKGETSIZE)
  unsigned long sectors = 0;
  int ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  *psize = sectors * 512ULL;
#else
# error "Linux configuration error (get_block_device_size)"
#endif
  if (ret < 0)
    ret = -errno;
  return ret;
}

/**
 * get the base device (strip off partition suffix and /dev/ prefix)
 *  e.g.,
 *   /dev/sda3 -> sda
 *   /dev/cciss/c0d1p2 -> cciss/c0d1
 */
int get_block_device_base(const char *dev, char *out, size_t out_len)
{
  struct stat st;
  int r = 0;
  char buf[PATH_MAX*2];
  struct dirent *de;
  DIR *dir;
  char devname[PATH_MAX], fn[PATH_MAX];
  char *p;

  if (strncmp(dev, "/dev/", 5) != 0)
    return -EINVAL;

  strncpy(devname, dev + 5, PATH_MAX-1);
  devname[PATH_MAX-1] = '\0';
  for (p = devname; *p; ++p)
    if (*p == '/')
      *p = '!';

  snprintf(fn, sizeof(fn), "%s/sys/block/%s", sandbox_dir, devname);
  if (stat(fn, &st) == 0) {
    if (strlen(devname) + 1 > out_len) {
      return -ERANGE;
    }
    strncpy(out, devname, out_len);
    return 0;
  }

  snprintf(fn, sizeof(fn), "%s/sys/block", sandbox_dir);
  dir = opendir(fn);
  if (!dir)
    return -errno;

  while (!::readdir_r(dir, reinterpret_cast<struct dirent*>(buf), &de)) {
    if (!de) {
      if (errno) {
	r = -errno;
	goto out;
      }
      break;
    }
    if (de->d_name[0] == '.')
      continue;
    snprintf(fn, sizeof(fn), "%s/sys/block/%s/%s", sandbox_dir, de->d_name,
	     devname);

    if (stat(fn, &st) == 0) {
      // match!
      if (strlen(de->d_name) + 1 > out_len) {
	r = -ERANGE;
	goto out;
      }
      strncpy(out, de->d_name, out_len);
      r = 0;
      goto out;
    }
  }
  r = -ENOENT;

 out:
  closedir(dir);
  return r;
}

/**
 * get a block device property
 *
 * return the value (we assume it is positive)
 * return negative error on error
 */
int64_t get_block_device_int_property(const char *devname, const char *property)
{
  char basename[PATH_MAX], filename[PATH_MAX];
  int64_t r;

  r = get_block_device_base(devname, basename, sizeof(basename));
  if (r < 0)
    return r;

  snprintf(filename, sizeof(filename),
	   "%s/sys/block/%s/queue/discard_granularity", sandbox_dir, basename);

  FILE *fp = fopen(filename, "r");
  if (fp == NULL) {
    return -errno;
  }

  char buff[256] = {0};
  if (fgets(buff, sizeof(buff) - 1, fp)) {
    // strip newline etc
    for (char *p = buff; *p; ++p) {
      if (!isdigit(*p)) {
	*p = 0;
	break;
      }
    }
    char *endptr = 0;
    r = strtoll(buff, &endptr, 10);
    if (endptr != buff + strlen(buff))
      r = -EINVAL;
  } else {
    r = 0;
  }
  fclose(fp);
  return r;
}

bool block_device_support_discard(const char *devname)
{
  return get_block_device_int_property(devname, "discard_granularity") > 0;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  char uuid_str[UUID_LEN+1];
  char basename[PATH_MAX];
  const char* temp_partition_ptr = NULL;
  blkid_cache cache = NULL;
  blkid_dev dev = NULL;
  int rc = 0;

  dev_uuid.print(uuid_str);

  if (blkid_get_cache(&cache, NULL) >= 0)
    dev = blkid_find_dev_with_tag(cache, label, (const char*)uuid_str);
  else
    rc = -EINVAL;

  if (dev) {
    temp_partition_ptr = blkid_dev_devname(dev);
    strncpy(partition, temp_partition_ptr, PATH_MAX);
    rc = get_block_device_base(partition, basename,
      sizeof(basename));
    if (rc >= 0) {
      strncpy(device, basename, sizeof(basename));
      rc = 0;
    } else {
      rc = -ENODEV;
    }
  } else {
    rc = -EINVAL;
  }

  /* From what I can tell, blkid_put_cache cleans up dev, which
   * appears to be a pointer into cache, as well */
  if (cache)
    blkid_put_cache(cache);
  return rc;
}
#elif defined(__APPLE__)
#include <sys/disk.h>

int get_block_device_size(int fd, int64_t *psize)
{
  unsigned long blocksize = 0;
  int ret = ::ioctl(fd, DKIOCGETBLOCKSIZE, &blocksize);
  if (!ret) {
    unsigned long nblocks;
    ret = ::ioctl(fd, DKIOCGETBLOCKCOUNT, &nblocks);
    if (!ret)
      *psize = (int64_t)nblocks * blocksize;
  }
  if (ret < 0)
    ret = -errno;
  return ret;
}

bool block_device_support_discard(const char *devname)
{
  return false;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  return -EOPNOTSUPP;
}

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}
#elif defined(__FreeBSD__)
#include <sys/disk.h>

int get_block_device_size(int fd, int64_t *psize)
{
  int ret = ::ioctl(fd, DIOCGMEDIASIZE, psize);
  if (ret < 0)
    ret = -errno;
  return ret;
}

bool block_device_support_discard(const char *devname)
{
  return false;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  return -EOPNOTSUPP;
}

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}
#else
int get_block_device_size(int fd, int64_t *psize)
{
  return -EOPNOTSUPP;
}

bool block_device_support_discard(const char *devname)
{
  return false;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  return -EOPNOTSUPP;
}

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}
#endif
