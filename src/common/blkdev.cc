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
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <dirent.h>
#include "include/uuid.h"
#include "common/blkdev.h"

#ifdef __linux__
#include <linux/fs.h>
#include <linux/kdev_t.h>
#include <blkid/blkid.h>

static const char *blkdev_props2strings[] = {
  [BLKDEV_PROP_DEV]                 = "dev",
  [BLKDEV_PROP_DISCARD_GRANULARITY] = "queue/discard_granularity",
  [BLKDEV_PROP_MODEL]               = "device/model",
  [BLKDEV_PROP_ROTATIONAL]          = "queue/rotational",
  [BLKDEV_PROP_SERIAL]              = "device/serial",
  [BLKDEV_PROP_VENDOR]              = "device/device/vendor",
};

BlkDev::BlkDev(int f) {
  fd = f;
}

const char *BlkDev::sysfsdir() {
  return "/sys";
}

int BlkDev::get_devid(dev_t *id) {
  struct stat st;
  int r;

  r = fstat(fd, &st);

  if (r < 0)
    return -errno;

  *id = S_ISBLK(st.st_mode) ? st.st_rdev : st.st_dev;
  return 0;
}

int BlkDev::get_block_device_size(int64_t *psize)
{
#ifdef BLKGETSIZE64
  int ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif defined(BLKGETSIZE)
  unsigned long sectors = 0;
  int ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  *psize = sectors * 512ULL;
#else
// cppcheck-suppress preprocessorErrorDirective
# error "Linux configuration error (get_block_device_size)"
#endif
  if (ret < 0)
    ret = -errno;
  return ret;
}

/**
 * get a block device property as a string
 *
 * store property in *val, up to maxlen chars
 * return 0 on success
 * return negative error on error
 */
int64_t BlkDev::get_block_device_string_property( blkdev_prop_t prop,
                                        char *val, size_t maxlen)
{
  int r;
  const char *propstr;
  char filename[PATH_MAX], wholedisk[PATH_MAX];

  assert(prop < BLKDEV_PROP_NUMPROPS);
  propstr = blkdev_props2strings[prop];

  // sysfs isn't fully populated for partitions, so we need to lookup the sysfs
  // entry for the underlying whole disk.
  if ((r = block_device_wholedisk(wholedisk, maxlen)) < 0)
    return r;

  snprintf(filename, sizeof(filename),
    "%s/block/%s/%s", sysfsdir(), wholedisk, propstr);

  FILE *fp = fopen(filename, "r");
  if (fp == NULL) {
    return -errno;
  }

  int r = 0;
  if (fgets(val, maxlen - 1, fp)) {
    // truncate at newline
    char *p = val;
    while (*p && *p != '\n')
      ++p;
    *p = 0;
  } else {
    r = -EINVAL;
  }
  fclose(fp);
  return r;
}

/**
 * get a block device property
 *
 * return the value (we assume it is positive)
 * return negative error on error
 */
int64_t BlkDev::get_block_device_int_property(blkdev_prop_t prop)
{
  char buff[256] = {0};
  int r = get_block_device_string_property(prop, buff, sizeof(buff));
  if (r < 0)
    return r;
  // take only digits
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
  return r;
}

bool BlkDev::block_device_support_discard()
{
  return get_block_device_int_property(BLKDEV_PROP_DISCARD_GRANULARITY) > 0;
}

int BlkDev::block_device_discard(int64_t offset, int64_t len)
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

bool BlkDev::block_device_is_nvme()
{
  char vendor[80];
  // nvme has a device/device/vendor property; infer from that.  There is
  // probably a better way?
  int r = get_block_device_string_property(BLKDEV_PROP_VENDOR, vendor, 80);
  return (r == 0);
}

bool BlkDev::block_device_is_rotational()
{
  return get_block_device_int_property(BLKDEV_PROP_ROTATIONAL) > 0;
}

int BlkDev::block_device_dev(char *dev, size_t max)
{
  return get_block_device_string_property(BLKDEV_PROP_DEV, dev, max);
}

int BlkDev::block_device_model(char *model, size_t max)
{
  return get_block_device_string_property(BLKDEV_PROP_MODEL, model, max);
}

int BlkDev::block_device_serial(char *serial, size_t max)
{
  return get_block_device_string_property(BLKDEV_PROP_SERIAL, serial, max);
}

int BlkDev::block_device_partition(char *partition, size_t max)
{
  dev_t id;
  int r = get_devid(&id);
  if (r < 0)
    return -EINVAL;  // hrm.

  char *t = blkid_devno_to_devname(id);
  if (!t) {
    return -EINVAL;
  }
  strncpy(partition, t, max);
  free(t);
  return 0;
}

int BlkDev::block_device_wholedisk(char *device, size_t max)
{
  dev_t id;
  int r = get_devid(&id);
  if (r < 0)
    return -EINVAL;  // hrm.

  r = blkid_devno_to_wholedisk(id, device, max, nullptr);
  if (r < 0) {
    return -EINVAL;
  }
  return 0;
}

#elif defined(__APPLE__)
#include <sys/disk.h>

const char *BlkDev::sysfsdir() {
  assert(false);  // Should never be called on Apple
  return "";
}

int BlkDev::block_device_dev(char *dev, size_t max)
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_block_device_size(int64_t *psize)
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

bool BlkDev::block_device_support_discard()
{
  return false;
}

int BlkDev::block_device_discard(int64_t offset, int64_t len)
{
  return -EOPNOTSUPP;
}

bool BlkDev::block_device_is_nvme()
{
  return false;
}

bool BlkDev::block_device_is_rotational()
{
  return false;
}

int BlkDev::block_device_model(char *model, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_serial(char *serial, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_partition(char *partition, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_wholedisk(char *device, size_t max)
{
  return -EOPNOTSUPP;
}
#elif defined(__FreeBSD__)
#include <sys/disk.h>

const char *BlkDev::sysfsdir() {
  assert(false);  // Should never be called on FreeBSD
  return "";
}

int BlkDev::block_device_dev(char *dev, size_t max)
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_block_device_size(int64_t *psize)
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

bool block_device_is_nvme(const char *devname)
{
  return false;
}

bool block_device_is_rotational(const char *devname)
{
  return false;
}

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}
int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  return -EOPNOTSUPP;
}

#else

const char *BlkDev::sysfsdir() {
  assert(false);  // Should never be called on non-Linux
  return "";
}

int BlkDev::block_device_dev(char *dev, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::get_block_device_size(int64_t *psize)
{
  return -EOPNOTSUPP;
}

bool BlkDev::block_device_support_discard()
{
  return false;
}

int BlkDev::block_device_discard(int fd, int64_t offset, int64_t len)
{
  return -EOPNOTSUPP;
}

bool BlkDev::block_device_is_nvme(const char *devname)
{
  return false;
}

bool BlkDev::block_device_is_rotational(const char *devname)
{
  return false;
}

int BlkDev::block_device_model(char *model, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_serial(char *serial, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_partition(char *partition, size_t max)
{
  return -EOPNOTSUPP;
}

int BlkDev::block_device_wholedisk(char *wholedisk, size_t max)
{
  return -EOPNOTSUPP;
}
#endif
