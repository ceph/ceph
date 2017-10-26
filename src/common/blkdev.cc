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

#ifdef __FreeBSD__
#include <sys/param.h>
#include <geom/geom_disk.h>
#include <sys/disk.h>
#include <fcntl.h>
#endif

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
#endif

BlkDev::BlkDev(int f) {
  fd = f;
}

int BlkDev::get_devid(dev_t *id) const {
  struct stat st;

  int r = fstat(fd, &st);

  if (r < 0)
    return -errno;

  *id = S_ISBLK(st.st_mode) ? st.st_rdev : st.st_dev;
  return 0;
}

#ifdef __linux__
static const char *blkdev_props2strings[] = {
  [BLKDEV_PROP_DEV]                 = "dev",
  [BLKDEV_PROP_DISCARD_GRANULARITY] = "queue/discard_granularity",
  [BLKDEV_PROP_MODEL]               = "device/model",
  [BLKDEV_PROP_ROTATIONAL]          = "queue/rotational",
  [BLKDEV_PROP_SERIAL]              = "device/serial",
  [BLKDEV_PROP_VENDOR]              = "device/device/vendor",
};

const char *BlkDev::sysfsdir() const {
  return "/sys";
}

int BlkDev::get_size(int64_t *psize) const
{
#ifdef BLKGETSIZE64
  int ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif defined(BLKGETSIZE)
  unsigned long sectors = 0;
  int ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  *psize = sectors * 512ULL;
#else
// cppcheck-suppress preprocessorErrorDirective
# error "Linux configuration error (get_size)"
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
int64_t BlkDev::get_string_property(blkdev_prop_t prop,
                                        char *val, size_t maxlen) const
{
  int r;
  char filename[PATH_MAX], wd[PATH_MAX];

  assert(prop < BLKDEV_PROP_NUMPROPS);
  const char *propstr = blkdev_props2strings[prop];

  // sysfs isn't fully populated for partitions, so we need to lookup the sysfs
  // entry for the underlying whole disk.
  if ((r = wholedisk(wd, maxlen)) < 0)
    return r;

  snprintf(filename, sizeof(filename),
    "%s/block/%s/%s", sysfsdir(), wd, propstr);

  FILE *fp = fopen(filename, "r");
  if (fp == NULL) {
    return -errno;
  }

  r = 0;
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
int64_t BlkDev::get_int_property(blkdev_prop_t prop) const
{
  char buff[256] = {0};
  int r = get_string_property(prop, buff, sizeof(buff));
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

bool BlkDev::support_discard() const
{
  return get_int_property(BLKDEV_PROP_DISCARD_GRANULARITY) > 0;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

bool BlkDev::is_nvme() const
{
  char vendor[80];
  // nvme has a device/device/vendor property; infer from that.  There is
  // probably a better way?
  int r = get_string_property(BLKDEV_PROP_VENDOR, vendor, 80);
  return (r == 0);
}

bool BlkDev::is_rotational() const
{
  return get_int_property(BLKDEV_PROP_ROTATIONAL) > 0;
}

int BlkDev::dev(char *dev, size_t max) const
{
  return get_string_property(BLKDEV_PROP_DEV, dev, max);
}

int BlkDev::model(char *model, size_t max) const
{
  return get_string_property(BLKDEV_PROP_MODEL, model, max);
}

int BlkDev::serial(char *serial, size_t max) const
{
  return get_string_property(BLKDEV_PROP_SERIAL, serial, max);
}

int BlkDev::partition(char *partition, size_t max) const
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

int BlkDev::wholedisk(char *device, size_t max) const
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

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on Apple
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_size(int64_t *psize) const
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

bool BlkDev::support_discard() const
{
  return false;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme() const
{
  return false;
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

int BlkDev::wholedisk(char *device, size_t max) const
{
  return -EOPNOTSUPP;
}
#elif defined(__FreeBSD__)

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on FreeBSD
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_size(int64_t *psize) const
{
  int ret = ::ioctl(fd, DIOCGMEDIASIZE, psize);
  if (ret < 0)
    ret = -errno;
  return ret;
}

bool BlkDev::support_discard() const
{
  struct diocgattr_arg arg;
  int ret;

  strlcpy(arg.name, "GEOM::candelete", sizeof(arg.name));
  arg.len = sizeof(arg.value.i);
  if (ioctl(fd, DIOCGATTR, &arg) == 0)
    ret = (arg.value.i != 0);
  else
    ret = false;

  return ret;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme() const
{
  // FreeBSD doesn't have a good way to tell if a device's underlying protocol
  // is NVME, especially since multiple GEOM transforms may be involved.  So
  // we'll just guess based on the device name.
  struct fiodgname_arg arg;
  const char *nda = "nda";        //CAM-based attachment
  const char *nvd = "nvd";        //CAM-less attachment
  char devname[PATH_MAX];

  arg.buf = devname;
  arg.len = sizeof(devname);
  if (ioctl(fd, FIODGNAME, &arg) < 0)
    return false; //When in doubt, it's probably not NVME

  return (strncmp(nvd, devname, strlen(nvd)) == 0 ||
          strncmp(nda, devname, strlen(nda)) == 0);
}

bool BlkDev::is_rotational() const
{
#if __FreeBSD_version >= 1200049
  struct diocgattr_arg arg;
  bool ret;

  strlcpy(arg.name, "GEOM::rotation_rate", sizeof(arg.name));
  arg.len = sizeof(arg.value.u16);

  int ioctl_ret = ioctl(fd, DIOCGATTR, &arg);
  if (ioctl_ret < 0 || arg.value.u16 == DISK_RR_UNKNOWN)
    // DISK_RR_UNKNOWN usually indicates an old drive, which is usually spinny
    ret = true;
  else if (arg.value.u16 == DISK_RR_NON_ROTATING)
    ret = false;
  else if (arg.value.u16 >= DISK_RR_MIN && arg.value.u16 <= DISK_RR_MAX)
    ret = true;
  else
    ret = true;     // Invalid value.  Probably spinny?

  return ret;
#else
  return true;      // When in doubt, it's probably spinny
#endif
}

int BlkDev::model(char *model, size_t max) const
{
  struct diocgattr_arg arg;

  strlcpy(arg.name, "GEOM::descr", sizeof(arg.name));
  arg.len = sizeof(arg.value.str);
  if (ioctl(fd, DIOCGATTR, &arg) < 0) {
    return -errno;
  }

  // The GEOM description is of the form "vendor product" for SCSI disks
  // and "ATA device_model" for ATA disks.  Some vendors choose to put the
  // vendor name in device_model, and some don't.  Strip the first bit.
  char *p = arg.value.str;
  if (p == NULL || *p == '\0') {
    *model = '\0';
  } else {
    (void) strsep(&p, " ");
    snprintf(model, max, "%s", p);
  }

  return 0;
}

int BlkDev::serial(char *serial, size_t max) const
{
  char ident[DISK_IDENT_SIZE];

  if (ioctl(fd, DIOCGIDENT, ident) < 0)
    return -errno;

  snprintf(serial, max, "%s", ident);

  return 0;
}

static int block_device_devname(int fd, char *devname, size_t max)
{
  struct fiodgname_arg arg;

  arg.buf = devname;
  arg.len = max;
  if (ioctl(fd, FIODGNAME, &arg) < 0)
    return -errno;
  return 0;
}

int BlkDev::partition(char *partition, size_t max) const
{
  char devname[PATH_MAX];

  if (block_device_devname(fd, devname, sizeof(devname)) < 0)
    return -errno;
  snprintf(partition, max, "/dev/%s", devname);
  return 0;
}

int BlkDev::wholedisk(char *wd, size_t max) const
{
  char devname[PATH_MAX];

  if (block_device_devname(fd, devname, sizeof(devname)) < 0)
    return -errno;

  size_t first_digit = strcspn(devname, "0123456789");
  // first_digit now indexes the first digit or null character of devname
  size_t next_nondigit = strspn(&devname[first_digit], "0123456789");
  next_nondigit += first_digit;
  // next_nondigit now indexes the first alphabetic or null character after the
  // unit number
  strlcpy(wd, devname, next_nondigit + 1);
  return 0;
}

#else

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on non-Linux
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

int BlkDev::discard(int fd, int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme(const char *devname) const
{
  return false;
}

bool BlkDev::is_rotational(const char *devname) const
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
#endif
