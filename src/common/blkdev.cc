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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include "include/uuid.h"
#include "blkdev.h"

#ifdef __linux__
#include <linux/fs.h>
#include <blkid/blkid.h>

#include <set>


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
// cppcheck-suppress preprocessorErrorDirective
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
 *  dev can a symbolic link.
 */
int get_block_device_base(const char *dev, char *out, size_t out_len)
{
  struct stat st;
  int r = 0;
  DIR *dir;
  char devname[PATH_MAX] = {0}, fn[PATH_MAX] = {0};
  char *p;
  char realname[PATH_MAX] = {0};

  if (strncmp(dev, "/dev/", 5) != 0) {
    if (realpath(dev, realname) == NULL || (strncmp(realname, "/dev/", 5) != 0)) {
      return -EINVAL;
    }
  }

  if (strlen(realname))
    strncpy(devname, realname + 5, PATH_MAX - 5);
  else
    strncpy(devname, dev + 5, strlen(dev) - 5);

  devname[PATH_MAX - 1] = '\0';

  for (p = devname; *p; ++p)
    if (*p == '/')
      *p = '!';

  if (static_cast<size_t>(snprintf(fn, sizeof(fn), "%s/sys/block/%s",
                                   sandbox_dir, devname))
      >= sizeof(fn))
    return -ERANGE;
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

  struct dirent *de = nullptr;
  while ((de = ::readdir(dir))) {
    if (de->d_name[0] == '.')
      continue;
    if (static_cast<size_t>(snprintf(fn, sizeof(fn), "%s/sys/block/%s/%s",
                                     sandbox_dir, de->d_name,
                                     devname)) >= sizeof(fn))
      return -ERANGE;

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
 * get a block device property as a string
 *
 * store property in *val, up to maxlen chars
 * return 0 on success
 * return negative error on error
 */
int64_t get_block_device_string_property(const char *devname,
					 const char *property,
					 char *val, size_t maxlen)
{
  char filename[PATH_MAX];
  snprintf(filename, sizeof(filename),
	   "%s/sys/block/%s/%s", sandbox_dir, devname, property);

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
int64_t get_block_device_int_property(const char *devname, const char *property)
{
  char buff[256] = {0};
  int r = get_block_device_string_property(devname, property, buff, sizeof(buff));
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

bool block_device_support_discard(const char *devname)
{
  return get_block_device_int_property(devname, "queue/discard_granularity") > 0;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

bool block_device_is_rotational(const char *devname)
{
  return get_block_device_int_property(devname, "queue/rotational") > 0;
}

int block_device_model(const char *devname, char *model, size_t max)
{
  return get_block_device_string_property(devname, "device/model", model, max);
}

int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  struct stat st;
  int r = fstat(fd, &st);
  if (r < 0) {
    return -EINVAL;  // hrm.
  }
  dev_t devid = S_ISBLK(st.st_mode) ? st.st_rdev : st.st_dev;
  char *t = blkid_devno_to_devname(devid);
  if (!t) {
    return -EINVAL;
  }
  strncpy(partition, t, max);
  free(t);
  dev_t diskdev;
  r = blkid_devno_to_wholedisk(devid, device, max, &diskdev);
  if (r < 0) {
    return -EINVAL;
  }
  return 0;
}

static int easy_readdir(const std::string& dir, std::set<std::string> *out)
{
  DIR *h = ::opendir(dir.c_str());
  if (!h) {
    return -errno;
  }
  struct dirent *de = nullptr;
  while ((de = ::readdir(h))) {
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0) {
      continue;
    }
    out->insert(de->d_name);
  }
  closedir(h);
  return 0;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
  std::string p = std::string("/sys/block/") + dev + "/slaves";
  std::set<std::string> parents;
  easy_readdir(p, &parents);
  for (auto& d : parents) {
    ls->insert(d);
    // recurse in case it is dm-on-dm
    if (d.find("dm-") == 0) {
      get_dm_parents(d, ls);
    }
  }
}

int _get_vdo_stats_handle(const char *devname, std::string *vdo_name)
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

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
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

int64_t get_vdo_stat(int vdo_fd, const char *property)
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
  TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  int64_t block_size = get_vdo_stat(fd, "block_size");
  int64_t physical_blocks = get_vdo_stat(fd, "physical_blocks");
  int64_t overhead_blocks_used = get_vdo_stat(fd, "overhead_blocks_used");
  int64_t data_blocks_used = get_vdo_stat(fd, "data_blocks_used");
  if (!block_size
      || !physical_blocks
      || !overhead_blocks_used
      || !data_blocks_used) {
    return false;
  }
  int64_t avail_blocks =
    physical_blocks - overhead_blocks_used - data_blocks_used;
  *total = block_size * physical_blocks;
  *avail = block_size * avail_blocks;
  return true;
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

bool block_device_is_rotational(const char *devname)
{
  return false;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
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

bool block_device_is_rotational(const char *devname)
{
  return false;
}

int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  return -EOPNOTSUPP;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
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

bool block_device_is_rotational(const char *devname)
{
  return false;
}

int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  return -EOPNOTSUPP;
}
void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
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
#endif
