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
#include "blkdev.h"


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

  struct dirent *de = nullptr;
  while ((de = ::readdir(dir))) {
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
    return -EINVAL;

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

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
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

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}
int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  return -EOPNOTSUPP;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
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

int get_device_by_uuid(uuid_d dev_uuid, const char* label, char* partition,
	char* device)
{
  return -EOPNOTSUPP;
}

int get_device_by_fd(int fd, char *partition, char *device, size_t max)
{
  return -EOPNOTSUPP;
}
void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
}
#endif
