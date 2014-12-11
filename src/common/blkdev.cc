#include <errno.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include "include/int_types.h"

#ifdef __linux__
#include <linux/fs.h>

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

  strcpy(devname, dev + 5);
  for (p = devname; *p; ++p)
    if (*p == '/')
      *p = '!';

  snprintf(fn, sizeof(fn), "/sys/block/%s", devname);
  if (stat(fn, &st) == 0) {
    if (strlen(devname) + 1 > out_len) {
      return -ERANGE;
    }
    strncpy(out, devname, out_len);
    return 0;
  }

  dir = opendir("/sys/block");
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
    snprintf(fn, sizeof(fn), "/sys/block/%s/%s", de->d_name, devname);

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

bool block_device_support_discard(const char *devname)
{
  bool can_trim = false;
  char *p = strstr((char *)devname, "sd");
  char name[32];

  strncpy(name, p, sizeof(name) - 1);
  name[sizeof(name) - 1] = '\0';

  for (unsigned int i = 0; i < strlen(name); i++) {
    if(isdigit(name[i])) {
      name[i] = 0;
      break;
    }
  }

  char filename[100] = {0};
  sprintf(filename, "/sys/block/%s/queue/discard_granularity", name);

  FILE *fp = fopen(filename, "r");
  if (fp == NULL) {
    can_trim = false;
  } else {
    char buff[256] = {0};
    if (fgets(buff, sizeof(buff) - 1, fp)) {
      if (strcmp(buff, "0"))
	can_trim = false;
      else
	can_trim = true;
    } else
      can_trim = false;
    fclose(fp);
  }
  return can_trim;
}

int block_device_discard(int fd, int64_t offset, int64_t len)
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
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
#else
# error "Unable to query block device size: unsupported platform, please report."
#endif
