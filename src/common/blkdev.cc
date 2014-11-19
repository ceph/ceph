#include <errno.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
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

bool block_device_support_discard(const char *devname)
{
  bool can_trim = false;
  char *p = strstr((char *)devname, "sd");
  char name[32] = {0};

  strcpy(name, p);
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
