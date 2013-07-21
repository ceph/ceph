#include <acconfig.h>

#include <inttypes.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <iostream>
#include "include/compat.h"

#ifdef HAVE_SYS_DISK_H
#include <sys/disk.h>
#endif

#ifdef HAVE_LINUX_FS_H
#include <linux/fs.h>
#endif

#ifdef __linux__

int get_block_device_size(int fd, int64_t *psize)
{
#ifdef BLKGETSIZE64
  int ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif BLKGETSIZE
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

#elif defined(DARWIN)

int get_block_device_size(int fd, int64_t *psize)
{
  unsigned long blocksize = 0;
  int ret = ::ioctl(fd, DKIOCGETBLOCKSIZE, &blocksize);
  if (!ret) {
    unsigned long nblocks;
    ret = ::ioctl(fd, DKIOCGETBLOCKCOUNT, &nblocks);
    if (!ret)
      *psize = nblocks * blocksize;
  }
  if (ret < 0)
    ret = -errno;
  return ret;
}

#elif defined(__FreeBSD__)

int get_block_device_size(int fd, int64_t *psize)
{
  int ret = ::ioctl(fd, DIOCGMEDIASIZE, psize);
  if (ret < 0)
    ret = -errno;
  return ret;
}

#else
# warning "Unsupported platform. Please report."
# error   "Unable to query block device size."
#endif
