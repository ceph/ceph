#include "include/int_types.h"

#include <fcntl.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <iostream>

#include "acconfig.h"
#include "include/compat.h"

#if defined(__FreeBSD__)
#include <sys/disk.h>
#endif

int get_block_device_size(int fd, int64_t *psize)
{
  int ret = 0;
  
#if defined(__FreeBSD__)
  ret = ::ioctl(fd, DIOCGMEDIASIZE, psize);
#elif defined(__linux__)
#ifdef BLKGETSIZE64
  // ioctl block device
  ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif BLKGETSIZE
  // hrm, try the 32 bit ioctl?
  unsigned long sectors = 0;
  ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  *psize = sectors * 512ULL;
#endif
#else
#error "Compile error: we don't know how to get the size of a raw block device."
#endif /* !__FreeBSD__ */
  if (ret < 0)
    ret = -errno;
  return ret;
}
