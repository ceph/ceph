#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

extern int get_block_device_size(int fd, int64_t *psize);
extern bool block_device_support_discard(const char *devname);
extern int block_device_discard(int fd, int64_t offset, int64_t len);
#endif
