#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

enum blkdev_prop_t {
  BLKDEV_PROP_DEV,
  BLKDEV_PROP_DISCARD_GRANULARITY,
  BLKDEV_PROP_MODEL,
  BLKDEV_PROP_ROTATIONAL,
  BLKDEV_PROP_SERIAL,
  BLKDEV_PROP_VENDOR,
  BLKDEV_PROP_NUMPROPS
};

/* for testing purposes */
extern void set_block_device_sandbox_dir(const char *dir);

// from a path (e.g., "/dev/sdb")
extern int get_block_device_base(const char *path, char *devname, size_t len);

// from an fd
extern int block_device_discard(int fd, int64_t offset, int64_t len);
extern int get_block_device_size(int fd, int64_t *psize);
extern int get_device_by_fd(int fd, char* partition, char* device, size_t max);

// from a uuid
extern int get_device_by_uuid(uuid_d dev_uuid, const char* label,
			      char* partition, char* device);

// from a device (e.g., "sdb")
extern bool block_device_support_discard(const char *devname);
extern bool block_device_is_nvme(const char *devname);
extern bool block_device_is_rotational(const char *devname);
extern int block_device_dev(const char *devname, char *dev, size_t max);
extern int block_device_model(const char *devname, char *model, size_t max);
extern int block_device_serial(const char *devname, char *serial, size_t max);

#endif
