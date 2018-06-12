#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

#include <set>
#include <string>

/* for testing purposes */
extern void set_block_device_sandbox_dir(const char *dir);

// from a path (e.g., "/dev/sdb")
extern int get_block_device_base(const char *path, char *devname, size_t len);

// from an fd
extern int block_device_discard(int fd, int64_t offset, int64_t len);
extern int get_block_device_size(int fd, int64_t *psize);
extern int get_device_by_fd(int fd, char* partition, char* device, size_t max);

// from a device (e.g., "sdb")
extern int64_t get_block_device_int_property(
	const char *devname, const char *property);
extern int64_t get_block_device_string_property(
	const char *devname, const char *property,
	char *val, size_t maxlen);
extern bool block_device_support_discard(const char *devname);
extern bool block_device_is_rotational(const char *devname);
extern int block_device_vendor(const char *devname, char *vendor, size_t max);
extern int block_device_model(const char *devname, char *model, size_t max);
extern int block_device_serial(const char *devname, char *serial, size_t max);

extern void get_dm_parents(const std::string& dev, std::set<std::string> *ls);
extern std::string get_device_id(const std::string& devname);

// for VDO

/// return an op fd for the sysfs stats dir, if this is a VDO device
extern int get_vdo_stats_handle(const char *devname, std::string *vdo_name);
extern int64_t get_vdo_stat(int fd, const char *property);
extern bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail);

#endif
