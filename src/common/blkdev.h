// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

#include <set>
#include <string>
#include "json_spirit/json_spirit_value.h"

enum blkdev_prop_t {
  BLKDEV_PROP_DEV,
  BLKDEV_PROP_DISCARD_GRANULARITY,
  BLKDEV_PROP_MODEL,
  BLKDEV_PROP_ROTATIONAL,
  BLKDEV_PROP_SERIAL,
  BLKDEV_PROP_VENDOR,
  BLKDEV_PROP_NUMA_NODE,
  BLKDEV_PROP_NUMA_CPUS,
  BLKDEV_PROP_NUMPROPS,
};

extern int get_device_by_path(const char *path, char* partition, char* device, size_t max);

extern std::string _decode_model_enc(const std::string& in);  // helper, exported only so we can unit test

extern std::string get_device_id(const std::string& devname,
				 std::string *err=0);
extern void get_dm_parents(const std::string& dev, std::set<std::string> *ls);
extern int block_device_get_metrics(const std::string& devname, int timeout,
				    json_spirit::mValue *result);

// do everything to translate a device to the raw physical devices that
// back it, including partitions -> wholedisks and dm -> constituent devices.
extern void get_raw_devices(const std::string& in,
			    std::set<std::string> *ls);

// for VDO
/// return an op fd for the sysfs stats dir, if this is a VDO device
extern int get_vdo_stats_handle(const char *devname, std::string *vdo_name);
extern int64_t get_vdo_stat(int fd, const char *property);
extern bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail);

class BlkDev {
public:
  BlkDev(int fd);
  BlkDev(const std::string& devname);
  /* GoogleMock requires a virtual destructor */
  virtual ~BlkDev() {}

  // from an fd
  int discard(int64_t offset, int64_t len) const;
  int get_size(int64_t *psize) const;
  int get_devid(dev_t *id) const;
  int partition(char* partition, size_t max) const;
  // from a device (e.g., "sdb")
  bool support_discard() const;
  bool is_nvme() const;
  bool is_rotational() const;
  int get_numa_node(int *node) const;
  int dev(char *dev, size_t max) const;
  int vendor(char *vendor, size_t max) const;
  int model(char *model, size_t max) const;
  int serial(char *serial, size_t max) const;

  /* virtual for testing purposes */
  virtual const char *sysfsdir() const;
  virtual int wholedisk(char* device, size_t max) const;
  int wholedisk(std::string *s) const {
    char out[PATH_MAX] = {0};
    int r = wholedisk(out, sizeof(out));
    if (r < 0) {
      return r;
    }
    *s = out;
    return r;
  }

protected:
  int64_t get_int_property(blkdev_prop_t prop) const;
  int64_t get_string_property( blkdev_prop_t prop, char *val,
    size_t maxlen) const;

private:
  int fd = -1;
  std::string devname;
};

#endif
