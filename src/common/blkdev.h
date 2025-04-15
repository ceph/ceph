// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

#include <set>
#include <map>
#include <string>
#include "json_spirit/json_spirit_value.h"

extern int get_device_by_path(const char *path, char* partition, char* device, size_t max);

extern std::string _decode_model_enc(const std::string& in);  // helper, exported only so we can unit test

// get $vendor_$model_$serial style device id
extern std::string get_device_id(const std::string& devname,
				 std::string *err=0);

// get /dev/disk/by-path/... style device id that is stable for a disk slot across reboots etc
extern std::string get_device_path(const std::string& devname,
				   std::string *err=0);

// populate daemon metadata map with device info
extern void get_device_metadata(
  const std::set<std::string>& devnames,
  std::map<std::string,std::string> *pm,
  std::map<std::string,std::string> *errs);

extern void get_dm_parents(const std::string& dev, std::set<std::string> *ls);
extern int block_device_get_metrics(const std::string& devname, int timeout,
				    json_spirit::mValue *result);

// do everything to translate a device to the raw physical devices that
// back it, including partitions -> wholedisks and dm -> constituent devices.
extern void get_raw_devices(const std::string& in,
			    std::set<std::string> *ls);

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
  int get_optimal_io_size() const;
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
  int64_t get_int_property(const char* prop) const;
  int64_t get_string_property(const char* prop, char *val,
    size_t maxlen) const;

private:
  int fd = -1;
  std::string devname;
};

#endif
