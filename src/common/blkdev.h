#ifndef __CEPH_COMMON_BLKDEV_H
#define __CEPH_COMMON_BLKDEV_H

#include <set>
#include <string>

enum blkdev_prop_t {
  BLKDEV_PROP_DEV,
  BLKDEV_PROP_DISCARD_GRANULARITY,
  BLKDEV_PROP_MODEL,
  BLKDEV_PROP_ROTATIONAL,
  BLKDEV_PROP_SERIAL,
  BLKDEV_PROP_VENDOR,
  BLKDEV_PROP_NUMPROPS
};

extern int get_device_by_path(const char *path, char* partition, char* device, size_t max);


extern std::string get_device_id(const std::string& devname);
extern void get_dm_parents(const std::string& dev, std::set<std::string> *ls);
extern int block_device_run_smartctl(const char *device, int timeout,
				     std::string *result);

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
  int dev(char *dev, size_t max) const;
  int vendor(char *vendor, size_t max) const;
  int model(char *model, size_t max) const;
  int serial(char *serial, size_t max) const;

  /* virtual for testing purposes */
  virtual const char *sysfsdir() const;
  virtual int wholedisk(char* device, size_t max) const;

protected:
  int64_t get_int_property(blkdev_prop_t prop) const;
  int64_t get_string_property( blkdev_prop_t prop, char *val,
    size_t maxlen) const;

private:
  int fd = -1;
  std::string devname;
};

#endif
