// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifdef __FreeBSD__
#include <sys/param.h>
#include <geom/geom_disk.h>
#include <sys/disk.h>
#include <fcntl.h>
#endif

#include <errno.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
//#include "common/debug.h"
#include "include/scope_guard.h"
#include "include/uuid.h"
#include "blkdev.h"

int get_device_by_path(const char *path, char* partition, char* device,
		       size_t max)
{
  int fd = ::open(path, O_RDONLY|O_DIRECTORY);
  if (fd < 0) {
    return -errno;
  }
  auto close_fd = make_scope_guard([fd] {
    ::close(fd);
  });
  BlkDev blkdev(fd);
  if (auto ret = blkdev.partition(partition, max); ret) {
    return ret;
  }
  if (auto ret = blkdev.wholedisk(device, max); ret) {
    return ret;
  }
  return 0;
}


#include "common/blkdev.h"

#ifdef __linux__
#include <libudev.h>
#include <linux/fs.h>
#include <linux/kdev_t.h>
#include <blkid/blkid.h>

#include <set>

#include "common/SubProcess.h"
#include "common/errno.h"


#define UUID_LEN 36

#endif


BlkDev::BlkDev(int f)
  : fd(f)
{}

BlkDev::BlkDev(const std::string& devname)
  : devname(devname)
{}

int BlkDev::get_devid(dev_t *id) const {
  struct stat st;

  int r = fstat(fd, &st);

  if (r < 0)
    return -errno;

  *id = S_ISBLK(st.st_mode) ? st.st_rdev : st.st_dev;
  return 0;
}

#ifdef __linux__
static const char *blkdev_props2strings[] = {
  [BLKDEV_PROP_DEV]                 = "dev",
  [BLKDEV_PROP_DISCARD_GRANULARITY] = "queue/discard_granularity",
  [BLKDEV_PROP_MODEL]               = "device/model",
  [BLKDEV_PROP_ROTATIONAL]          = "queue/rotational",
  [BLKDEV_PROP_SERIAL]              = "device/serial",
  [BLKDEV_PROP_VENDOR]              = "device/device/vendor",
};

const char *BlkDev::sysfsdir() const {
  return "/sys";
}

int BlkDev::get_size(int64_t *psize) const
{
#ifdef BLKGETSIZE64
  int ret = ::ioctl(fd, BLKGETSIZE64, psize);
#elif defined(BLKGETSIZE)
  unsigned long sectors = 0;
  int ret = ::ioctl(fd, BLKGETSIZE, &sectors);
  *psize = sectors * 512ULL;
#else
// cppcheck-suppress preprocessorErrorDirective
# error "Linux configuration error (get_size)"
#endif
  if (ret < 0)
    ret = -errno;
  return ret;
}

/**
 * get a block device property as a string
 *
 * store property in *val, up to maxlen chars
 * return 0 on success
 * return negative error on error
 */
int64_t BlkDev::get_string_property(blkdev_prop_t prop,
				    char *val, size_t maxlen) const
{
  char filename[PATH_MAX], wd[PATH_MAX];
  const char* dev = nullptr;
  assert(prop < BLKDEV_PROP_NUMPROPS);
  const char *propstr = blkdev_props2strings[prop];

  if (fd >= 0) {
    // sysfs isn't fully populated for partitions, so we need to lookup the sysfs
    // entry for the underlying whole disk.
    if (int r = wholedisk(wd, sizeof(wd)); r < 0)
      return r;
    dev = wd;
  } else {
    dev = devname.c_str();
  }
  snprintf(filename, sizeof(filename),
	   "%s/block/%s/%s", sysfsdir(), dev, propstr);

  FILE *fp = fopen(filename, "r");
  if (fp == NULL) {
    return -errno;
  }

  int r = 0;
  if (fgets(val, maxlen - 1, fp)) {
    // truncate at newline
    char *p = val;
    while (*p && *p != '\n')
      ++p;
    *p = 0;
  } else {
    r = -EINVAL;
  }
  fclose(fp);
  return r;
}

/**
 * get a block device property
 *
 * return the value (we assume it is positive)
 * return negative error on error
 */
int64_t BlkDev::get_int_property(blkdev_prop_t prop) const
{
  char buff[256] = {0};
  int r = get_string_property(prop, buff, sizeof(buff));
  if (r < 0)
    return r;
  // take only digits
  for (char *p = buff; *p; ++p) {
    if (!isdigit(*p)) {
      *p = 0;
      break;
    }
  }
  char *endptr = 0;
  r = strtoll(buff, &endptr, 10);
  if (endptr != buff + strlen(buff))
    r = -EINVAL;
  return r;
}

bool BlkDev::support_discard() const
{
  return get_int_property(BLKDEV_PROP_DISCARD_GRANULARITY) > 0;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

bool BlkDev::is_nvme() const
{
  char vendor[80];
  // nvme has a device/device/vendor property; infer from that.  There is
  // probably a better way?
  int r = get_string_property(BLKDEV_PROP_VENDOR, vendor, 80);
  return (r == 0);
}

bool BlkDev::is_rotational() const
{
  return get_int_property(BLKDEV_PROP_ROTATIONAL) > 0;
}

int BlkDev::dev(char *dev, size_t max) const
{
  return get_string_property(BLKDEV_PROP_DEV, dev, max);
}

int BlkDev::vendor(char *vendor, size_t max) const
{
  return get_string_property(BLKDEV_PROP_VENDOR, vendor, max);
}

int BlkDev::model(char *model, size_t max) const
{
  return get_string_property(BLKDEV_PROP_MODEL, model, max);
}

int BlkDev::serial(char *serial, size_t max) const
{
  return get_string_property(BLKDEV_PROP_SERIAL, serial, max);
}

int BlkDev::partition(char *partition, size_t max) const
{
  dev_t id;
  int r = get_devid(&id);
  if (r < 0)
    return -EINVAL;  // hrm.

  char *t = blkid_devno_to_devname(id);
  if (!t) {
    return -EINVAL;
  }
  strncpy(partition, t, max);
  free(t);
  return 0;
}

int BlkDev::wholedisk(char *device, size_t max) const
{
  dev_t id;
  int r = get_devid(&id);
  if (r < 0)
    return -EINVAL;  // hrm.

  r = blkid_devno_to_wholedisk(id, device, max, nullptr);
  if (r < 0) {
    return -EINVAL;
  }
  return 0;
}

static int easy_readdir(const std::string& dir, std::set<std::string> *out)
{
  DIR *h = ::opendir(dir.c_str());
  if (!h) {
    return -errno;
  }
  struct dirent *de = nullptr;
  while ((de = ::readdir(h))) {
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0) {
      continue;
    }
    out->insert(de->d_name);
  }
  closedir(h);
  return 0;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
  std::string p = std::string("/sys/block/") + dev + "/slaves";
  std::set<std::string> parents;
  easy_readdir(p, &parents);
  for (auto& d : parents) {
    ls->insert(d);
    // recurse in case it is dm-on-dm
    if (d.find("dm-") == 0) {
      get_dm_parents(d, ls);
    }
  }
}

int _get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  int vdo_fd = -1;

  // we need to go from the raw devname (e.g., dm-4) to the VDO volume name.
  // currently the best way seems to be to look at /dev/mapper/* ...
  std::string expect = std::string("../") + devname;  // expected symlink target
  DIR *dir = ::opendir("/dev/mapper");
  if (!dir) {
    return -1;
  }
  struct dirent *de = nullptr;
  while ((de = ::readdir(dir))) {
    if (de->d_name[0] == '.')
      continue;
    char fn[4096], target[4096];
    snprintf(fn, sizeof(fn), "/dev/mapper/%s", de->d_name);
    int r = readlink(fn, target, sizeof(target));
    if (r < 0 || r >= (int)sizeof(target))
      continue;
    target[r] = 0;
    if (expect == target) {
      snprintf(fn, sizeof(fn), "/sys/kvdo/%s/statistics", de->d_name);
      vdo_fd = ::open(fn, O_RDONLY|O_CLOEXEC); //DIRECTORY);
      if (vdo_fd >= 0) {
	*vdo_name = de->d_name;
	break;
      }
    }
  }
  closedir(dir);
  return vdo_fd;
}

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  std::set<std::string> devs = { devname };
  while (!devs.empty()) {
    std::string dev = *devs.begin();
    devs.erase(devs.begin());
    int fd = _get_vdo_stats_handle(dev.c_str(), vdo_name);
    if (fd >= 0) {
      // yay, it's vdo
      return fd;
    }
    // ok, see if there are constituent devices
    if (dev.find("dm-") == 0) {
      get_dm_parents(dev, &devs);
    }
  }
  return -1;
}

int64_t get_vdo_stat(int vdo_fd, const char *property)
{
  int64_t ret = 0;
  int fd = ::openat(vdo_fd, property, O_RDONLY|O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  char buf[1024];
  int r = ::read(fd, buf, sizeof(buf) - 1);
  if (r > 0) {
    buf[r] = 0;
    ret = atoll(buf);
  }
  TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  int64_t block_size = get_vdo_stat(fd, "block_size");
  int64_t physical_blocks = get_vdo_stat(fd, "physical_blocks");
  int64_t overhead_blocks_used = get_vdo_stat(fd, "overhead_blocks_used");
  int64_t data_blocks_used = get_vdo_stat(fd, "data_blocks_used");
  if (!block_size
      || !physical_blocks
      || !overhead_blocks_used
      || !data_blocks_used) {
    return false;
  }
  int64_t avail_blocks =
    physical_blocks - overhead_blocks_used - data_blocks_used;
  *total = block_size * physical_blocks;
  *avail = block_size * avail_blocks;
  return true;
}

// trying to use udev first, and if it doesn't work, we fall back to 
// reading /sys/block/$devname/device/(vendor/model/serial).
std::string get_device_id(const std::string& devname)
{
  struct udev_device *dev;
  static struct udev *udev;
  const char *data;
  std::string device_id;

  udev = udev_new();
  if (!udev) {
    return {};
  }
  dev = udev_device_new_from_subsystem_sysname(udev, "block", devname.c_str());
  if (!dev) {
    udev_unref(udev);
    return {};
  }

  // "ID_SERIAL_SHORT" returns only the serial number;
  // "ID_SERIAL" returns vendor model_serial.
  data = udev_device_get_property_value(dev, "ID_SERIAL");
  if (data) {
    device_id = data;
  }

  udev_device_unref(dev);
  udev_unref(udev);

  if (!device_id.empty()) {
    std::replace(device_id.begin(), device_id.end(), ' ', '_');
    return device_id;
  }

  // either udev_device_get_property_value() failed, or succeeded but
  // returned nothing; trying to read from files.  note that the 'vendor'
  // file rarely contains the actual vendor; it's usually 'ATA'.
  std::string model, serial;
  char buf[1024] = {0};
  BlkDev blkdev(devname);
  if (!blkdev.model(buf, sizeof(buf))) {
    model = buf;
  }
  if (blkdev.serial(buf, sizeof(buf))) {
    serial = buf;
  }
  if (!model.size() || serial.size()) {
    return {};
  }

  device_id = model + "_" + serial;
  std::replace(device_id.begin(), device_id.end(), ' ', '_');
  return device_id;
}

int block_device_run_smartctl(const char *device, int timeout,
			      std::string *result)
{
  // when using --json, smartctl will report its errors in JSON format to stdout 
  SubProcessTimed smartctl(
    "sudo", SubProcess::CLOSE, SubProcess::PIPE, SubProcess::CLOSE,
    timeout);
  smartctl.add_cmd_args(
    "smartctl",
    "-a",
    //"-x",
    "--json",
    device,
    NULL);

  int ret = smartctl.spawn();
  if (ret != 0) {
    *result = std::string("error spawning smartctl: ") + smartctl.err();
    return ret;
  }

  bufferlist output;
  ret = output.read_fd(smartctl.get_stdout(), 100*1024);
  if (ret < 0) {
    *result = std::string("failed read smartctl output: ") + cpp_strerror(-ret);
  } else {
    ret = 0;
    *result = output.to_str();
  }

  if (smartctl.join() != 0) {
    *result = std::string("smartctl returned an error:") + smartctl.err();
    return -EINVAL;
  }

  return ret;
}


#elif defined(__APPLE__)
#include <sys/disk.h>

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on Apple
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_size(int64_t *psize) const
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

bool BlkDev::support_discard() const
{
  return false;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme() const
{
  return false;
}

bool BlkDev::is_rotational() const
{
  return false;
}

int BlkDev::model(char *model, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::serial(char *serial, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::partition(char *partition, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::wholedisk(char *device, size_t max) const
{
}


void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
}

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  return -1;
}

int64_t get_vdo_stat(int fd, const char *property)
{
  return 0;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  return false;
}

#elif defined(__FreeBSD__)

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on FreeBSD
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  struct stat sb;

  if (fstat(fd, &sb) < 0)
    return -errno;

  snprintf(dev, max, "%" PRIu64, (uint64_t)sb.st_rdev);

  return 0;
}

int BlkDev::get_size(int64_t *psize) const
{
  int ret = ::ioctl(fd, DIOCGMEDIASIZE, psize);
  if (ret < 0)
    ret = -errno;
  return ret;
}

bool BlkDev::support_discard() const
{
#ifdef FREEBSD_WITH_TRIM
  // there is no point to claim support of discard, but
  // unable to do so.
  struct diocgattr_arg arg;

  strlcpy(arg.name, "GEOM::candelete", sizeof(arg.name));
  arg.len = sizeof(arg.value.i);
  if (ioctl(fd, DIOCGATTR, &arg) == 0) {
    return (arg.value.i != 0);
  } else {
    return false;
  }
#endif
  return false;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme() const
{
  // FreeBSD doesn't have a good way to tell if a device's underlying protocol
  // is NVME, especially since multiple GEOM transforms may be involved.  So
  // we'll just guess based on the device name.
  struct fiodgname_arg arg;
  const char *nda = "nda";        //CAM-based attachment
  const char *nvd = "nvd";        //CAM-less attachment
  char devname[PATH_MAX];

  arg.buf = devname;
  arg.len = sizeof(devname);
  if (ioctl(fd, FIODGNAME, &arg) < 0)
    return false; //When in doubt, it's probably not NVME

  return (strncmp(nvd, devname, strlen(nvd)) == 0 ||
          strncmp(nda, devname, strlen(nda)) == 0);
}

bool BlkDev::is_rotational() const
{
#if __FreeBSD_version >= 1200049
  struct diocgattr_arg arg;

  strlcpy(arg.name, "GEOM::rotation_rate", sizeof(arg.name));
  arg.len = sizeof(arg.value.u16);

  int ioctl_ret = ioctl(fd, DIOCGATTR, &arg);
  bool ret;
  if (ioctl_ret < 0 || arg.value.u16 == DISK_RR_UNKNOWN)
    // DISK_RR_UNKNOWN usually indicates an old drive, which is usually spinny
    ret = true;
  else if (arg.value.u16 == DISK_RR_NON_ROTATING)
    ret = false;
  else if (arg.value.u16 >= DISK_RR_MIN && arg.value.u16 <= DISK_RR_MAX)
    ret = true;
  else
    ret = true;     // Invalid value.  Probably spinny?

  return ret;
#else
  return true;      // When in doubt, it's probably spinny
#endif
}

int BlkDev::model(char *model, size_t max) const
{
  struct diocgattr_arg arg;

  strlcpy(arg.name, "GEOM::descr", sizeof(arg.name));
  arg.len = sizeof(arg.value.str);
  if (ioctl(fd, DIOCGATTR, &arg) < 0) {
    return -errno;
  }

  // The GEOM description is of the form "vendor product" for SCSI disks
  // and "ATA device_model" for ATA disks.  Some vendors choose to put the
  // vendor name in device_model, and some don't.  Strip the first bit.
  char *p = arg.value.str;
  if (p == NULL || *p == '\0') {
    *model = '\0';
  } else {
    (void) strsep(&p, " ");
    snprintf(model, max, "%s", p);
  }

  return 0;
}

int BlkDev::serial(char *serial, size_t max)
{
  char ident[DISK_IDENT_SIZE];

  if (ioctl(fd, DIOCGIDENT, ident) < 0)
    return -errno;

  snprintf(serial, max, "%s", ident);

  return 0;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
}

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  return -1;
}

int64_t get_vdo_stat(int fd, const char *property)
{
  return 0;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  return false;
}

std::string get_device_id(const std::string& devname)
{
  // FIXME: implement me for freebsd
  return std::string();
}

int block_device_run_smartctl(const char *device, int timeout,
			      std::string *result)
{
  // FIXME: implement me for freebsd
  return -EOPNOTSUPP;  
}

static int block_device_devname(int fd, char *devname, size_t max)
{
  struct fiodgname_arg arg;

  arg.buf = devname;
  arg.len = max;
  if (ioctl(fd, FIODGNAME, &arg) < 0)
    return -errno;
  return 0;
}

int BlkDev::partition(char *partition, size_t max) const
{
  char devname[PATH_MAX];

  if (block_device_devname(fd, devname, sizeof(devname)) < 0)
    return -errno;
  snprintf(partition, max, "/dev/%s", devname);
  return 0;
}

int BlkDev::wholedisk(char *wd, size_t max) const
{
  char devname[PATH_MAX];

  if (block_device_devname(fd, devname, sizeof(devname)) < 0)
    return -errno;

  size_t first_digit = strcspn(devname, "0123456789");
  // first_digit now indexes the first digit or null character of devname
  size_t next_nondigit = strspn(&devname[first_digit], "0123456789");
  next_nondigit += first_digit;
  // next_nondigit now indexes the first alphabetic or null character after the
  // unit number
  strlcpy(wd, devname, next_nondigit + 1);
  return 0;
}

#else

const char *BlkDev::sysfsdir() const {
  assert(false);  // Should never be called on non-Linux
  return "";
}

int BlkDev::dev(char *dev, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::get_size(int64_t *psize) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::support_discard() const
{
  return false;
}

int BlkDev::discard(int fd, int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

bool BlkDev::is_nvme(const char *devname) const
{
  return false;
}

bool BlkDev::is_rotational(const char *devname) const
{
  return false;
}

int BlkDev::model(char *model, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::serial(char *serial, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::partition(char *partition, size_t max) const
{
  return -EOPNOTSUPP;
}

int BlkDev::wholedisk(char *wd, size_t max) const
{
  return -EOPNOTSUPP;
}

void get_dm_parents(const std::string& dev, std::set<std::string> *ls)
{
}

int get_vdo_stats_handle(const char *devname, std::string *vdo_name)
{
  return -1;
}

int64_t get_vdo_stat(int fd, const char *property)
{
  return 0;
}

bool get_vdo_utilization(int fd, uint64_t *total, uint64_t *avail)
{
  return false;
}

std::string get_device_id(const std::string& devname)
{
  // not implemented
  return std::string();
}

int block_device_run_smartctl(const char *device, int timeout,
			      std::string *result)
{
  return -EOPNOTSUPP;
}

#endif
