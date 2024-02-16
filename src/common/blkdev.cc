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

#include "include/compat.h"

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
#include <boost/algorithm/string/replace.hpp>
//#include "common/debug.h"
#include "include/scope_guard.h"
#include "include/uuid.h"
#include "include/stringify.h"
#include "blkdev.h"
#include "numa.h"

#include "json_spirit/json_spirit_reader.h"


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

using namespace std::literals;

using std::string;

using ceph::bufferlist;


BlkDev::BlkDev(int f)
  : fd(f)
{}

BlkDev::BlkDev(const std::string& devname)
  : devname(devname)
{}

int BlkDev::get_devid(dev_t *id) const
{
  struct stat st;
  int r;
  if (fd >= 0) {
    r = fstat(fd, &st);
  } else {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/dev/%s", devname.c_str());
    r = stat(path, &st);
  }
  if (r < 0) {
    return -errno;
  }
  *id = S_ISBLK(st.st_mode) ? st.st_rdev : st.st_dev;
  return 0;
}

#ifdef __linux__

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
int64_t BlkDev::get_string_property(const char* prop,
				    char *val, size_t maxlen) const
{
  char filename[PATH_MAX], wd[PATH_MAX];
  const char* dev = nullptr;

  if (fd >= 0) {
    // sysfs isn't fully populated for partitions, so we need to lookup the sysfs
    // entry for the underlying whole disk.
    if (int r = wholedisk(wd, sizeof(wd)); r < 0)
      return r;
    dev = wd;
  } else {
    dev = devname.c_str();
  }
  if (snprintf(filename, sizeof(filename), "%s/block/%s/%s", sysfsdir(), dev,
	       prop) >= static_cast<int>(sizeof(filename))) {
    return -ERANGE;
  }

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
int64_t BlkDev::get_int_property(const char* prop) const
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
  return get_int_property("queue/discard_granularity") > 0;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  uint64_t range[2] = {(uint64_t)offset, (uint64_t)len};
  return ioctl(fd, BLKDISCARD, range);
}

int BlkDev::get_optimal_io_size() const
{
	return get_int_property("queue/optimal_io_size");
}

bool BlkDev::is_rotational() const
{
  return get_int_property("queue/rotational") > 0;
}

int BlkDev::get_numa_node(int *node) const
{
  int numa = get_int_property("device/device/numa_node");
  if (numa < 0)
    return -1;
  *node = numa;
  return 0;
}

int BlkDev::dev(char *dev, size_t max) const
{
  return get_string_property("dev", dev, max);
}

int BlkDev::vendor(char *vendor, size_t max) const
{
  return get_string_property("device/device/vendor", vendor, max);
}

int BlkDev::model(char *model, size_t max) const
{
  return get_string_property("device/model", model, max);
}

int BlkDev::serial(char *serial, size_t max) const
{
  return get_string_property("device/serial", serial, max);
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

void get_raw_devices(const std::string& in,
		     std::set<std::string> *ls)
{
  if (in.substr(0, 3) == "dm-") {
    std::set<std::string> o;
    get_dm_parents(in, &o);
    for (auto& d : o) {
      get_raw_devices(d, ls);
    }
  } else {
    BlkDev d(in);
    std::string wholedisk;
    if (d.wholedisk(&wholedisk) == 0) {
      ls->insert(wholedisk);
    } else {
      ls->insert(in);
    }
  }
}

std::string _decode_model_enc(const std::string& in)
{
  auto v = boost::replace_all_copy(in, "\\x20", " ");
  if (auto found = v.find_last_not_of(" "); found != v.npos) {
    v.erase(found + 1);
  }
  std::replace(v.begin(), v.end(), ' ', '_');

  // remove "__", which seems to come up on by ubuntu box for some reason.
  while (true) {
    auto p = v.find("__");
    if (p == std::string::npos) break;
    v.replace(p, 2, "_");
  }

  return v;
}

// trying to use udev first, and if it doesn't work, we fall back to 
// reading /sys/block/$devname/device/(vendor/model/serial).
std::string get_device_id(const std::string& devname,
			  std::string *err)
{
  struct udev_device *dev;
  static struct udev *udev;
  const char *data;

  udev = udev_new();
  if (!udev) {
    if (err) {
      *err = "udev_new failed";
    }
    return {};
  }
  dev = udev_device_new_from_subsystem_sysname(udev, "block", devname.c_str());
  if (!dev) {
    if (err) {
      *err = std::string("udev_device_new_from_subsystem_sysname failed on '")
	+ devname + "'";
    }
    udev_unref(udev);
    return {};
  }

  // ****
  //   NOTE: please keep this implementation in sync with _get_device_id() in
  //   src/ceph-volume/ceph_volume/util/device.py
  // ****

  std::string id_vendor, id_model, id_serial, id_serial_short, id_scsi_serial;
  data = udev_device_get_property_value(dev, "ID_VENDOR");
  if (data) {
    id_vendor = data;
  }
  data = udev_device_get_property_value(dev, "ID_MODEL");
  if (data) {
    id_model = data;
    // sometimes, ID_MODEL is "LVM ..." but ID_MODEL_ENC is correct (but
    // encoded with \x20 for space).
    if (id_model.substr(0, 7) == "LVM PV ") {
      const char *enc = udev_device_get_property_value(dev, "ID_MODEL_ENC");
      if (enc) {
	id_model = _decode_model_enc(enc);
      } else {
	// ignore ID_MODEL then
	id_model.clear();
      }
    }
  }
  data = udev_device_get_property_value(dev, "ID_SERIAL_SHORT");
  if (data) {
    id_serial_short = data;
  }
  data = udev_device_get_property_value(dev, "ID_SCSI_SERIAL");
  if (data) {
    id_scsi_serial = data;
  }
  data = udev_device_get_property_value(dev, "ID_SERIAL");
  if (data) {
    id_serial = data;
  }
  udev_device_unref(dev);
  udev_unref(udev);

  // ID_SERIAL is usually $vendor_$model_$serial, but not always
  // ID_SERIAL_SHORT is mostly always just the serial
  // ID_MODEL is sometimes $vendor_$model, but
  // ID_VENDOR is sometimes $vendor and ID_MODEL just $model and ID_SCSI_SERIAL the real serial number, with ID_SERIAL and ID_SERIAL_SHORT gibberish (ick)
  std::string device_id;
  if (id_vendor.size() && id_model.size() && id_scsi_serial.size()) {
    device_id = id_vendor + '_' + id_model + '_' + id_scsi_serial;
  } else if (id_model.size() && id_serial_short.size()) {
    device_id = id_model + '_' + id_serial_short;
  } else if (id_serial.size()) {
    device_id = id_serial;
    if (device_id.substr(0, 4) == "MTFD") {
      // Micron NVMes hide the vendor
      device_id = "Micron_" + device_id;
    }
  }
  if (device_id.size()) {
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
  if (!blkdev.serial(buf, sizeof(buf))) {
    serial = buf;
  }
  if (err) {
    if (model.empty() && serial.empty()) {
      *err = std::string("fallback method has no model nor serial");
      return {};
    } else if (model.empty()) {
      *err = std::string("fallback method has serial '") + serial
        + "' but no model'";
      return {};
    } else if (serial.empty()) {
      *err = std::string("fallback method has model '") + model
        + "' but no serial'";
      return {};
    }
  }

  device_id = model + "_" + serial;
  std::replace(device_id.begin(), device_id.end(), ' ', '_');
  return device_id;
}

static std::string get_device_vendor(const std::string& devname)
{
  struct udev_device *dev;
  static struct udev *udev;
  const char *data;

  udev = udev_new();
  if (!udev) {
    return {};
  }
  dev = udev_device_new_from_subsystem_sysname(udev, "block", devname.c_str());
  if (!dev) {
    udev_unref(udev);
    return {};
  }

  std::string id_vendor, id_model;
  data = udev_device_get_property_value(dev, "ID_VENDOR");
  if (data) {
    id_vendor = data;
  }
  data = udev_device_get_property_value(dev, "ID_MODEL");
  if (data) {
    id_model = data;
  }
  udev_device_unref(dev);
  udev_unref(udev);

  std::transform(id_vendor.begin(), id_vendor.end(), id_vendor.begin(),
		 ::tolower);
  std::transform(id_model.begin(), id_model.end(), id_model.begin(),
		 ::tolower);

  if (id_vendor.size()) {
    return id_vendor;
  }
  if (id_model.size()) {
    int pos = id_model.find(" ");
    if (pos > 0) {
      return id_model.substr(0, pos);
    } else {
      return id_model;
    }
  }

  std::string vendor, model;
  char buf[1024] = {0};
  BlkDev blkdev(devname);
  if (!blkdev.vendor(buf, sizeof(buf))) {
    vendor = buf;
  }
  if (!blkdev.model(buf, sizeof(buf))) {
    model = buf;
  }
  if (vendor.size()) {
    return vendor;
  }
  if (model.size()) {
     int pos = model.find(" ");
    if (pos > 0) {
      return model.substr(0, pos);
    } else {
      return model;
    }
  }

  return {};
}

static int block_device_run_vendor_nvme(
  const string& devname, const string& vendor, int timeout,
  std::string *result)
{
  string device = "/dev/" + devname;

  SubProcessTimed nvmecli(
    "sudo", SubProcess::CLOSE, SubProcess::PIPE, SubProcess::CLOSE,
    timeout);
  nvmecli.add_cmd_args(
    "nvme",
    vendor.c_str(),
    "smart-log-add",
    "--json",
    device.c_str(),
    NULL);
  int ret = nvmecli.spawn();
  if (ret != 0) {
    *result = std::string("error spawning nvme command: ") + nvmecli.err();
    return ret;
  }

  bufferlist output;
  ret = output.read_fd(nvmecli.get_stdout(), 100*1024);
  if (ret < 0) {
    bufferlist err;
    err.read_fd(nvmecli.get_stderr(), 100 * 1024);
    *result = std::string("failed to execute nvme: ") + err.to_str();
  } else {
    ret = 0;
    *result = output.to_str();
  }

  if (nvmecli.join() != 0) {
    *result = std::string("nvme returned an error: ") + nvmecli.err();
    return -EINVAL;
  }

  return ret;
}

std::string get_device_path(const std::string& devname,
			    std::string *err)
{
  std::set<std::string> links;
  int r = easy_readdir("/dev/disk/by-path", &links);
  if (r < 0) {
    *err = "unable to list contents of /dev/disk/by-path: "s +
      cpp_strerror(r);
    return {};
  }
  for (auto& i : links) {
    char fn[PATH_MAX];
    char target[PATH_MAX+1];
    snprintf(fn, sizeof(fn), "/dev/disk/by-path/%s", i.c_str());
    int r = readlink(fn, target, sizeof(target));
    if (r < 0 || r >= (int)sizeof(target))
      continue;
    target[r] = 0;
    if ((unsigned)r > devname.size() + 1 &&
	strncmp(target + r - devname.size(), devname.c_str(), r) == 0 &&
	target[r - devname.size() - 1] == '/') {
      return fn;
    }
  }
  *err = "no symlink to "s + devname + " in /dev/disk/by-path";
  return {};
}

static int block_device_run_smartctl(const string& devname, int timeout,
				     std::string *result)
{
  string device = "/dev/" + devname;

  // when using --json, smartctl will report its errors in JSON format to stdout 
  SubProcessTimed smartctl(
    "sudo", SubProcess::CLOSE, SubProcess::PIPE, SubProcess::CLOSE,
    timeout);
  smartctl.add_cmd_args(
    "smartctl",
    //"-a",    // all SMART info
    "-x",    // all SMART and non-SMART info
    "--json=o",
    device.c_str(),
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

  int joinerr = smartctl.join();
  // Bit 0: Command line did not parse.
  // Bit 1: Device open failed, device did not return an IDENTIFY DEVICE structure, or device is in a low-power mode (see '-n' option above).
  // Bit 2: Some SMART or other ATA command to the disk failed, or there was a checksum error in a SMART data structure (see '-b' option above).
  // Bit 3: SMART status check returned "DISK FAILING".
  // Bit 4: We found prefail Attributes <= threshold.
  // Bit 5: SMART status check returned "DISK OK" but we found that some (usage or prefail) Attributes have been <= threshold at some time in the past.
  // Bit 6: The device error log contains records of errors.
  // Bit 7: The device self-test log contains records of errors.  [ATA only] Failed self-tests outdated by a newer successful extended self-test are ignored.
  if (joinerr & 3) {
    *result = "smartctl returned an error ("s + stringify(joinerr) +
      "): stderr:\n"s + smartctl.err() + "\nstdout:\n"s + *result;
    return -EINVAL;
  }

  return ret;
}

static std::string escape_quotes(const std::string& s)
{
  std::string r = s;
  auto pos = r.find("\"");
  while (pos != std::string::npos) {
    r.replace(pos, 1, "\"");
    pos = r.find("\"", pos + 1);
  }
  return r;
}

int block_device_get_metrics(const string& devname, int timeout,
			     json_spirit::mValue *result)
{
  std::string s;

  // smartctl
  if (int r = block_device_run_smartctl(devname, timeout, &s);
      r != 0) {
    string orig = s;
    s = "{\"error\": \"smartctl failed\", \"dev\": \"/dev/";
    s += devname;
    s += "\", \"smartctl_error_code\": " + stringify(r);
    s += ", \"smartctl_output\": \"" + escape_quotes(orig);
    s += + "\"}";
  } else if (!json_spirit::read(s, *result)) {
    string orig = s;
    s = "{\"error\": \"smartctl returned invalid JSON\", \"dev\": \"/dev/";
    s += devname;
    s += "\",\"output\":\"";
    s += escape_quotes(orig);
    s += "\"}";
  }
  if (!json_spirit::read(s, *result)) {
    return -EINVAL;
  }

  json_spirit::mObject& base = result->get_obj();
  string vendor = get_device_vendor(devname);
  if (vendor.size()) {
    base["nvme_vendor"] = vendor;
    s.clear();
    json_spirit::mValue nvme_json;
    if (int r = block_device_run_vendor_nvme(devname, vendor, timeout, &s);
	r == 0) {
      if (json_spirit::read(s, nvme_json) != 0) {
	base["nvme_smart_health_information_add_log"] = nvme_json;
      } else {
	base["nvme_smart_health_information_add_log_error"] = "bad json output: "
	  + s;
      }
    } else {
      base["nvme_smart_health_information_add_log_error_code"] = r;
      base["nvme_smart_health_information_add_log_error"] = s;
    }
  } else {
    base["nvme_vendor"] = "unknown";
  }

  return 0;
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

int64_t BlkDev::get_int_property(const char* prop) const
{
  return 0;
}

bool BlkDev::support_discard() const
{
  return false;
}

int BlkDev::discard(int64_t offset, int64_t len) const
{
  return -EOPNOTSUPP;
}

int BlkDev::get_optimal_io_size() const
{
  return 0;
}

bool BlkDev::is_rotational() const
{
  return false;
}

int BlkDev::get_numa_node(int *node) const
{
  return -1;
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

void get_raw_devices(const std::string& in,
		     std::set<std::string> *ls)
{
}

std::string get_device_id(const std::string& devname,
			  std::string *err)
{
  // FIXME: implement me
  if (err) {
    *err = "not implemented";
  }
  return std::string();
}

std::string get_device_path(const std::string& devname,
			    std::string *err)
{
  // FIXME: implement me
  if (err) {
    *err = "not implemented";
  }
  return std::string();
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

int64_t BlkDev::get_int_property(const char* prop) const
{
  return 0;
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

int BlkDev::get_optimal_io_size() const
{
  return 0;
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

int BlkDev::get_numa_node(int *node) const
{
  int numa = get_int_property("device/device/numa_node");
  if (numa < 0)
    return -1;
  *node = numa;
  return 0;
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

int BlkDev::serial(char *serial, size_t max) const
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

void get_raw_devices(const std::string& in,
		     std::set<std::string> *ls)
{
}

std::string get_device_id(const std::string& devname,
			  std::string *err)
{
  // FIXME: implement me for freebsd
  if (err) {
    *err = "not implemented for FreeBSD";
  }
  return std::string();
}

std::string get_device_path(const std::string& devname,
			    std::string *err)
{
  // FIXME: implement me for freebsd
  if (err) {
    *err = "not implemented for FreeBSD";
  }
  return std::string();
}

int block_device_run_smartctl(const char *device, int timeout,
			      std::string *result)
{
  // FIXME: implement me for freebsd
  return -EOPNOTSUPP;  
}

int block_device_get_metrics(const string& devname, int timeout,
                             json_spirit::mValue *result)
{
  // FIXME: implement me for freebsd
  return -EOPNOTSUPP;  
}

int block_device_run_nvme(const char *device, const char *vendor, int timeout,
             std::string *result)
{
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

void get_raw_devices(const std::string& in,
		     std::set<std::string> *ls)
{
}

std::string get_device_id(const std::string& devname,
			  std::string *err)
{
  // not implemented
  if (err) {
    *err = "not implemented";
  }
  return std::string();
}

std::string get_device_path(const std::string& devname,
			  std::string *err)
{
  // not implemented
  if (err) {
    *err = "not implemented";
  }
  return std::string();
}

int block_device_run_smartctl(const char *device, int timeout,
			      std::string *result)
{
  return -EOPNOTSUPP;
}

int block_device_get_metrics(const string& devname, int timeout,
                             json_spirit::mValue *result)
{
  return -EOPNOTSUPP;
}

int block_device_run_nvme(const char *device, const char *vendor, int timeout,
            std::string *result)
{
  return -EOPNOTSUPP;
}

#endif



void get_device_metadata(
  const std::set<std::string>& devnames,
  std::map<std::string,std::string> *pm,
  std::map<std::string,std::string> *errs)
{
  (*pm)["devices"] = stringify(devnames);
  string &devids = (*pm)["device_ids"];
  string &devpaths = (*pm)["device_paths"];
  for (auto& dev : devnames) {
    string err;
    string id = get_device_id(dev, &err);
    if (id.size()) {
      if (!devids.empty()) {
	devids += ",";
      }
      devids += dev + "=" + id;
    } else {
      (*errs)[dev] = " no unique device id for "s + dev + ": " + err;
    }
    string path = get_device_path(dev, &err);
    if (path.size()) {
      if (!devpaths.empty()) {
	devpaths += ",";
      }
      devpaths += dev + "=" + path;
    } else {
      (*errs)[dev] += " no unique device path for "s + dev + ": " + err;
    }
  }
}
