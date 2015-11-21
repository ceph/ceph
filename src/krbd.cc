/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "auth/KeyRing.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/module.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/secret.h"
#include "common/TextTable.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/krbd.h"
#include "mon/MonMap.h"

#include <blkid/blkid.h>
#include <libudev.h>

using namespace std;

struct krbd_ctx {
  CephContext *cct;
  struct udev *udev;
};

static string get_kernel_rbd_name(const char *id)
{
  return string("/dev/rbd") + id;
}

static int sysfs_write_rbd(const char *which, const string& buf)
{
  const string s = string("/sys/bus/rbd/") + which;
  const string t = s + "_single_major";
  int fd;
  int r;

  /*
   * 'add' and 'add_single_major' interfaces are identical, but if rbd
   * kernel module is new enough and is configured to use single-major
   * scheme, 'add' is disabled in order to prevent old userspace from
   * doing weird things at unmap time.
   *
   * Same goes for 'remove' vs 'remove_single_major'.
   */
  fd = open(t.c_str(), O_WRONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      fd = open(s.c_str(), O_WRONLY);
      if (fd < 0)
        return -errno;
    } else {
      return -errno;
    }
  }

  r = safe_write(fd, buf.c_str(), buf.size());

  close(fd);
  return r;
}

static int sysfs_write_rbd_add(const string& buf)
{
  return sysfs_write_rbd("add", buf);
}

static int sysfs_write_rbd_remove(const string& buf)
{
  return sysfs_write_rbd("remove", buf);
}

static int should_match_minor(void)
{
  /*
   * 'minor' attribute was added as part of single_major merge, which
   * exposed the 'single_major' parameter.  'minor' is always present,
   * regardless of whether single-major scheme is turned on or not.
   */
  return access("/sys/module/rbd/parameters/single_major", F_OK) == 0;
}

/*
 * options can be NULL
 */
static int build_map_buf(CephContext *cct, const char *pool, const char *image,
                         const char *snap, const char *options, string *pbuf)
{
  ostringstream oss;
  int r;

  MonMap monmap;
  r = monmap.build_initial(cct, cerr);
  if (r < 0)
    return r;

  for (map<string, entity_addr_t>::const_iterator it = monmap.mon_addr.begin();
       it != monmap.mon_addr.end();
       ++it) {
    if (it != monmap.mon_addr.begin())
      oss << ",";
    oss << it->second.addr;
  }

  oss << " name=" << cct->_conf->name.get_id();

  KeyRing keyring;
  r = keyring.from_ceph_context(cct);
  if (r == -ENOENT && !(cct->_conf->keyfile.length() ||
                        cct->_conf->key.length()))
    r = 0;
  if (r < 0) {
    cerr << "rbd: failed to get secret" << std::endl;
    return r;
  }

  CryptoKey secret;
  string key_name = string("client.") + cct->_conf->name.get_id();
  if (keyring.get_secret(cct->_conf->name, secret)) {
    string secret_str;
    secret.encode_base64(secret_str);

    r = set_kernel_secret(secret_str.c_str(), key_name.c_str());
    if (r >= 0) {
      if (r == 0)
        cerr << "rbd: warning: secret has length 0" << std::endl;
      oss << ",key=" << key_name;
    } else if (r == -ENODEV || r == -ENOSYS) {
      // running against older kernel; fall back to secret= in options
      oss << ",secret=" << secret_str;
    } else {
      cerr << "rbd: failed to add secret '" << key_name << "' to kernel"
           << std::endl;
      return r;
    }
  } else if (is_kernel_secret(key_name.c_str())) {
    oss << ",key=" << key_name;
  }

  if (options && strcmp(options, "") != 0)
    oss << "," << options;

  oss << " " << pool << " " << image << " " << snap;

  *pbuf = oss.str();
  return 0;
}

static int wait_for_udev_add(struct udev_monitor *mon, const char *pool,
                             const char *image, const char *snap,
                             string *pname)
{
  struct udev_device *bus_dev = NULL;

  /*
   * Catch /sys/devices/rbd/<id>/ and wait for the corresponding
   * block device to show up.  This is necessary because rbd devices
   * and block devices aren't linked together in our sysfs layout.
   */
  for (;;) {
    struct pollfd fds[1];
    struct udev_device *dev;

    fds[0].fd = udev_monitor_get_fd(mon);
    fds[0].events = POLLIN;
    if (poll(fds, 1, -1) < 0)
      return -errno;

    dev = udev_monitor_receive_device(mon);
    if (!dev)
      continue;

    if (strcmp(udev_device_get_action(dev), "add") != 0)
      goto next;

    if (!bus_dev) {
      if (strcmp(udev_device_get_subsystem(dev), "rbd") == 0) {
        const char *this_pool = udev_device_get_sysattr_value(dev, "pool");
        const char *this_image = udev_device_get_sysattr_value(dev, "name");
        const char *this_snap = udev_device_get_sysattr_value(dev,
                                                              "current_snap");

        if (strcmp(this_pool, pool) == 0 &&
            strcmp(this_image, image) == 0 &&
            strcmp(this_snap, snap) == 0) {
          bus_dev = dev;
          continue;
        }
      }
    } else {
      if (strcmp(udev_device_get_subsystem(dev), "block") == 0) {
        const char *major = udev_device_get_sysattr_value(bus_dev, "major");
        const char *minor = udev_device_get_sysattr_value(bus_dev, "minor");
        const char *this_major = udev_device_get_property_value(dev, "MAJOR");
        const char *this_minor = udev_device_get_property_value(dev, "MINOR");

        assert(!minor ^ should_match_minor());

        if (strcmp(this_major, major) == 0 &&
            (!minor || strcmp(this_minor, minor) == 0)) {
          string name = get_kernel_rbd_name(udev_device_get_sysname(bus_dev));

          assert(strcmp(udev_device_get_devnode(dev), name.c_str()) == 0);
          *pname = name;

          udev_device_unref(dev);
          udev_device_unref(bus_dev);
          break;
        }
      }
    }

  next:
    udev_device_unref(dev);
  }

  return 0;
}

static int do_map(struct udev *udev, const char *pool, const char *image,
                  const char *snap, const string& buf, string *pname)
{
  struct udev_monitor *mon;
  int r;

  mon = udev_monitor_new_from_netlink(udev, "udev");
  if (!mon)
    return -ENOMEM;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "rbd", NULL);
  if (r < 0)
    goto out_mon;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "block", "disk");
  if (r < 0)
    goto out_mon;

  r = udev_monitor_enable_receiving(mon);
  if (r < 0)
    goto out_mon;

  r = sysfs_write_rbd_add(buf);
  if (r < 0) {
    cerr << "rbd: sysfs write failed" << std::endl;
    goto out_mon;
  }

  r = wait_for_udev_add(mon, pool, image, snap, pname);
  if (r < 0) {
    cerr << "rbd: wait failed" << std::endl;
    goto out_mon;
  }

out_mon:
  udev_monitor_unref(mon);
  return r;
}

/*
 * snap and options can be NULL
 */
static int map_image(struct krbd_ctx *ctx, const char *pool, const char *image,
                     const char *snap, const char *options, string *pname)
{
  string buf;
  int r;

  if (!snap)
    snap = "-";

  r = build_map_buf(ctx->cct, pool, image, snap, options, &buf);
  if (r < 0)
    return r;

  /*
   * Modprobe rbd kernel module.  If it supports single-major device
   * number allocation scheme, make sure it's turned on.
   */
  if (access("/sys/bus/rbd", F_OK) != 0) {
    const char *module_options = NULL;
    if (module_has_param("rbd", "single_major"))
      module_options = "single_major=Y";

    r = module_load("rbd", module_options);
    if (r) {
      cerr << "rbd: failed to load rbd kernel module (" << r << ")"
           << std::endl;
      /*
       * Ignore the error: modprobe failing doesn't necessarily prevent
       * from working.
       */
    }
  }

  return do_map(ctx->udev, pool, image, snap, buf, pname);
}

static int devno_to_krbd_id(struct udev *udev, dev_t devno, string *pid)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l;
  struct udev_device *dev;
  int r;

  enm = udev_enumerate_new(udev);
  if (!enm)
    return -ENOMEM;

  r = udev_enumerate_add_match_subsystem(enm, "rbd");
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_add_match_sysattr(enm, "major",
                                       stringify(major(devno)).c_str());
  if (r < 0)
    goto out_enm;

  if (should_match_minor()) {
    r = udev_enumerate_add_match_sysattr(enm, "minor",
                                         stringify(minor(devno)).c_str());
    if (r < 0)
      goto out_enm;
  }

  r = udev_enumerate_scan_devices(enm);
  if (r < 0)
    goto out_enm;

  l = udev_enumerate_get_list_entry(enm);
  if (!l) {
    r = -ENOENT;
    goto out_enm;
  }

  /* make sure there is only one match */
  assert(!udev_list_entry_get_next(l));

  dev = udev_device_new_from_syspath(udev, udev_list_entry_get_name(l));
  if (!dev) {
    r = -ENOMEM;
    goto out_enm;
  }

  *pid = udev_device_get_sysname(dev);

  udev_device_unref(dev);
out_enm:
  udev_enumerate_unref(enm);
  return r;
}

static int wait_for_udev_remove(struct udev_monitor *mon, dev_t devno)
{
  for (;;) {
    struct pollfd fds[1];
    struct udev_device *dev;

    fds[0].fd = udev_monitor_get_fd(mon);
    fds[0].events = POLLIN;
    if (poll(fds, 1, -1) < 0)
      return -errno;

    dev = udev_monitor_receive_device(mon);
    if (!dev)
      continue;

    if (strcmp(udev_device_get_action(dev), "remove") == 0 &&
        udev_device_get_devnum(dev) == devno) {
      udev_device_unref(dev);
      break;
    }

    udev_device_unref(dev);
  }

  return 0;
}

static int do_unmap(struct udev *udev, dev_t devno, const string& id)
{
  struct udev_monitor *mon;
  int r;

  mon = udev_monitor_new_from_netlink(udev, "udev");
  if (!mon)
    return -ENOMEM;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "block", "disk");
  if (r < 0)
    goto out_mon;

  r = udev_monitor_enable_receiving(mon);
  if (r < 0)
    goto out_mon;

  /*
   * On final device close(), kernel sends a block change event, in
   * response to which udev apparently runs blkid on the device.  This
   * makes unmap fail with EBUSY, if issued right after final close().
   * Try to circumvent this with a retry before turning to udev.
   */
  for (int tries = 0; ; tries++) {
    r = sysfs_write_rbd_remove(id);
    if (r >= 0) {
      break;
    } else if (r == -EBUSY && tries < 2) {
      if (!tries) {
        usleep(250 * 1000);
      } else {
        /*
         * libudev does not provide the "wait until the queue is empty"
         * API or the sufficient amount of primitives to build it from.
         */
        string err = run_cmd("udevadm", "settle", "--timeout", "10", NULL);
        if (!err.empty())
          cerr << "rbd: " << err << std::endl;
      }
    } else {
      cerr << "rbd: sysfs write failed" << std::endl;
      goto out_mon;
    }
  }

  r = wait_for_udev_remove(mon, devno);
  if (r < 0) {
    cerr << "rbd: wait failed" << std::endl;
    goto out_mon;
  }

out_mon:
  udev_monitor_unref(mon);
  return r;
}

static int unmap_image(struct krbd_ctx *ctx, const char *devnode)
{
  struct stat sb;
  dev_t wholedevno;
  string id;
  int r;

  if (stat(devnode, &sb) < 0 || !S_ISBLK(sb.st_mode)) {
    cerr << "rbd: '" << devnode << "' is not a block device" << std::endl;
    return -EINVAL;
  }

  r = blkid_devno_to_wholedisk(sb.st_rdev, NULL, 0, &wholedevno);
  if (r < 0) {
    cerr << "rbd: couldn't compute wholedevno: " << cpp_strerror(r)
         << std::endl;
    /*
     * Ignore the error: we are given whole disks most of the time, and
     * if it turns out this is a partition we will fail later anyway.
     */
    wholedevno = sb.st_rdev;
  }

  r = devno_to_krbd_id(ctx->udev, wholedevno, &id);
  if (r < 0) {
    if (r == -ENOENT) {
      cerr << "rbd: '" << devnode << "' is not an rbd device" << std::endl;
      r = -EINVAL;
    }
    return r;
  }

  return do_unmap(ctx->udev, wholedevno, id);
}

static void dump_one_image(Formatter *f, TextTable *tbl,
                           const char *id, const char *pool,
                           const char *image, const char *snap)
{
  assert(id && pool && image && snap);

  string kname = get_kernel_rbd_name(id);

  if (f) {
    f->open_object_section(id);
    f->dump_string("pool", pool);
    f->dump_string("name", image);
    f->dump_string("snap", snap);
    f->dump_string("device", kname);
    f->close_section();
  } else {
    *tbl << id << pool << image << snap << kname << TextTable::endrow;
  }
}

static int do_dump(struct udev *udev, Formatter *f, TextTable *tbl)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l;
  bool have_output = false;
  int r;

  enm = udev_enumerate_new(udev);
  if (!enm)
    return -ENOMEM;

  r = udev_enumerate_add_match_subsystem(enm, "rbd");
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_scan_devices(enm);
  if (r < 0)
    goto out_enm;

  udev_list_entry_foreach(l, udev_enumerate_get_list_entry(enm)) {
    struct udev_device *dev;

    dev = udev_device_new_from_syspath(udev, udev_list_entry_get_name(l));
    if (!dev) {
      r = -ENOMEM;
      goto out_enm;
    }

    dump_one_image(f, tbl, udev_device_get_sysname(dev),
                   udev_device_get_sysattr_value(dev, "pool"),
                   udev_device_get_sysattr_value(dev, "name"),
                   udev_device_get_sysattr_value(dev, "current_snap"));

    have_output = true;
    udev_device_unref(dev);
  }

  r = have_output;
out_enm:
  udev_enumerate_unref(enm);
  return r;
}

int dump_images(struct krbd_ctx *ctx, Formatter *f)
{
  TextTable tbl;
  int r;

  if (f) {
    f->open_object_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("snap", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
  }

  r = do_dump(ctx->udev, f, &tbl);

  if (f) {
    f->close_section();
    f->flush(cout);
  } else {
    if (r > 0)
      cout << tbl;
  }

  return r;
}

extern "C" int krbd_create_from_context(rados_config_t cct,
                                        struct krbd_ctx **pctx)
{
  struct krbd_ctx *ctx = new struct krbd_ctx();

  ctx->cct = reinterpret_cast<CephContext *>(cct);
  ctx->udev = udev_new();
  if (!ctx->udev) {
    delete ctx;
    return -ENOMEM;
  }

  *pctx = ctx;
  return 0;
}

extern "C" void krbd_destroy(struct krbd_ctx *ctx)
{
  if (!ctx)
    return;

  udev_unref(ctx->udev);

  delete ctx;
}

extern "C" int krbd_map(struct krbd_ctx *ctx, const char *pool,
                        const char *image, const char *snap,
                        const char *options, char **pdevnode)
{
  string name;
  char *devnode;
  int r;

  r = map_image(ctx, pool, image, snap, options, &name);
  if (r < 0)
    return r;

  devnode = strdup(name.c_str());
  if (!devnode)
    return -ENOMEM;

  *pdevnode = devnode;
  return r;
}

extern "C" int krbd_unmap(struct krbd_ctx *ctx, const char *devnode)
{
  return unmap_image(ctx, devnode);
}

int krbd_showmapped(struct krbd_ctx *ctx, Formatter *f)
{
  return dump_images(ctx, f);
}
