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
#include <optional>
#include <poll.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <tuple>
#include <unistd.h>
#include <utility>

#include "auth/KeyRing.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/module.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/secret.h"
#include "common/TextTable.h"
#include "common/Thread.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "include/krbd.h"
#include "mon/MonMap.h"

#include <blkid/blkid.h>
#include <libudev.h>

static const int UDEV_BUF_SIZE = 1 << 20;  /* doubled to 2M (SO_RCVBUFFORCE) */

struct krbd_ctx {
  CephContext *cct;
  struct udev *udev;
};

static const std::string SNAP_HEAD_NAME("-");

struct krbd_spec {
  std::string pool_name;
  std::string nspace_name;
  std::string image_name;
  std::string snap_name;

  krbd_spec(const char *pool_name, const char *nspace_name,
            const char *image_name, const char *snap_name)
      : pool_name(pool_name),
        nspace_name(nspace_name),
        image_name(image_name),
        snap_name(*snap_name ? snap_name : SNAP_HEAD_NAME) { }

  bool operator==(const krbd_spec& rhs) const {
    return pool_name == rhs.pool_name &&
           nspace_name == rhs.nspace_name &&
           image_name == rhs.image_name &&
           snap_name == rhs.snap_name;
  }
};

std::ostream& operator<<(std::ostream& os, const krbd_spec& spec) {
  os << spec.pool_name << "/";
  if (!spec.nspace_name.empty())
    os << spec.nspace_name << "/";
  os << spec.image_name;
  if (spec.snap_name != SNAP_HEAD_NAME)
    os << "@" << spec.snap_name;
  return os;
}

std::optional<krbd_spec> spec_from_dev(udev_device *dev) {
  const char *pool_name = udev_device_get_sysattr_value(dev, "pool");
  const char *nspace_name = udev_device_get_sysattr_value(dev, "pool_ns");
  const char *image_name = udev_device_get_sysattr_value(dev, "name");
  const char *snap_name = udev_device_get_sysattr_value(dev, "current_snap");

  if (!pool_name || !image_name || !snap_name)
    return std::nullopt;

  return std::make_optional<krbd_spec>(
      pool_name, nspace_name ?: "", image_name, snap_name);
}

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

static int have_minor_attr(void)
{
  /*
   * 'minor' attribute was added as part of single_major merge, which
   * exposed the 'single_major' parameter.  'minor' is always present,
   * regardless of whether single-major scheme is turned on or not.
   *
   * (Something like ver >= KERNEL_VERSION(3, 14, 0) is a no-go because
   * this has to work with rbd.ko backported to various kernels.)
   */
  return access("/sys/module/rbd/parameters/single_major", F_OK) == 0;
}

static int build_map_buf(CephContext *cct, const krbd_spec& spec,
                         const char *options, string *pbuf)
{
  ostringstream oss;
  int r;

  MonMap monmap;
  r = monmap.build_initial(cct, false, cerr);
  if (r < 0)
    return r;

  list<entity_addr_t> mon_addr;
  monmap.list_addrs(mon_addr);

  for (const auto &p : mon_addr) {
    if (oss.tellp() > 0) {
      oss << ",";
    }
    oss << p.get_sockaddr();
  }

  oss << " name=" << cct->_conf->name.get_id();

  KeyRing keyring;
  auto auth_client_required =
    cct->_conf.get_val<std::string>("auth_client_required");
  if (auth_client_required != "none") {
    r = keyring.from_ceph_context(cct);
    auto keyfile = cct->_conf.get_val<std::string>("keyfile");
    auto key = cct->_conf.get_val<std::string>("key");
    if (r == -ENOENT && keyfile.empty() && key.empty())
      r = 0;
    if (r < 0) {
      cerr << "rbd: failed to get secret" << std::endl;
      return r;
    }
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

  if (strcmp(options, "") != 0)
    oss << "," << options;
  if (!spec.nspace_name.empty())
    oss << ",_pool_ns=" << spec.nspace_name;

  oss << " " << spec.pool_name << " " << spec.image_name << " "
      << spec.snap_name;

  *pbuf = oss.str();
  return 0;
}

/*
 * Return:
 *   <kernel error, false> - didn't map
 *   <0 or udev error, true> - mapped
 */
template <typename F>
static std::pair<int, bool> wait_for_mapping(int sysfs_r_fd, udev_monitor *mon,
                                             F udev_device_handler)
{
  struct pollfd fds[2];
  int sysfs_r = INT_MAX, udev_r = INT_MAX;
  int r;

  fds[0].fd = sysfs_r_fd;
  fds[0].events = POLLIN;
  fds[1].fd = udev_monitor_get_fd(mon);
  fds[1].events = POLLIN;

  for (;;) {
    if (poll(fds, 2, -1) < 0) {
      ceph_abort_msgf("poll failed: %d", -errno);
    }

    if (fds[0].revents) {
      r = safe_read_exact(sysfs_r_fd, &sysfs_r, sizeof(sysfs_r));
      if (r < 0) {
        ceph_abort_msgf("safe_read_exact failed: %d", r);
      }
      if (sysfs_r < 0) {
        return std::make_pair(sysfs_r, false);
      }
      if (udev_r != INT_MAX) {
        ceph_assert(!sysfs_r);
        return std::make_pair(udev_r, true);
      }
      fds[0].fd = -1;
    }

    if (fds[1].revents) {
      for (;;) {
        struct udev_device *dev;

        dev = udev_monitor_receive_device(mon);
        if (!dev) {
          if (errno != EINTR && errno != EAGAIN) {
            udev_r = -errno;
            if (sysfs_r != INT_MAX) {
              ceph_assert(!sysfs_r);
              return std::make_pair(udev_r, true);
            }
            fds[1].fd = -1;
          }
          break;
        }
        if (udev_device_handler(dev)) {
          udev_r = 0;
          if (sysfs_r != INT_MAX) {
            ceph_assert(!sysfs_r);
            return std::make_pair(udev_r, true);
          }
          fds[1].fd = -1;
          break;
        }
      }
    }
  }
}

class UdevMapHandler {
public:
  UdevMapHandler(const krbd_spec *spec, std::string *pdevnode) :
      m_spec(spec), m_pdevnode(pdevnode) {}

  /*
   * Catch /sys/devices/rbd/<id>/ and wait for the corresponding
   * block device to show up.  This is necessary because rbd devices
   * and block devices aren't linked together in our sysfs layout.
   */
  bool operator()(udev_device *dev) {
    if (strcmp(udev_device_get_action(dev), "add")) {
      goto next;
    }
    if (!strcmp(udev_device_get_subsystem(dev), "rbd")) {
      if (!m_bus_dev) {
        auto spec = spec_from_dev(dev);
        if (spec && *spec == *m_spec) {
          m_bus_dev = dev;
          goto check;
        }
      }
    } else if (!strcmp(udev_device_get_subsystem(dev), "block")) {
      m_block_devs.push_back(dev);
      goto check;
    }

next:
    udev_device_unref(dev);
    return false;

check:
    if (m_bus_dev && !m_block_devs.empty()) {
      const char *major = udev_device_get_sysattr_value(m_bus_dev, "major");
      const char *minor = udev_device_get_sysattr_value(m_bus_dev, "minor");
      ceph_assert(!minor ^ have_minor_attr());

      for (auto p : m_block_devs) {
        const char *this_major = udev_device_get_property_value(p, "MAJOR");
        const char *this_minor = udev_device_get_property_value(p, "MINOR");

        if (strcmp(this_major, major) == 0 &&
            (!minor || strcmp(this_minor, minor) == 0)) {
          string name = get_kernel_rbd_name(udev_device_get_sysname(m_bus_dev));

          ceph_assert(strcmp(udev_device_get_devnode(p), name.c_str()) == 0);
          *m_pdevnode = name;
          return true;
        }
      }
    }
    return false;
  }

  ~UdevMapHandler() {
    if (m_bus_dev) {
      udev_device_unref(m_bus_dev);
    }

    for (auto p : m_block_devs) {
      udev_device_unref(p);
    }
  }

private:
  udev_device *m_bus_dev = nullptr;
  std::vector<udev_device *> m_block_devs;
  const krbd_spec *m_spec;
  std::string *m_pdevnode;
};

static int do_map(struct udev *udev, const krbd_spec& spec, const string& buf,
                  string *pname)
{
  struct udev_monitor *mon;
  std::thread mapper;
  bool mapped;
  int fds[2];
  int r;

  mon = udev_monitor_new_from_netlink(udev, "udev");
  if (!mon)
    return -ENOMEM;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "rbd", nullptr);
  if (r < 0)
    goto out_mon;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "block", "disk");
  if (r < 0)
    goto out_mon;

  r = udev_monitor_set_receive_buffer_size(mon, UDEV_BUF_SIZE);
  if (r < 0) {
    std::cerr << "rbd: failed to set udev buffer size: " << cpp_strerror(r)
              << std::endl;
    /* not fatal */
  }

  r = udev_monitor_enable_receiving(mon);
  if (r < 0)
    goto out_mon;

  if (pipe2(fds, O_NONBLOCK) < 0) {
    r = -errno;
    goto out_mon;
  }

  mapper = make_named_thread("mapper", [&buf, sysfs_r_fd = fds[1]]() {
    int sysfs_r = sysfs_write_rbd_add(buf);
    int r = safe_write(sysfs_r_fd, &sysfs_r, sizeof(sysfs_r));
    if (r < 0) {
      ceph_abort_msgf("safe_write failed: %d", r);
    }
  });

  std::tie(r, mapped) = wait_for_mapping(fds[0], mon,
                                         UdevMapHandler(&spec, pname));
  if (r < 0) {
    if (!mapped) {
      std::cerr << "rbd: sysfs write failed" << std::endl;
    } else {
      std::cerr << "rbd: udev wait failed" << std::endl;
      /* TODO: fall back to enumeration */
    }
  }

  mapper.join();
  close(fds[0]);
  close(fds[1]);

out_mon:
  udev_monitor_unref(mon);
  return r;
}

static int map_image(struct krbd_ctx *ctx, const krbd_spec& spec,
                     const char *options, string *pname)
{
  string buf;
  int r;

  r = build_map_buf(ctx->cct, spec, options, &buf);
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

  return do_map(ctx->udev, spec, buf, pname);
}

static int devno_to_krbd_id(struct udev *udev, dev_t devno, string *pid)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l;
  struct udev_device *dev;
  int r;

retry:
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

  if (have_minor_attr()) {
    r = udev_enumerate_add_match_sysattr(enm, "minor",
                                         stringify(minor(devno)).c_str());
    if (r < 0)
      goto out_enm;
  }

  r = udev_enumerate_scan_devices(enm);
  if (r < 0) {
    if (r == -ENOENT || r == -ENODEV) {
      std::cerr << "rbd: udev enumerate failed, retrying" << std::endl;
      udev_enumerate_unref(enm);
      goto retry;
    }
    goto out_enm;
  }

  l = udev_enumerate_get_list_entry(enm);
  if (!l) {
    r = -ENOENT;
    goto out_enm;
  }

  /* make sure there is only one match */
  ceph_assert(!udev_list_entry_get_next(l));

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

static int __enumerate_devices(struct udev *udev, const krbd_spec& spec,
                               bool match_nspace, struct udev_enumerate **penm)
{
  struct udev_enumerate *enm;
  int r;

retry:
  enm = udev_enumerate_new(udev);
  if (!enm)
    return -ENOMEM;

  r = udev_enumerate_add_match_subsystem(enm, "rbd");
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_add_match_sysattr(enm, "pool", spec.pool_name.c_str());
  if (r < 0)
    goto out_enm;

  if (match_nspace) {
    r = udev_enumerate_add_match_sysattr(enm, "pool_ns",
                                         spec.nspace_name.c_str());
  } else {
    /*
     * Match _only_ devices that don't have pool_ns attribute.
     * If the kernel supports namespaces, the result will be empty.
     */
    r = udev_enumerate_add_nomatch_sysattr(enm, "pool_ns", nullptr);
  }
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_add_match_sysattr(enm, "name", spec.image_name.c_str());
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_add_match_sysattr(enm, "current_snap",
                                       spec.snap_name.c_str());
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_scan_devices(enm);
  if (r < 0) {
    if (r == -ENOENT || r == -ENODEV) {
      std::cerr << "rbd: udev enumerate failed, retrying" << std::endl;
      udev_enumerate_unref(enm);
      goto retry;
    }
    goto out_enm;
  }

  *penm = enm;
  return 0;

out_enm:
  udev_enumerate_unref(enm);
  return r;
}

static int enumerate_devices(struct udev *udev, const krbd_spec& spec,
                             struct udev_enumerate **penm)
{
  struct udev_enumerate *enm;
  int r;

  r = __enumerate_devices(udev, spec, true, &enm);
  if (r < 0)
    return r;

  /*
   * If no namespace is set, try again with match_nspace=false to
   * handle older kernels.  On a newer kernel the result will remain
   * the same (i.e. empty).
   */
  if (!udev_enumerate_get_list_entry(enm) && spec.nspace_name.empty()) {
    udev_enumerate_unref(enm);
    r = __enumerate_devices(udev, spec, false, &enm);
    if (r < 0)
      return r;
  }

  *penm = enm;
  return 0;
}

static int spec_to_devno_and_krbd_id(struct udev *udev, const krbd_spec& spec,
                                     dev_t *pdevno, string *pid)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l;
  struct udev_device *dev;
  unsigned int maj, min = 0;
  string err;
  int r;

  r = enumerate_devices(udev, spec, &enm);
  if (r < 0)
    return r;

  l = udev_enumerate_get_list_entry(enm);
  if (!l) {
    r = -ENOENT;
    goto out_enm;
  }

  dev = udev_device_new_from_syspath(udev, udev_list_entry_get_name(l));
  if (!dev) {
    r = -ENOMEM;
    goto out_enm;
  }

  maj = strict_strtoll(udev_device_get_sysattr_value(dev, "major"), 10, &err);
  if (!err.empty()) {
    cerr << "rbd: couldn't parse major: " << err << std::endl;
    r = -EINVAL;
    goto out_dev;
  }
  if (have_minor_attr()) {
    min = strict_strtoll(udev_device_get_sysattr_value(dev, "minor"), 10, &err);
    if (!err.empty()) {
      cerr << "rbd: couldn't parse minor: " << err << std::endl;
      r = -EINVAL;
      goto out_dev;
    }
  }

  /*
   * If an image is mapped more than once don't bother trying to unmap
   * all devices - let users run unmap the same number of times they
   * ran map.
   */
  if (udev_list_entry_get_next(l))
    cerr << "rbd: " << spec << ": mapped more than once, unmapping "
         << get_kernel_rbd_name(udev_device_get_sysname(dev))
         << " only" << std::endl;

  *pdevno = makedev(maj, min);
  *pid = udev_device_get_sysname(dev);

out_dev:
  udev_device_unref(dev);
out_enm:
  udev_enumerate_unref(enm);
  return r;
}

static string build_unmap_buf(const string& id, const char *options)
{
  string buf(id);
  if (strcmp(options, "") != 0) {
    buf += " ";
    buf += options;
  }
  return buf;
}

class UdevUnmapHandler {
public:
  UdevUnmapHandler(dev_t devno) : m_devno(devno) {}

  bool operator()(udev_device *dev) {
    bool match = false;

    if (!strcmp(udev_device_get_action(dev), "remove") &&
        udev_device_get_devnum(dev) == m_devno) {
      match = true;
    }
    udev_device_unref(dev);
    return match;
  }

private:
  dev_t m_devno;
};

static int do_unmap(struct udev *udev, dev_t devno, const string& buf)
{
  struct udev_monitor *mon;
  std::thread unmapper;
  bool unmapped;
  int fds[2];
  int r;

  mon = udev_monitor_new_from_netlink(udev, "udev");
  if (!mon)
    return -ENOMEM;

  r = udev_monitor_filter_add_match_subsystem_devtype(mon, "block", "disk");
  if (r < 0)
    goto out_mon;

  r = udev_monitor_set_receive_buffer_size(mon, UDEV_BUF_SIZE);
  if (r < 0) {
    std::cerr << "rbd: failed to set udev buffer size: " << cpp_strerror(r)
              << std::endl;
    /* not fatal */
  }

  r = udev_monitor_enable_receiving(mon);
  if (r < 0)
    goto out_mon;

  if (pipe2(fds, O_NONBLOCK) < 0) {
    r = -errno;
    goto out_mon;
  }

  unmapper = make_named_thread("unmapper", [&buf, sysfs_r_fd = fds[1]]() {
    /*
     * On final device close(), kernel sends a block change event, in
     * response to which udev apparently runs blkid on the device.  This
     * makes unmap fail with EBUSY, if issued right after final close().
     * Try to circumvent this with a retry before turning to udev.
     */
    for (int tries = 0; ; tries++) {
      int sysfs_r = sysfs_write_rbd_remove(buf);
      if (sysfs_r == -EBUSY && tries < 2) {
        if (!tries) {
          usleep(250 * 1000);
        } else {
          /*
           * libudev does not provide the "wait until the queue is empty"
           * API or the sufficient amount of primitives to build it from.
           */
          std::string err = run_cmd("udevadm", "settle", "--timeout", "10",
                                    (char *)NULL);
          if (!err.empty())
            std::cerr << "rbd: " << err << std::endl;
        }
      } else {
        int r = safe_write(sysfs_r_fd, &sysfs_r, sizeof(sysfs_r));
        if (r < 0) {
          ceph_abort_msgf("safe_write failed: %d", r);
        }
        break;
      }
    }
  });

  std::tie(r, unmapped) = wait_for_mapping(fds[0], mon,
                                           UdevUnmapHandler(devno));
  if (r < 0) {
    if (!unmapped) {
      std::cerr << "rbd: sysfs write failed" << std::endl;
    } else {
      std::cerr << "rbd: udev wait failed: " << cpp_strerror(r) << std::endl;
      r = 0;
    }
  }

  unmapper.join();
  close(fds[0]);
  close(fds[1]);

out_mon:
  udev_monitor_unref(mon);
  return r;
}

static int unmap_image(struct krbd_ctx *ctx, const char *devnode,
                       const char *options)
{
  struct stat sb;
  dev_t wholedevno = 0;
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

  for (int tries = 0; ; tries++) {
    r = devno_to_krbd_id(ctx->udev, wholedevno, &id);
    if (r == -ENOENT && tries < 2) {
      usleep(250 * 1000);
    } else {
      if (r < 0) {
        if (r == -ENOENT) {
          std::cerr << "rbd: '" << devnode << "' is not an rbd device"
                    << std::endl;
          r = -EINVAL;
        }
        return r;
      }
      if (tries) {
        std::cerr << "rbd: udev enumerate missed a device, tries = " << tries
                  << std::endl;
      }
      break;
    }
  }

  return do_unmap(ctx->udev, wholedevno, build_unmap_buf(id, options));
}

static int unmap_image(struct krbd_ctx *ctx, const krbd_spec& spec,
                       const char *options)
{
  dev_t devno = 0;
  string id;
  int r;

  for (int tries = 0; ; tries++) {
    r = spec_to_devno_and_krbd_id(ctx->udev, spec, &devno, &id);
    if (r == -ENOENT && tries < 2) {
      usleep(250 * 1000);
    } else {
      if (r < 0) {
        if (r == -ENOENT) {
          std::cerr << "rbd: " << spec << ": not a mapped image or snapshot"
                    << std::endl;
          r = -EINVAL;
        }
        return r;
      }
      if (tries) {
        std::cerr << "rbd: udev enumerate missed a device, tries = " << tries
                  << std::endl;
      }
      break;
    }
  }

  return do_unmap(ctx->udev, devno, build_unmap_buf(id, options));
}

static bool dump_one_image(Formatter *f, TextTable *tbl,
                           struct udev_device *dev)
{
  const char *id = udev_device_get_sysname(dev);
  auto spec = spec_from_dev(dev);
  string kname = get_kernel_rbd_name(id);

  if (!spec)
    return false;

  if (f) {
    f->open_object_section("device");
    f->dump_string("id", id);
    f->dump_string("pool", spec->pool_name);
    f->dump_string("namespace", spec->nspace_name);
    f->dump_string("name", spec->image_name);
    f->dump_string("snap", spec->snap_name);
    f->dump_string("device", kname);
    f->close_section();
  } else {
    *tbl << id << spec->pool_name << spec->nspace_name << spec->image_name
         << spec->snap_name << kname << TextTable::endrow;
  }

  return true;
}

static int do_dump(struct udev *udev, Formatter *f, TextTable *tbl)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l = NULL;
  bool have_output = false;
  int r;

retry:
  enm = udev_enumerate_new(udev);
  if (!enm)
    return -ENOMEM;

  r = udev_enumerate_add_match_subsystem(enm, "rbd");
  if (r < 0)
    goto out_enm;

  r = udev_enumerate_scan_devices(enm);
  if (r < 0) {
    if (r == -ENOENT || r == -ENODEV) {
      std::cerr << "rbd: udev enumerate failed, retrying" << std::endl;
      udev_enumerate_unref(enm);
      goto retry;
    }
    goto out_enm;
  }

  udev_list_entry_foreach(l, udev_enumerate_get_list_entry(enm)) {
    struct udev_device *dev;

    dev = udev_device_new_from_syspath(udev, udev_list_entry_get_name(l));
    if (dev) {
      have_output |= dump_one_image(f, tbl, dev);
      udev_device_unref(dev);
    }
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
    f->open_array_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("namespace", TextTable::LEFT, TextTable::LEFT);
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

static int is_mapped_image(struct udev *udev, const krbd_spec& spec,
                           string *pname)
{
  struct udev_enumerate *enm;
  struct udev_list_entry *l;
  int r;

  r = enumerate_devices(udev, spec, &enm);
  if (r < 0)
    return r;

  l = udev_enumerate_get_list_entry(enm);
  if (l) {
    struct udev_device *dev;

    dev = udev_device_new_from_syspath(udev, udev_list_entry_get_name(l));
    if (!dev) {
      r = -ENOMEM;
      goto out_enm;
    }

    r = 1;
    *pname = get_kernel_rbd_name(udev_device_get_sysname(dev));
    udev_device_unref(dev);
  } else {
    r = 0;  /* not mapped */
  }

out_enm:
  udev_enumerate_unref(enm);
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

extern "C" int krbd_map(struct krbd_ctx *ctx,
                        const char *pool_name,
                        const char *nspace_name,
                        const char *image_name,
                        const char *snap_name,
                        const char *options,
                        char **pdevnode)
{
  krbd_spec spec(pool_name, nspace_name, image_name, snap_name);
  string name;
  char *devnode;
  int r;

  r = map_image(ctx, spec, options, &name);
  if (r < 0)
    return r;

  devnode = strdup(name.c_str());
  if (!devnode)
    return -ENOMEM;

  *pdevnode = devnode;
  return r;
}

extern "C" int krbd_unmap(struct krbd_ctx *ctx, const char *devnode,
                          const char *options)
{
  return unmap_image(ctx, devnode, options);
}

extern "C" int krbd_unmap_by_spec(struct krbd_ctx *ctx,
                                  const char *pool_name,
                                  const char *nspace_name,
                                  const char *image_name,
                                  const char *snap_name,
                                  const char *options)
{
  krbd_spec spec(pool_name, nspace_name, image_name, snap_name);
  return unmap_image(ctx, spec, options);
}

int krbd_showmapped(struct krbd_ctx *ctx, Formatter *f)
{
  return dump_images(ctx, f);
}

extern "C" int krbd_is_mapped(struct krbd_ctx *ctx,
                              const char *pool_name,
                              const char *nspace_name,
                              const char *image_name,
                              const char *snap_name,
                              char **pdevnode)
{
  krbd_spec spec(pool_name, nspace_name, image_name, snap_name);
  string name;
  char *devnode;
  int r;

  r = is_mapped_image(ctx->udev, spec, &name);
  if (r <= 0)  /* error or not mapped */
    return r;

  devnode = strdup(name.c_str());
  if (!devnode)
    return -ENOMEM;

  *pdevnode = devnode;
  return r;
}
