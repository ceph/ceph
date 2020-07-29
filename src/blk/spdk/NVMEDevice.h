// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BLK_NVMEDEVICE
#define CEPH_BLK_NVMEDEVICE

#include <queue>
#include <map>
#include <limits>

// since _Static_assert introduced in c11
#define _Static_assert static_assert


#include "include/interval_set.h"
#include "common/ceph_time.h"
#include "BlockDevice.h"

enum class IOCommand {
  READ_COMMAND,
  WRITE_COMMAND,
  FLUSH_COMMAND
};

class SharedDriverData;
class SharedDriverQueueData;

class NVMEDevice : public BlockDevice {
  /**
   * points to pinned, physically contiguous memory region;
   * contains 4KB IDENTIFY structure for controller which is
   * target for CONTROLLER IDENTIFY command during initialization
   */
  SharedDriverData *driver;
  string name;

 public:
  std::atomic_int queue_number = {0};
  SharedDriverData *get_driver() { return driver; }

  NVMEDevice(CephContext* cct, aio_callback_t cb, void *cbpriv);

  bool supported_bdev_label() override { return false; }

  static bool support(const std::string& path);

  void aio_submit(IOContext *ioc) override;

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
           IOContext *ioc,
           bool buffered) override;
  int aio_read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc) override;
  int aio_write(uint64_t off, bufferlist& bl,
                IOContext *ioc,
                bool buffered,
		int write_hint = WRITE_LIFE_NOT_SET) override;
  int write(uint64_t off, bufferlist& bl, bool buffered, int write_hint = WRITE_LIFE_NOT_SET) override;
  int flush() override;
  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(const string& path) override;
  void close() override;
  int collect_metadata(const string& prefix, map<string,string> *pm) const override;
};

#endif
