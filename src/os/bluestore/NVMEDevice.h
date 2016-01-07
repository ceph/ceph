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

#ifndef CEPH_OS_BLUESTORE_NVMEDEVICE
#define CEPH_OS_BLUESTORE_NVMEDEVICE

#include <queue>
#include <pciaccess.h>

// since _Static_assert introduced in c11
#define _Static_assert static_assert

#ifdef __cplusplus
extern "C" {
#endif

#include "spdk/pci.h"
#include "spdk/nvme.h"

#ifdef __cplusplus
}
#endif

#include "include/atomic.h"
#include "include/utime.h"
#include "common/Mutex.h"
#include "BlockDevice.h"

enum class IOCommand {
  READ_COMMAND,
  WRITE_COMMAND,
  FLUSH_COMMAND
};

class NVMEDevice;

struct Task {
  NVMEDevice *device;
  IOContext *ctx;
  IOCommand command;
  uint64_t offset, len;
  void *buf;
  Task *next, *prev;
  int64_t return_code;
  utime_t start;
};

class PerfCounters;

class NVMEDevice : public BlockDevice {
  /**
   * points to pinned, physically contiguous memory region;
   * contains 4KB IDENTIFY structure for controller which is
   *  target for CONTROLLER IDENTIFY command during initialization
   */
  nvme_controller *ctrlr;
  nvme_namespace	*ns;
  string name;

  uint64_t size;
  uint64_t block_size;

  bool aio_stop;
  bufferptr zeros;

  atomic_t queue_empty;
  Mutex queue_lock;
  Cond queue_cond;
  std::queue<Task*> task_queue;

  struct AioCompletionThread : public Thread {
    NVMEDevice *dev;
    AioCompletionThread(NVMEDevice *b) : dev(b) {}
    void *entry() {
      dev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  void _aio_thread();

  static void init();

 public:
  PerfCounters *logger;
  atomic_t inflight_ops;
  aio_callback_t aio_callback;
  void *aio_callback_priv;

  NVMEDevice(aio_callback_t cb, void *cbpriv);

  void aio_submit(IOContext *ioc) override;

  uint64_t get_size() const override {
    return size;
  }
  uint64_t get_block_size() const override {
    return block_size;
  }

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
           IOContext *ioc,
           bool buffered) override;

  int aio_write(uint64_t off, bufferlist& bl,
                IOContext *ioc,
                bool buffered) override ;
  int aio_zero(uint64_t off, uint64_t len,
               IOContext *ioc) override;
  int flush() override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(string path) override;
  void close() override;
};

#endif
