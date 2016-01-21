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

#ifndef CEPH_OS_BLUESTORE_BLOCKDEVICE_H
#define CEPH_OS_BLUESTORE_BLOCKDEVICE_H

#include "acconfig.h"
#include "os/fs/FS.h"

#define SPDK_PREFIX "spdk:"

/// track in-flight io
struct IOContext {
  void *priv;
#ifdef HAVE_SPDK
  void *nvme_task_first = nullptr;
  void *nvme_task_last = nullptr;
#endif

  Mutex lock;
  Cond cond;
  //interval_set<uint64_t> blocks;  ///< blocks with aio in flight

  list<FS::aio_t> pending_aios;    ///< not yet submitted
  list<FS::aio_t> running_aios;    ///< submitting or submitted
  atomic_t num_pending;
  atomic_t num_running;
  atomic_t num_reading;
  atomic_t num_waiting;

  IOContext(void *p)
    : priv(p),
      lock("IOContext::lock")
    {}

  // no copying
  IOContext(const IOContext& other);
  IOContext &operator=(const IOContext& other);

  bool has_aios() {
    Mutex::Locker l(lock);
    return num_pending.read() + num_running.read();
  }

  void aio_wait();
};


class BlockDevice {
  Mutex ioc_reap_lock;
  vector<IOContext*> ioc_reap_queue;
  atomic_t ioc_reap_count;

public:
  BlockDevice(): ioc_reap_lock("BlockDevice::ioc_reap_lock") {}
  virtual ~BlockDevice() {}
  typedef void (*aio_callback_t)(void *handle, void *aio);

  static BlockDevice *create(
      const string& path, aio_callback_t cb, void *cbpriv);
  virtual bool supported_bdev_label() { return true; }

  virtual void aio_submit(IOContext *ioc) = 0;

  virtual uint64_t get_size() const = 0;
  virtual uint64_t get_block_size() const = 0;

  virtual int read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc, bool buffered) = 0;
  virtual int read_buffered(uint64_t off, uint64_t len, char *buf) = 0;

  virtual int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc, bool buffered) = 0;
  virtual int aio_zero(uint64_t off, uint64_t len, IOContext *ioc) = 0;
  virtual int flush() = 0;

  void queue_reap_ioc(IOContext *ioc);
  void reap_ioc();

  // for managing buffered readers/writers
  virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
  virtual int open(string path) = 0;
  virtual void close() = 0;
};

#endif //CEPH_OS_BLUESTORE_BLOCKDEVICE_H
