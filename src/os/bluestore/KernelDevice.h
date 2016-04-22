// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_KERNELDEVICE_H
#define CEPH_OS_BLUESTORE_KERNELDEVICE_H

#include <atomic>

#include "os/fs/FS.h"
#include "include/interval_set.h"

#include "BlockDevice.h"

class KernelDevice : public BlockDevice {
  int fd_direct, fd_buffered;
  uint64_t size;
  uint64_t block_size;
  string path;
  FS *fs;
  bool aio, dio;
  bufferptr zeros;

  Mutex debug_lock;
  interval_set<uint64_t> debug_inflight;

  Mutex flush_lock;
  atomic_t io_since_flush;

  FS::aio_queue_t aio_queue;
  aio_callback_t aio_callback;
  void *aio_callback_priv;
  bool aio_stop;

  struct AioCompletionThread : public Thread {
    KernelDevice *bdev;
    explicit AioCompletionThread(KernelDevice *b) : bdev(b) {}
    void *entry() {
      bdev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  std::atomic_int injecting_crash;

  void _aio_thread();
  int _aio_start();
  void _aio_stop();

  void _aio_log_start(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_log_finish(IOContext *ioc, uint64_t offset, uint64_t length);

  int _lock();

public:
  KernelDevice(aio_callback_t cb, void *cbpriv);

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
  int read_buffered(uint64_t off, uint64_t len, char *buf) override;

  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc,
		bool buffered) override;
  int aio_zero(uint64_t off, uint64_t len,
	       IOContext *ioc) override;
  int flush() override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(string path) override;
  void close() override;

  string get_type() override;
};

#endif
