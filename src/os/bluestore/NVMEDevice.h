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

#include "include/interval_set.h"

#include "BlockDevice.h"

class NVMEDevice : public BlockDevice {
  typedef void (*aio_callback_t)(void *handle, void *aio);

 private:
  /**
   * points to pinned, physically contiguous memory region;
   * contains 4KB IDENTIFY structure for controller which is
   *  target for CONTROLLER IDENTIFY command during initialization
   */
  nvme_controller *ctrlr;
  nvme_namespace	*ns;
	uint64_t blocklen;
  int unbindfromkernel = 0;

  uint64_t size;
  uint64_t block_size;

  aio_callback_t aio_callback;
  void *aio_callback_priv;
  bool aio_stop;

  struct AioCompletionThread : public Thread {
    NVMEDevice *bdev;
    AioCompletionThread(NVMEDevice *b) : bdev(b) {}
    void *entry() {
      bdev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  void _aio_thread();
  int _aio_start();
  void _aio_stop();

 public:
  NVMEDevice(aio_callback_t cb, void *cbpriv);

  void aio_submit(IOContext *ioc) {}

  uint64_t get_size() const {
    return size;
  }
  uint64_t get_block_size() const {
    return block_size;
  }

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
           IOContext *ioc,
           bool buffered);

  int aio_write(uint64_t off, bufferlist& bl,
                IOContext *ioc,
                bool buffered);
  int aio_zero(uint64_t off, uint64_t len,
               IOContext *ioc);
  int flush();

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len);
  int open(string path);
  void close();
};

#endif
