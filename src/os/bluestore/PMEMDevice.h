// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *  Copyright (C) 2015 Intel <jianpeng.ma@intel.com>
 *
 * Author: Jianpeng Ma <jianpeng.ma@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_PMEMDEVICE_H
#define CEPH_OS_BLUESTORE_PMEMDEVICE_H

#include <atomic>

#include "os/fs/FS.h"
#include "os/fs/aio.h"
#include "include/interval_set.h"
#include "BlockDevice.h"

class PMEMDevice : public BlockDevice {
  int fd;
  char *addr; //the address of mmap
  uint64_t size;
  uint64_t block_size;
  std::string path;

  Mutex debug_lock;
  interval_set<uint64_t> debug_inflight;

  std::atomic_int injecting_crash;
  int _lock();

public:
  PMEMDevice(CephContext *cct, aio_callback_t cb, void *cbpriv);


  void aio_submit(IOContext *ioc) override;

  uint64_t get_size() const override {
    return size;
  }
  uint64_t get_block_size() const override {
    return block_size;
  }

  int collect_metadata(std::string prefix, map<std::string,std::string> *pm) const override;

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc,
	   bool buffered) override;
  int aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
	       IOContext *ioc) override;

  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;
  int write(uint64_t off, bufferlist& bl, bool buffered) override;
  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc,
		bool buffered) override;
  int flush() override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(const std::string &path) override;
  void close() override;
};

#endif
