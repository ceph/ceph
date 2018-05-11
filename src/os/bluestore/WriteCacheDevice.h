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

#ifndef CEPH_OS_BLUESTORE_WRITECACHEDEVICE_H
#define CEPH_OS_BLUESTORE_WRITECACHEDEVICE_H

#include <atomic>

#include "include/types.h"
#include "include/interval_set.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include <memory>
#include "aio.h"
#include "BlockDevice.h"
#include "KernelDevice.h"

class WriteCacheDevice : public KernelDevice {

  //KernelDevice block_data;
  unique_ptr<KernelDevice> write_cache;
  std::mutex lock;

public:
  WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  virtual ~WriteCacheDevice();

  void aio_submit(IOContext *ioc) override;
  int write(uint64_t off, bufferlist& bl, bool buffered) override;
  int aio_write(uint64_t off, bufferlist& bl, IOContext *ioc, bool buffered) override;
  int flush() override;
  int open_write_cache(CephContext* cct, const std::string& path);
private:
  int flush_main();
  bool store_in_cache(uint64_t disk_off, bufferlist& bl);

  struct header {
    uint64_t id;
    size_t size;
    uint64_t dest;
  };

  struct row {
    size_t disk_offset;
    size_t size;
    size_t pos;
    bool unflushed;
  };

  static constexpr size_t block_size = 4096;
  static constexpr size_t minimum_cache_size = 2 * 1024 * 1024;
  uint64_t last_id;
  row* current;
  row* flushing;
  row* empty;
};

#endif
