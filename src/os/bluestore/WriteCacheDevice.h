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

  unique_ptr<KernelDevice> write_cache;
  std::mutex lock;
  std::string path;

public:
  WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  virtual ~WriteCacheDevice();

  void aio_submit(IOContext *ioc) override;
  int write(uint64_t off, bufferlist& bl, bool buffered) override;
  int aio_write(uint64_t off, bufferlist& bl, IOContext *ioc, bool buffered) override;
  int flush() override;
  int open_write_cache(CephContext* cct, const std::string& path);
private:
  struct header_t {
    uint64_t fixed_begin;
    uint64_t id;
    uint64_t id_reversed;
    size_t   size;
    size_t   size_reversed;
    uint64_t dest;
    uint64_t dest_reversed;
    uint64_t fixed_end;
    void setup();
    bool check();
  };

  struct row_t {
    size_t disk_offset;
    size_t size;
    size_t pos;
    bool unflushed;
  };

  int flush_main();
  bool store_in_cache(uint64_t disk_off, bufferlist& bl);
  bool replay(size_t row_size);
  bool peek(row_t& r, uint64_t& id);
  bool replay_row(row_t& r, uint64_t& last_id);
  bool read_header(const row_t& r, header_t& h);


  static uint64_t next_id(uint64_t);
  static constexpr size_t block_size = 4096;
  static constexpr size_t minimum_cache_size = 2 * 1024 * 1024;
  uint64_t last_used_id;
  row_t* current;
  row_t* empty;
  row_t* flushing;
};

#endif
