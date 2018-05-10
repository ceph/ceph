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

#include "aio.h"
#include "BlockDevice.h"
#include "KernelDevice.h"

class WriteCacheDevice : public KernelDevice {

  //KernelDevice block_data;
  KernelDevice* write_cache;
  std::mutex lock;

public:
  //WriteCacheDevice();
  WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  virtual ~WriteCacheDevice();
  void aio_submit(IOContext *ioc) override;
  //AK void discard_drain() override;

  //AK int collect_metadata(const std::string& prefix, map<std::string,std::string> *pm) const override;
  /*
  int get_devname(std::string *s) override {
    return block_data.get_devname(s);
  }
  */
  //AK int get_devices(std::set<std::string> *ls) override;

  //AK bool get_thin_utilization(uint64_t *total, uint64_t *avail) const override;

  /* AKint read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc,
	   bool buffered) override;
  int aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
	       IOContext *ioc) override;
  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;
*/
  int write(uint64_t off, bufferlist& bl, bool buffered) override;
  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc,
		bool buffered) override;
  int flush() override;
  //AK int discard(uint64_t offset, uint64_t len) override;

  // for managing buffered readers/writers
  //AK int invalidate_cache(uint64_t off, uint64_t len) override;
  //AK int open(const std::string& path) override;
  //AK void close() override;

  int open_write_cache(CephContext* cct, const std::string& path);
private:
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
  };

  static constexpr size_t block_size = 4096;
  uint64_t last_id;
  row* current;
  row* flushing;
  row* empty;
};

#endif
