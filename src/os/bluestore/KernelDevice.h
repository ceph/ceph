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

#include <boost/container/static_vector.hpp>

#include "os/fs/FS.h"
#include "include/interval_set.h"

#include "aio.h"
#include "BlockDevice.h"

class KernelDevice : public BlockDevice {
  friend class aio_service_t;

  int fd_direct, fd_buffered;
  std::string path;
  FS *fs;
  bool aio, dio;

  std::string devname;  ///< kernel dev name (/sys/block/$devname), if any

  Mutex debug_lock;
  interval_set<uint64_t> debug_inflight;

  std::atomic<bool> io_since_flush = {false};
  std::mutex flush_mutex;
  std::atomic_int injecting_crash;

  struct aio_service_t {
    aio_queue_t aio_queue;
    std::unique_ptr<Thread> aio_thread;
    bool aio_stop = false;

    explicit aio_service_t(CephContext* cct,
                           KernelDevice* const bdev)
      : aio_queue(cct->_conf->bdev_aio_max_queue_depth),
        aio_thread(make_lambda_thread([bdev, this]() {
          // NOTE: the physical location of the object SHALL NOT change!
          // Othwerwise we'll end with bstore_aios  interpreting random
          // junks as their qio_queue or the stop signal.
          bdev->_aio_thread(this->aio_queue, this->aio_stop);
        })) {
    }

    int start(CephContext* cct);
    void stop(CephContext* cct);
  };
  aio_service_t aio_main_service;
  static constexpr size_t expected_max_shard_num = 16;
  boost::container::static_vector<
    aio_service_t,
    expected_max_shard_num> aio_rdonly_services;

  void _aio_thread(aio_queue_t& aio_queue, const bool& aio_stop);
  int _aio_start();
  void _aio_stop();
  void _aio_log_start(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_log_finish(IOContext *ioc, uint64_t offset, uint64_t length);

  int _sync_write(uint64_t off, bufferlist& bl, bool buffered);

  int _lock();

  int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

  // stalled aio debugging
  aio_list_t debug_queue;
  std::mutex debug_queue_lock;
  aio_t *debug_oldest = nullptr;
  utime_t debug_stall_since;
  void debug_aio_link(aio_t& aio);
  void debug_aio_unlink(aio_t& aio);

public:
  KernelDevice(CephContext* cct,
               aio_callback_t cb,
               void *cbpriv,
               size_t max_shard_num);

  void aio_submit(IOContext *ioc) override;
  void aio_submit(RDOnlyIOContext *ioc) override;

  int collect_metadata(const std::string& prefix, map<std::string,std::string> *pm) const override;
  int get_devname(std::string *s) override {
    if (devname.empty()) {
      return -ENOENT;
    }
    *s = devname;
    return 0;
  }
  int get_devices(std::set<std::string> *ls) override;

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
  int open(const std::string& path) override;
  void close() override;
};

#endif
