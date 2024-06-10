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

#ifndef CEPH_BLK_KERNELDEVICE_H
#define CEPH_BLK_KERNELDEVICE_H

#include <atomic>

#include "include/types.h"
#include "include/interval_set.h"
#include "common/config_obs.h"
#include "common/Thread.h"
#include "include/utime.h"

#include "aio/aio.h"
#include "BlockDevice.h"
#include "extblkdev/ExtBlkDevPlugin.h"

#define RW_IO_MAX (INT_MAX & CEPH_PAGE_MASK)

class KernelDevice : public BlockDevice,
                     public md_config_obs_t {
protected:
  std::string path;
private:
  std::vector<int> fd_directs, fd_buffereds;
  bool enable_wrt = true;
  bool aio, dio;

  ExtBlkDevInterfaceRef ebd_impl;  // structure for retrieving compression state from extended block device

  std::string devname;  ///< kernel dev name (/sys/block/$devname), if any

  ceph::mutex debug_lock = ceph::make_mutex("KernelDevice::debug_lock");
  interval_set<uint64_t> debug_inflight;

  std::atomic<bool> io_since_flush = {false};
  ceph::mutex flush_mutex = ceph::make_mutex("KernelDevice::flush_mutex");

  std::unique_ptr<io_queue_t> io_queue;
  aio_callback_t discard_callback;
  void *discard_callback_priv;
  bool aio_stop;

  ceph::mutex discard_lock = ceph::make_mutex("KernelDevice::discard_lock");
  ceph::condition_variable discard_cond;
  int discard_running = 0;
  interval_set<uint64_t> discard_queued;

  struct AioCompletionThread : public Thread {
    KernelDevice *bdev;
    explicit AioCompletionThread(KernelDevice *b) : bdev(b) {}
    void *entry() override {
      bdev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  struct DiscardThread : public Thread {
    KernelDevice *bdev;
    const uint64_t id;
    bool stop = false;
    explicit DiscardThread(KernelDevice *b, uint64_t id) : bdev(b), id(id) {}
    void *entry() override {
      bdev->_discard_thread(id);
      return NULL;
    }
  };
  std::vector<std::shared_ptr<DiscardThread>> discard_threads;
  uint64_t target_discard_threads = 0;

  std::atomic_int injecting_crash;

  virtual int _post_open() { return 0; }  // hook for child implementations
  virtual void  _pre_close() { }  // hook for child implementations

  void _aio_thread();
  void _discard_thread(uint64_t tid);
  void _queue_discard(interval_set<uint64_t> &to_release);
  bool try_discard(interval_set<uint64_t> &to_release, bool async = true) override;

  int _aio_start();
  void _aio_stop();

  void _discard_start();
  void _discard_stop();
  bool _discard_started();

  void _aio_log_start(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_log_finish(IOContext *ioc, uint64_t offset, uint64_t length);

  int _sync_write(uint64_t off, ceph::buffer::list& bl, bool buffered, int write_hint);

  int _lock();

  int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

  // stalled aio debugging
  aio_list_t debug_queue;
  ceph::mutex debug_queue_lock = ceph::make_mutex("KernelDevice::debug_queue_lock");
  aio_t *debug_oldest = nullptr;
  utime_t debug_stall_since;
  void debug_aio_link(aio_t& aio);
  void debug_aio_unlink(aio_t& aio);

  int choose_fd(bool buffered, int write_hint) const;

  ceph::unique_leakable_ptr<buffer::raw> create_custom_aligned(size_t len, IOContext* ioc) const;

public:
  KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  ~KernelDevice();

  void aio_submit(IOContext *ioc) override;
  void discard_drain() override;
  void swap_discard_queued(interval_set<uint64_t>& other) override;
  int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) const override;
  int get_devname(std::string *s) const override {
    if (devname.empty()) {
      return -ENOENT;
    }
    *s = devname;
    return 0;
  }
  int get_devices(std::set<std::string> *ls) const override;

  int get_ebd_state(ExtBlkDevState &state) const override;

  int read(uint64_t off, uint64_t len, ceph::buffer::list *pbl,
	   IOContext *ioc,
	   bool buffered) override;
  int aio_read(uint64_t off, uint64_t len, ceph::buffer::list *pbl,
	       IOContext *ioc) override;
  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

  int write(uint64_t off, ceph::buffer::list& bl, bool buffered, int write_hint = WRITE_LIFE_NOT_SET) override;
  int aio_write(uint64_t off, ceph::buffer::list& bl,
		IOContext *ioc,
		bool buffered,
		int write_hint = WRITE_LIFE_NOT_SET) override;
  int flush() override;
  int _discard(uint64_t offset, uint64_t len);

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(const std::string& path) override;
  void close() override;

  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;
};

#endif
