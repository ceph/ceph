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

#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "acconfig.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
#include "ceph_aio.h"
#endif
#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/interval_set.h"
#define SPDK_PREFIX "spdk:"

#if defined(__linux__)
#if !defined(F_SET_FILE_RW_HINT)
#define F_LINUX_SPECIFIC_BASE 1024
#define F_SET_FILE_RW_HINT         (F_LINUX_SPECIFIC_BASE + 14)
#endif
// These values match Linux definition
// https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
#define  WRITE_LIFE_NOT_SET  	0 	// No hint information set
#define  WRITE_LIFE_NONE  	1       // No hints about write life time
#define  WRITE_LIFE_SHORT  	2       // Data written has a short life time
#define  WRITE_LIFE_MEDIUM  	3    	// Data written has a medium life time
#define  WRITE_LIFE_LONG  	4       // Data written has a long life time
#define  WRITE_LIFE_EXTREME  	5     	// Data written has an extremely long life time
#define  WRITE_LIFE_MAX  	6
#else
// On systems don't have WRITE_LIFE_* only use one FD 
// And all files are created equal
#define  WRITE_LIFE_NOT_SET  	0 	// No hint information set
#define  WRITE_LIFE_NONE  	0       // No hints about write life time
#define  WRITE_LIFE_SHORT  	0       // Data written has a short life time
#define  WRITE_LIFE_MEDIUM  	0    	// Data written has a medium life time
#define  WRITE_LIFE_LONG  	0       // Data written has a long life time
#define  WRITE_LIFE_EXTREME  	0    	// Data written has an extremely long life time
#define  WRITE_LIFE_MAX  	1
#endif


/// track in-flight io
struct IOContext {
private:
  ceph::mutex lock = ceph::make_mutex("IOContext::lock");
  ceph::condition_variable cond;
  int r = 0;

public:
  CephContext* cct;
  void *priv;
#ifdef HAVE_SPDK
  void *nvme_task_first = nullptr;
  void *nvme_task_last = nullptr;
  std::atomic_int total_nseg = {0};
#endif

#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
  std::list<aio_t> pending_aios;    ///< not yet submitted
  std::list<aio_t> running_aios;    ///< submitting or submitted
#endif
  std::atomic_int num_pending = {0};
  std::atomic_int num_running = {0};
  bool allow_eio;

  explicit IOContext(CephContext* cct, void *p, bool allow_eio = false)
    : cct(cct), priv(p), allow_eio(allow_eio)
    {}

  // no copying
  IOContext(const IOContext& other) = delete;
  IOContext &operator=(const IOContext& other) = delete;

  bool has_pending_aios() {
    return num_pending.load();
  }
  void release_running_aios();
  void aio_wait();
  uint64_t get_num_ios() const;

  void try_aio_wake() {
    assert(num_running >= 1);

    std::lock_guard l(lock);
    if (num_running.fetch_sub(1) == 1) {

      // we might have some pending IOs submitted after the check
      // as there is no lock protection for aio_submit.
      // Hence we might have false conditional trigger.
      // aio_wait has to handle that hence do not care here.
      cond.notify_all();
    }
  }

  void set_return_value(int _r) {
    r = _r;
  }

  int get_return_value() const {
    return r;
  }
};


class BlockDevice {
public:
  CephContext* cct;
  typedef void (*aio_callback_t)(void *handle, void *aio);
private:
  ceph::mutex ioc_reap_lock = ceph::make_mutex("BlockDevice::ioc_reap_lock");
  std::vector<IOContext*> ioc_reap_queue;
  std::atomic_int ioc_reap_count = {0};

protected:
  uint64_t size = 0;
  uint64_t block_size = 0;
  bool support_discard = false;
  bool rotational = true;
  bool lock_exclusive = true;

  // HM-SMR specific properties.  In HM-SMR drives the LBA space is divided into
  // fixed-size zones.  Typically, the first few zones are randomly writable;
  // they form a conventional region of the drive.  The remaining zones must be
  // written sequentially and they must be reset before rewritten.  For example,
  // a 14 TB HGST HSH721414AL drive has 52156 zones each of size is 256 MiB.
  // The zones 0-523 are randomly writable and they form the conventional region
  // of the drive.  The zones 524-52155 are sequential zones.
  uint64_t conventional_region_size = 0;
  uint64_t zone_size = 0;

public:
  aio_callback_t aio_callback;
  void *aio_callback_priv;
  BlockDevice(CephContext* cct, aio_callback_t cb, void *cbpriv)
  : cct(cct),
    aio_callback(cb),
    aio_callback_priv(cbpriv)
 {}
  virtual ~BlockDevice() = default;

  static BlockDevice *create(
    CephContext* cct, const std::string& path, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  virtual bool supported_bdev_label() { return true; }
  virtual bool is_rotational() { return rotational; }

  // HM-SMR-specific calls
  virtual bool is_smr() const { return false; }
  virtual uint64_t get_zone_size() const {
    ceph_assert(is_smr());
    return zone_size;
  }
  virtual uint64_t get_conventional_region_size() const {
    ceph_assert(is_smr());
    return conventional_region_size;
  }

  virtual void aio_submit(IOContext *ioc) = 0;

  void set_no_exclusive_lock() {
    lock_exclusive = false;
  }
  
  uint64_t get_size() const { return size; }
  uint64_t get_block_size() const { return block_size; }

  /// hook to provide utilization of thinly-provisioned device
  virtual bool get_thin_utilization(uint64_t *total, uint64_t *avail) const {
    return false;
  }

  virtual int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) const = 0;

  virtual int get_devname(std::string *out) const {
    return -ENOENT;
  }
  virtual int get_devices(std::set<std::string> *ls) const {
    std::string s;
    if (get_devname(&s) == 0) {
      ls->insert(s);
    }
    return 0;
  }
  virtual int get_numa_node(int *node) const {
    return -EOPNOTSUPP;
  }

  virtual int read(
    uint64_t off,
    uint64_t len,
    ceph::buffer::list *pbl,
    IOContext *ioc,
    bool buffered) = 0;
  virtual int read_random(
    uint64_t off,
    uint64_t len,
    char *buf,
    bool buffered) = 0;
  virtual int write(
    uint64_t off,
    ceph::buffer::list& bl,
    bool buffered,
    int write_hint = WRITE_LIFE_NOT_SET) = 0;

  virtual int aio_read(
    uint64_t off,
    uint64_t len,
    ceph::buffer::list *pbl,
    IOContext *ioc) = 0;
  virtual int aio_write(
    uint64_t off,
    ceph::buffer::list& bl,
    IOContext *ioc,
    bool buffered,
    int write_hint = WRITE_LIFE_NOT_SET) = 0;
  virtual int flush() = 0;
  virtual int discard(uint64_t offset, uint64_t len) { return 0; }
  virtual int queue_discard(interval_set<uint64_t> &to_release) { return -1; }
  virtual void discard_drain() { return; }

  void queue_reap_ioc(IOContext *ioc);
  void reap_ioc();

  // for managing buffered readers/writers
  virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
  virtual int open(const std::string& path) = 0;
  virtual void close() = 0;

protected:
  bool is_valid_io(uint64_t off, uint64_t len) const {
    return (off % block_size == 0 &&
            len % block_size == 0 &&
            len > 0 &&
            off < size &&
            off + len <= size);
  }
};

#endif //CEPH_OS_BLUESTORE_BLOCKDEVICE_H
