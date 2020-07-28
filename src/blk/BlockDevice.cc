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

#include <libgen.h>
#include <unistd.h>

#include "BlockDevice.h"

#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
#include "kernel/KernelDevice.h"
#endif

#if defined(HAVE_SPDK)
#include "spdk/NVMEDevice.h"
#endif

#if defined(HAVE_BLUESTORE_PMEM)
#include "pmem/PMEMDevice.h"
#endif

#if defined(HAVE_LIBZBC)
#include "zoned/HMSMRDevice.h"
#endif

#include "common/debug.h"
#include "common/EventTrace.h"
#include "common/errno.h"
#include "include/compat.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

using std::string;

void IOContext::aio_wait()
{
  std::unique_lock l(lock);
  // see _aio_thread for waker logic
  while (num_running.load() > 0) {
    dout(10) << __func__ << " " << this
	     << " waiting for " << num_running.load() << " aios to complete"
	     << dendl;
    cond.wait(l);
  }
  dout(20) << __func__ << " " << this << " done" << dendl;
}

uint64_t IOContext::get_num_ios() const
{
  // this is about the simplest model for transaction cost you can
  // imagine.  there is some fixed overhead cost by saying there is a
  // minimum of one "io".  and then we have some cost per "io" that is
  // a configurable (with different hdd and ssd defaults), and add
  // that to the bytes value.
  uint64_t ios = 0;
#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
  ios += pending_aios.size();
#endif
#ifdef HAVE_SPDK
  ios += total_nseg;
#endif
  return ios;
}

void IOContext::release_running_aios()
{
  ceph_assert(!num_running);
#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
  // release aio contexts (including pinned buffers).
  running_aios.clear();
#endif
}

BlockDevice::block_device_t
BlockDevice::detect_device_type(const std::string& path)
{
#if defined(HAVE_SPDK)
  if (NVMEDevice::support(path)) {
    return block_device_t::spdk;
  }
#endif
#if defined(HAVE_BLUESTORE_PMEM)
  if (PMEMDevice::support(path)) {
    return block_device_t::pmem;
  }
#endif
#if (defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)) && defined(HAVE_LIZBC)
  if (HMSMRDevice::support(path)) {
    return block_device_t::hm_smr;
  }
#endif

  return block_device_t::aio;
}

BlockDevice::block_device_t
BlockDevice::device_type_from_name(const std::string& blk_dev_name)
{
#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
  if (blk_dev_name == "aio") {
    return block_device_t::aio;
  }
#endif
#if defined(HAVE_SPDK)
  if (blk_dev_name == "spdk") {
    return block_device_t::spdk;
  }
#endif
#if defined(HAVE_BLUESTORE_PMEM)
  if (blk_dev_name == "pmem") {
    return block_device_t::pmem;
  }
#endif
#if (defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)) && defined(HAVE_LIZBC)
  if (blk_dev_name == "hm_smr") {
    return block_device_t::hm_smr;
  }
#endif
  return block_device_t::unknown;
}

BlockDevice* BlockDevice::create_with_type(block_device_t device_type,
  CephContext* cct, const std::string& path, aio_callback_t cb,
  void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
{

  switch (device_type) {
#if defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)
  case block_device_t::aio:
    return new KernelDevice(cct, cb, cbpriv, d_cb, d_cbpriv);
#endif
#if defined(HAVE_SPDK)
  case block_device_t::spdk:
    return new NVMEDevice(cct, cb, cbpriv);
#endif
#if defined(HAVE_BLUESTORE_PMEM)
  case block_device_t::pmem:
    return new PMEMDevice(cct, cb, cbpriv);
#endif
#if (defined(HAVE_LIBAIO) || defined(HAVE_POSIXAIO)) && defined(HAVE_LIZBC)
  case block_device_t::hm_smr:
    return new HMSMRDevice(cct, cb, cbpriv, d_cb, d_cbpriv);
#endif
  default:
    ceph_abort_msg("unsupported device");
    return nullptr;
  }
}

BlockDevice *BlockDevice::create(
    CephContext* cct, const string& path, aio_callback_t cb,
    void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
{
  const string blk_dev_name = cct->_conf.get_val<string>("bdev_type");
  block_device_t device_type = block_device_t::unknown;
  if (blk_dev_name.empty()) {
    device_type = detect_device_type(path);
  } else {
    device_type = device_type_from_name(blk_dev_name);
  }
  return create_with_type(device_type, cct, path, cb, cbpriv, d_cb, d_cbpriv);
}

void BlockDevice::queue_reap_ioc(IOContext *ioc)
{
  std::lock_guard l(ioc_reap_lock);
  if (ioc_reap_count.load() == 0)
    ++ioc_reap_count;
  ioc_reap_queue.push_back(ioc);
}

void BlockDevice::reap_ioc()
{
  if (ioc_reap_count.load()) {
    std::lock_guard l(ioc_reap_lock);
    for (auto p : ioc_reap_queue) {
      dout(20) << __func__ << " reap ioc " << p << dendl;
      delete p;
    }
    ioc_reap_queue.clear();
    --ioc_reap_count;
  }
}
