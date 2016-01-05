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

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "include/types.h"
#include "include/compat.h"
#include "common/errno.h"
#include "common/debug.h"

#include "NVMEDevice.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

void IOContext::aio_wait()
{
  Mutex::Locker l(lock);
  // see _aio_thread for waker logic
  num_waiting.inc();
  while (num_running.read() > 0 || num_reading.read() > 0) {
    dout(10) << __func__ << " " << this
    << " waiting for " << num_running.read() << " aios and/or "
    << num_reading.read() << " readers to complete" << dendl;
    cond.Wait(lock);
  }
  num_waiting.dec();
  dout(20) << __func__ << " " << this << " done" << dendl;
}

static void io_complete(void *ctx, const struct nvme_completion *completion) {
  if (nvme_completion_is_error(completion)) {
    assert(0);
  }

  IOContext *ioc = (IOContext*)ctx;
  NVMEDevice *device = (NVMEDevice*)ioc->backend;
  if (ioc->priv) {
    device->aio_callback(device->aio_callback_priv, ioc->priv);
  }
}

// ----------------
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << name << ") "

NVMEDevice::NVMEDevice(aio_callback_t cb, void *cbpriv)
    : ctrlr(nullptr),
      ns(nullptr),
      aio_stop(false),
      aio_thread(this),
      aio_callback(cb),
      aio_callback_priv(cbpriv)
{
  zeros = buffer::create_page_aligned(1048576);
  zeros.zero();
}

int NVMEDevice::open(string p)
{
  int r = 0;
  dout(1) << __func__ << " path " << p << dendl;

  pci_device *pci_dev;

  pci_system_init();

  // Search for matching devices
  pci_id_match match;
  match.vendor_id = PCI_MATCH_ANY;
  match.subvendor_id = PCI_MATCH_ANY;
  match.subdevice_id = PCI_MATCH_ANY;
  match.device_id = PCI_MATCH_ANY;
  match.device_class = NVME_CLASS_CODE;
  match.device_class_mask = 0xFFFFFF;

  pci_device_iterator *iter = pci_id_match_iterator_create(&match);

  nvme_retry_count = g_conf->bdev_nvme_retry_count;
  if (nvme_retry_count < 0)
    nvme_retry_count = NVME_DEFAULT_RETRY_COUNT;

  string sn_tag = g_conf->bdev_nvme_serial_number;
  if (sn_tag.empty()) {
    int r = -ENOENT;
    derr << __func__ << " empty serial number: " << cpp_strerror(r) << dendl;
    return r;
  }

  unbindfromkernel = g_conf->bdev_nvme_unbind_from_kernel;

  char serial_number[128];
  while ((pci_dev = pci_device_next(iter)) != NULL) {
    dout(10) << __func__ << " found device at "<< pci_dev->bus << ":" << pci_dev->dev << ":"
             << pci_dev->func << " vendor:0x" << pci_dev->vendor_id << " device:0x" << pci_dev->device_id
             << " name:" << pci_device_get_device_name(pci_dev) << dendl;
    r = pci_device_get_serial_number(pci_dev, serial_number, 128);
    if (r < 0) {
      dout(10) << __func__ << " failed to get serial number from " << pci_device_get_device_name(pci_dev) << dendl;
      continue;
    }

    if (sn_tag.compare(string(serial_number, 16))) {
      dout(10) << __func__ << " device serial number not match " << serial_number << dendl;
      continue;
    }

    if (pci_device_has_kernel_driver(pci_dev)) {
      if (!pci_device_has_uio_driver(pci_dev)) {
        /*NVMe kernel driver case*/
        if (unbindfromkernel) {
          r =  pci_device_switch_to_uio_driver(pci_dev);
          if (r < 0) {
            derr << __func__ << " device " << pci_device_get_device_name(pci_dev) << " " << pci_dev->bus
                 << ":" << pci_dev->dev << ":" << pci_dev->func
                 << " switch to uio driver failed" << dendl;
            return r;
          }
        } else {
          derr << __func__ << " device has kernel nvme driver attached, exiting..." << dendl;
          r = -EBUSY;
          return r;
        }
      }
    } else {
      r =  pci_device_bind_uio_driver(pci_dev, PCI_UIO_DRIVER);
      if (r < 0) {
        derr << __func__ << " device " << pci_device_get_device_name(pci_dev) << " " << pci_dev->bus
             << ":" << pci_dev->dev << ":" << pci_dev->func
             << " bind to uio driver failed" << dendl;
        return r;
      }
    }

    /* Claim the device in case conflict with other ids process */
    r =  pci_device_claim(pci_dev);
    if (r < 0) {
      derr << __func__ << " device " << pci_device_get_device_name(pci_dev) << " " << pci_dev->bus
           << ":" << pci_dev->dev << ":" << pci_dev->func
           << " claim failed" << dendl;
      return r;
    }

    pci_device_probe(pci_dev);

    ctrlr = nvme_attach(pci_dev);
    if (!ctrlr) {
      derr << __func__ << " device attach nvme failed" << dendl;
      r = -1;
      return r;
    }

    int num_ns = nvme_ctrlr_get_num_ns(ctrlr);
    assert(num_ns >= 1);
    if (num_ns > 1) {
      dout(0) << __func__ << " namespace count larger than 1, currently only use the first namespace" << dendl;
    }
    nvme_namespace *ns = nvme_ctrlr_get_ns(ctrlr, 0);
    block_size = nvme_ns_get_sector_size(ns);
    size = block_size * nvme_ns_get_num_sectors(ns);
    dout(1) << __func__ << " successfully attach nvme device at" << pci_device_get_device_name(pci_dev)
                        << " " << pci_dev->bus << ":" << pci_dev->dev << ":" << pci_dev->func << dendl;
    break;
  }

  pci_iterator_destroy(iter);

  dout(1) << __func__ << " size " << size << " (" << pretty_si_t(size) << "B)"
          << " block_size " << block_size << " (" << pretty_si_t(block_size)
          << "B)" << dendl;

  r = nvme_register_io_thread();
  assert(r == 0);
  r = _aio_start();
  assert(r == 0);
  name = pci_device_get_device_name(pci_dev);
  return 0;
}

void NVMEDevice::close()
{
  dout(1) << __func__ << dendl;
  nvme_unregister_io_thread();
  _aio_stop();
  name.clear();
}

int NVMEDevice::flush()
{
  dout(10) << __func__ << " start" << dendl;
  return 0;
}

int NVMEDevice::_aio_start()
{
  dout(10) << __func__ << dendl;
  aio_thread.create();
  return 0;
}

void NVMEDevice::_aio_stop()
{
  dout(10) << __func__ << dendl;
  aio_stop = true;
  aio_thread.join();
  aio_stop = false;
}

void NVMEDevice::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;

	  nvme_ctrlr_process_io_completions(ctrlr, 0);

  }
  dout(10) << __func__ << " end" << dendl;
}

int NVMEDevice::aio_write(
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc,
    bool buffered)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  if (!bl.is_n_page_sized() || !bl.is_page_aligned()) {
    dout(20) << __func__ << " rebuilding buffer to be page-aligned" << dendl;
    bl.rebuild();
  }

  ioc->backend = this;
  int rc = nvme_ns_cmd_write(ns, bl.c_str(), off,
                         bl.length()/block_size, io_complete, ioc);
  if (rc < 0) {
    derr << __func__ << " failed to do write command" << dendl;
    return rc;
  }

  dout(5) << __func__ << " " << off << "~" << len << dendl;

  return 0;
}

int NVMEDevice::aio_zero(
    uint64_t off,
    uint64_t len,
    IOContext *ioc)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  bufferlist bl;
  while (len > 0) {
    bufferlist t;
    t.append(zeros, 0, MIN(zeros.length(), len));
    len -= t.length();
    bl.claim_append(t);
  }
  bufferlist foo;
  // note: this works with aio only becaues the actual buffer is
  // this->zeros, which is page-aligned and never freed.
  return aio_write(off, bl, ioc, false);
}

int NVMEDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
                     IOContext *ioc,
                     bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  bufferptr p = buffer::create_page_aligned(len);
  ioc->backend = this;
  int r = nvme_ns_cmd_read(ns, p.c_str(), off, len / block_size, io_complete, ioc);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " failed to read" << dendl;
    return r;
  }
  pbl->clear();
  pbl->push_back(p);

  return r < 0 ? r : 0;
}

int NVMEDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}
