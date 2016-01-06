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
#include <map>

#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
#include <rte_lcore.h>

#include "include/types.h"
#include "include/compat.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/Initialize.h"

#include "NVMEDevice.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

rte_mempool *request_mempool = nullptr;
rte_mempool *task_pool = nullptr;

static void io_complete(void *t, const struct nvme_completion *completion) {
  Task *task = static_cast<Task*>(t);
  IOContext *ctx = task->ctx;
  if (task->command == IOCommand::WRITE_COMMAND) {
    auto left = ctx->num_running.dec();
    assert(!nvme_completion_is_error(completion));
    // check waiting count before doing callback (which may
    // destroy this ioc).
    dout(20) << __func__ << " write op successfully, left " << left << dendl;
    if (!left) {
      ctx->backend_priv = nullptr;
      if (ctx->priv)
        task->device->aio_callback(task->device->aio_callback_priv, ctx->priv);
      if (ctx->num_waiting.read()) {
        Mutex::Locker l(ctx->lock);
        ctx->cond.Signal();
      }
    }
    rte_free(task->buf);
    rte_mempool_put(task_pool, task);
  } else if (task->command == IOCommand::READ_COMMAND) {
    ctx->num_reading.dec();
    dout(20) << __func__ << " read op successfully" << dendl;
    if (nvme_completion_is_error(completion))
      task->return_code = -1; // FIXME
    else
      task->return_code = 0;
    Mutex::Locker l(ctx->lock);
    ctx->cond.Signal();
  } else {
    assert(task->command == IOCommand::FLUSH_COMMAND);
    dout(20) << __func__ << " flush op successfully" << dendl;
    if (nvme_completion_is_error(completion))
      task->return_code = -1; // FIXME
    else
      task->return_code = 0;
    Mutex::Locker l(ctx->lock);
    ctx->cond.Signal();
  }
}

static char *ealargs[] = {
    "ceph-osd",
    "-c 0x1", /* This must be the second parameter. It is overwritten by index in main(). */
    "-n 4",
};

class SharedDriverData {
  std::map<string, std::pair<nvme_controller*, int> > controllers;
  bool init = false;
  Mutex lock;

  int _scan_nvme_device(const string &sn_tag, nvme_controller **c, string *name);

 public:
  SharedDriverData(): lock("NVMEDevice::SharedDriverData::lock") {}
  int try_get(const string &sn_tag, nvme_controller **c, string *name);
  void release(nvme_controller *c);
};

static SharedDriverData driver_data;

int SharedDriverData::_scan_nvme_device(const string &sn_tag, nvme_controller **c, string *name)
{
  int r = 0;
  dout(1) << __func__ << " serial number " << sn_tag << dendl;

  assert(c);
  if (sn_tag.empty()) {
    r = -ENOENT;
    derr << __func__ << " empty serial number: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto ctr_it = controllers.find(sn_tag);
  if (ctr_it != controllers.end()) {
    ctr_it->second.second++;
    *c = ctr_it->second.first;
    return 0;
  }

  pci_device *pci_dev;

  // Search for matching devices
  pci_id_match match;
  match.vendor_id = PCI_MATCH_ANY;
  match.subvendor_id = PCI_MATCH_ANY;
  match.subdevice_id = PCI_MATCH_ANY;
  match.device_id = PCI_MATCH_ANY;
  match.device_class = NVME_CLASS_CODE;
  match.device_class_mask = 0xFFFFFF;

  pci_device_iterator *iter = pci_id_match_iterator_create(&match);

  char serial_number[128];
  const struct nvme_controller_data	*cdata;
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
    break;
  }
  if (pci_dev == NULL) {
    derr << __func__ << " failed to found nvme serial number " << sn_tag << dendl;
    return -ENOENT;
  }

  pci_device_probe(pci_dev);
  *name = pci_device_get_device_name(pci_dev) ? pci_device_get_device_name(pci_dev) : "Unknown";
  if (pci_device_has_kernel_driver(pci_dev)) {
    if (!pci_device_has_uio_driver(pci_dev)) {
      /*NVMe kernel driver case*/
      if (g_conf->bdev_nvme_unbind_from_kernel) {
        r =  pci_device_switch_to_uio_driver(pci_dev);
        if (r < 0) {
          derr << __func__ << " device " << *name << " " << pci_dev->bus
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
      derr << __func__ << " device " << *name << " " << pci_dev->bus
           << ":" << pci_dev->dev << ":" << pci_dev->func
           << " bind to uio driver failed" << dendl;
      return r;
    }
  }

  /* Claim the device in case conflict with other ids process */
  r =  pci_device_claim(pci_dev);
  if (r < 0) {
    derr << __func__ << " device " << *name << " " << pci_dev->bus
         << ":" << pci_dev->dev << ":" << pci_dev->func
         << " claim failed" << dendl;
    return r;
  }

  *c = nvme_attach(pci_dev);
  if (!*c) {
    derr << __func__ << " device attach nvme failed" << dendl;
    r = -1;
    return r;
  }

  controllers[sn_tag] = make_pair(*c, 1);

  pci_iterator_destroy(iter);

  dout(1) << __func__ << " successfully attach nvme device at" << *name
          << " " << pci_dev->bus << ":" << pci_dev->dev << ":" << pci_dev->func << dendl;

  return 0;
}

int SharedDriverData::try_get(const string &sn_tag, nvme_controller **c, string *name)
{
  Mutex::Locker l(lock);
  int r = 0;
  if (!init) {
    r = rte_eal_init(sizeof(ealargs) / sizeof(ealargs[0]), (char **)(void *)(uintptr_t)ealargs);
    if (r < 0) {
      derr << __func__ << " failed to do rte_eal_init" << dendl;
      return r;
    }

	  request_mempool = rte_mempool_create("nvme_request", 8192,
                                         nvme_request_size(), 128, 0,
                                         NULL, NULL, NULL, NULL,
                                         SOCKET_ID_ANY, 0);
    if (request_mempool == NULL) {
      derr << __func__ << " failed to create memory pool for nvme requests" << dendl;
      return -ENOMEM;
    }

 	  task_pool = rte_mempool_create(
        "task_pool", 8192, sizeof(Task),
        64, 0, NULL, NULL, NULL, NULL,
        SOCKET_ID_ANY, 0);
    if (task_pool == NULL) {
      derr << __func__ << " failed to create memory pool for nvme requests" << dendl;
      return -ENOMEM;
    }

    pci_system_init();
    nvme_retry_count = g_conf->bdev_nvme_retry_count;
	  if (nvme_retry_count < 0)
		  nvme_retry_count = NVME_DEFAULT_RETRY_COUNT;

    init = true;
  }
  return _scan_nvme_device(sn_tag, c, name);
}

void SharedDriverData::release(nvme_controller *c)
{
  dout(1) << __func__ << " " << c << dendl;

  Mutex::Locker l(lock);
  auto it = controllers.begin();
  for (; it != controllers.end(); ++it) {
    if (c == it->second.first)
      break;
  }
  if (it == controllers.end()) {
    derr << __func__ << " not found registered nvme controller " << c << dendl;
    assert(0);
  }

  if (--it->second.second == 0) {
    dout(1) << __func__ << " detach device " << c << dendl;
    nvme_detach(c);
    controllers.erase(it);
  }
}

// ----------------
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << name << ") "

NVMEDevice::NVMEDevice(aio_callback_t cb, void *cbpriv)
    : ctrlr(nullptr),
      ns(nullptr),
      aio_stop(false),
      queue_lock("NVMEDevice::queue_lock"),
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

  r = driver_data.try_get(g_conf->bdev_nvme_serial_number, &ctrlr, &name);
  if (r < 0) {
    derr << __func__ << " failed to get nvme deivce with sn " << g_conf->bdev_nvme_serial_number << dendl;
    return r;
  }

  int num_ns = nvme_ctrlr_get_num_ns(ctrlr);
  assert(num_ns >= 1);
  if (num_ns > 1) {
    dout(0) << __func__ << " namespace count larger than 1, currently only use the first namespace" << dendl;
  }
  ns = nvme_ctrlr_get_ns(ctrlr, 1);
  if (!ns) {
    derr << __func__ << " failed to get namespace at 1" << dendl;
    return -1;
  }
  block_size = nvme_ns_get_sector_size(ns);
  size = block_size * nvme_ns_get_num_sectors(ns);
  aio_thread.create();

  dout(1) << __func__ << " size " << size << " (" << pretty_si_t(size) << "B)"
          << " block_size " << block_size << " (" << pretty_si_t(block_size)
          << "B)" << dendl;

  return 0;
}

void NVMEDevice::close()
{
  dout(1) << __func__ << dendl;

  aio_stop = true;
  aio_thread.join();
  aio_stop = false;
  name.clear();
  driver_data.release(ctrlr);
  ctrlr = nullptr;
}

void NVMEDevice::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  if (nvme_register_io_thread() != 0) {
    assert(0);
  }

  Task *t;
  int r = 0;
  const int max = 16;
  uint64_t lba_off, lba_count;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;
    t = nullptr;
    {
      Mutex::Locker l(queue_lock);
      if (!task_queue.empty()) {
        t = task_queue.front();
        task_queue.pop();
      }
    }

    if (t) {
      switch (t->command) {
        case IOCommand::WRITE_COMMAND:
        {
          while (t) {
            lba_off = t->offset / block_size;
            lba_count = t->len / block_size;
            dout(20) << __func__ << " write command issued " << lba_off << "~" << lba_count << dendl;
            r = nvme_ns_cmd_write(ns, t->buf, lba_off, lba_count, io_complete, t);
            if (r < 0) {
              t->ctx->backend_priv = nullptr;
              rte_free(t->buf);
              rte_mempool_put(task_pool, t);
              derr << __func__ << " failed to do write command" << dendl;
              assert(0);
            }
            t = t->prev;
          }
          break;
        }
        case IOCommand::READ_COMMAND:
        {
          dout(20) << __func__ << " read command issueed " << lba_off << "~" << lba_count << dendl;
          lba_off = t->offset / block_size;
          lba_count = t->len / block_size;
          r = nvme_ns_cmd_read(ns, t->buf, lba_off, lba_count, io_complete, t);
          if (r < 0) {
            derr << __func__ << " failed to read" << dendl;
            t->ctx->num_reading.dec();
            t->return_code = r;
            Mutex::Locker l(t->ctx->lock);
            t->ctx->cond.Signal();
          }
          break;
        }
        case IOCommand::FLUSH_COMMAND:
        {
          dout(20) << __func__ << " flush command issueed " << dendl;
          r = nvme_ns_cmd_flush(ns, io_complete, t);
          if (r < 0) {
            derr << __func__ << " failed to flush" << dendl;
            t->return_code = r;
            Mutex::Locker l(t->ctx->lock);
            t->ctx->cond.Signal();
          }
          break;
        }
      }
    }

    nvme_ctrlr_process_io_completions(ctrlr, max);
  }
  nvme_unregister_io_thread();
  dout(10) << __func__ << " end" << dendl;
}

int NVMEDevice::flush()
{
  dout(10) << __func__ << " start" << dendl;
  Task *t;
  int r = rte_mempool_get(task_pool, (void **)&t);
  if (r < 0) {
    derr << __func__ << " task_pool rte_mempool_get failed" << dendl;
    return r;
  }

  IOContext ioc(nullptr);
  t->buf = nullptr;
  t->ctx = &ioc;
  t->command = IOCommand::FLUSH_COMMAND;
  t->offset = 0;
  t->len = 0;
  t->device = this;
  t->return_code = 1;
  t->next = t->prev = nullptr;
  {
    Mutex::Locker l(queue_lock);
    task_queue.push(t);
  }
  {
    Mutex::Locker l(ioc.lock);
    while (t->return_code > 0)
      ioc.cond.Wait(ioc.lock);
  }
  r = t->return_code;
  rte_mempool_put(task_pool, t);
  return 0;
}

void NVMEDevice::aio_submit(IOContext *ioc)
{
  dout(20) << __func__ << " ioc " << ioc << " pending "
           << ioc->num_pending.read() << " running "
           << ioc->num_running.read() << dendl;
  Task *t = static_cast<Task*>(ioc->backend_priv);
  int pending = ioc->num_pending.read();
  ioc->num_running.add(pending);
  ioc->num_pending.sub(pending);
  assert(ioc->num_pending.read() == 0);  // we should be only thread doing this
  Mutex::Locker l(queue_lock);
  // Only need to push the first entry
  task_queue.push(t);
}

int NVMEDevice::aio_write(
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc,
    bool buffered)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len << " ioc " << ioc << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  Task *t;
  int r = rte_mempool_get(task_pool, (void **)&t);
  if (r < 0) {
		derr << __func__ << " task_pool rte_mempool_get failed" << dendl;
    return r;
	}

  t->buf = rte_malloc(NULL, len, block_size);
	if (t->buf == NULL) {
		derr << __func__ << " task->buf rte_malloc failed" << dendl;
    rte_mempool_put(task_pool, t);
    return -ENOMEM;
	}
  bl.copy(0, len, static_cast<char*>(t->buf));

  t->ctx = ioc;
  t->command = IOCommand::WRITE_COMMAND;
  t->offset = off;
  t->len = len;
  t->device = this;
  t->return_code = 0;
  Task *prev = static_cast<Task*>(ioc->backend_priv);
  t->prev = prev;
  if (prev)
    prev->next = t;
  ioc->backend_priv = t;
  t->next = nullptr;
  ioc->num_pending.inc();

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
  dout(5) << __func__ << " " << off << "~" << len << " ioc " << ioc << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  Task *t;
  int r = rte_mempool_get(task_pool, (void **)&t);
  if (r < 0) {
    derr << __func__ << " task_pool rte_mempool_get failed" << dendl;
    return r;
  }

  bufferptr p = buffer::create_page_aligned(len);
  t->buf = rte_malloc(NULL, len, block_size);
  if (t->buf == NULL) {
    derr << __func__ << " task->buf rte_malloc failed" << dendl;
    r = -ENOMEM;
    goto out;
  }
  t->ctx = ioc;
  t->command = IOCommand::READ_COMMAND;
  t->offset = off;
  t->len = len;
  t->device = this;
  t->return_code = 1;
  t->next = t->prev = nullptr;
  ioc->num_reading.inc();;
  {
    Mutex::Locker l(queue_lock);
    task_queue.push(t);
  }

  {
    Mutex::Locker l(ioc->lock);
    while (t->return_code > 0)
      ioc->cond.Wait(ioc->lock);
  }
  memcpy(p.c_str(), t->buf, len);
  pbl->clear();
  pbl->push_back(p);
  r = t->return_code;
  rte_free(t->buf);

 out:
  rte_mempool_put(task_pool, t);
  if (ioc->num_waiting.read()) {
    dout(20) << __func__ << " waking waiter" << dendl;
    Mutex::Locker l(ioc->lock);
    ioc->cond.Signal();
  }
  return r;
}

int NVMEDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}
