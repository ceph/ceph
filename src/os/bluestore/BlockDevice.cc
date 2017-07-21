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

#include "KernelDevice.h"
#if defined(HAVE_SPDK)
#include "NVMEDevice.h"
#endif

#if defined(HAVE_PMEM)
#include "PMEMDevice.h"
#include <libpmem.h>
#endif

#include "common/debug.h"
#include "common/EventTrace.h"
#include "common/errno.h"
#include "include/compat.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

void IOContext::aio_wait()
{
  std::unique_lock<std::mutex> l(lock);
  // see _aio_thread for waker logic
  while (num_running.load() > 0) {
    dout(10) << __func__ << " " << this
	     << " waiting for " << num_running.load() << " aios to complete"
	     << dendl;
    cond.wait(l);
  }
  dout(20) << __func__ << " " << this << " done" << dendl;
}

BlockDevice *BlockDevice::create(CephContext* cct, const string& path,
				 aio_callback_t cb, void *cbpriv)
{
  string type = "kernel";
  char buf[PATH_MAX + 1];
  int r = ::readlink(path.c_str(), buf, sizeof(buf) - 1);
  if (r >= 0) {
    buf[r] = '\0';
    char *bname = ::basename(buf);
    if (strncmp(bname, SPDK_PREFIX, sizeof(SPDK_PREFIX)-1) == 0)
      type = "ust-nvme";
  }

#if defined(HAVE_PMEM)
  if (type == "kernel") {
    int is_pmem = 0;
    void *addr = pmem_map_file(path.c_str(), 1024*1024, PMEM_FILE_EXCL, O_RDONLY, NULL, &is_pmem);
    if (addr != NULL) {
      if (is_pmem)
	type = "pmem";
      pmem_unmap(addr, 1024*1024);
    }
  }
#endif

  dout(1) << __func__ << " path " << path << " type " << type << dendl;

#if defined(HAVE_PMEM)
  if (type == "pmem") {
    return new PMEMDevice(cct, cb, cbpriv);
  }
#endif

  if (type == "kernel") {
    return new KernelDevice(cct, cb, cbpriv);
  }
#if defined(HAVE_SPDK)
  if (type == "ust-nvme") {
    return new NVMEDevice(cct, cb, cbpriv);
  }
#endif


  derr << __func__ << " unknown backend " << type << dendl;
  ceph_abort();
  return NULL;
}

void BlockDevice::queue_reap_ioc(IOContext *ioc)
{
  std::lock_guard<std::mutex> l(ioc_reap_lock);
  if (ioc_reap_count.load() == 0)
    ++ioc_reap_count;
  ioc_reap_queue.push_back(ioc);
}

void BlockDevice::reap_ioc()
{
  if (ioc_reap_count.load()) {
    std::lock_guard<std::mutex> l(ioc_reap_lock);
    for (auto p : ioc_reap_queue) {
      dout(20) << __func__ << " reap ioc " << p << dendl;
      delete p;
    }
    ioc_reap_queue.clear();
    --ioc_reap_count;
  }
}
