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
#include "PMEMDevice.h"
#if defined(HAVE_SPDK)
#include "NVMEDevice.h"
#endif

#if defined(HAVE_PMEM)
#include <libpmem.h>
#endif

#include "common/debug.h"
#include "common/errno.h"
#include "include/compat.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

void IOContext::aio_wait()
{
  std::unique_lock<std::mutex> l(lock);
  // see _aio_thread for waker logic
  ++num_waiting;
  while (num_running.load() > 0 || num_reading.load() > 0) {
    dout(10) << __func__ << " " << this
	     << " waiting for " << num_running.load() << " aios and/or "
	     << num_reading.load() << " readers to complete" << dendl;
    cond.wait(l);
  }
  --num_waiting;
  dout(20) << __func__ << " " << this << " done" << dendl;
}

BlockDevice *BlockDevice::create(const string& path, aio_callback_t cb, void *cbpriv)
{
  string type = "kernel";
  char buf[PATH_MAX];
  int r = ::readlink(path.c_str(), buf, sizeof(buf));
  if (r >= 0) {
    char *bname = ::basename(buf);
    if (strncmp(bname, SPDK_PREFIX, sizeof(SPDK_PREFIX)-1) == 0)
      type = "ust-nvme";
  }

#if defined(HAVE_PMEM)
  if (type == "kernel") {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
      r = -errno;
      derr << __func__ << " open " << path.c_str() << " error " << cpp_strerror(r) << dendl;
    } else {
      void *addr = ::mmap(0, 4096, PROT_WRITE, MAP_SHARED, fd, 0);
      if (addr == MAP_FAILED) {
	r = - errno;
	derr << __func__ << " mmap " << path.c_str() << " error " << cpp_strerror(r) << dendl;
      } else {
	if (pmem_is_pmem(addr, 4096)) {
	  type = "pmem";
	}
	::munmap(addr, 4096);
      }
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
  }
#endif

  dout(1) << __func__ << " path " << path << " type " << type << dendl;

#if defined(HAVE_PMEM)
  if (type == "pmem") {
    return new PMEMDevice(cb, cbpriv);
  }
#endif

  if (type == "kernel") {
    return new KernelDevice(cb, cbpriv);
  }


#if defined(HAVE_SPDK)
  if (type == "ust-nvme") {
    return new NVMEDevice(cb, cbpriv);
  }
#endif

  derr << __func__ << " unknown backend " << type << dendl;
  assert(0);
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
