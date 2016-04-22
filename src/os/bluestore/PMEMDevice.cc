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

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <libpmem.h>

#include "PMEMDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev-PMEM(" << path << ") "

PMEMDevice::PMEMDevice(aio_callback_t cb, void *cbpriv)
  : fd(-1), addr(0),
    size(0), block_size(0),
    debug_lock("PMEMDevice::debug_lock"),
    injecting_crash(0)
{
}

int PMEMDevice::_lock()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fd, F_SETLK, &l);
  if (r < 0)
    return -errno;
  return 0;
}

int PMEMDevice::open(string p)
{
  path = p;
  int r = 0;
  dout(1) << __func__ << " path " << path << dendl;

  fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = _lock();
  if (r < 0) {
    derr << __func__ << " failed to lock " << path << ": " << cpp_strerror(r)
	 << dendl;
    goto out_fail;
  }

  struct stat st;
  r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat got " << cpp_strerror(r) << dendl;
    goto out_fail;
  }
  if (S_ISBLK(st.st_mode)) {
    int64_t s;
    r = get_block_device_size(fd, &s);
    if (r < 0) {
      goto out_fail;
    }
    size = s;
  } else {
    size = st.st_size;
  }

  addr = (char *)::mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    r = -errno;
    derr << __func__ << " mmap got " << cpp_strerror(r) << dendl;
    goto out_fail;
  }

  // Operate as though the block size is 4 KB.  The backing file
  // blksize doesn't strictly matter except that some file systems may
  // require a read/modify/write if we write something smaller than
  // it.
  block_size = g_conf->bdev_block_size;
  if (block_size != (unsigned)st.st_blksize) {
    dout(1) << __func__ << " backing device/file reports st_blksize "
	    << st.st_blksize << ", using bdev_block_size "
	    << block_size << " anyway" << dendl;
  }

  dout(1) << __func__
    << " size " << size
    << " (" << pretty_si_t(size) << "B)"
    << " block_size " << block_size
    << " (" << pretty_si_t(block_size) << "B)"
    << dendl;
  return 0;

 out_fail:
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;
  return r;
}

void PMEMDevice::close()
{
  dout(1) << __func__ << dendl;

  assert(addr != NULL);
  munmap(addr, size);
  assert(fd >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;

  path.clear();
}

int PMEMDevice::flush()
{
  //Because all write is persist. So no need
  return 0;
}


void PMEMDevice::_aio_log_start(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__ << " " << offset << "~" << length << dendl;
  if (g_conf->bdev_debug_inflight_ios) {
    Mutex::Locker l(debug_lock);
    if (debug_inflight.intersects(offset, length)) {
      derr << __func__ << " inflight overlap of "
	   << offset << "~" << length
	   << " with " << debug_inflight << dendl;
      assert(0);
    }
    debug_inflight.insert(offset, length);
  }
}

void PMEMDevice::_aio_log_finish(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__  << " " << offset << "~" << length << dendl;
  if (g_conf->bdev_debug_inflight_ios) {
    Mutex::Locker l(debug_lock);
    debug_inflight.erase(offset, length);
  }
}

void PMEMDevice::aio_submit(IOContext *ioc)
{
  return;
}

int PMEMDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  _aio_log_start(ioc, off, bl.length());

  {
    dout(5) << __func__ << " " << off << "~" << len << " buffered" << dendl;
    if (g_conf->bdev_inject_crash &&
	rand() % g_conf->bdev_inject_crash == 0) {
      derr << __func__ << " bdev_inject_crash: dropping io " << off << "~" << len
	   << dendl;
      ++injecting_crash;
      return 0;
    }
    bl.copy(0, bl.length(), addr + off);
    pmem_persist(addr + off, bl.length());
  }

  _aio_log_finish(ioc, off, bl.length());
  return 0;
}

int PMEMDevice::aio_zero(
  uint64_t off,
  uint64_t len,
  IOContext *ioc)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  pmem_memset_persist(addr + off, 0, len);
  return 0;
}

int PMEMDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len
	  << (buffered ? " (buffered)" : " (direct)")
	  << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  _aio_log_start(ioc, off, len);
  ++ioc->num_reading;

  bufferptr p = buffer::create_page_aligned(len);
  memcpy(p.c_str(), addr + off, len);

  pbl->clear();
  pbl->push_back(std::move(p));

  dout(40) << "data: ";
  pbl->hexdump(*_dout);
  *_dout << dendl;

  _aio_log_finish(ioc, off, len);
  --ioc->num_reading;
  ioc->aio_wake();
  return 0;
}

int PMEMDevice::read_buffered(uint64_t off, uint64_t len, char *buf)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  memcpy(buf, addr + off, len);

  dout(40) << __func__ << " data: ";
  bufferlist bl;
  bl.append(buf, len);
  bl.hexdump(*_dout);
  *_dout << dendl;

  return 0;
}

int PMEMDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}

string PMEMDevice::get_type()
{
  return "pmem";
}
