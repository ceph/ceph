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

#include "WriteCacheDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"
#include "common/align.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << this << " " << path << ") "

WriteCacheDevice::WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
  : BlockDevice(cct, cb, cbpriv),
    block_data(cct, cb, cbpriv, d_cb, d_cbpriv),
    write_cache(nullptr)
{
}


int WriteCacheDevice::open(const string& p)
{
  int res;
  res = block_data.open(p);
  return res;
}

int WriteCacheDevice::get_devices(std::set<std::string> *ls)
{
  return block_data.get_devices(ls);
}

void WriteCacheDevice::close()
{
  //TODO AK writeback cache before close...
  block_data.close();
}

static string get_dev_property(const char *dev, const char *property)
{
  char val[1024] = {0};
  get_block_device_string_property(dev, property, val, sizeof(val));
  return val;
}

int WriteCacheDevice::collect_metadata(const string& prefix, map<string,string> *pm) const
{
  return
      block_data.collect_metadata(prefix, pm);
}

bool WriteCacheDevice::get_thin_utilization(uint64_t *total, uint64_t *avail) const
{
  return block_data.get_thin_utilization(total,avail);
}

int WriteCacheDevice::flush()
{
  // protect flush with a mutex.  note that we are not really protecting
  // data here.  instead, we're ensuring that if any flush() caller
  // sees that io_since_flush is true, they block any racing callers
  // until the flush is observed.  that allows racing threads to be
  // calling flush while still ensuring that *any* of them that got an
  // aio completion notification will not return before that aio is
  // stable on disk: whichever thread sees the flag first will block
  // followers until the aio is stable.

  //AK make flush to write_cache
  int res;
  res = write_cache->flush();
  return res;
#if AKAK
  std::lock_guard<std::mutex> l(flush_mutex);

  bool expect = true;
  if (false == io_since_flush.compare_exchange_strong(expect, false)) {
    dout(10) << __func__ << " no-op (no ios since last flush), flag is "
	     << (int)io_since_flush.load() << dendl;
    return 0;
  }

  dout(10) << __func__ << " start" << dendl;
  if (cct->_conf->bdev_inject_crash) {
    ++injecting_crash;
    // sleep for a moment to give other threads a chance to submit or
    // wait on io that races with a flush.
    derr << __func__ << " injecting crash. first we sleep..." << dendl;
    sleep(cct->_conf->bdev_inject_crash_flush_delay);
    derr << __func__ << " and now we die" << dendl;
    cct->_log->flush();
    _exit(1);
  }
  utime_t start = ceph_clock_now();
  int r = ::fdatasync(fd_direct);
  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fdatasync got: " << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  dout(5) << __func__ << " in " << dur << dendl;;
  return r;
#endif
}


void WriteCacheDevice::discard_drain()
{
  block_data.discard_drain();
}



void WriteCacheDevice::aio_submit(IOContext *ioc)
{
  block_data.aio_submit(ioc);
}


int WriteCacheDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered)
{
  //TODO AK - write also to write cache
  int res;
  res = block_data.write(off, bl, buffered);
  return res;
#if AKAK
  uint64_t len = bl.length();
  dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  assert(is_valid_io(off, len));

  if ((!buffered || bl.get_num_buffers() >= IOV_MAX) &&
      bl.rebuild_aligned_size_and_memory(block_size, block_size, IOV_MAX)) {
    dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
  }
  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  return _sync_write(off, bl, buffered);
#endif
}

int WriteCacheDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered)
{
  //TODO AK - write also to write cache
  int res;
  res = block_data.aio_write(off, bl, ioc, buffered);
  return res;
#if AKAK
  uint64_t len = bl.length();
  dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  if ((!buffered || bl.get_num_buffers() >= IOV_MAX) &&
      bl.rebuild_aligned_size_and_memory(block_size, block_size, IOV_MAX)) {
    dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
  }
  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  _aio_log_start(ioc, off, len);

#ifdef HAVE_LIBAIO
  if (aio && dio && !buffered) {
    ioc->pending_aios.push_back(aio_t(ioc, fd_direct));
    ++ioc->num_pending;
    aio_t& aio = ioc->pending_aios.back();
    if (cct->_conf->bdev_inject_crash &&
	rand() % cct->_conf->bdev_inject_crash == 0) {
      derr << __func__ << " bdev_inject_crash: dropping io 0x" << std::hex
	   << off << "~" << len << std::dec
	   << dendl;
      // generate a real io so that aio_wait behaves properly, but make it
      // a read instead of write, and toss the result.
      aio.pread(off, len);
      ++injecting_crash;
    } else {
      bl.prepare_iov(&aio.iov);
      dout(30) << aio << dendl;
      aio.bl.claim_append(bl);
      aio.pwritev(off, len);
    }
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
	    << std::dec << " aio " << &aio << dendl;
  } else
#endif
  {
    int r = _sync_write(off, bl, buffered);
    _aio_log_finish(ioc, off, len);
    if (r < 0)
      return r;
  }
  return 0;
#endif
}

int WriteCacheDevice::discard(uint64_t offset, uint64_t len)
{
  int res;
  res = block_data.discard(offset, len);
  return res;
#if AKAK
  int r = 0;
  if (!rotational) {
      dout(10) << __func__
	       << " 0x" << std::hex << offset << "~" << len << std::dec
	       << dendl;

    r = block_device_discard(fd_direct, (int64_t)offset, (int64_t)len);
  }
  return r;
#endif
}

int WriteCacheDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  //TODO AK - check if data is not previously changed
  int res;
  res = block_data.read(off, len, pbl, ioc, buffered);
  return res;
  /*
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << (buffered ? " (buffered)" : " (direct)")
	  << dendl;
  assert(is_valid_io(off, len));

  _aio_log_start(ioc, off, len);

  bufferptr p = buffer::create_page_aligned(len);
  int r = ::pread(buffered ? fd_buffered : fd_direct,
		  p.c_str(), len, off);
  if (r < 0) {
    r = -errno;
    goto out;
  }
  assert((uint64_t)r == len);
  pbl->push_back(std::move(p));

  dout(40) << "data: ";
  pbl->hexdump(*_dout);
  *_dout << dendl;

 out:
  _aio_log_finish(ioc, off, len);
  return r < 0 ? r : 0;
  */
}

int WriteCacheDevice::aio_read(
  uint64_t off,
  uint64_t len,
  bufferlist *pbl,
  IOContext *ioc)
{
  //TODO AK - check if we have not already wrote over data
  int res;
  res = block_data.aio_read(off, len, pbl, ioc);
  return res;
#if AKAK
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << dendl;

  int r = 0;
#ifdef HAVE_LIBAIO
  if (aio && dio) {
    _aio_log_start(ioc, off, len);
    ioc->pending_aios.push_back(aio_t(ioc, fd_direct));
    ++ioc->num_pending;
    aio_t& aio = ioc->pending_aios.back();
    aio.pread(off, len);
    dout(30) << aio << dendl;
    pbl->append(aio.bl);
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
	    << std::dec << " aio " << &aio << dendl;
  } else
#endif
  {
    r = read(off, len, pbl, ioc, false);
  }

  return r;
#endif
}



int WriteCacheDevice::read_random(uint64_t off, uint64_t len, char *buf,
                       bool buffered)
{
  int res;
  res = block_data.read_random(off, len, buf, buffered);
  return res;
#if AKAK
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);
  int r = 0;

  //if it's direct io and unaligned, we have to use a internal buffer
  if (!buffered && ((off % block_size != 0)
                    || (len % block_size != 0)
                    || (uintptr_t(buf) % CEPH_PAGE_SIZE != 0)))
    return direct_read_unaligned(off, len, buf);

  if (buffered) {
    //buffered read
    char *t = buf;
    uint64_t left = len;
    while (left > 0) {
      r = ::pread(fd_buffered, t, left, off);
      if (r < 0) {
	r = -errno;
        derr << __func__ << " 0x" << std::hex << off << "~" << left 
          << std::dec << " error: " << cpp_strerror(r) << dendl;
	goto out;
      }
      off += r;
      t += r;
      left -= r;
    }
  } else {
    //direct and aligned read
    r = ::pread(fd_direct, buf, len, off);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " direct_aligned_read" << " 0x" << std::hex 
        << off << "~" << left << std::dec << " error: " << cpp_strerror(r) 
        << dendl;
      goto out;
    }
    assert((uint64_t)r == len);
  }

  dout(40) << __func__ << " data: ";
  bufferlist bl;
  bl.append(buf, len);
  bl.hexdump(*_dout);
  *_dout << dendl;

 out:
  return r < 0 ? r : 0;
#endif
}

int WriteCacheDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  int res;
  res = block_data.invalidate_cache(off, len);
  return res;
#if AKAK
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  int r = posix_fadvise(fd_buffered, off, len, POSIX_FADV_DONTNEED);
  if (r) {
    r = -r;
    derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	 << " error: " << cpp_strerror(r) << dendl;
  }
  return r;
#endif
}

