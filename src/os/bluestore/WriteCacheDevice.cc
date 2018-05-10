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

/**
 * Cache organization
 *
 * Cache area split for 2 separate rows
 * ROW1: [0 ... size/2]
 * [header] [data] [header] [data] ....
 * ROW2: [size/2 ... size]
 * [header] [data] [header] [data] ....
 *
 * header: 4K
 * - cons.id (8bytes)
 * - size
 * - destination position
 */



WriteCacheDevice::WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
  : KernelDevice(cct, cb, cbpriv, d_cb, d_cbpriv),
    write_cache(nullptr),
    flushing(nullptr),
    current(nullptr),
    empty(nullptr),
    last_id(0)
{
}

WriteCacheDevice::~WriteCacheDevice()
{
  if (write_cache)
    delete write_cache;
}
bool WriteCacheDevice::store_in_cache(uint64_t disk_off, bufferlist& bl)
{
  std::lock_guard<std::mutex> l(lock);
  size_t length_align_up = align_up((size_t)bl.length(), block_size);
  string path;
  dout(20) << __func__ <<
      " disk_off=" << disk_off <<
      " bl.length()=" << bl.length() <<
      " length_align_up=" << length_align_up << dendl;

  if (current->pos + (block_size + length_align_up) > current->size) {
    /* must switch cache row */
    if (flushing != nullptr) {
      /* wait for flushing to finish */
      //TODO!!!
    }
    assert(flushing == nullptr);
    assert(empty != nullptr);
    flushing = current;
    current = empty;
    flushing->pos = 0;
    empty = flushing;
    flushing = nullptr;
    KernelDevice::flush();
  }
  dout(20) << __func__ <<
      " current->pos=" << current->pos <<
      " current->size=" << current->size <<
      " current->disk_offset=" << current->disk_offset << dendl;

  bufferlist header_bl;
  buffer::raw* header_data = buffer::create(block_size);
  header_bl.append(header_data);
  header* h = reinterpret_cast<header*>(header_bl.c_str());
  h->id = last_id;
  last_id++;
  h->dest = disk_off;
  h->size = bl.length();

  write_cache->write(current->disk_offset + current->pos, header_bl, false);
  current->pos += block_size;
  write_cache->write(current->disk_offset + current->pos, bl, false);
  current->pos += length_align_up;
  return true;
}

static void aio_cb(void *priv, void *priv2)
{
  //BlueStore *store = static_cast<BlueStore*>(priv);
  //BlueStore::AioContext *c = static_cast<BlueStore::AioContext*>(priv2);
  //c->aio_finish(store);
}

static void discard_cb(void *priv, void *priv2)
{
  //BlueStore *store = static_cast<BlueStore*>(priv);
  //interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  //store->handle_discard(*tmp);
}

int WriteCacheDevice::open_write_cache(CephContext* cct, const std::string& path)
{
  int r;
  int fd;
  fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    derr << __func__ << " cannot open '" << path << "'"  << dendl;
    return -1;
  }
  struct stat st;
  r = ::fstat(fd, &st);
  ::close(fd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat '" << path << "' failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  if (st.st_size < 2*10*1024*1024) {
    dout(5) << __func__ << " size of '" << path << "' is " << st.st_size << ", writecache NOT ENABLED" << dendl;
    return -1;
  }
  if (write_cache)
    delete write_cache;
  write_cache = new KernelDevice(cct, aio_cb, static_cast<void*>(this), discard_cb, static_cast<void*>(this));
  r = write_cache->open(path);
  if (r < 0) {
    dout(20) << __func__ << " cannot open write cache '" << path << "'" << dendl;
    delete write_cache;
    write_cache = nullptr;
    return r;
  }

  current = new row{0, 10*1024*1024, 0};
  empty = new row{10*1024*1024, 10*1024*1024, 0};

  return r;
}

/*
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
*/
static string get_dev_property(const char *dev, const char *property)
{
  char val[1024] = {0};
  get_block_device_string_property(dev, property, val, sizeof(val));
  return val;
}
/*
int WriteCacheDevice::collect_metadata(const string& prefix, map<string,string> *pm) const
{
  return
      block_data.collect_metadata(prefix, pm);
}

bool WriteCacheDevice::get_thin_utilization(uint64_t *total, uint64_t *avail) const
{
  return block_data.get_thin_utilization(total,avail);
}
*/
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
  if (write_cache)
    res = write_cache->flush();
  else
    res = KernelDevice::flush();
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

/*
void WriteCacheDevice::discard_drain()
{
  block_data.discard_drain();
}
*/


void WriteCacheDevice::aio_submit(IOContext *ioc)
{
  KernelDevice::aio_submit(ioc);
}


int WriteCacheDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered)
{
  if (write_cache)
    store_in_cache(off, bl);
  //TODO AK - write also to write cache
  int res;
  res = KernelDevice::write(off, bl, buffered);
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
  if (write_cache)
     store_in_cache(off, bl);
  //TODO AK - write also to write cache
  int res;
  res = KernelDevice::aio_write(off, bl, ioc, buffered);
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
/*
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
*/

#if AKAK
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
#endif

#if AKAK
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
#endif

#if AKAK
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
#endif

#if AKAK
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
#endif
