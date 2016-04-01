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

#include "KernelDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << path << ") "

KernelDevice::KernelDevice(aio_callback_t cb, void *cbpriv)
  : fd_direct(-1),
    fd_buffered(-1),
    size(0), block_size(0),
    fs(NULL), aio(false), dio(false),
    debug_lock("KernelDevice::debug_lock"),
    flush_lock("KernelDevice::flush_lock"),
    aio_queue(g_conf->bdev_aio_max_queue_depth),
    aio_callback(cb),
    aio_callback_priv(cbpriv),
    aio_stop(false),
    aio_thread(this),
    injecting_crash(0)
{
  zeros = buffer::create_page_aligned(1048576);
  zeros.zero();
}

int KernelDevice::_lock()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fd_direct, F_SETLK, &l);
  if (r < 0)
    return -errno;
  return 0;
}

int KernelDevice::open(string p)
{
  path = p;
  int r = 0;
  dout(1) << __func__ << " path " << path << dendl;

  fd_direct = ::open(path.c_str(), O_RDWR | O_DIRECT);
  if (fd_direct < 0) {
    int r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    return r;
  }
  fd_buffered = ::open(path.c_str(), O_RDWR);
  if (fd_buffered < 0) {
    r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    goto out_direct;
  }
  dio = true;
  aio = g_conf->bdev_aio;
  if (!aio) {
    assert(0 == "non-aio not supported");
  }

  // disable readahead as it will wreak havoc on our mix of
  // directio/aio and buffered io.
  r = posix_fadvise(fd_buffered, 0, 0, POSIX_FADV_RANDOM);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    goto out_fail;
  }

  r = _lock();
  if (r < 0) {
    derr << __func__ << " failed to lock " << path << ": " << cpp_strerror(r)
	 << dendl;
    goto out_fail;
  }

  struct stat st;
  r = ::fstat(fd_direct, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat got " << cpp_strerror(r) << dendl;
    goto out_fail;
  }
  if (S_ISBLK(st.st_mode)) {
    int64_t s;
    r = get_block_device_size(fd_direct, &s);
    if (r < 0) {
      goto out_fail;
    }
    size = s;
  } else {
    size = st.st_size;
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

  fs = FS::create_by_fd(fd_direct);
  assert(fs);

  r = _aio_start();
  assert(r == 0);

  dout(1) << __func__
	  << " size " << size
	  << " (" << pretty_si_t(size) << "B)"
	  << " block_size " << block_size
	  << " (" << pretty_si_t(block_size) << "B)"
	  << dendl;
  return 0;

 out_fail:
  VOID_TEMP_FAILURE_RETRY(::close(fd_buffered));
  fd_buffered = -1;
 out_direct:
  VOID_TEMP_FAILURE_RETRY(::close(fd_direct));
  fd_direct = -1;
  return r;
}

void KernelDevice::close()
{
  dout(1) << __func__ << dendl;
  _aio_stop();

  assert(fs);
  delete fs;
  fs = NULL;

  assert(fd_direct >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(fd_direct));
  fd_direct = -1;

  assert(fd_buffered >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(fd_buffered));
  fd_buffered = -1;

  path.clear();
}

int KernelDevice::flush()
{
  // serialize flushers, so that we can avoid weird io_since_flush
  // races (w/ multipler flushers).
  Mutex::Locker l(flush_lock);
  if (io_since_flush.read() == 0) {
    dout(10) << __func__ << " no-op (no ios since last flush)" << dendl;
    return 0;
  }
  dout(10) << __func__ << " start" << dendl;
  io_since_flush.set(0);
  if (g_conf->bdev_inject_crash) {
    ++injecting_crash;
    // sleep for a moment to give other threads a chance to submit or
    // wait on io that races with a flush.
    derr << __func__ << " injecting crash. first we sleep..." << dendl;
    sleep(g_conf->bdev_inject_crash_flush_delay);
    derr << __func__ << " and now we die" << dendl;
    g_ceph_context->_log->flush();
    _exit(1);
  }
  utime_t start = ceph_clock_now(NULL);
  int r = ::fdatasync(fd_direct);
  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fdatasync got: " << cpp_strerror(r) << dendl;
  }
  dout(5) << __func__ << " in " << dur << dendl;;
  return r;
}

int KernelDevice::_aio_start()
{
  if (aio) {
    dout(10) << __func__ << dendl;
    int r = aio_queue.init();
    if (r < 0) {
      derr << __func__ << " failed: " << cpp_strerror(r) << dendl;
      return r;
    }
    aio_thread.create("bstore_aio");
  }
  return 0;
}

void KernelDevice::_aio_stop()
{
  if (aio) {
    dout(10) << __func__ << dendl;
    aio_stop = true;
    aio_thread.join();
    aio_stop = false;
    aio_queue.shutdown();
  }
}

void KernelDevice::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  int inject_crash_count = 0;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;
    int max = 16;
    FS::aio_t *aio[max];
    int r = aio_queue.get_next_completed(g_conf->bdev_aio_poll_ms,
					 aio, max);
    if (r < 0) {
      derr << __func__ << " got " << cpp_strerror(r) << dendl;
    }
    if (r > 0) {
      dout(30) << __func__ << " got " << r << " completed aios" << dendl;
      for (int i = 0; i < r; ++i) {
	IOContext *ioc = static_cast<IOContext*>(aio[i]->priv);
	_aio_log_finish(ioc, aio[i]->offset, aio[i]->length);
	int left = --ioc->num_running;
	int r = aio[i]->get_return_value();
	dout(10) << __func__ << " finished aio " << aio[i] << " r " << r
		 << " ioc " << ioc
		 << " with " << left << " aios left" << dendl;
	assert(r >= 0);
	if (left == 0) {
	  // check waiting count before doing callback (which may
	  // destroy this ioc).
	  ioc->aio_wake();
	  if (ioc->priv) {
	    aio_callback(aio_callback_priv, ioc->priv);
	  }
	}
      }
    }
    reap_ioc();
    if (g_conf->bdev_inject_crash) {
      ++inject_crash_count;
      if (inject_crash_count * g_conf->bdev_aio_poll_ms / 1000 >
	  g_conf->bdev_inject_crash + g_conf->bdev_inject_crash_flush_delay) {
	derr << __func__ << " bdev_inject_crash trigger from aio thread"
	     << dendl;
	g_ceph_context->_log->flush();
	_exit(1);
      }
    }
  }
  dout(10) << __func__ << " end" << dendl;
}

void KernelDevice::_aio_log_start(
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

void KernelDevice::_aio_log_finish(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__ << " " << aio << " " << offset << "~" << length << dendl;
  if (g_conf->bdev_debug_inflight_ios) {
    Mutex::Locker l(debug_lock);
    debug_inflight.erase(offset, length);
  }
}

void KernelDevice::aio_submit(IOContext *ioc)
{
  dout(20) << __func__ << " ioc " << ioc
	   << " pending " << ioc->num_pending.load()
	   << " running " << ioc->num_running.load()
	   << dendl;
  // move these aside, and get our end iterator position now, as the
  // aios might complete as soon as they are submitted and queue more
  // wal aio's.
  list<FS::aio_t>::iterator e = ioc->running_aios.begin();
  ioc->running_aios.splice(e, ioc->pending_aios);
  list<FS::aio_t>::iterator p = ioc->running_aios.begin();

  int pending = ioc->num_pending.load();
  ioc->num_running += pending;
  ioc->num_pending -= pending;
  assert(ioc->num_pending.load() == 0);  // we should be only thread doing this

  bool done = false;
  while (!done) {
    FS::aio_t& aio = *p;
    aio.priv = static_cast<void*>(ioc);
    dout(20) << __func__ << "  aio " << &aio << " fd " << aio.fd
	     << " " << aio.offset << "~" << aio.length << dendl;
    for (vector<iovec>::iterator q = aio.iov.begin(); q != aio.iov.end(); ++q)
      dout(30) << __func__ << "   iov " << (void*)q->iov_base
	       << " len " << q->iov_len << dendl;

    // be careful: as soon as we submit aio we race with completion.
    // since we are holding a ref take care not to dereference txc at
    // all after that point.
    list<FS::aio_t>::iterator cur = p;
    ++p;
    done = (p == e);

    // do not dereference txc (or it's contents) after we submit (if
    // done == true and we don't loop)
    int retries = 0;
    int r = aio_queue.submit(*cur, &retries);
    if (retries)
      derr << __func__ << " retries " << retries << dendl;
    if (r) {
      derr << " aio submit got " << cpp_strerror(r) << dendl;
      assert(r == 0);
    }
  }
}

int KernelDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  if (!bl.is_n_align_sized(block_size) || !bl.is_aligned(block_size)) {
    dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
    bl.rebuild_aligned(block_size);
  }

  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  _aio_log_start(ioc, off, bl.length());

#ifdef HAVE_LIBAIO
  if (aio && dio && !buffered) {
    ioc->pending_aios.push_back(FS::aio_t(ioc, fd_direct));
    ++ioc->num_pending;
    FS::aio_t& aio = ioc->pending_aios.back();
    if (g_conf->bdev_inject_crash &&
	rand() % g_conf->bdev_inject_crash == 0) {
      derr << __func__ << " bdev_inject_crash: dropping io " << off << "~" << len
	   << dendl;
      // generate a real io so that aio_wait behaves properly, but make it
      // a read instead of write, and toss the result.
      aio.pread(off, len);
      ++injecting_crash;
    } else {
      bl.prepare_iov(&aio.iov);
      for (unsigned i=0; i<aio.iov.size(); ++i) {
	dout(30) << "aio " << i << " " << aio.iov[i].iov_base
		 << " " << aio.iov[i].iov_len << dendl;
      }
      aio.bl.claim_append(bl);
      aio.pwritev(off);
    }
    dout(5) << __func__ << " " << off << "~" << len << " aio " << &aio << dendl;
  } else
#endif
  {
    dout(5) << __func__ << " " << off << "~" << len << " buffered" << dendl;
    if (g_conf->bdev_inject_crash &&
	rand() % g_conf->bdev_inject_crash == 0) {
      derr << __func__ << " bdev_inject_crash: dropping io " << off << "~" << len
	   << dendl;
      ++injecting_crash;
      return 0;
    }
    vector<iovec> iov;
    bl.prepare_iov(&iov);
    int r = ::pwritev(buffered ? fd_buffered : fd_direct,
		      &iov[0], iov.size(), off);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " pwritev error: " << cpp_strerror(r) << dendl;
      return r;
    }
    if (buffered) {
      // initiate IO (but do not wait)
      r = ::sync_file_range(fd_buffered, off, len, SYNC_FILE_RANGE_WRITE);
      if (r < 0) {
        r = -errno;
        derr << __func__ << " sync_file_range error: " << cpp_strerror(r) << dendl;
        return r;
      }
    }
  }

  _aio_log_finish(ioc, off, bl.length());
  io_since_flush.set(1);
  return 0;
}

int KernelDevice::aio_zero(
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
  // note: this works with aio only becaues the actual buffer is
  // this->zeros, which is page-aligned and never freed.
  return aio_write(off, bl, ioc, false);
}

int KernelDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len
	  << (buffered ? " (buffered)" : " (direct)")
	  << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  _aio_log_start(ioc, off, len);
  ++ioc->num_reading;

  bufferptr p = buffer::create_page_aligned(len);
  int r = ::pread(buffered ? fd_buffered : fd_direct,
		  p.c_str(), len, off);
  if (r < 0) {
    r = -errno;
    goto out;
  }
  assert((uint64_t)r == len);
  pbl->clear();
  pbl->push_back(std::move(p));

  dout(40) << "data: ";
  pbl->hexdump(*_dout);
  *_dout << dendl;

 out:
  _aio_log_finish(ioc, off, len);
  --ioc->num_reading;
  ioc->aio_wake();
  return r < 0 ? r : 0;
}

int KernelDevice::read_buffered(uint64_t off, uint64_t len, char *buf)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  int r = 0;
  char *t = buf;
  uint64_t left = len;
  while (left > 0) {
    r = ::pread(fd_buffered, t, left, off);
    if (r < 0) {
      r = -errno;
      goto out;
    }
    off += r;
    t += r;
    left -= r;
  }

  dout(40) << __func__ << " data: ";
  bufferlist bl;
  bl.append(buf, len);
  bl.hexdump(*_dout);
  *_dout << dendl;

 out:
  return r < 0 ? r : 0;
}

int KernelDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  int r = posix_fadvise(fd_buffered, off, len, POSIX_FADV_DONTNEED);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " " << off << "~" << len << " error: "
	 << cpp_strerror(r) << dendl;
  }
  return r;
}

