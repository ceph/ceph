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

#include "KernelDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"
#include "common/align.h"
#include "common/blkdev.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << this << " " << path << ") "

KernelDevice::KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv)
  : BlockDevice(cct),
    fd_direct(-1),
    fd_buffered(-1),
    size(0), block_size(0),
    fs(NULL), aio(false), dio(false),
    debug_lock("KernelDevice::debug_lock"),
    aio_queue(cct->_conf->bdev_aio_max_queue_depth),
    aio_callback(cb),
    aio_callback_priv(cbpriv),
    aio_stop(false),
    aio_thread(this),
    injecting_crash(0)
{
}

int KernelDevice::_lock()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  int r = ::fcntl(fd_direct, F_SETLK, &l);
  if (r < 0)
    return -errno;
  return 0;
}

int KernelDevice::open(const string& p)
{
  path = p;
  int r = 0;
  dout(1) << __func__ << " path " << path << dendl;

  fd_direct = ::open(path.c_str(), O_RDWR | O_DIRECT);
  if (fd_direct < 0) {
    r = -errno;
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
  aio = cct->_conf->bdev_aio;
  if (!aio) {
    assert(0 == "non-aio not supported");
  }

  // disable readahead as it will wreak havoc on our mix of
  // directio/aio and buffered io.
  r = posix_fadvise(fd_buffered, 0, 0, POSIX_FADV_RANDOM);
  if (r) {
    r = -r;
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

  // Operate as though the block size is 4 KB.  The backing file
  // blksize doesn't strictly matter except that some file systems may
  // require a read/modify/write if we write something smaller than
  // it.
  block_size = cct->_conf->bdev_block_size;
  if (block_size != (unsigned)st.st_blksize) {
    dout(1) << __func__ << " backing device/file reports st_blksize "
	    << st.st_blksize << ", using bdev_block_size "
	    << block_size << " anyway" << dendl;
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

  {
    char partition[PATH_MAX], devname[PATH_MAX];
    r = get_device_by_fd(fd_buffered, partition, devname, sizeof(devname));
    if (r < 0) {
      derr << "unable to get device name for " << path << ": "
	   << cpp_strerror(r) << dendl;
      rotational = true;
    } else {
      dout(20) << __func__ << " devname " << devname << dendl;
      rotational = block_device_is_rotational(devname);
    }
  }

  r = _aio_start();
  if (r < 0) {
    goto out_fail;
  }

  fs = FS::create_by_fd(fd_direct);
  assert(fs);

  // round size down to an even block
  size &= ~(block_size - 1);

  dout(1) << __func__
	  << " size " << size
	  << " (0x" << std::hex << size << std::dec << ", "
	  << pretty_si_t(size) << "B)"
	  << " block_size " << block_size
	  << " (" << pretty_si_t(block_size) << "B)"
	  << " " << (rotational ? "rotational" : "non-rotational")
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

static string get_dev_property(const char *dev, const char *property)
{
  char val[1024] = {0};
  get_block_device_string_property(dev, property, val, sizeof(val));
  return val;
}

int KernelDevice::collect_metadata(string prefix, map<string,string> *pm) const
{
  (*pm)[prefix + "rotational"] = stringify((int)(bool)rotational);
  (*pm)[prefix + "size"] = stringify(get_size());
  (*pm)[prefix + "block_size"] = stringify(get_block_size());
  (*pm)[prefix + "driver"] = "KernelDevice";
  if (rotational) {
    (*pm)[prefix + "type"] = "hdd";
  } else {
    (*pm)[prefix + "type"] = "ssd";
  }

  struct stat st;
  int r = ::fstat(fd_buffered, &st);
  if (r < 0)
    return -errno;
  if (S_ISBLK(st.st_mode)) {
    (*pm)[prefix + "access_mode"] = "blk";
    char partition_path[PATH_MAX];
    char dev_node[PATH_MAX];
    int rc = get_device_by_fd(fd_buffered, partition_path, dev_node, PATH_MAX);
    switch (rc) {
    case -EOPNOTSUPP:
    case -EINVAL:
      (*pm)[prefix + "partition_path"] = "unknown";
      (*pm)[prefix + "dev_node"] = "unknown";
      break;
    case -ENODEV:
      (*pm)[prefix + "partition_path"] = string(partition_path);
      (*pm)[prefix + "dev_node"] = "unknown";
      break;
    default:
      {
	(*pm)[prefix + "partition_path"] = string(partition_path);
	(*pm)[prefix + "dev_node"] = string(dev_node);
	(*pm)[prefix + "model"] = get_dev_property(dev_node, "device/model");
	(*pm)[prefix + "dev"] = get_dev_property(dev_node, "dev");

	// nvme exposes a serial number
	string serial = get_dev_property(dev_node, "device/serial");
	if (serial.length()) {
	  (*pm)[prefix + "serial"] = serial;
	}

	// nvme has a device/device/* structure; infer from that.  there
	// is probably a better way?
	string nvme_vendor = get_dev_property(dev_node, "device/device/vendor");
	if (nvme_vendor.length()) {
	  (*pm)[prefix + "type"] = "nvme";
	}
      }
    }
  } else {
    (*pm)[prefix + "access_mode"] = "file";
    (*pm)[prefix + "path"] = path;
  }
  return 0;
}

int KernelDevice::flush()
{
  // protect flush with a mutex.  note that we are not really protecting
  // data here.  instead, we're ensuring that if any flush() caller
  // sees that io_since_flush is true, they block any racing callers
  // until the flush is observed.  that allows racing threads to be
  // calling flush while still ensuring that *any* of them that got an
  // aio completion notification will not return before that aio is
  // stable on disk: whichever thread sees the flag first will block
  // followers until the aio is stable.
  std::lock_guard<std::mutex> l(flush_mutex);

  bool expect = true;
  if (!io_since_flush.compare_exchange_strong(expect, false)) {
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
}

int KernelDevice::_aio_start()
{
  if (aio) {
    dout(10) << __func__ << dendl;
    int r = aio_queue.init();
    if (r < 0) {
      if (r == -EAGAIN) {
	derr << __func__ << " io_setup(2) failed with EAGAIN; "
	     << "try increasing /proc/sys/fs/aio-max-nr" << dendl;
      } else {
	derr << __func__ << " io_setup(2) failed: " << cpp_strerror(r) << dendl;
      }
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
    int max = cct->_conf->bdev_aio_reap_max;
    aio_t *aio[max];
    int r = aio_queue.get_next_completed(cct->_conf->bdev_aio_poll_ms,
					 aio, max);
    if (r < 0) {
      derr << __func__ << " got " << cpp_strerror(r) << dendl;
    }
    if (r > 0) {
      dout(30) << __func__ << " got " << r << " completed aios" << dendl;
      for (int i = 0; i < r; ++i) {
	IOContext *ioc = static_cast<IOContext*>(aio[i]->priv);
	_aio_log_finish(ioc, aio[i]->offset, aio[i]->length);
	if (aio[i]->queue_item.is_linked()) {
	  std::lock_guard<std::mutex> l(debug_queue_lock);
	  debug_aio_unlink(*aio[i]);
	}

	// set flag indicating new ios have completed.  we do this *before*
	// any completion or notifications so that any user flush() that
	// follows the observed io completion will include this io.  Note
	// that an earlier, racing flush() could observe and clear this
	// flag, but that also ensures that the IO will be stable before the
	// later flush() occurs.
	io_since_flush.store(true);

	int r = aio[i]->get_return_value();
	dout(10) << __func__ << " finished aio " << aio[i] << " r " << r
		 << " ioc " << ioc
		 << " with " << (ioc->num_running.load() - 1)
		 << " aios left" << dendl;
	assert(r >= 0);

	// NOTE: once num_running and we either call the callback or
	// call aio_wake we cannot touch ioc or aio[] as the caller
	// may free it.
	if (ioc->priv) {
	  if (--ioc->num_running == 0) {
	    aio_callback(aio_callback_priv, ioc->priv);
	  }
	} else {
          ioc->try_aio_wake();
	}
      }
    }
    if (cct->_conf->bdev_debug_aio) {
      utime_t now = ceph_clock_now();
      std::lock_guard<std::mutex> l(debug_queue_lock);
      if (debug_oldest) {
	if (debug_stall_since == utime_t()) {
	  debug_stall_since = now;
	} else {
	  utime_t cutoff = now;
	  cutoff -= cct->_conf->bdev_debug_aio_suicide_timeout;
	  if (debug_stall_since < cutoff) {
	    derr << __func__ << " stalled aio " << debug_oldest
		 << " since " << debug_stall_since << ", timeout is "
		 << cct->_conf->bdev_debug_aio_suicide_timeout
		 << "s, suicide" << dendl;
	    assert(0 == "stalled aio... buggy kernel or bad device?");
	  }
	}
      }
    }
    reap_ioc();
    if (cct->_conf->bdev_inject_crash) {
      ++inject_crash_count;
      if (inject_crash_count * cct->_conf->bdev_aio_poll_ms / 1000 >
	  cct->_conf->bdev_inject_crash + cct->_conf->bdev_inject_crash_flush_delay) {
	derr << __func__ << " bdev_inject_crash trigger from aio thread"
	     << dendl;
	cct->_log->flush();
	_exit(1);
      }
    }
  }
  reap_ioc();
  dout(10) << __func__ << " end" << dendl;
}

void KernelDevice::_aio_log_start(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (cct->_conf->bdev_debug_inflight_ios) {
    Mutex::Locker l(debug_lock);
    if (debug_inflight.intersects(offset, length)) {
      derr << __func__ << " inflight overlap of 0x"
	   << std::hex
	   << offset << "~" << length << std::dec
	   << " with " << debug_inflight << dendl;
      ceph_abort();
    }
    debug_inflight.insert(offset, length);
  }
}

void KernelDevice::debug_aio_link(aio_t& aio)
{
  if (debug_queue.empty()) {
    debug_oldest = &aio;
  }
  debug_queue.push_back(aio);
}

void KernelDevice::debug_aio_unlink(aio_t& aio)
{
  if (aio.queue_item.is_linked()) {
    debug_queue.erase(debug_queue.iterator_to(aio));
    if (debug_oldest == &aio) {
      if (debug_queue.empty()) {
	debug_oldest = nullptr;
      } else {
	debug_oldest = &debug_queue.front();
      }
      debug_stall_since = utime_t();
    }
  }
}

void KernelDevice::_aio_log_finish(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__ << " " << aio << " 0x"
	   << std::hex << offset << "~" << length << std::dec << dendl;
  if (cct->_conf->bdev_debug_inflight_ios) {
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

  if (ioc->num_pending.load() == 0) {
    return;
  }

  // move these aside, and get our end iterator position now, as the
  // aios might complete as soon as they are submitted and queue more
  // wal aio's.
  list<aio_t>::iterator e = ioc->running_aios.begin();
  ioc->running_aios.splice(e, ioc->pending_aios);

  int pending = ioc->num_pending.load();
  ioc->num_running += pending;
  ioc->num_pending -= pending;
  assert(ioc->num_pending.load() == 0);  // we should be only thread doing this
  assert(ioc->pending_aios.size() == 0);
  
  if (cct->_conf->bdev_debug_aio) {
    list<aio_t>::iterator p = ioc->running_aios.begin();
    while (p != e) {
      for (auto& io : p->iov)
	dout(30) << __func__ << "   iov " << (void*)io.iov_base
		 << " len " << io.iov_len << dendl;

      std::lock_guard<std::mutex> l(debug_queue_lock);
      debug_aio_link(*p++);
    }
  }

  void *priv = static_cast<void*>(ioc);
  int r, retries = 0;
  r = aio_queue.submit_batch(ioc->running_aios.begin(), e, 
			     ioc->num_running.load(), priv, &retries);
  
  if (retries)
    derr << __func__ << " retries " << retries << dendl;
  if (r < 0) {
    derr << " aio submit got " << cpp_strerror(r) << dendl;
    assert(r == 0);
  }
}

int KernelDevice::_sync_write(uint64_t off, bufferlist &bl, bool buffered)
{
  uint64_t len = bl.length();
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
	  << std::dec << " buffered" << dendl;
  if (cct->_conf->bdev_inject_crash &&
      rand() % cct->_conf->bdev_inject_crash == 0) {
    derr << __func__ << " bdev_inject_crash: dropping io 0x" << std::hex
	 << off << "~" << len << std::dec << dendl;
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

  io_since_flush.store(true);

  return 0;
}

int KernelDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered)
{
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
      bl.rebuild_aligned_size_and_memory(block_size, block_size)) {
    dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
  }
  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  return _sync_write(off, bl, buffered);
}

int KernelDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered)
{
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
      bl.rebuild_aligned_size_and_memory(block_size, block_size)) {
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
      for (unsigned i=0; i<aio.iov.size(); ++i) {
	dout(30) << "aio " << i << " " << aio.iov[i].iov_base
		 << " " << aio.iov[i].iov_len << dendl;
      }
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
}

int KernelDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << (buffered ? " (buffered)" : " (direct)")
	  << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

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
}

int KernelDevice::aio_read(
  uint64_t off,
  uint64_t len,
  bufferlist *pbl,
  IOContext *ioc)
{
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
    for (unsigned i=0; i<aio.iov.size(); ++i) {
      dout(30) << "aio " << i << " " << aio.iov[i].iov_base
	       << " " << aio.iov[i].iov_len << dendl;
    }
    pbl->append(aio.bl);
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
	    << std::dec << " aio " << &aio << dendl;
  } else
#endif
  {
    r = read(off, len, pbl, ioc, false);
  }

  return r;
}

int KernelDevice::direct_read_unaligned(uint64_t off, uint64_t len, char *buf)
{
  uint64_t aligned_off = align_down(off, block_size);
  uint64_t aligned_len = align_up(off+len, block_size) - aligned_off;
  bufferptr p = buffer::create_page_aligned(aligned_len);
  int r = 0;

  r = ::pread(fd_direct, p.c_str(), aligned_len, aligned_off);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec 
      << " error: " << cpp_strerror(r) << dendl;
    goto out;
  }
  assert((uint64_t)r == aligned_len);
  memcpy(buf, p.c_str() + (off - aligned_off), len);

  dout(40) << __func__ << " data: ";
  bufferlist bl;
  bl.append(buf, len);
  bl.hexdump(*_dout);
  *_dout << dendl;

 out:
  return r < 0 ? r : 0;
}

int KernelDevice::read_random(uint64_t off, uint64_t len, char *buf,
                       bool buffered)
{
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
}

int KernelDevice::invalidate_cache(uint64_t off, uint64_t len)
{
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
}

