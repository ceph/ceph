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
#include <sys/file.h>

#include "KernelDevice.h"
#include "include/intarith.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/blkdev.h"
#include "common/errno.h"
#if defined(__FreeBSD__)
#include "bsm/audit_errno.h"
#endif
#include "common/debug.h"
#include "common/numa.h"

#include "global/global_context.h"
#include "io_uring.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << this << " " << path << ") "

using std::list;
using std::map;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::make_timespan;
using ceph::mono_clock;
using ceph::operator <<;

KernelDevice::KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
  : BlockDevice(cct, cb, cbpriv),
    aio(false), dio(false),
    discard_callback(d_cb),
    discard_callback_priv(d_cbpriv),
    aio_stop(false),
    discard_started(false),
    discard_stop(false),
    aio_thread(this),
    discard_thread(this),
    injecting_crash(0)
{
  fd_directs.resize(WRITE_LIFE_MAX, -1);
  fd_buffereds.resize(WRITE_LIFE_MAX, -1);

  bool use_ioring = cct->_conf.get_val<bool>("bluestore_ioring");
  unsigned int iodepth = cct->_conf->bdev_aio_max_queue_depth;

  if (use_ioring && ioring_queue_t::supported()) {
    io_queue = std::make_unique<ioring_queue_t>(iodepth);
  } else {
    static bool once;
    if (use_ioring && !once) {
      derr << "WARNING: io_uring API is not supported! Fallback to libaio!"
           << dendl;
      once = true;
    }
    io_queue = std::make_unique<aio_queue_t>(iodepth);
  }
}

int KernelDevice::_lock()
{
  dout(10) << __func__ << " " << fd_directs[WRITE_LIFE_NOT_SET] << dendl;
  int r = ::flock(fd_directs[WRITE_LIFE_NOT_SET], LOCK_EX | LOCK_NB);
  if (r < 0) {
    derr << __func__ << " flock failed on " << path << dendl;
    return -errno;
  }
  return 0;
}

int KernelDevice::open(const string& p)
{
  path = p;
  int r = 0, i = 0;
  dout(1) << __func__ << " path " << path << dendl;

  for (i = 0; i < WRITE_LIFE_MAX; i++) {
    int fd = ::open(path.c_str(), O_RDWR | O_DIRECT);
    if (fd  < 0) {
      r = -errno;
      break;
    }
    fd_directs[i] = fd;

    fd  = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd  < 0) {
      r = -errno;
      break;
    }
    fd_buffereds[i] = fd;
  }

  if (i != WRITE_LIFE_MAX) {
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    goto out_fail;
  }

#if defined(F_SET_FILE_RW_HINT)
  for (i = WRITE_LIFE_NONE; i < WRITE_LIFE_MAX; i++) {
    if (fcntl(fd_directs[i], F_SET_FILE_RW_HINT, &i) < 0) {
      r = -errno;
      break;
    }
    if (fcntl(fd_buffereds[i], F_SET_FILE_RW_HINT, &i) < 0) {
      r = -errno;
      break;
    }
  }
  if (i != WRITE_LIFE_MAX) {
    enable_wrt = false;
    dout(0) << "ioctl(F_SET_FILE_RW_HINT) on " << path << " failed: " << cpp_strerror(r) << dendl;
  }
#endif

  dio = true;
  aio = cct->_conf->bdev_aio;
  if (!aio) {
    ceph_abort_msg("non-aio not supported");
  }

  // disable readahead as it will wreak havoc on our mix of
  // directio/aio and buffered io.
  r = posix_fadvise(fd_buffereds[WRITE_LIFE_NOT_SET], 0, 0, POSIX_FADV_RANDOM);
  if (r) {
    r = -r;
    derr << __func__ << " posix_fadvise got: " << cpp_strerror(r) << dendl;
    goto out_fail;
  }

  if (lock_exclusive) {
    r = _lock();
    if (r < 0) {
      derr << __func__ << " failed to lock " << path << ": " << cpp_strerror(r)
	   << dendl;
      goto out_fail;
    }
  }

  struct stat st;
  r = ::fstat(fd_directs[WRITE_LIFE_NOT_SET], &st);
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


  {
    BlkDev blkdev_direct(fd_directs[WRITE_LIFE_NOT_SET]);
    BlkDev blkdev_buffered(fd_buffereds[WRITE_LIFE_NOT_SET]);

    if (S_ISBLK(st.st_mode)) {
      int64_t s;
      r = blkdev_direct.get_size(&s);
      if (r < 0) {
	goto out_fail;
      }
      size = s;
    } else {
      size = st.st_size;
    }

    char partition[PATH_MAX], devname[PATH_MAX];
    if ((r = blkdev_buffered.partition(partition, PATH_MAX)) ||
	(r = blkdev_buffered.wholedisk(devname, PATH_MAX))) {
      derr << "unable to get device name for " << path << ": "
	<< cpp_strerror(r) << dendl;
      rotational = true;
    } else {
      dout(20) << __func__ << " devname " << devname << dendl;
      rotational = blkdev_buffered.is_rotational();
      support_discard = blkdev_buffered.support_discard();
      this->devname = devname;
      _detect_vdo();
    }
  }

  r = _aio_start();
  if (r < 0) {
    goto out_fail;
  }
  _discard_start();

  // round size down to an even block
  size &= ~(block_size - 1);

  dout(1) << __func__
	  << " size " << size
	  << " (0x" << std::hex << size << std::dec << ", "
	  << byte_u_t(size) << ")"
	  << " block_size " << block_size
	  << " (" << byte_u_t(block_size) << ")"
	  << " " << (rotational ? "rotational" : "non-rotational")
      << " discard " << (support_discard ? "supported" : "not supported")
	  << dendl;
  return 0;

out_fail:
  for (i = 0; i < WRITE_LIFE_MAX; i++) {
    if (fd_directs[i] >= 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd_directs[i]));
      fd_directs[i] = -1;
    } else {
      break;
    }
    if (fd_buffereds[i] >= 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd_buffereds[i]));
      fd_buffereds[i] = -1;
    } else {
      break;
    }
  }
  return r;
}

int KernelDevice::get_devices(std::set<std::string> *ls) const
{
  if (devname.empty()) {
    return 0;
  }
  get_raw_devices(devname, ls);
  return 0;
}

void KernelDevice::close()
{
  dout(1) << __func__ << dendl;
  _aio_stop();
  _discard_stop();

  if (vdo_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(vdo_fd));
    vdo_fd = -1;
  }

  for (int i = 0; i < WRITE_LIFE_MAX; i++) {
    assert(fd_directs[i] >= 0);
    VOID_TEMP_FAILURE_RETRY(::close(fd_directs[i]));
    fd_directs[i] = -1;

    assert(fd_buffereds[i] >= 0);
    VOID_TEMP_FAILURE_RETRY(::close(fd_buffereds[i]));
    fd_buffereds[i] = -1;
  }
  path.clear();
}

int KernelDevice::collect_metadata(const string& prefix, map<string,string> *pm) const
{
  (*pm)[prefix + "support_discard"] = stringify((int)(bool)support_discard);
  (*pm)[prefix + "rotational"] = stringify((int)(bool)rotational);
  (*pm)[prefix + "size"] = stringify(get_size());
  (*pm)[prefix + "block_size"] = stringify(get_block_size());
  (*pm)[prefix + "driver"] = "KernelDevice";
  if (rotational) {
    (*pm)[prefix + "type"] = "hdd";
  } else {
    (*pm)[prefix + "type"] = "ssd";
  }
  if (vdo_fd >= 0) {
    (*pm)[prefix + "vdo"] = "true";
    uint64_t total, avail;
    get_vdo_utilization(vdo_fd, &total, &avail);
    (*pm)[prefix + "vdo_physical_size"] = stringify(total);
  }

  {
    string res_names;
    std::set<std::string> devnames;
    if (get_devices(&devnames) == 0) {
      for (auto& dev : devnames) {
	if (!res_names.empty()) {
	  res_names += ",";
	}
	res_names += dev;
      }
      if (res_names.size()) {
	(*pm)[prefix + "devices"] = res_names;
      }
    }
  }

  struct stat st;
  int r = ::fstat(fd_buffereds[WRITE_LIFE_NOT_SET], &st);
  if (r < 0)
    return -errno;
  if (S_ISBLK(st.st_mode)) {
    (*pm)[prefix + "access_mode"] = "blk";

    char buffer[1024] = {0};
    BlkDev blkdev{fd_buffereds[WRITE_LIFE_NOT_SET]};
    if (r = blkdev.partition(buffer, sizeof(buffer)); r) {
      (*pm)[prefix + "partition_path"] = "unknown";
    } else {
      (*pm)[prefix + "partition_path"] = buffer;
    }
    buffer[0] = '\0';
    if (r = blkdev.partition(buffer, sizeof(buffer)); r) {
      (*pm)[prefix + "dev_node"] = "unknown";
    } else {
      (*pm)[prefix + "dev_node"] = buffer;
    }
    if (!r) {
      return 0;
    }
    buffer[0] = '\0';
    blkdev.model(buffer, sizeof(buffer));
    (*pm)[prefix + "model"] = buffer;

    buffer[0] = '\0';
    blkdev.dev(buffer, sizeof(buffer));
    (*pm)[prefix + "dev"] = buffer;

    // nvme exposes a serial number
    buffer[0] = '\0';
    blkdev.serial(buffer, sizeof(buffer));
    (*pm)[prefix + "serial"] = buffer;

    // numa
    int node;
    r = blkdev.get_numa_node(&node);
    if (r >= 0) {
      (*pm)[prefix + "numa_node"] = stringify(node);
    }
  } else {
    (*pm)[prefix + "access_mode"] = "file";
    (*pm)[prefix + "path"] = path;
  }
  return 0;
}

void KernelDevice::_detect_vdo()
{
  vdo_fd = get_vdo_stats_handle(devname.c_str(), &vdo_name);
  if (vdo_fd >= 0) {
    dout(1) << __func__ << " VDO volume " << vdo_name
	    << " maps to " << devname << dendl;
  } else {
    dout(20) << __func__ << " no VDO volume maps to " << devname << dendl;
  }
  return;
}

bool KernelDevice::get_thin_utilization(uint64_t *total, uint64_t *avail) const
{
  if (vdo_fd < 0) {
    return false;
  }
  return get_vdo_utilization(vdo_fd, total, avail);
}

int KernelDevice::choose_fd(bool buffered, int write_hint) const
{
  assert(write_hint >= WRITE_LIFE_NOT_SET && write_hint < WRITE_LIFE_MAX);
  if (!enable_wrt)
    write_hint = WRITE_LIFE_NOT_SET;
  return buffered ? fd_buffereds[write_hint] : fd_directs[write_hint];
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
  std::lock_guard l(flush_mutex);

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
  int r = ::fdatasync(fd_directs[WRITE_LIFE_NOT_SET]);
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
    int r = io_queue->init(fd_directs);
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
    io_queue->shutdown();
  }
}

int KernelDevice::_discard_start()
{
    discard_thread.create("bstore_discard");
    return 0;
}

void KernelDevice::_discard_stop()
{
  dout(10) << __func__ << dendl;
  {
    std::unique_lock l(discard_lock);
    while (!discard_started) {
      discard_cond.wait(l);
    }
    discard_stop = true;
    discard_cond.notify_all();
  }
  discard_thread.join();
  {
    std::lock_guard l(discard_lock);
    discard_stop = false;
  }
  dout(10) << __func__ << " stopped" << dendl;
}

void KernelDevice::discard_drain()
{
  dout(10) << __func__ << dendl;
  std::unique_lock l(discard_lock);
  while (!discard_queued.empty() || discard_running) {
    discard_cond.wait(l);
  }
}

static bool is_expected_ioerr(const int r)
{
  // https://lxr.missinglinkelectronics.com/linux+v4.15/block/blk-core.c#L135
  return (r == -EOPNOTSUPP || r == -ETIMEDOUT || r == -ENOSPC ||
	  r == -ENOLINK || r == -EREMOTEIO  || r == -EAGAIN || r == -EIO ||
	  r == -ENODATA || r == -EILSEQ || r == -ENOMEM ||
#if defined(__linux__)
	  r == -EREMCHG || r == -EBADE
#elif defined(__FreeBSD__)
	  r == - BSM_ERRNO_EREMCHG || r == -BSM_ERRNO_EBADE
#endif
	  );
}

void KernelDevice::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  int inject_crash_count = 0;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;
    int max = cct->_conf->bdev_aio_reap_max;
    aio_t *aio[max];
    int r = io_queue->get_next_completed(cct->_conf->bdev_aio_poll_ms,
					 aio, max);
    if (r < 0) {
      derr << __func__ << " got " << cpp_strerror(r) << dendl;
      ceph_abort_msg("got unexpected error from io_getevents");
    }
    if (r > 0) {
      dout(30) << __func__ << " got " << r << " completed aios" << dendl;
      for (int i = 0; i < r; ++i) {
	IOContext *ioc = static_cast<IOContext*>(aio[i]->priv);
	_aio_log_finish(ioc, aio[i]->offset, aio[i]->length);
	if (aio[i]->queue_item.is_linked()) {
	  std::lock_guard l(debug_queue_lock);
	  debug_aio_unlink(*aio[i]);
	}

	// set flag indicating new ios have completed.  we do this *before*
	// any completion or notifications so that any user flush() that
	// follows the observed io completion will include this io.  Note
	// that an earlier, racing flush() could observe and clear this
	// flag, but that also ensures that the IO will be stable before the
	// later flush() occurs.
	io_since_flush.store(true);

	long r = aio[i]->get_return_value();
        if (r < 0) {
          derr << __func__ << " got r=" << r << " (" << cpp_strerror(r) << ")"
	       << dendl;
          if (ioc->allow_eio && is_expected_ioerr(r)) {
            derr << __func__ << " translating the error to EIO for upper layer"
		 << dendl;
            ioc->set_return_value(-EIO);
          } else {
	    if (is_expected_ioerr(r)) {
	      note_io_error_event(
		devname.c_str(),
		path.c_str(),
		r,
#if defined(HAVE_POSIXAIO)
                aio[i]->aio.aiocb.aio_lio_opcode,
#else
                aio[i]->iocb.aio_lio_opcode,
#endif
		aio[i]->offset,
		aio[i]->length);
	      ceph_abort_msg(
		"Unexpected IO error. "
		"This may suggest a hardware issue. "
		"Please check your kernel log!");
	    }
	    ceph_abort_msg(
	      "Unexpected IO error. "
	      "This may suggest HW issue. Please check your dmesg!");
          }
        } else if (aio[i]->length != (uint64_t)r) {
          derr << "aio to 0x" << std::hex << aio[i]->offset
	       << "~" << aio[i]->length << std::dec
               << " but returned: " << r << dendl;
          ceph_abort_msg("unexpected aio return value: does not match length");
        }

        dout(10) << __func__ << " finished aio " << aio[i] << " r " << r
                 << " ioc " << ioc
                 << " with " << (ioc->num_running.load() - 1)
                 << " aios left" << dendl;

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
      std::lock_guard l(debug_queue_lock);
      if (debug_oldest) {
	if (debug_stall_since == utime_t()) {
	  debug_stall_since = now;
	} else {
	  if (cct->_conf->bdev_debug_aio_suicide_timeout) {
            utime_t cutoff = now;
	    cutoff -= cct->_conf->bdev_debug_aio_suicide_timeout;
	    if (debug_stall_since < cutoff) {
	      derr << __func__ << " stalled aio " << debug_oldest
		   << " since " << debug_stall_since << ", timeout is "
		   << cct->_conf->bdev_debug_aio_suicide_timeout
		   << "s, suicide" << dendl;
	      ceph_abort_msg("stalled aio... buggy kernel or bad device?");
	    }
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

void KernelDevice::_discard_thread()
{
  std::unique_lock l(discard_lock);
  ceph_assert(!discard_started);
  discard_started = true;
  discard_cond.notify_all();
  while (true) {
    ceph_assert(discard_finishing.empty());
    if (discard_queued.empty()) {
      if (discard_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      discard_cond.notify_all(); // for the thread trying to drain...
      discard_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      discard_finishing.swap(discard_queued);
      discard_running = true;
      l.unlock();
      dout(20) << __func__ << " finishing" << dendl;
      for (auto p = discard_finishing.begin();p != discard_finishing.end(); ++p) {
	discard(p.get_start(), p.get_len());
      }

      discard_callback(discard_callback_priv, static_cast<void*>(&discard_finishing));
      discard_finishing.clear();
      l.lock();
      discard_running = false;
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  discard_started = false;
}

int KernelDevice::queue_discard(interval_set<uint64_t> &to_release)
{
  if (!support_discard)
    return -1;

  if (to_release.empty())
    return 0;

  std::lock_guard l(discard_lock);
  discard_queued.insert(to_release);
  discard_cond.notify_all();
  return 0;
}

void KernelDevice::_aio_log_start(
  IOContext *ioc,
  uint64_t offset,
  uint64_t length)
{
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  if (cct->_conf->bdev_debug_inflight_ios) {
    std::lock_guard l(debug_lock);
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
      auto age = cct->_conf->bdev_debug_aio_log_age;
      if (age && debug_stall_since != utime_t()) {
        utime_t cutoff = ceph_clock_now();
	cutoff -= age;
	if (debug_stall_since < cutoff) {
	  derr << __func__ << " stalled aio " << debug_oldest
		<< " since " << debug_stall_since << ", timeout is "
		<< age
		<< "s" << dendl;
	}
      }

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
    std::lock_guard l(debug_lock);
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
  ceph_assert(ioc->num_pending.load() == 0);  // we should be only thread doing this
  ceph_assert(ioc->pending_aios.size() == 0);

  if (cct->_conf->bdev_debug_aio) {
    list<aio_t>::iterator p = ioc->running_aios.begin();
    while (p != e) {
      dout(30) << __func__ << " " << *p << dendl;
      std::lock_guard l(debug_queue_lock);
      debug_aio_link(*p++);
    }
  }

  void *priv = static_cast<void*>(ioc);
  int r, retries = 0;
  r = io_queue->submit_batch(ioc->running_aios.begin(), e,
			     pending, priv, &retries);

  if (retries)
    derr << __func__ << " retries " << retries << dendl;
  if (r < 0) {
    derr << " aio submit got " << cpp_strerror(r) << dendl;
    ceph_assert(r == 0);
  }
}

int KernelDevice::_sync_write(uint64_t off, bufferlist &bl, bool buffered, int write_hint)
{
  uint64_t len = bl.length();
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
	  << std::dec << (buffered ? " (buffered)" : " (direct)") << dendl;
  if (cct->_conf->bdev_inject_crash &&
      rand() % cct->_conf->bdev_inject_crash == 0) {
    derr << __func__ << " bdev_inject_crash: dropping io 0x" << std::hex
	 << off << "~" << len << std::dec << dendl;
    ++injecting_crash;
    return 0;
  }
  vector<iovec> iov;
  bl.prepare_iov(&iov);

  auto left = len;
  auto o = off;
  size_t idx = 0;
  do {
    auto r = ::pwritev(choose_fd(buffered, write_hint),
      &iov[idx], iov.size() - idx, o);

    if (r < 0) {
      r = -errno;
      derr << __func__ << " pwritev error: " << cpp_strerror(r) << dendl;
      return r;
    }
    o += r;
    left -= r;
    if (left) {
      // skip fully processed IOVs
      while (idx < iov.size() && (size_t)r >= iov[idx].iov_len) {
        r -= iov[idx++].iov_len;
      }
      // update partially processed one if any
      if (r) {
        ceph_assert(idx < iov.size());
        ceph_assert((size_t)r < iov[idx].iov_len);
        iov[idx].iov_base = static_cast<char*>(iov[idx].iov_base) + r;
        iov[idx].iov_len -= r;
        r = 0;
      }
      ceph_assert(r == 0);
    }
  } while (left);

#ifdef HAVE_SYNC_FILE_RANGE
  if (buffered) {
    // initiate IO and wait till it completes
    auto r = ::sync_file_range(fd_buffereds[WRITE_LIFE_NOT_SET], off, len, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER|SYNC_FILE_RANGE_WAIT_BEFORE);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " sync_file_range error: " << cpp_strerror(r) << dendl;
      return r;
    }
  }
#endif

  io_since_flush.store(true);

  return 0;
}

int KernelDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered,
  int write_hint)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  ceph_assert(is_valid_io(off, len));
  if (cct->_conf->objectstore_blackhole) {
    lderr(cct) << __func__ << " objectstore_blackhole=true, throwing out IO"
	       << dendl;
    return 0;
  }

  if ((!buffered || bl.get_num_buffers() >= IOV_MAX) &&
      bl.rebuild_aligned_size_and_memory(block_size, block_size, IOV_MAX)) {
    dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
  }
  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  return _sync_write(off, bl, buffered, write_hint);
}

int KernelDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered,
  int write_hint)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << (buffered ? " (buffered)" : " (direct)")
	   << dendl;
  ceph_assert(is_valid_io(off, len));
  if (cct->_conf->objectstore_blackhole) {
    lderr(cct) << __func__ << " objectstore_blackhole=true, throwing out IO"
	       << dendl;
    return 0;
  }

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
    if (cct->_conf->bdev_inject_crash &&
	rand() % cct->_conf->bdev_inject_crash == 0) {
      derr << __func__ << " bdev_inject_crash: dropping io 0x" << std::hex
	   << off << "~" << len << std::dec
	   << dendl;
      // generate a real io so that aio_wait behaves properly, but make it
      // a read instead of write, and toss the result.
      ioc->pending_aios.push_back(aio_t(ioc, choose_fd(false, write_hint)));
      ++ioc->num_pending;
      auto& aio = ioc->pending_aios.back();
      bufferptr p = ceph::buffer::create_small_page_aligned(len);
      aio.bl.append(std::move(p));
      aio.bl.prepare_iov(&aio.iov);
      aio.preadv(off, len);
      ++injecting_crash;
    } else {
      if (bl.length() <= RW_IO_MAX) {
	// fast path (non-huge write)
	ioc->pending_aios.push_back(aio_t(ioc, choose_fd(false, write_hint)));
	++ioc->num_pending;
	auto& aio = ioc->pending_aios.back();
	bl.prepare_iov(&aio.iov);
	aio.bl.claim_append(bl);
	aio.pwritev(off, len);
	dout(30) << aio << dendl;
	dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
		<< std::dec << " aio " << &aio << dendl;
      } else {
	// write in RW_IO_MAX-sized chunks
	uint64_t prev_len = 0;
	while (prev_len < bl.length()) {
	  bufferlist tmp;
	  if (prev_len + RW_IO_MAX < bl.length()) {
	    tmp.substr_of(bl, prev_len, RW_IO_MAX);
	  } else {
	    tmp.substr_of(bl, prev_len, bl.length() - prev_len);
	  }
	  auto len = tmp.length();
	  ioc->pending_aios.push_back(aio_t(ioc, choose_fd(false, write_hint)));
	  ++ioc->num_pending;
	  auto& aio = ioc->pending_aios.back();
	  tmp.prepare_iov(&aio.iov);
	  aio.bl.claim_append(tmp);
	  aio.pwritev(off + prev_len, len);
	  dout(30) << aio << dendl;
	  dout(5) << __func__ << " 0x" << std::hex << off + prev_len
		  << "~" << len
		  << std::dec << " aio " << &aio << " (piece)" << dendl;
	  prev_len += len;
	}
      }
    }
  } else
#endif
  {
    int r = _sync_write(off, bl, buffered, write_hint);
    _aio_log_finish(ioc, off, len);
    if (r < 0)
      return r;
  }
  return 0;
}

int KernelDevice::discard(uint64_t offset, uint64_t len)
{
  int r = 0;
  if (cct->_conf->objectstore_blackhole) {
    lderr(cct) << __func__ << " objectstore_blackhole=true, throwing out IO"
	       << dendl;
    return 0;
  }
  if (support_discard) {
      dout(10) << __func__
	       << " 0x" << std::hex << offset << "~" << len << std::dec
	       << dendl;

      r = BlkDev{fd_directs[WRITE_LIFE_NOT_SET]}.discard((int64_t)offset, (int64_t)len);
  }
  return r;
}

int KernelDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	  << (buffered ? " (buffered)" : " (direct)")
	  << dendl;
  ceph_assert(is_valid_io(off, len));

  _aio_log_start(ioc, off, len);

  auto start1 = mono_clock::now();

  auto p = ceph::buffer::ptr_node::create(ceph::buffer::create_small_page_aligned(len));
  int r = ::pread(buffered ? fd_buffereds[WRITE_LIFE_NOT_SET] : fd_directs[WRITE_LIFE_NOT_SET],
		  p->c_str(), len, off);
  auto age = cct->_conf->bdev_debug_aio_log_age;
  if (mono_clock::now() - start1 >= make_timespan(age)) {
    derr << __func__ << " stalled read "
         << " 0x" << std::hex << off << "~" << len << std::dec
         << (buffered ? " (buffered)" : " (direct)")
	 << " since " << start1 << ", timeout is "
	 << age
	 << "s" << dendl;
  }

  if (r < 0) {
    if (ioc->allow_eio && is_expected_ioerr(r)) {
      r = -EIO;
    } else {
      r = -errno;
    }
    goto out;
  }
  ceph_assert((uint64_t)r == len);
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
    ceph_assert(is_valid_io(off, len));
    _aio_log_start(ioc, off, len);
    ioc->pending_aios.push_back(aio_t(ioc, fd_directs[WRITE_LIFE_NOT_SET]));
    ++ioc->num_pending;
    aio_t& aio = ioc->pending_aios.back();
    bufferptr p = ceph::buffer::create_small_page_aligned(len);
    aio.bl.append(std::move(p));
    aio.bl.prepare_iov(&aio.iov);
    aio.preadv(off, len);
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
}

int KernelDevice::direct_read_unaligned(uint64_t off, uint64_t len, char *buf)
{
  uint64_t aligned_off = p2align(off, block_size);
  uint64_t aligned_len = p2roundup(off+len, block_size) - aligned_off;
  bufferptr p = ceph::buffer::create_small_page_aligned(aligned_len);
  int r = 0;

  auto start1 = mono_clock::now();
  r = ::pread(fd_directs[WRITE_LIFE_NOT_SET], p.c_str(), aligned_len, aligned_off);
  auto age = cct->_conf->bdev_debug_aio_log_age;
  if (mono_clock::now() - start1 >= make_timespan(age)) {
    derr << __func__ << " stalled read "
         << " 0x" << std::hex << off << "~" << len << std::dec
	 << " since " << start1 << ", timeout is "
	 << age
	 << "s" << dendl;
  }

  if (r < 0) {
    r = -errno;
    derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
      << " error: " << cpp_strerror(r) << dendl;
    goto out;
  }
  ceph_assert((uint64_t)r == aligned_len);
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
          << "buffered " << buffered
	  << dendl;
  ceph_assert(len > 0);
  ceph_assert(off < size);
  ceph_assert(off + len <= size);
  int r = 0;
  auto age = cct->_conf->bdev_debug_aio_log_age;

  //if it's direct io and unaligned, we have to use a internal buffer
  if (!buffered && ((off % block_size != 0)
                    || (len % block_size != 0)
                    || (uintptr_t(buf) % CEPH_PAGE_SIZE != 0)))
    return direct_read_unaligned(off, len, buf);

  auto start1 = mono_clock::now();
  if (buffered) {
    //buffered read
    auto off0 = off;
    char *t = buf;
    uint64_t left = len;
    while (left > 0) {
      r = ::pread(fd_buffereds[WRITE_LIFE_NOT_SET], t, left, off);
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
    if (mono_clock::now() - start1 >= make_timespan(age)) {
      derr << __func__ << " stalled read "
	   << " 0x" << std::hex << off0 << "~" << len << std::dec
           << " (buffered) since " << start1 << ", timeout is "
	   << age
	   << "s" << dendl;
    }
  } else {
    //direct and aligned read
    r = ::pread(fd_directs[WRITE_LIFE_NOT_SET], buf, len, off);
    if (mono_clock::now() - start1 >= make_timespan(age)) {
      derr << __func__ << " stalled read "
	   << " 0x" << std::hex << off << "~" << len << std::dec
           << " (direct) since " << start1 << ", timeout is "
	   << age
	   << "s" << dendl;
    }
    if (r < 0) {
      r = -errno;
      derr << __func__ << " direct_aligned_read" << " 0x" << std::hex
	   << off << "~" << std::left << std::dec << " error: " << cpp_strerror(r)
        << dendl;
      goto out;
    }
    ceph_assert((uint64_t)r == len);
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
  ceph_assert(off % block_size == 0);
  ceph_assert(len % block_size == 0);
  int r = posix_fadvise(fd_buffereds[WRITE_LIFE_NOT_SET], off, len, POSIX_FADV_DONTNEED);
  if (r) {
    r = -r;
    derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	 << " error: " << cpp_strerror(r) << dendl;
  }
  return r;
}
