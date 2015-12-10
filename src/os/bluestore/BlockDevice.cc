// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "BlockDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "


void IOContext::aio_wait()
{
  Mutex::Locker l(lock);
  _aio_wait();
}

void IOContext::_aio_wait()
{
  // see _aio_thread for waker logic
  while (num_running > 0 || num_reading > 0) {
    dout(10) << __func__ << " " << this
	     << " waiting for " << num_running << " aios and/or "
	     << num_reading << " readers to complete" << dendl;
    cond.Wait(lock);
  }
  dout(20) << __func__ << " " << this << " done" << dendl;
}

// ----------------

BlockDevice::BlockDevice(aio_callback_t cb, void *cbpriv)
  : fd(-1),
    size(0), block_size(0),
    fs(NULL), aio(false), dio(false),
    aio_queue(g_conf->bluestore_aio_max_queue_depth),
    aio_callback(cb),
    aio_callback_priv(cbpriv),
    aio_stop(false),
    aio_thread(this)
{
  zeros = buffer::create_page_aligned(1048576);
  zeros.zero();
}

int BlockDevice::open(string path)
{
  dout(1) << __func__ << " path " << path << dendl;

  fd = ::open(path.c_str(), O_RDWR | O_DIRECT);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    return r;
  }
  dio = true;
#ifdef HAVE_LIBAIO
  aio = g_conf->bluestore_aio;
#endif

  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat got " << cpp_strerror(r) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    fd = -1;
    return r;
  }
  if (S_ISBLK(st.st_mode)) {
    int64_t s;
    r = get_block_device_size(fd, &s);
    if (r < 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd));
      fd = -1;
      return r;
    }
    size = s;
  } else {
    size = st.st_size;
  }
  block_size = st.st_blksize;

  fs = FS::create_by_fd(fd);
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
}

void BlockDevice::close()
{
  dout(1) << __func__ << dendl;
  _aio_stop();
  assert(fd >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;
}

int BlockDevice::flush()
{
  dout(10) << __func__ << " start" << dendl;
  utime_t start = ceph_clock_now(NULL);
  int r = ::fdatasync(fd);
  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fdatasync got: " << cpp_strerror(r) << dendl;
  }
  dout(10) << __func__ << " done in " << dur << dendl;;
  return r;
}

int BlockDevice::_aio_start()
{
  if (g_conf->bluestore_aio) {
    dout(10) << __func__ << dendl;
    int r = aio_queue.init();
    if (r < 0)
      return r;
    aio_thread.create();
  }
  return 0;
}

void BlockDevice::_aio_stop()
{
  if (g_conf->bluestore_aio) {
    dout(10) << __func__ << dendl;
    aio_stop = true;
    aio_thread.join();
    aio_stop = false;
    aio_queue.shutdown();
  }
}

void BlockDevice::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;
    int max = 16;
    FS::aio_t *aio[max];
    int r = aio_queue.get_next_completed(g_conf->bluestore_aio_poll_ms,
					 aio, max);
    if (r < 0) {
      derr << __func__ << " got " << cpp_strerror(r) << dendl;
    }
    if (r > 0) {
      dout(30) << __func__ << " got " << r << " completed aios" << dendl;
      for (int i = 0; i < r; ++i) {
	IOContext *ioc = static_cast<IOContext*>(aio[i]->priv);
	Mutex::Locker l(ioc->lock);
	--ioc->num_running;
	int r = aio[i]->get_return_value();
	dout(10) << __func__ << " finished aio " << aio[i] << " r " << r
		 << " ioc " << ioc
		 << " with " << ioc->num_running << " aios left" << dendl;
	assert(r >= 0);
	_aio_finish(ioc, aio[i]->offset, aio[i]->length);
	if (ioc->num_running == 0) {
	  ioc->running_bl.clear();
	  ioc->running_aios.clear();
	  if (ioc->priv) {
	    aio_callback(aio_callback_priv, ioc->priv);
	  }
	}
      }
    }
  }
  dout(10) << __func__ << " end" << dendl;
}

void BlockDevice::_aio_prepare(IOContext *ioc, uint64_t offset, uint64_t length)
{
  dout(20) << __func__ << " " << offset << "~" << length
	   << " (" << ioc->blocks << ")" << dendl;
  while (ioc->blocks.intersects(offset, length)) {
    dout(20) << __func__ << " waiting for overlapping io on "
	     << offset << "~" << length
	     << " (" << ioc->blocks << ")" << dendl;
    if (ioc->num_pending) {
      aio_submit(ioc);
    }
    ioc->_aio_wait();
    dout(20) << __func__ << " done waiting" << dendl;
  }
  ioc->blocks.insert(offset, length);

  if (g_conf->bdev_debug_inflight_ios) {
    if (debug_inflight.intersects(offset, length)) {
      derr << __func__ << " inflight overlap of "
	   << offset << "~" << length
	   << " with " << debug_inflight << dendl;
      assert(0);
    }
    debug_inflight.insert(offset, length);
  }
}

void BlockDevice::_aio_finish(IOContext *ioc, uint64_t offset, uint64_t length)
{
  assert(ioc->lock.is_locked());
  dout(20) << __func__ << " " << aio << " " << offset << "~" << length
	   << " (" << ioc->blocks << ")"
	   << dendl;
  ioc->blocks.erase(offset, length);
  ioc->cond.Signal();

  if (g_conf->bdev_debug_inflight_ios) {
    debug_inflight.erase(offset, length);
  }
}

void BlockDevice::aio_submit(IOContext *ioc)
{
  Mutex::Locker l(ioc->lock);
  dout(20) << __func__ << " ioc " << ioc
	   << " pending " << ioc->num_pending
	   << " running " << ioc->num_running
	   << dendl;

#warning fixme can we make this avoid a mutex?
  // move these aside, and get our end iterator position now, as the
  // aios might complete as soon as they are submitted and queue more
  // wal aio's.
  list<FS::aio_t>::iterator e = ioc->running_aios.begin();
  ioc->running_aios.splice(e, ioc->pending_aios);
  list<FS::aio_t>::iterator p = ioc->running_aios.begin();
  ioc->num_running += ioc->num_pending;
  ioc->num_pending = 0;
  ioc->running_bl.claim_append(ioc->pending_bl);
  bool done = false;
  while (!done) {
    FS::aio_t& aio = *p;
    aio.priv = static_cast<void*>(ioc);
    dout(20) << __func__ << " aio " << &aio << " fd " << aio.fd
	     << " " << aio.offset << "~" << aio.length << dendl;
    for (vector<iovec>::iterator q = aio.iov.begin(); q != aio.iov.end(); ++q)
      dout(30) << __func__ << "  iov " << (void*)q->iov_base
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

int BlockDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc)
{
  uint64_t len = bl.length();
  dout(10) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  if (!bl.is_n_page_sized() || !bl.is_page_aligned()) {
    dout(20) << __func__ << " rebuilding buffer to be page-aligned" << dendl;
    bl.rebuild();
  }

  dout(40) << "data: ";
  bl.hexdump(*_dout);
  *_dout << dendl;

  {
    Mutex::Locker l(ioc->lock);
    _aio_prepare(ioc, off, bl.length());
  }

#ifdef HAVE_LIBAIO
  if (aio && dio) {
    ioc->pending_aios.push_back(FS::aio_t(ioc, fd));
    ++ioc->num_pending;
    FS::aio_t& aio = ioc->pending_aios.back();
    bl.prepare_iov(&aio.iov);
    for (unsigned i=0; i<aio.iov.size(); ++i) {
      dout(30) << "aio " << i << " " << aio.iov[i].iov_base
	       << " " << aio.iov[i].iov_len << dendl;
    }
    ioc->pending_bl.append(bl);
    aio.pwritev(off);
    dout(2) << __func__ << " prepared aio " << &aio << dendl;
  } else
#endif
  {
    dout(2) << __func__ << " write to " << off << "~" << len << dendl;
    vector<iovec> iov;
    bl.prepare_iov(&iov);
    int r = ::pwritev(fd, &iov[0], iov.size(), off);
    if (r < 0) {
      derr << __func__ << " pwritev error: " << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return 0;
}

int BlockDevice::aio_zero(
  uint64_t off,
  uint64_t len,
  IOContext *ioc)
{
  dout(10) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

#warning fix discard (aio?)
  //return fs->zero(fd, off, len);
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
  return aio_write(off, bl, ioc);
}

int BlockDevice::read(uint64_t off, uint64_t len, bufferlist *pbl, IOContext *ioc)
{
  dout(10) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  {
    Mutex::Locker l(ioc->lock);
    _aio_prepare(ioc, off, len);
    ++ioc->num_reading;
  }

  bufferptr p = buffer::create_page_aligned(len);
  int r = ::pread(fd, p.c_str(), len, off);
  if (r < 0) {
    r = -errno;
    goto out;
  }
  pbl->clear();
  pbl->push_back(p);
  dout(40) << "data: ";
  pbl->hexdump(*_dout);
  *_dout << dendl;
 out:
  Mutex::Locker l(ioc->lock);
  --ioc->num_reading;
  _aio_finish(ioc, off, len);
  return r < 0 ? r : 0;
}
