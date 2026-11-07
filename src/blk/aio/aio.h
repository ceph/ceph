// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "acconfig.h"

#if defined(HAVE_LIBAIO)
#include <libaio.h>
#elif defined(HAVE_POSIXAIO)
#include <aio.h>
#include <sys/event.h>
#endif

#include <boost/intrusive/list.hpp>
#include <boost/container/small_vector.hpp>

#include "include/buffer.h"
#include "include/types.h"

struct aio_t {
#if defined(HAVE_LIBAIO)
  struct iocb iocb{};  // must be first element; see shenanigans in aio_queue_t
#elif defined(HAVE_POSIXAIO)
  // A single aiocb carrying the iov array, submitted with
  // aio_writev()/aio_readv(). One aiocb per aio_t, mirroring the libaio path's
  //  io_prep_pwritev() shape -- no per-segment aiocb array, no lio_listio(), 
  // no allocation.
  struct aiocb aio{};
#endif
  void *priv;
  int fd;
  boost::container::small_vector<iovec,4> iov;
  uint64_t offset, length;
  long rval;
  ceph::buffer::list bl;  ///< write payload (so that it remains stable for duration)

  boost::intrusive::list_member_hook<> queue_item;

  aio_t(void *p, int f) : priv(p), fd(f), offset(0), length(0), rval(-1000) {
  }

  void pwritev(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
#if defined(HAVE_LIBAIO)
    io_prep_pwritev(&iocb, fd, &iov[0], iov.size(), offset);
#elif defined(HAVE_POSIXAIO)
    aio = {};
    aio.aio_fildes = fd;
    aio.aio_offset = offset;
    aio.aio_iov = iov.data();
    aio.aio_iovcnt = iov.size();
    // aio_writev() ignores aio_lio_opcode; we keep it as our own tag so
    // submit_batch() knows which call to make.
    aio.aio_lio_opcode = LIO_WRITE;
#endif
  }

  void preadv(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
#if defined(HAVE_LIBAIO)
    io_prep_preadv(&iocb, fd, &iov[0], iov.size(), offset);
#elif defined(HAVE_POSIXAIO)
    aio = {};
    aio.aio_fildes = fd;
    aio.aio_offset = offset;
    aio.aio_iov = iov.data();
    aio.aio_iovcnt = iov.size();
    aio.aio_lio_opcode = LIO_READ;
#endif
  }

  long get_return_value() {
    return rval;
  }

#if defined(HAVE_POSIXAIO)
  /**
   * aio_writev()/aio_readv() on a raw device may complete with fewer bytes
   * than requested.  Account for the first n bytes having been transferred
   * and re-prepare this request for the rest.
   *
   * The rest is a direct-IO request again, so it must stay aligned: n and
   * the new start of the first iovec must be a multiple of align.  Returns
   * false, and leaves the request untouched, when that cannot be done.
   */
  bool advance(uint64_t n, uint64_t align) {
    if (n == 0 || n >= length || align == 0 || (n % align) != 0) {
      return false;
    }
    // find the iovec that holds byte n, without changing anything yet
    uint64_t left = n;
    size_t skip = 0;
    while (skip < iov.size() && left >= iov[skip].iov_len) {
      left -= iov[skip].iov_len;
      ++skip;
    }
    if (skip == iov.size()) {
      return false;
    }
    if (left != 0 &&
        (left % align != 0 ||
         ((uintptr_t)iov[skip].iov_base + left) % align != 0)) {
      return false;
    }
    const bool is_write = (aio.aio_lio_opcode == LIO_WRITE);
    iov.erase(iov.begin(), iov.begin() + skip);
    if (left != 0) {
      iov[0].iov_base = (char*)iov[0].iov_base + left;
      iov[0].iov_len -= left;
    }
    if (is_write) {
      pwritev(offset + n, length - n);
    } else {
      preadv(offset + n, length - n);
    }
    rval = -1000;
    return true;
  }
#endif
};

std::ostream& operator<<(std::ostream& os, const aio_t& aio);

typedef boost::intrusive::list<
  aio_t,
  boost::intrusive::constant_time_size<false>,
  boost::intrusive::member_hook<
    aio_t,
    boost::intrusive::list_member_hook<>,
    &aio_t::queue_item> > aio_list_t;

struct io_queue_t {
  typedef std::list<aio_t>::iterator aio_iter;

  virtual ~io_queue_t() {};

  virtual int init(std::vector<int> &fds) = 0;
  virtual void shutdown() = 0;
  virtual int submit_batch(aio_iter begin, aio_iter end,
			   void *priv, int *retries, int submit_retries, int initial_delay_us) = 0;
  virtual int get_next_completed(int timeout_ms, aio_t **paio, int max) = 0;
  /// Submit one aio_t again (the rest of a request that completed short).
  /// Only the POSIX AIO queue implements this.
  virtual int resubmit(aio_t *a, int *retries,
                       int submit_retries, int initial_delay_us) {
    (void)a; (void)retries; (void)submit_retries; (void)initial_delay_us;
    return -ENOTSUP;
  }
};

struct aio_queue_t final : public io_queue_t {
  int max_iodepth;
#if defined(HAVE_LIBAIO)
  io_context_t ctx;
#elif defined(HAVE_POSIXAIO)
  int ctx;
#endif

  explicit aio_queue_t(unsigned max_iodepth)
    : max_iodepth(max_iodepth),
      ctx(0) {
  }
  ~aio_queue_t() final {
    ceph_assert(ctx == 0);
  }

  int init(std::vector<int> &fds) final {
    (void)fds;
    ceph_assert(ctx == 0);
#if defined(HAVE_LIBAIO)
    int r = io_setup(max_iodepth, &ctx);
    if (r < 0) {
      if (ctx) {
	io_destroy(ctx);
	ctx = 0;
      }
    }
    return r;
#elif defined(HAVE_POSIXAIO)
    ctx = kqueue();
    if (ctx < 0)
      return -errno;
    else
      return 0;
#endif
  }
  void shutdown() final {
    if (ctx) {
#if defined(HAVE_LIBAIO)
      int r = io_destroy(ctx);
#elif defined(HAVE_POSIXAIO)
      int r = close(ctx);
#endif
      ceph_assert(r == 0);
      ctx = 0;
    }
  }

  int submit_batch(aio_iter begin, aio_iter end,
		   void *priv, int *retries, int submit_retries, int initial_delay_us) final;
  int get_next_completed(int timeout_ms, aio_t **paio, int max) final;
  int resubmit(aio_t *a, int *retries,
               int submit_retries, int initial_delay_us) final;
};
