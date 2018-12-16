// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
  //  static long aio_listio_max = -1;
  union {
    struct aiocb aiocb;
    struct aiocb *aiocbp;
  } aio;
  int n_aiocb;
#endif
  void *priv;
  int fd;
  boost::container::small_vector<iovec,4> iov;
  uint64_t offset, length;
  long rval;
  bufferlist bl;  ///< write payload (so that it remains stable for duration)

  boost::intrusive::list_member_hook<> queue_item;

  aio_t(void *p, int f) : priv(p), fd(f), offset(0), length(0), rval(-1000) {
  }

  void pwritev(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
#if defined(HAVE_LIBAIO)
    io_prep_pwritev(&iocb, fd, &iov[0], iov.size(), offset);
#elif defined(HAVE_POSIXAIO)
    n_aiocb = iov.size();
    aio.aiocbp = (struct aiocb*)calloc(iov.size(), sizeof(struct aiocb));
    for (int i = 0; i < iov.size(); i++) {
      aio.aiocbp[i].aio_fildes = fd;
      aio.aiocbp[i].aio_offset = offset;
      aio.aiocbp[i].aio_buf = iov[i].iov_base;
      aio.aiocbp[i].aio_nbytes = iov[i].iov_len;
      aio.aiocbp[i].aio_lio_opcode = LIO_WRITE;
      offset += iov[i].iov_len;
    }
#endif
  }
  void pread(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
    bufferptr p = buffer::create_small_page_aligned(length);
#if defined(HAVE_LIBAIO)
    io_prep_pread(&iocb, fd, p.c_str(), length, offset);
#elif defined(HAVE_POSIXAIO)
    n_aiocb = 1;
    aio.aiocb.aio_fildes = fd;
    aio.aiocb.aio_buf = p.c_str();
    aio.aiocb.aio_nbytes = length;
    aio.aiocb.aio_offset = offset;
#endif
    bl.append(std::move(p));
  }

  long get_return_value() {
    return rval;
  }
};

std::ostream& operator<<(std::ostream& os, const aio_t& aio);

typedef boost::intrusive::list<
  aio_t,
  boost::intrusive::member_hook<
    aio_t,
    boost::intrusive::list_member_hook<>,
    &aio_t::queue_item> > aio_list_t;

struct aio_queue_t {
  int max_iodepth;
#if defined(HAVE_LIBAIO)
  io_context_t ctx;
#elif defined(HAVE_POSIXAIO)
  int ctx;
#endif

  typedef list<aio_t>::iterator aio_iter;

  explicit aio_queue_t(unsigned max_iodepth)
    : max_iodepth(max_iodepth),
      ctx(0) {
  }
  ~aio_queue_t() {
    ceph_assert(ctx == 0);
  }

  int init() {
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
  void shutdown() {
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

  int submit_batch(aio_iter begin, aio_iter end, uint16_t aios_size, 
		   void *priv, int *retries);
  int get_next_completed(int timeout_ms, aio_t **paio, int max);
};
