// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "ceph_aio.h"

std::ostream& operator<<(std::ostream& os, const aio_t& aio)
{
  unsigned i = 0;
  os << "aio: ";
  for (auto& iov : aio.iov) {
    os << "\n [" << i++ << "] 0x"
       << std::hex << iov.iov_base << "~" << iov.iov_len << std::dec;
  }
  return os;
}

int aio_queue_t::submit_batch(aio_iter begin, aio_iter end, 
			      uint16_t aios_size, void *priv, 
			      int *retries)
{
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;
  int r;

  aio_iter cur = begin;
  struct aio_t *piocb[aios_size];
  int left = 0;
  while (cur != end) {
    cur->priv = priv;
    *(piocb+left) = &(*cur);
    ++left;
    ++cur;
  }
  ceph_assert(aios_size >= left);
  int done = 0;
  while (left > 0) {
#if defined(HAVE_LIBAIO)
    {
      std::unique_lock l{lock};
      r = io_submit(ctx, std::min(left, max_iodepth), (struct iocb**)(piocb + done));
    }
#elif defined(HAVE_POSIXAIO)
    if (piocb[done]->n_aiocb == 1) {
      // TODO: consider batching multiple reads together with lio_listio
      piocb[done]->aio.aiocb.aio_sigevent.sigev_notify = SIGEV_KEVENT;
      piocb[done]->aio.aiocb.aio_sigevent.sigev_notify_kqueue = ctx;
      piocb[done]->aio.aiocb.aio_sigevent.sigev_value.sival_ptr = piocb[done];
      r = aio_read(&piocb[done]->aio.aiocb);
    } else {
      struct sigevent sev;
      sev.sigev_notify = SIGEV_KEVENT;
      sev.sigev_notify_kqueue = ctx;
      sev.sigev_value.sival_ptr = piocb[done];
      r = lio_listio(LIO_NOWAIT, &piocb[done]->aio.aiocbp, piocb[done]->n_aiocb, &sev);
    }
#endif
    if (r < 0) {
      if (r == -EAGAIN && attempts-- > 0) {
	usleep(delay);
	delay *= 2;
	(*retries)++;
	continue;
      }
      return r;
    }
    ceph_assert(r > 0);
    done += r;
    left -= r;
    attempts = 16;
    delay = 125;
  }
  return done;
}

namespace {

// see linux kernel source source/fs/aio.c
struct aio_ring_t {
  unsigned id; // kernel internal index number
  unsigned nr; // number of io_events
  // Written to by userland or under ring_lock mutex by aio_read_events_ring().
  std::atomic<unsigned> head;
  std::atomic<unsigned> tail;

  static constexpr unsigned MAGIC = 0xa10a10a1;
  unsigned magic;
  unsigned compat_features;
  unsigned incompat_features;
  unsigned header_length; // size of aio_ring
  struct io_event io_events[0];
};

int user_io_getevent(aio_ring_t* ring, long max_nr, io_event* events)
{
  auto head = ring->head.load(std::memory_order_relaxed);
  auto tail = ring->tail.load(std::memory_order_acquire);
  auto avail = (head <= tail ? tail : tail + ring->nr) - head;
  if (!avail) {
    return 0;
  }
  auto nr = std::min<uint32_t>(max_nr, avail);
  auto start = std::next(ring->io_events, head);
  head += nr;
  if (head < ring->nr) {
    std::copy(start, start + nr, events);
  } else {
    head -= ring->nr;
    auto p = std::copy(start, start + ring->nr, events);
    start = ring->io_events;
    std::copy(start, start + head, p);
  }
  ring->head.store(head, std::memory_order_release);
  return nr;
}
}

int aio_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
#if defined(HAVE_LIBAIO)
  io_event events[max];
  int r = 0;
  if (auto ring = reinterpret_cast<aio_ring_t*>(ctx);
      ring->magic == aio_ring_t::MAGIC) {
    std::unique_lock l{lock};
    r = user_io_getevent(ring, max, events);
  } else {
    struct timespec t = {
      timeout_ms / 1000,
      (timeout_ms % 1000) * 1000 * 1000
    };
    do {
      r = io_getevents(ctx, 1, max, events, &t);
    } while (r == -EINTR);
  }
  for (int i=0; i<r; ++i) {
    paio[i] = (aio_t *)events[i].obj;
    paio[i]->rval = events[i].res;
  }
#elif defined(HAVE_POSIXAIO)
  struct kevent events[max];
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };
  int r = 0;
  do {
    r = kevent(ctx, NULL, 0, events, max, &t);
    if (r < 0)
      r = -errno;
  } while (r == -EINTR);
  for (int i=0; i<r; ++i) {
    paio[i] = (aio_t*)events[i].udata;
    if (paio[i]->n_aiocb == 1) {
      paio[i]->rval = aio_return(&paio[i]->aio.aiocb);
    } else {
      // Emulate the return value of pwritev.  I can't find any documentation
      // for what the value of io_event.res is supposed to be.  I'm going to
      // assume that it's just like pwritev/preadv/pwrite/pread.
      paio[i]->rval = 0;
      for (int j = 0; j < paio[i]->n_aiocb; j++) {
	int res = aio_return(&paio[i]->aio.aiocbp[j]);
	if (res < 0) {
	  paio[i]->rval = res;
	  break;
	} else {
	  paio[i]->rval += res;
	}
      }
      free(paio[i]->aio.aiocbp);
    }
  }
#endif
  return r;
}
