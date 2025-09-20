// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include "aio.h"

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
			      void *priv,
			      int *retries, int submit_retries, int initial_delay_us)
{
  // 2^16 * 125us = ~8 seconds, so default max sleep is ~16 seconds
  int attempts = submit_retries;
  uint64_t delay = initial_delay_us;
  int r;

  aio_iter cur = begin;
#if defined(HAVE_LIBAIO)
  struct aio_t *piocb[max_iodepth];
#endif
  int done = 0;
  int pushed = 0; //used for LIBAIO only
  int pulled = 0;
  while (cur != end || pushed < pulled) {
#if defined(HAVE_LIBAIO)
    while (cur != end && pulled < max_iodepth) {
      cur->priv = priv;
      piocb[pulled] = &(*cur);
      ++pulled;
      ++cur;
    }
    int toSubmit = pulled - pushed;
    r = io_submit(ctx, toSubmit, (struct iocb**)(piocb + pushed));
    if (r >= 0 && r < toSubmit) {
      pushed += r;
      done += r;
      r = -EAGAIN;
    }
#elif defined(HAVE_POSIXAIO)
    cur->priv = priv;
    if ((cur->n_aiocb == 1) {
      // TODO: consider batching multiple reads together with lio_listio
      cur->aio.aiocb.aio_sigevent.sigev_notify = SIGEV_KEVENT;
      cur->aio.aiocb.aio_sigevent.sigev_notify_kqueue = ctx;
      cur->aio.aiocb.aio_sigevent.sigev_value.sival_ptr = &(*cur);
      r = aio_write(&cur->aio.aiocb);
    } else {
      struct sigevent sev;
      sev.sigev_notify = SIGEV_KEVENT;
      sev.sigev_notify_kqueue = ctx;
      sev.sigev_value.sival_ptr = &(*cur);
      r = lio_listio(LIO_NOWAIT, &cur->aio.aiocbp, cur->n_aiocb, &sev);
    }
    ++cur;
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
    attempts = submit_retries;
    delay = initial_delay_us;
    pushed = pulled = 0;
  }
  return done;
}

int aio_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
#if defined(HAVE_LIBAIO)
  io_event events[max];
#elif defined(HAVE_POSIXAIO)
  struct kevent events[max];
#endif
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };

  int r = 0;
  do {
#if defined(HAVE_LIBAIO)
    r = io_getevents(ctx, 1, max, events, &t);
#elif defined(HAVE_POSIXAIO)
    r = kevent(ctx, NULL, 0, events, max, &t);
    if (r < 0)
      r = -errno;
#endif
  } while (r == -EINTR);

  for (int i=0; i<r; ++i) {
#if defined(HAVE_LIBAIO)
    paio[i] = (aio_t *)events[i].obj;
    paio[i]->rval = events[i].res;
#else
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
#endif
  }
  return r;
}
