// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
    cur->aio.aio_sigevent.sigev_notify = SIGEV_KEVENT;
    cur->aio.aio_sigevent.sigev_notify_kqueue = ctx;
    cur->aio.aio_sigevent.sigev_notify_kevent_flags = EV_ONESHOT;
    cur->aio.aio_sigevent.sigev_value.sival_ptr = &(*cur);
    if (cur->aio.aio_lio_opcode == LIO_WRITE) {
      r = aio_writev(&cur->aio);
    } else {
      r = aio_readv(&cur->aio);
    }
    // aio_writev()/aio_readv() follow the classic POSIX convention: 0 on
    // success, -1 with errno set on failure -- not -errno, and not a count
    // of submitted items like Linux io_submit(). Normalize to Ceph's
    // -errno convention so the retry handling below works.
    if (r < 0) {
      r = -errno;
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
#if defined(HAVE_LIBAIO)
    ceph_assert(r > 0);
    done += r;
#elif defined(HAVE_POSIXAIO)
    // Success means r == 0; count one aio_t submitted (matching the
    // pending/running accounting in KernelDevice, which is per aio_t).
    // Advance only here -- on -EAGAIN the retry above must resubmit this
    // same aio_t, not skip past it.
    ++cur;
    done += 1;
#endif
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
#elif defined(HAVE_POSIXAIO)
    paio[i] = (aio_t*)events[i].udata;
    // Always reap with aio_return(), even when aio_error() reports failure:
    // a completed aiocb that is never returned leaks its kernel job entry.
    int err = aio_error(&paio[i]->aio);
    ssize_t ret = aio_return(&paio[i]->aio);
    paio[i]->rval = err ? -err : ret;
#endif
  }
  return r;
}

