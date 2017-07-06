// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "aio.h"

#if defined(HAVE_LIBAIO)


int aio_queue_t::submit(aio_t &aio, int *retries)
{
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;
  iocb *piocb = &aio.iocb;
  int r;
  while (true) {
    r = io_submit(ctx, 1, &piocb);
    if (r < 0) {
      if (r == -EAGAIN && attempts-- > 0) {
	usleep(delay);
	delay *= 2;
	(*retries)++;
	continue;
      }
    }
    assert(r == 1);
    break;
  }
  return r;
}

int aio_queue_t::submit_batch(aio_iter begin, aio_iter end, 
			      uint16_t aios_size, void *priv, 
			      int *retries)
{
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;

  aio_iter cur = begin;
  struct iocb *piocb[aios_size];
  int r, pos = 0;
  while (cur != end) {
    cur->priv = priv;
    *(piocb+pos) = &cur->iocb;
    ++pos;
    ++cur;
  }
  while (true) {
    r = io_submit(ctx, pos, piocb);
    if (r < 0) {
      if (r == -EAGAIN && attempts-- > 0) {
	usleep(delay);
	delay *= 2;
	(*retries)++;
	continue;
      }
    }
    break;
  }
  return r;
}

int aio_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
  io_event event[max];
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };

  int r = 0;
  do {
    r = io_getevents(ctx, 1, max, event, &t);
  } while (r == -EINTR);

  for (int i=0; i<r; ++i) {
    paio[i] = (aio_t *)event[i].obj;
    paio[i]->rval = event[i].res;
  }
  return r;
}

#endif
