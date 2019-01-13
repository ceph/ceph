// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>

#include "acconfig.h"
#include "include/compat.h"
#include "ceph_aio.h"
#include "common/errno.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev:aio:" << __LINE__ << " "

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

#if defined(HAVE_POSIXAIO)
std::ostream& operator<<(std::ostream& os, const struct aiocb *cb)
{
  os << "aiocb @ "    << &cb << "(";
  os << "fildes = "   << cb->aio_fildes;
  os << ", offset = " << cb->aio_offset;
  os << ", nbytes = " << cb->aio_nbytes;
  os << ", buf = 0x"    << std::hex << (size_t)cb->aio_buf << std::dec;
  os << ", lio_opcode = " << cb->aio_lio_opcode;
  os << ", priv.status = " << cb->_aiocb_private.status;
  os << ", priv.error = " << cb->_aiocb_private.error;
  os << ", priv.sigevent.notify = " << cb->aio_sigevent.sigev_notify;
  os << ", priv.sigevent.notify_kqueue = " << cb->aio_sigevent.sigev_notify_kqueue;
  os << ", priv.sigevent.kevent_flags = " << cb->aio_sigevent.sigev_notify_kevent_flags;
  os << ")";
  return os;
}
#endif

int aio_queue_t::submit_batch(aio_iter begin, aio_iter end, 
			      uint16_t aios_size, void *priv, 
			      int *retries)
{
  // 2^16 * 125us = ~8 seconds, so max sleep is ~16 seconds
  int attempts = 16;
  int delay = 125;
  int r;

  dout(20) << __func__
           << " aios_size = " << aios_size
           << " retries = " << (*retries)
           << dendl;

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
    r = io_submit(ctx, std::min(left, max_iodepth), (struct iocb**)(piocb + done));
#elif defined(HAVE_POSIXAIO)
    dout(20) << __func__
        << " left = " << left
        << " aio " << &(piocb[done]->aio)
        << dendl;
    if (piocb[done]->n_aiocb == 1) {
      dout(25) << __func__
          << "  n_aiocb == 1 "
          << " &piocb[done]->aio.aiocbp[0] = " << &(piocb[done]->aio.aiocbp[0])
          << " on kqueue = " << ctx
          << " for (fd, off, len) = ("
            << piocb[done]->aio.aiocbp[0].aio_fildes << ","
            << piocb[done]->aio.aiocbp[0].aio_offset << ","
            << piocb[done]->aio.aiocbp[0].aio_nbytes
          << ")"
          << dendl;
      ceph_assert( 0 < piocb[done]->aio.aiocbp[0].aio_fildes
                   && piocb[done]->aio.aiocbp[0].aio_fildes < 4096 );
      // TODO: consider batching multiple reads together with lio_listio
      piocb[done]->aio.aiocbp[0].aio_sigevent.sigev_notify = SIGEV_KEVENT;
      piocb[done]->aio.aiocbp[0].aio_sigevent.sigev_notify_kqueue = ctx;
      piocb[done]->aio.aiocbp[0].aio_sigevent.sigev_notify_kevent_flags =
                                                                EV_ONESHOT;
      // make that kevent can read back this piocb
      piocb[done]->aio.aiocbp[0].aio_sigevent.sigev_value.sival_ptr =
                                                    (void*)&(piocb[done]->aio);
      r = 0;
      switch (piocb[done]->aio.aiocbp[0].aio_lio_opcode) {
        case LIO_WRITE:
          r = aio_write(&(piocb[done]->aio.aiocbp[0]));
          break;
        case LIO_READ:
          r = aio_read(&(piocb[done]->aio.aiocbp[0]));
          break;
        case LIO_NOP:
          break;
        default:
          ;;
      }
      if (r < 0 )
        r = -errno;
      else
        r = 1;
    } else {
      dout(30) << __func__
               << "  using lio_listio"
               << dendl;
      struct sigevent sev;
      sev.sigev_notify = SIGEV_KEVENT;
      sev.sigev_notify_kqueue = ctx;
      sev.sigev_value.sival_ptr = piocb[done];
      r = lio_listio(LIO_NOWAIT, &piocb[done]->aio.aiocbp,
                     piocb[done]->n_aiocb, &sev);
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

#if defined(HAVE_LIBAIO)
int aio_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
  io_event events[max];
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };

  int r = 0;
  do {
    r = io_getevents(ctx, 1, max, events, &t);
  } while (r == -EINTR);

  for (int i=0; i<r; ++i) {
    paio[i] = (aio_t *)events[i].obj;
    paio[i]->rval = events[i].res;
  }
  return r;
}
#elif defined(HAVE_POSIXAIO)
// return value: the number of completed AIOs.
//        paio:  the completed AIOs
//        paio->rval: the number of bytes read/written
// or            zero if a timeout has occurred
// or            any negative return will result in a assert upon return

int aio_queue_t::get_next_completed(int fd, int timeout_ms, aio_t **paio, int max)
{
  struct timespec t = {
    timeout_ms / 1000,
    (timeout_ms % 1000) * 1000 * 1000
  };

  struct kevent events[max];
  struct kevent ev;
  dout(21) << __func__ << " kqueue registration"
      << " on fd = " << fd
      << " , with kqueue handle ctx = " << ctx
      << dendl;

  EV_SET(&ev, fd, 0, EV_ADD | EV_CLEAR, 0, 0, 0);
  int rev = kevent(ctx, &ev, 1, NULL, 0, 0 );
  if (rev < 0) {
    dout(1) << __func__ << " kqueue registration error "
        << " kevent return(" << errno << ") "
        << cpp_strerror(errno)
        << dendl;
    return -errno;
  }
  do {
    dout(21) << __func__ << " running POSIX AIO" << dendl;
    memset(&events, 0, sizeof(events));
    rev = kevent(ctx, NULL, 0, events, max, &t);
    dout(25) << __func__
        << " on kqueue handle = " << ctx
        << " kevent return = " << rev
        << " max = " << max
        << dendl;
    if (rev < 0) {
        dout(5) << __func__
            << " error event " << cpp_strerror(errno)
            << dendl;
        return -errno;
    } else if (rev == 0) {
        // Got a timeout
        timeouts++;
        dout(30) << __func__ << " return"
            << " timeouts " << timeouts
            << " rev = 0"
            << dendl;
        return rev;
      } else {
        dout(25) << __func__ << " process " << rev << " events"
            << dendl;
        // Process all the events posted
        // Emulate the return value of pwritev. I can't find any documentation
        // for what the value of io_event.res is supposed to be. I'm going to
        // assume that it's just like pwritev/preadv/pwrite/pread.
        for(int i = 0; i < rev; i++) {
            struct aio_t* aio = (struct aio_t*)events[i].udata;
            struct aiocb* slot = &(aio->aio.aiocbp[i]);
            dout(10) << __func__ << " slot = " << &slot << dendl;
            int ree = aio_error(slot);
            if (ree < 0) {
                dout(10) << __func__ << " aio_error" << cpp_strerror(errno)
                    << dendl;
            }
            int res = aio_return(slot);
            if (res < 0) {
                // Most common error to be returned here is 22: Invalid argument
                // Meaning that there was a problem submitting the aio in
                // aio_{read/write}. Usually a coding problem in submit_batch
                dout(1) << __func__
                    << " error in processing event i = " << i
                    << " aio_return"
                    << cpp_strerror(errno)
                    << dendl;
                return -errno;
            } else {
                dout(10) << __func__ << " aio completed: "
                    << res << " bytes."
                    << dendl;
                paio[i]         = aio;
                paio[i]->rval   = res;
                paio[i]->fd     = slot->aio_fildes;
                paio[i]->offset = slot->aio_offset;
                paio[i]->length = slot->aio_nbytes;
                paio[i]->cct    = cct;
                paio[i]->priv   = aio->priv;
                dout(25) << __func__
                         << " processing event i = " << i
                         << " ident = "  << events[i].ident
                         << " flags = "  << events[i].flags
                         << " fflags = " << events[i].fflags
                         << " udata = "  << aio
                         << " aiocbp = " << slot
                         << dendl;
           }
        }
    }
  } while (rev == 0);

  dout(21) << __func__
           << " finished return = " << rev
           << dendl;
  return rev;
}
#endif
