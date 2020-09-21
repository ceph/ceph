// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "io_uring.h"

#if defined(HAVE_LIBURING) && defined(__x86_64__)

#include "liburing.h"
#include <sys/epoll.h>

/* Options */

static bool hipri = false;      /* use IO polling */
static bool sq_thread = false;  /* use kernel submission/poller thread */

struct ioring_data {
  struct io_uring io_uring;
  pthread_mutex_t cq_mutex;
  pthread_mutex_t sq_mutex;
  int epoll_fd = -1;
  std::map<int, int> fixed_fds_map;
};

static int ioring_get_cqe(struct ioring_data *d, unsigned int max,
			  struct aio_t **paio)
{
  struct io_uring *ring = &d->io_uring;
  struct io_uring_cqe *cqe;

  unsigned nr = 0;
  unsigned head;
  io_uring_for_each_cqe(ring, head, cqe) {
    struct aio_t *io = (struct aio_t *)(uintptr_t) io_uring_cqe_get_data(cqe);
    io->rval = cqe->res;

    paio[nr++] = io;

    if (nr == max)
      break;
  }
  io_uring_cq_advance(ring, nr);

  return nr;
}

static int find_fixed_fd(struct ioring_data *d, int real_fd)
{
  auto it = d->fixed_fds_map.find(real_fd);
  if (it == d->fixed_fds_map.end())
    return -1;

  return it->second;
}

static void init_sqe(struct ioring_data *d, struct io_uring_sqe *sqe,
		     struct aio_t *io)
{
  int fixed_fd = find_fixed_fd(d, io->fd);

  ceph_assert(fixed_fd != -1);

  if (io->iocb.aio_lio_opcode == IO_CMD_PWRITEV)
    io_uring_prep_writev(sqe, fixed_fd, &io->iov[0],
			 io->iov.size(), io->offset);
  else if (io->iocb.aio_lio_opcode == IO_CMD_PREADV)
    io_uring_prep_readv(sqe, fixed_fd, &io->iov[0],
			io->iov.size(), io->offset);
  else
    ceph_assert(0);

  io_uring_sqe_set_data(sqe, io);
  io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
}

static int ioring_queue(struct ioring_data *d, void *priv,
			list<aio_t>::iterator beg, list<aio_t>::iterator end)
{
  struct io_uring *ring = &d->io_uring;
  struct aio_t *io = nullptr;

  ceph_assert(beg != end);

  do {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe)
      break;

    io = &*beg;
    io->priv = priv;

    init_sqe(d, sqe, io);

  } while (++beg != end);

  if (!io)
    /* Queue is full, go and reap something first */
    return 0;

  return io_uring_submit(ring);
}

static void build_fixed_fds_map(struct ioring_data *d,
				std::vector<int> &fds)
{
  int fixed_fd = 0;
  for (int real_fd : fds) {
    d->fixed_fds_map[real_fd] = fixed_fd++;
  }
}

ioring_queue_t::ioring_queue_t(unsigned iodepth_) :
  d(make_unique<ioring_data>()),
  iodepth(iodepth_)
{
}

ioring_queue_t::~ioring_queue_t()
{
}

int ioring_queue_t::init(std::vector<int> &fds)
{
  unsigned flags = 0;

  pthread_mutex_init(&d->cq_mutex, NULL);
  pthread_mutex_init(&d->sq_mutex, NULL);

  if (hipri)
    flags |= IORING_SETUP_IOPOLL;
  if (sq_thread)
    flags |= IORING_SETUP_SQPOLL;

  int ret = io_uring_queue_init(iodepth, &d->io_uring, flags);
  if (ret < 0)
    return ret;

  ret = io_uring_register(d->io_uring.ring_fd, IORING_REGISTER_FILES,
			  &fds[0], fds.size());
  if (ret < 0) {
    ret = -errno;
    goto close_ring_fd;
  }

  build_fixed_fds_map(d.get(), fds);

  d->epoll_fd = epoll_create1(0);
  if (d->epoll_fd < 0) {
    ret = -errno;
    goto close_ring_fd;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ret = epoll_ctl(d->epoll_fd, EPOLL_CTL_ADD, d->io_uring.ring_fd, &ev);
  if (ret < 0) {
    ret = -errno;
    goto close_epoll_fd;
  }

  return 0;

close_epoll_fd:
  close(d->epoll_fd);
close_ring_fd:
  io_uring_queue_exit(&d->io_uring);

  return ret;
}

void ioring_queue_t::shutdown()
{
  d->fixed_fds_map.clear();
  close(d->epoll_fd);
  d->epoll_fd = -1;
  io_uring_queue_exit(&d->io_uring);
}

int ioring_queue_t::submit_batch(aio_iter beg, aio_iter end,
                                 uint16_t aios_size, void *priv,
                                 int *retries)
{
  (void)aios_size;
  (void)retries;

  pthread_mutex_lock(&d->sq_mutex);
  int rc = ioring_queue(d.get(), priv, beg, end);
  pthread_mutex_unlock(&d->sq_mutex);

  return rc;
}

int ioring_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
get_cqe:
  pthread_mutex_lock(&d->cq_mutex);
  int events = ioring_get_cqe(d.get(), max, paio);
  pthread_mutex_unlock(&d->cq_mutex);

  if (events == 0) {
    struct epoll_event ev;
    int ret = epoll_wait(d->epoll_fd, &ev, 1, timeout_ms);
    if (ret < 0)
      events = -errno;
    else if (ret > 0)
      /* Time to reap */
      goto get_cqe;
  }

  return events;
}

bool ioring_queue_t::supported()
{
  struct io_uring_params p;

  memset(&p, 0, sizeof(p));
  int fd = io_uring_setup(16, &p);
  if (fd < 0)
    return false;

  close(fd);

  return true;
}

#else // #if defined(HAVE_LIBURING) && defined(__x86_64__)

struct ioring_data {};

ioring_queue_t::ioring_queue_t(unsigned iodepth_)
{
  ceph_assert(0);
}

ioring_queue_t::~ioring_queue_t()
{
  ceph_assert(0);
}

int ioring_queue_t::init(std::vector<int> &fds)
{
  ceph_assert(0);
}

void ioring_queue_t::shutdown()
{
  ceph_assert(0);
}

int ioring_queue_t::submit_batch(aio_iter beg, aio_iter end,
                                 uint16_t aios_size, void *priv,
                                 int *retries)
{
  ceph_assert(0);
}

int ioring_queue_t::get_next_completed(int timeout_ms, aio_t **paio, int max)
{
  ceph_assert(0);
}

bool ioring_queue_t::supported()
{
  return false;
}

#endif // #if defined(HAVE_LIBURING) && defined(__x86_64__)
