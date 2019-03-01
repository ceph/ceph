// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * Polling from userspace support by Roman Penyaev <rpenyaev@suse.de>
 * Copyright (C) 2019 SUSE.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include <fcntl.h>
#include "EventEpoll.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EpollDriver."

#define BUILD_BUG_ON(condition) ((void )sizeof(char [1 - 2*!!(condition)]))
#define READ_ONCE(v) (*(volatile decltype(v)*)&(v))

#ifdef __x86_64__
#ifndef __NR_sys_epoll_create2
#define __NR_sys_epoll_create2  428
#endif
#endif

static inline long epoll_create2(int flags, size_t size)
{
#ifdef __NR_sys_epoll_create2
  return syscall(__NR_sys_epoll_create2, flags, size);
#else
  errno = ENOSYS;
  return -1;
#endif
}

int EpollDriver::uepoll_mmap(int epfd,
			     struct epoll_uheader **_header,
			     unsigned int **_index)
{
  struct epoll_uheader *header;
  unsigned int *index, len;
  int rc;

  len = sysconf(_SC_PAGESIZE);
again:
  header = (decltype(header))mmap(NULL, len, PROT_WRITE|PROT_READ,
				  MAP_SHARED, epfd, 0);
  if (header == MAP_FAILED) {
    rc = -errno;
    lderr(cct) << "mmap(header): " << cpp_strerror(errno) << dendl;
    return rc;
  }

  if (header->header_length != len) {
    unsigned int tmp_len = len;

    len = header->header_length;
    munmap(header, tmp_len);
    goto again;
  }
  ceph_assert(header->magic == EPOLL_USERPOLL_HEADER_MAGIC);

  index = (decltype(index))mmap(NULL, header->index_length,
				PROT_WRITE|PROT_READ, MAP_SHARED,
				epfd, header->header_length);
  if (index == MAP_FAILED) {
    rc = -errno;
    lderr(cct) << "mmap(index): " << cpp_strerror(errno) << dendl;
    return rc;
  }

  BUILD_BUG_ON(sizeof(*header) != EPOLL_USERPOLL_HEADER_SIZE);
  BUILD_BUG_ON(sizeof(header->items[0]) != 16);

  *_header = header;
  *_index = index;

  return 0;
}

int EpollDriver::init(EventCenter *c, int nevent)
{
  use_uepoll = g_ceph_context->_conf.get_val<bool>("ms_uepoll");
  events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*nevent);
  if (!events) {
    lderr(cct) << __func__ << " unable to malloc memory. " << dendl;
    return -ENOMEM;
  }
  memset(events, 0, sizeof(struct epoll_event)*nevent);

  if (use_uepoll) {
    epfd = epoll_create2(EPOLL_USERPOLL, 1024);
    if (epfd >= 0) {
      /* Mmap all pointers */
      uepoll_mmap(epfd, &uheader, &uindex);
      lderr(cct) << "UEPOLL enabled!" << dendl;
    }
  } else {
    epfd = epoll_create1(0);
  }
  if (epfd == -1) {
    lderr(cct) << __func__ << " unable to do epoll_create: "
                       << cpp_strerror(errno) << dendl;
    return -errno;
  }
  if (::fcntl(epfd, F_SETFD, FD_CLOEXEC) == -1) {
    int e = errno;
    ::close(epfd);
    lderr(cct) << __func__ << " unable to set cloexec: "
                       << cpp_strerror(e) << dendl;

    return -e;
  }

  size = nevent;

  return 0;
}

int EpollDriver::add_event(int fd, int cur_mask, int add_mask)
{
  ldout(cct, 20) << __func__ << " add event fd=" << fd << " cur_mask=" << cur_mask
                 << " add_mask=" << add_mask << " to " << epfd << dendl;
  struct epoll_event ee;
  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op;
  op = cur_mask == EVENT_NONE ? EPOLL_CTL_ADD: EPOLL_CTL_MOD;

  ee.events = EPOLLET;
  add_mask |= cur_mask; /* Merge old events */
  if (add_mask & EVENT_READABLE)
    ee.events |= EPOLLIN;
  if (add_mask & EVENT_WRITABLE)
    ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (epoll_ctl(epfd, op, fd, &ee) == -1) {
    lderr(cct) << __func__ << " epoll_ctl: add fd=" << fd << " failed. "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  return 0;
}

int EpollDriver::del_event(int fd, int cur_mask, int delmask)
{
  ldout(cct, 20) << __func__ << " del event fd=" << fd << " cur_mask=" << cur_mask
                 << " delmask=" << delmask << " to " << epfd << dendl;
  struct epoll_event ee = {0};
  int mask = cur_mask & (~delmask);
  int r = 0;

  if (mask != EVENT_NONE) {
    ee.events = EPOLLET;
    ee.data.fd = fd;
    if (mask & EVENT_READABLE)
      ee.events |= EPOLLIN;
    if (mask & EVENT_WRITABLE)
      ee.events |= EPOLLOUT;

    if ((r = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: modify fd=" << fd << " mask=" << mask
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    if ((r = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: delete fd=" << fd
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  }
  return 0;
}

int EpollDriver::resize_events(int newsize)
{
  return 0;
}

static inline unsigned int max_index_nr(struct epoll_uheader *header)
{
  return header->index_length >> 2;
}

static inline bool read_event(struct epoll_uheader *header, unsigned int *index,
			      unsigned int idx, struct epoll_event *event)
{
  struct epoll_uitem *item;
  unsigned int *item_idx_ptr;
  unsigned int indeces_mask;

  indeces_mask = max_index_nr(header) - 1;
  if (indeces_mask & max_index_nr(header)) {
    /* Should be pow2, corrupted header? */
    return false;
  }

  item_idx_ptr = &index[idx & indeces_mask];

  /*
   * Spin here till we see valid index
   */
  while (!(idx = __atomic_load_n(item_idx_ptr, __ATOMIC_ACQUIRE)))
    ;

  if (idx > header->max_items_nr) {
    /* Corrupted index? */
    return false;
  }

  item = &header->items[idx - 1];

  /*
   * Mark index as invalid, that is for userspace only, kernel does not care
   * and will refill this pointer only when observes that event is cleared,
   * which happens below.
   */
  *item_idx_ptr = 0;

  /*
   * Fetch data first, if event is cleared by the kernel we drop the data
   * returning false.
   */
  event->data.u64 = item->data;
  event->events = __atomic_exchange_n(&item->ready_events, 0,
				      __ATOMIC_RELEASE);

  return (event->events & ~EPOLLREMOVED);
}

int EpollDriver::uepoll_wait(int ep, struct epoll_event *events,
			     int maxevents, int timeout)

{
  unsigned int spins = 1000000;
  unsigned int tail;
  int i;

  ceph_assert(maxevents > 0);

again:
  /*
   * Cache the tail because we don't want refetch it on each iteration
   * and then catch live events updates, i.e. we don't want user @events
   * array consist of events from the same fds.
   */
  tail = READ_ONCE(uheader->tail);

  if (uheader->head == tail && timeout != 0) {
    if (spins--)
      /* Busy loop a bit */
      goto again;

    i = epoll_wait(ep, NULL, 0, timeout);
    ceph_assert(i <= 0);
    if (i == 0 || (i < 0 && errno != ESTALE))
      return i;

    tail = READ_ONCE(uheader->tail);
    ceph_assert(uheader->head != tail);
  }

  for (i = 0; uheader->head != tail && i < maxevents; uheader->head++) {
    if (read_event(uheader, uindex, uheader->head, &events[i]))
      i++;
    else {
      /*
       * Event cleared by kernel because EPOLL_CTL_DEL was called,
       * nothing interesting, continue.
       */
    }
  }

  return i;
}

int EpollDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
  int retval, numevents = 0;

  if (use_uepoll) {
    retval = uepoll_wait(epfd, events, size,
			 tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
  } else {
    retval = epoll_wait(epfd, events, size,
			tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
  }
  if (retval > 0) {
    int j;

    numevents = retval;
    fired_events.resize(numevents);
    for (j = 0; j < numevents; j++) {
      int mask = 0;
      struct epoll_event *e = events + j;

      if (e->events & EPOLLIN) mask |= EVENT_READABLE;
      if (e->events & EPOLLOUT) mask |= EVENT_WRITABLE;
      if (e->events & EPOLLERR) mask |= EVENT_READABLE|EVENT_WRITABLE;
      if (e->events & EPOLLHUP) mask |= EVENT_READABLE|EVENT_WRITABLE;
      fired_events[j].fd = e->data.fd;
      fired_events[j].mask = mask;
    }
  }
  return numevents;
}
