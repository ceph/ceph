// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_EVENTEPOLL_H
#define CEPH_MSG_EVENTEPOLL_H

#include <unistd.h>
#include <sys/epoll.h>

#include "Event.h"

#define EPOLL_USERPOLL_HEADER_MAGIC 0xeb01eb01
#define EPOLL_USERPOLL_HEADER_SIZE  128
#define EPOLL_USERPOLL 1

/* User item marked as removed for EPOLL_USERPOLL */
#define EPOLLREMOVED	(1U << 27)

/*
 * Item, shared with userspace.  Unfortunately we can't embed epoll_event
 * structure, because it is badly aligned on all 64-bit archs, except
 * x86-64 (see EPOLL_PACKED).  sizeof(epoll_uitem) == 16
 */
struct epoll_uitem {
  uint32_t ready_events;
  uint32_t events;
  uint64_t data;
};

/*
 * Header, shared with userspace. sizeof(epoll_uheader) == 128
 */
struct epoll_uheader {
  uint32_t magic;          /* epoll user header magic */
  uint32_t header_length;  /* length of the header + items */
  uint32_t index_length;   /* length of the index ring, always pow2 */
  uint32_t max_items_nr;   /* max number of items */
  uint32_t head;           /* updated by userland */
  uint32_t tail;           /* updated by kernel */

  struct epoll_uitem items[]
	__attribute__((aligned(EPOLL_USERPOLL_HEADER_SIZE)));
};

class EpollDriver : public EventDriver {
  int epfd;
  struct epoll_uheader *uheader;
  unsigned int         *uindex;
  bool use_uepoll;

  struct epoll_event *events;
  CephContext *cct;
  int size;

 public:
  explicit EpollDriver(CephContext *c):
    epfd(-1),
    uheader(NULL),
    uindex(NULL),
    use_uepoll(false),
    events(NULL),
    cct(c),
    size(0)
  {}
  ~EpollDriver() override {
    if (epfd != -1)
      close(epfd);

    if (events)
      free(events);
  }

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(vector<FiredFileEvent> &fired_events,
		 struct timeval *tp) override;

private:
  int uepoll_mmap(int epfd,
		  struct epoll_uheader **_header,
		  unsigned int **_index);
  int uepoll_wait(int ep, struct epoll_event *events,
		  int maxevents, int timeout);
};

#endif
