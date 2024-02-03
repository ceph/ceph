// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Rafael Lopez <rafael.lopez@softiron.com>
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "common/errno.h"
#include "EventPoll.h"

#include <unistd.h>
#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "PollDriver."

#ifndef POLL_ADD
#define POLL_ADD 1
#ifndef POLL_MOD
#define POLL_MOD 2
#ifndef POLL_DEL
#define POLL_DEL 3
#endif
#endif
#endif

int PollDriver::init(EventCenter *c, int nevent) {
  // pfds array will auto scale up to hard_max_pfds, which should be
  // greater than total daemons/op_threads (todo: cfg option?)
  hard_max_pfds = 8192;
  // 128 seems a good starting point, cover clusters up to ~350 OSDs
  // with default ms_async_op_threads
  max_pfds = 128;

  pfds = (POLLFD*)calloc(max_pfds, sizeof(POLLFD));
  if (!pfds) {
    lderr(cct) << __func__ << " unable to allocate memory " << dendl;
    return -ENOMEM;
  }

  //initialise pfds
  for(int i = 0; i < max_pfds; i++){
    pfds[i].fd = -1;
    pfds[i].events = 0;
    pfds[i].revents = 0;
  }
  return 0;
}

// Helper func to register/unregister interest in a FD's events by
// manipulating it's entry in pfds array
int PollDriver::poll_ctl(int fd, int op, int events) {
  int pos = 0;
  if (op == POLL_ADD) {
    // Find an empty pollfd slot
    for(pos = 0; pos < max_pfds ; pos++){
      if(pfds[pos].fd == -1){
	pfds[pos].fd = fd;
	pfds[pos].events = events;
	pfds[pos].revents = 0;
	return 0;
      }
    }
    // We ran out of slots, try to increase
    if (max_pfds < hard_max_pfds) {
      ldout(cct, 10) << __func__ << " exhausted pollfd slots"
		     << ", doubling to " << max_pfds*2 << dendl;
      pfds = (POLLFD*)realloc(pfds, max_pfds*2*sizeof(POLLFD));
      if (!pfds) {
	lderr(cct) << __func__ << " unable to realloc for more pollfd slots"
		   << dendl;
	return -ENOMEM;
      }
      // Initialise new slots
      for (int i = max_pfds ; i < max_pfds*2 ; i++){
	pfds[i].fd = -1;
	pfds[i].events = 0;
	pfds[i].revents = 0;
      }
      max_pfds = max_pfds*2;
      pfds[pos].fd = fd;
      pfds[pos].events = events;
      pfds[pos].revents = 0;
      return 0;
    } else {
    // Hit hard limit
    lderr(cct) << __func__ << " hard limit for file descriptors per op" 
	       << " thread reached (" << hard_max_pfds << ")" << dendl;
    return -EMFILE;
    }
  } else if (op == POLL_MOD) {
    for (pos = 0; pos < max_pfds; pos++ ){
      if (pfds[pos].fd == fd) {
	pfds[pos].events = events;
	return 0;
      }
    }
  } else if (op == POLL_DEL) {
    for (pos = 0; pos < max_pfds; pos++ ){
      if (pfds[pos].fd == fd) {
	pfds[pos].fd = -1;
	pfds[pos].events = 0;
	return 0;
      }
    }
  }
  return 0;
}

int PollDriver::add_event(int fd, int cur_mask, int add_mask) {
  ldout(cct, 10) << __func__ << " add event to fd=" << fd << " mask="
		 << add_mask << dendl;
  int op, events = 0;
  op = cur_mask == EVENT_NONE ? POLL_ADD: POLL_MOD;

  add_mask |= cur_mask; /* Merge old events */
  if (add_mask & EVENT_READABLE) {
    events |= POLLIN;
  }
  if (add_mask & EVENT_WRITABLE) {
    events |= POLLOUT;
  }
  int ret = poll_ctl(fd, op, events);
  return ret;
}

int PollDriver::del_event(int fd, int cur_mask, int delmask) {
  ldout(cct, 10) << __func__ << " del event fd=" << fd << " cur mask="
		 << cur_mask << dendl;
  int op, events = 0;
  int mask = cur_mask & (~delmask);

  if (mask != EVENT_NONE) {
    op = POLL_MOD;
    if (mask & EVENT_READABLE) {
      events |= POLLIN;
    }
    if (mask & EVENT_WRITABLE) {
      events |= POLLOUT;
    }
  } else {
    op = POLL_DEL;
  }
  poll_ctl(fd, op, events);
  return 0;
}

int PollDriver::resize_events(int newsize) {
  return 0;
}

int PollDriver::event_wait(std::vector<FiredFileEvent> &fired_events,
			  struct timeval *tvp) {
  int retval, numevents = 0;
#ifdef _WIN32
  retval = WSAPoll(pfds, max_pfds,
		      tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
#else
  retval = poll(pfds, max_pfds,
		      tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
#endif
  if (retval > 0) {
    for (int j = 0; j < max_pfds; j++) {
      if (pfds[j].fd != -1) {
	int mask = 0;
	struct FiredFileEvent fe;
	if (pfds[j].revents & POLLIN) {
	  mask |= EVENT_READABLE;
	}
	if (pfds[j].revents & POLLOUT) {
	  mask |= EVENT_WRITABLE;
	}
	if (pfds[j].revents & POLLHUP) {
	  mask |= EVENT_READABLE | EVENT_WRITABLE;
	}
	if (pfds[j].revents & POLLERR) {
	  mask |= EVENT_READABLE | EVENT_WRITABLE;
	}
	if (mask) {
	  fe.fd = pfds[j].fd;
	  fe.mask = mask;
	  fired_events.push_back(fe);
	  numevents++;
	}
      }
    }
  }
  return numevents;
}
