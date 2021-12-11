// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <rte_ethdev.h>
#include "common/errno.h"
#include "DPDKStack.h"
#include "EventDPDK.h"

#include "common/dout.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "DPDKDriver."
#define EVENTS_MAX 64

int DPDKDriver::init(EventCenter *c, int nevent)
{
	epfd = epoll_create(EVENTS_MAX);
	if (epfd < 0) {
            ldout(cct, 0) << __func__ << "can't create epoll fd" << dendl;
	    return -EIO;
	}
	if (::fcntl(epfd, F_SETFD, FD_CLOEXEC) == -1) {
	   int e = errno;
	   ::close(epfd);
	   lderr(cct) << __func__<< " unable to set cloexec: " << strerror(errno) << dendl;

	   return e;
	}
	return 0;
}

int DPDKDriver::add_event(int fd, int cur_mask, int add_mask)
{
	ldout(cct, 20) << __func__ << " add event fd=" << fd << " cur_mask=" << cur_mask
		       << " add_mask=" << add_mask << dendl;
        int r;
	if (fd != wakeup_fd) {
	   r = manager.listen(fd, add_mask);
	} else {
	    struct epoll_event ee;
	    ee.events = EPOLLIN | EPOLLET;
	    ee.data.u64 = 0;
	    ee.data.fd = wakeup_fd;
	    r = epoll_ctl(epfd, EPOLL_CTL_ADD, wakeup_fd, &ee);
	}
        if (r < 0) {
	    lderr(cct) << __func__<< " add fd=" << fd << " failed. " << cpp_strerror(-r) << dendl;
	    return -errno;
	}
	return 0;
}

int DPDKDriver::del_event(int fd, int cur_mask, int delmask)
{
	ldout(cct, 20) << __func__ << " del event fd=" << fd << " cur_mask=" << cur_mask << " delmask=" << delmask << dendl;
	if (delmask == EVENT_NONE)
	return 0;

	int r;
        if (fd != wakeup_fd) {
	   r = manager.unlisten(fd, delmask);
	} else {
	   r = epoll_ctl(epfd, EPOLL_CTL_DEL, wakeup_fd, nullptr);
	}
	if (r < 0) {
	   lderr(cct) << __func__ << " delete fd=" << fd << " delmask=" << delmask
		      << " failed." << cpp_strerror(-r) << dendl;
	   return r;
	}
	return 0;
}

int DPDKDriver::resize_events(int newsize)
{
        return 0;
}

int DPDKDriver::event_wait(std::vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
        int num_events = 512;
        int events[num_events];
        int masks[num_events];
        int i = 0;

        int timeout = tvp ? (tvp->tv_sec * 1000 + tvp ->tv_usec / 1000) : -1;
        if (timeout) {
           struct epoll_event ee[EVENTS_MAX];
           int retval = epoll_wait(epfd, ee, EVENTS_MAX, timeout);
           for (int event_id = 0;event_id < retval; event_id++) {
               struct epoll_event *e = &ee[event_id];
               if (e->data.u64 & 0xFFFFFFFF00000000) {
                 struct rte_epoll_event *rev = static_cast <struct rte_epoll_event *>(e->data.ptr);
                 if (!rev || !rte_atomic32_cmpset(&rev->status, RTE_EPOLL_VALID, RTE_EPOLL_EXEC))
                   continue;
                 if (rev->epdata.cb_fun)
                   rev->epdata.cb_fun(rev->fd, rev->epdata.cb_arg);

                 (*static_cast<std::function<void ()>*>(rev->epdata.data))();
                 rte_compiler_barrier();
                 rev->status = RTE_EPOLL_VALID;
                 continue;
               }
               fired_events.push_back({e->data.fd, EVENT_READABLE});
               i++;
           }
        }

        int retval = manager.poll(events, masks, num_events, tvp);
        if (retval > 0) {
           fired_events.resize(retval + i);
           for (int k=0; k < retval; k++, i++) {
             fired_events[i].fd = events[k];
             fired_events[i].mask = masks[k];
           }
        }
        return i;
}
