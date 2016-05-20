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

#include "common/errno.h"
#include "DPDKStack.h"
#include "EventDPDK.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "DPDKDriver."

int DPDKDriver::init(EventCenter *c, int nevent)
{
	return 0;
}

int DPDKDriver::add_event(int fd, int cur_mask, int add_mask)
{
	ldout(cct, 20) << __func__ << " add event fd=" << fd << " cur_mask=" << cur_mask
								 << " add_mask=" << add_mask << dendl;

	int r = manager.listen(fd, add_mask);
	if (r < 0) {
		lderr(cct) << __func__ << " add fd=" << fd << " failed. "
		           << cpp_strerror(-r) << dendl;
		return -errno;
	}

	return 0;
}

int DPDKDriver::del_event(int fd, int cur_mask, int delmask)
{
	ldout(cct, 20) << __func__ << " del event fd=" << fd << " cur_mask=" << cur_mask
								 << " delmask=" << delmask << dendl;
	int r = 0;

	if (delmask != EVENT_NONE) {
		if ((r = manager.unlisten(fd, delmask)) < 0) {
			lderr(cct) << __func__ << " delete fd=" << fd << " delmask=" << delmask
								 << " failed." << cpp_strerror(-r) << dendl;
			return r;
		}
	}
	return 0;
}

int DPDKDriver::resize_events(int newsize)
{
	return 0;
}

int DPDKDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
	int num_events = 512;
	int events[num_events];
  int masks[num_events];

	int retval = manager.poll(events, masks, num_events, tvp);
	if (retval > 0) {
		fired_events.resize(retval);
		for (int i = 0; i < retval; i++) {
			fired_events[i].fd = events[i];
			fired_events[i].mask = masks[i];
		}
	}
	return retval;
}
