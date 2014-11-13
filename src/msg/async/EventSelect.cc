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

#include "common/errno.h"
#include "EventSelect.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "SelectDriver."

int SelectDriver::init(int nevent)
{
  ldout(cct, 0) << "Select isn't suitable for production env, just avoid "
                << "compiling error or special purpose" << dendl;
  FD_ZERO(&rfds);
  FD_ZERO(&wfds);
  max_fd = 0;
  return 0;
}

int SelectDriver::add_event(int fd, int cur_mask, int add_mask)
{
  ldout(cct, 10) << __func__ << " add event to fd=" << fd << " mask=" << add_mask
                 << dendl;

  int mask = cur_mask | add_mask;
  if (mask & EVENT_READABLE)
    FD_SET(fd, &rfds);
  if (mask & EVENT_WRITABLE)
    FD_SET(fd, &wfds);
  if (fd > max_fd)
      max_fd = fd;

  return 0;
}

void SelectDriver::del_event(int fd, int cur_mask, int delmask)
{
  ldout(cct, 10) << __func__ << " del event fd=" << fd << " cur mask=" << cur_mask
                 << dendl;

  if (delmask & EVENT_READABLE)
    FD_CLR(fd, &rfds);
  if (delmask & EVENT_WRITABLE)
    FD_CLR(fd, &wfds);
}

int SelectDriver::resize_events(int newsize)
{
  return 0;
}

int SelectDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
  int retval, j, numevents = 0;

  memcpy(&_rfds, &rfds, sizeof(fd_set));
  memcpy(&_wfds, &wfds, sizeof(fd_set));

  retval = select(max_fd+1, &_rfds, &_wfds, NULL, tvp);
  if (retval > 0) {
    for (j = 0; j <= max_fd; j++) {
      int mask = 0;
      struct FiredFileEvent fe;
      if (FD_ISSET(j, &_rfds))
          mask |= EVENT_READABLE;
      if (FD_ISSET(j, &_wfds))
          mask |= EVENT_WRITABLE;
      if (mask) {
        fe.fd = j;
        fe.mask = mask;
        fired_events.push_back(fe);
        numevents++;
      }
    }
  }
  return numevents;
}
