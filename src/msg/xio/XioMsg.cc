// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XioMessenger.h"
#include "XioConnection.h"
#include "XioMsg.h"


int XioDispatchHook::release_msgs()
{
  XioCompletion *xcmp;
  int r = msg_seq.size();
  cl_flag = true;

  /* queue for release */
  xcmp = static_cast<XioCompletion *>(rsp_pool.alloc(sizeof(XioCompletion)));
  new (xcmp) XioCompletion(xcon, this);

  /* merge with portal traffic */
  xcon->portal->enqueue_for_send(xcon, xcmp);

  assert(r);
  return r;
}
