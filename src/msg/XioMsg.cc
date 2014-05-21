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

extern XioPool *xrp_pool;

int XioCompletionHook::release_msgs()
{
  XioRsp *xrsp;
  int r = msg_seq.size();
  cl_flag = true;

  XioConnection *xcon = get_xcon();

  /* queue for release */
  xrsp = (XioRsp *) rsp_pool.alloc(sizeof(XioRsp));
  new (xrsp) XioRsp(xcon, this);

  /* merge with portal traffic */
  xcon->portal->enqueue_for_send(xcon, xrsp);

  return r;
}

void XioCompletionHook::finish(int r)
{
  this->put();
}

void XioCompletionHook::on_err_finalize(XioConnection *xcon)
{
  /* can't decode message; even with one-way must free
   * xio_msg structures, and then xiopool
   */
  this->finish(-1);
}
