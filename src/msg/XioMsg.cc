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

void XioCompletionHook::finish(int r)
{
  XioRsp *xrsp;
  struct xio_msg *msg;
  list <struct xio_msg *>::iterator iter;

  nrefs.inc();

  for (iter = msg_seq.begin(); iter != msg_seq.end(); ++iter) {
    msg = *iter;
    switch (msg->type) {
    case XIO_MSG_TYPE_ONE_WAY:
    {
      ConnectionRef conn = m->get_connection();
      XioConnection *xcon = static_cast<XioConnection*>(conn.get());

      /* queue for release */
      xrsp = (XioRsp *) rsp_pool.alloc(sizeof(XioRsp));
      new (xrsp) XioRsp(xcon, this, msg);

      /* merge with portal traffic */
      xcon->portal->enqueue_for_send(xcon, xrsp);
    }
      break;
    case XIO_MSG_TYPE_REQ:
    case XIO_MSG_TYPE_RSP:
    default:
      abort();
      break;
    }
  }

  this->put();
}

void XioCompletionHook::on_err_finalize(XioConnection *xcon)
{
  /* with one-way this is now a no-op */
}
