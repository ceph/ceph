// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XioPortal.h"
#define dout_subsys ceph_subsys_xio

int XioPortal::bind(struct xio_session_ops *ops, const string &_uri)
{
  xio_uri = _uri;
  portal_id = strdup(xio_uri.c_str());
  server = xio_bind(ctx, ops, portal_id, NULL, 0, msgr);
  dout(4) << dout_format("xio_bind: portal %p %s returned server %p",
	 this, xio_uri.c_str(), server) << dendl;
  return (!!server);
}

int XioPortals::bind(struct xio_session_ops *ops, const string& base_uri,
       const int base_port)
{
  /* a server needs at least 1 portal */
  if (n < 1)
    return EINVAL;

  XioPortal *portal;
  int bind_size = portals.size();

  /* bind a consecutive range of ports */
  for (int bind_ix = 1, bind_port = base_port;
       bind_ix < bind_size; ++bind_ix, ++bind_port) {
    string xio_uri = base_uri;
    xio_uri += ":";
    xio_uri += boost::lexical_cast<std::string>(bind_port);
    portal = portals[bind_ix];
    (void) portal->bind(ops, xio_uri);
  }

  return 0;
}
