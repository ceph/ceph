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
#include <stdio.h>

#define dout_subsys ceph_subsys_xio

int XioPortal::bind(struct xio_session_ops *ops, const string &base_uri,
		    uint16_t port, uint16_t *assigned_port)
{
  // format uri
  char buf[40];
  xio_uri = base_uri;
  xio_uri += ":";
  sprintf(buf, "%d", port);
  xio_uri += buf;

  uint16_t assigned;
  server = xio_bind(ctx, ops, xio_uri.c_str(), &assigned, 0, msgr);
  if (server == NULL)
    return xio_errno();

  // update uri if port changed
  if (port != assigned) {
    xio_uri = base_uri;
    xio_uri += ":";
    sprintf(buf, "%d", assigned);
    xio_uri += buf;
  }

  portal_id = const_cast<char*>(xio_uri.c_str());
  if (assigned_port)
    *assigned_port = assigned;
  ldout(msgr->cct,20) << "xio_bind: portal " << xio_uri
    << " returned server " << server << dendl;
  return 0;
}

int XioPortals::bind(struct xio_session_ops *ops, const string& base_uri,
		     uint16_t port, uint16_t *port0)
{
  /* a server needs at least 1 portal */
  if (n < 1)
    return EINVAL;
  Messenger *msgr = portals[0]->msgr;
  portals.resize(n);

  uint16_t port_min = msgr->cct->_conf->ms_bind_port_min;
  const uint16_t port_max = msgr->cct->_conf->ms_bind_port_max;

  /* bind the portals */
  for (size_t i = 0; i < portals.size(); i++) {
    uint16_t result_port;
    if (port != 0) {
      // bind directly to the given port
      int r = portals[i]->bind(ops, base_uri, port, &result_port);
      if (r != 0)
        return -r;
    } else {
      int r = EADDRINUSE;
      // try ports within the configured range
      for (; port_min <= port_max; port_min++) {
        r = portals[i]->bind(ops, base_uri, port_min, &result_port);
        if (r == 0) {
          port_min++;
          break;
        }
      }
      if (r != 0) {
        lderr(msgr->cct) << "portal.bind unable to bind to " << base_uri
            << " on any port in range " << msgr->cct->_conf->ms_bind_port_min
            << "-" << port_max << ": " << xio_strerror(r) << dendl;
        return -r;
      }
    }

    ldout(msgr->cct,5) << "xp::bind: portal " << i << " bind OK: "
      << portals[i]->xio_uri << dendl;

    if (i == 0 && port0 != NULL)
      *port0 = result_port;
    port = 0; // use port 0 for all subsequent portals
  }

  return 0;
}
