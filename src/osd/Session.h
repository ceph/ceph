// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_SESSION_H
#define CEPH_OSD_SESSION_H

#include "common/RefCountedObj.h"

struct Session : public RefCountedObject {
  EntityName entity_name;
  OSDCap caps;
  int64_t auid;
  ConnectionRef con;
  WatchConState wstate;

  Mutex session_dispatch_lock;
  boost::intrusive::list<OpRequest> waiting_on_map;

  OSDMapRef osdmap;  /// Map as of which waiting_for_pg is current
  map<spg_t, boost::intrusive::list<OpRequest> > waiting_for_pg;

  Spinlock sent_epoch_lock;
  epoch_t last_sent_epoch;
  Spinlock received_map_lock;
  epoch_t received_map_epoch; // largest epoch seen in MOSDMap from here

  explicit Session(CephContext *cct) :
    RefCountedObject(cct),
    auid(-1), con(0),
    wstate(cct),
    session_dispatch_lock("Session::session_dispatch_lock"),
    last_sent_epoch(0), received_map_epoch(0)
    {}
  void maybe_reset_osdmap() {
    if (waiting_for_pg.empty()) {
      osdmap.reset();
    }
  }
};

typedef boost::intrusive_ptr<Session> SessionRef;

#endif
