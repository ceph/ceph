// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __MONITOR_H
#define __MONITOR_H

#include "include/types.h"
#include "msg/Messenger.h"

#include "MonMap.h"
#include "Elector.h"

class OSDMonitor;
class MDSMonitor;

class Monitor : public Dispatcher {
protected:
  // me
  int whoami;
  Messenger *messenger;
  Mutex lock;

  MonMap *monmap;

  // elector
  Elector elector;
  friend class Elector;

  // monitor state
  const static int STATE_STARTING = 0;
  const static int STATE_LEADER = 1;
  const static int STATE_PEON =   2;
  int state;

  // my public services
  OSDMonitor *osdmon;
  MDSMonitor *mdsmon;

  // messages
  void handle_shutdown(Message *m);
  void handle_ping_ack(class MPingAck *m);

  friend class OSDMonitor;
  friend class MDSMonitor;

 public:
  Monitor(int w, MonMap *mm, Messenger *m) : 
    whoami(w), 
    messenger(m),
    monmap(mm),
    elector(this, w),
    state(STATE_STARTING),
    osdmon(0),
    mdsmon(0)
  {
  }

  void init();
  void dispatch(Message *m);
  void tick();

};

#endif
