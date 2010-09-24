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


#ifndef CEPH_WATCH_H
#define CEPH_WATCH_H

#include <map>

#include "OSD.h"
#include "config.h"

/* keeps track and accounts sessions, watchers and notifiers */
class Watch {

public:
  enum WatcherState {
    WATCHER_PENDING,
    WATCHER_NOTIFIED,
  };

  struct Notification {
    std::map<entity_name_t, WatcherState> watchers;
    entity_name_t name;

    void add_watcher(const entity_name_t& name, WatcherState state) { watchers[name] = state; }

    Notification(entity_name_t& n) { name = n; }
  };

private:
  std::multimap<entity_name_t, Notification *> notifs;
  std::multimap<entity_name_t, Notification *> wtn; /* watchers to notifications */

  std::map<OSD::Session *, entity_name_t> ste; /* sessions to entities */
  std::map<entity_name_t, OSD::Session *> ets; /* entities to sessions */

public:

  Watch() {}

  void register_session(OSD::Session *session, entity_name_t& name);
  void remove_session(OSD::Session *session);
  void add_notification(Notification *notif);
  bool ack_notification(entity_name_t& watcher, Notification *notif);
};



#endif
