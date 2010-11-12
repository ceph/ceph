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

class MWatchNotify;

/* keeps track and accounts sessions, watchers and notifiers */
class Watch {
  uint64_t notif_id;

public:
  enum WatcherState {
    WATCHER_PENDING,
    WATCHER_NOTIFIED,
  };

  struct Notification {
    std::map<entity_name_t, WatcherState> watchers;
    entity_name_t name;
    uint64_t id;
    OSD::Session *session;
    uint64_t cookie;
    MWatchNotify *reply;
    Context *timeout;
    void *obc;
    pg_t pgid;

    void add_watcher(const entity_name_t& name, WatcherState state) { watchers[name] = state; }

    Notification(entity_name_t& n, OSD::Session *s, uint64_t c) : name(n), session(s), cookie(c) { }
  };

  class C_NotifyTimeout : public Context {
    OSD *osd;
    Notification *notif;
  public:
    C_NotifyTimeout(OSD *_osd, Notification *_notif) : osd(_osd), notif(_notif) {}
    void finish(int r);
  };

private:
  std::multimap<entity_name_t, Notification *> notifs;
  std::multimap<entity_name_t, Notification *> wtn; /* watchers to notifications */
  std::map<uint64_t, Notification *> itn; /* notif_id to notifications */

public:

  Watch() : notif_id(0) {}

  void register_session(OSD::Session *session, entity_name_t& name);
  void remove_session(OSD::Session *session);
  void add_notification(Notification *notif);
  void remove_notification(Notification *notif);
  bool ack_notification(entity_name_t& watcher, Notification *notif);

  Notification *get_notif(uint64_t id) {
    map<uint64_t, Notification *>::iterator iter = itn.find(id);
    if (iter != itn.end())
      return iter->second;
    return NULL;
  }
};



#endif
