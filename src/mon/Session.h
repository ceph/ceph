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

#ifndef __MON_SESSION_H
#define __MON_SESSION_H

#include "include/xlist.h"
#include "msg/msg_types.h"

#include "auth/AuthServiceManager.h"

struct Session;

struct Subscription {
  Session *session;
  nstring type;
  xlist<Subscription*>::item type_item;
  version_t last;
  bool onetime;
  
  Subscription(Session *s, const nstring& t) : session(s), type(t), type_item(this) {};
};


struct Session : public RefCountedObject {
  entity_inst_t inst;
  utime_t until;
  bool closed;
  xlist<Session*>::item item;

  map<nstring, Subscription*> sub_map;

  AuthServiceHandler *auth_handler;

  Session(entity_inst_t i) : inst(i), closed(false), item(this),
			     auth_handler(NULL) {}
  ~Session() {
    generic_dout(0) << "~Session " << this << dendl;
    // we should have been removed before we get destructed; see SessionMap::remove_session()
    assert(!item.is_on_xlist());
    assert(sub_map.empty());
    delete auth_handler;
  }
};

struct SessionMap {
  xlist<Session*> sessions;
  map<nstring, xlist<Subscription*> > subs;

  void remove_session(Session *s) {
    for (map<nstring,Subscription*>::iterator p = s->sub_map.begin(); p != s->sub_map.end(); ++p)
      p->second->type_item.remove_myself();
    s->sub_map.clear();
    s->item.remove_myself();
    s->put();
  }

  Session *new_session(entity_inst_t i) {
    Session *s = new Session(i);
    sessions.push_back(&s->item);
    s->get();  // caller gets a ref
    return s;
  }

  void add_update_sub(Session *s, const nstring& what, version_t have, bool onetime) {
    Subscription *sub = 0;
    if (s->sub_map.count(what)) {
      sub = s->sub_map[what];
    } else {
      sub = new Subscription(s, what);
      s->sub_map[what] = sub;
      subs[what].push_back(&sub->type_item);
    }
    sub->last = have;
    sub->onetime = onetime;
  }

  void remove_sub(Subscription *sub) {
    sub->session->sub_map.erase(sub->type);
    sub->type_item.remove_myself();
    delete sub;
  }
};

#endif
