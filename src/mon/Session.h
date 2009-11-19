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

#include "auth/AuthServiceHandler.h"

#include "MonCaps.h"

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
  set<__u64> routed_request_tids;
  MonCaps caps;
  uint64_t global_id;
  uint64_t notified_global_id;

  map<nstring, Subscription*> sub_map;

  AuthServiceHandler *auth_handler;

  Session(entity_inst_t i) : inst(i), closed(false), item(this),
			     global_id(0), notified_global_id(0), auth_handler(NULL) {}
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
  multimap<int, Session*> by_osd;

  void remove_session(Session *s) {
    assert(!s->closed);
    for (map<nstring,Subscription*>::iterator p = s->sub_map.begin(); p != s->sub_map.end(); ++p)
      p->second->type_item.remove_myself();
    s->sub_map.clear();
    s->item.remove_myself();
    if (s->inst.name.is_osd()) {
      for (multimap<int,Session*>::iterator p = by_osd.find(s->inst.name.num());
	   p->first == s->inst.name.num();
	   p++)
	if (p->second == s) {
	  by_osd.erase(p);
	  break;
	}
    }
    s->closed = true;
    s->put();
  }

  Session *new_session(entity_inst_t i) {
    Session *s = new Session(i);
    sessions.push_back(&s->item);
    if (i.name.is_osd())
      by_osd.insert(pair<int,Session*>(i.name.num(), s));
    s->get();  // caller gets a ref
    return s;
  }
  
  Session *get_random_osd_session() {
    // ok, this isn't actually random, but close enough.
    if (by_osd.empty())
      return 0;
    int n = by_osd.rbegin()->first + 1;
    int r = rand() % n;
    multimap<int,Session*>::iterator p = by_osd.lower_bound(r);
    if (p == by_osd.end())
      p--;
    return p->second;
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
