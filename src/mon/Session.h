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

#ifndef CEPH_MON_SESSION_H
#define CEPH_MON_SESSION_H

#include <string>
#include <string_view>

#include "include/utime.h"
#include "include/xlist.h"

#include "global/global_context.h"
#include "msg/msg_types.h"
#include "mon/mon_types.h"

#include "auth/AuthServiceHandler.h"
#include "osd/OSDMap.h"

#include "MonCap.h"

struct MonSession;

struct Subscription {
  MonSession *session;
  std::string type;
  xlist<Subscription*>::item type_item;
  version_t next;
  bool onetime;
  bool incremental_onetime;  // has CEPH_FEATURE_INCSUBOSDMAP
  
  Subscription(MonSession *s, const std::string& t) : session(s), type(t), type_item(this),
						 next(0), onetime(false), incremental_onetime(false) {}
};

struct MonSession : public RefCountedObject {
  ConnectionRef con;
  int con_type = 0;
  uint64_t con_features = 0;  // zero if AnonConnection
  entity_name_t name;
  entity_addrvec_t addrs;
  entity_addr_t socket_addr;
  utime_t session_timeout;
  bool closed = false;
  xlist<MonSession*>::item item;
  std::set<uint64_t> routed_request_tids;
  MonCap caps;

  bool authenticated = false;  ///< true if auth handshake is complete

  std::map<std::string, Subscription*> sub_map;
  epoch_t osd_epoch = 0;       ///< the osdmap epoch sent to the mon client

  AuthServiceHandler *auth_handler = nullptr;
  EntityName entity_name;

  ConnectionRef proxy_con;
  uint64_t proxy_tid = 0;

  std::string remote_host;                ///< remote host name
  std::map<std::string,std::string,std::less<>> last_config;    ///< most recently shared config
  bool any_config = false;

  MonSession(Connection *c)
    : RefCountedObject(g_ceph_context),
      con(c),
      item(this) { }

  void _ident(const entity_name_t& n, const entity_addrvec_t& av) {
    con_type = con->get_peer_type();
    name = n;
    addrs = av;
    socket_addr = con->get_peer_socket_addr();
    if (con->get_messenger()) {
      // only fill in features if this is a non-anonymous connection
      con_features = con->get_features();
    }
  }

  ~MonSession() override {
    //generic_dout(0) << "~MonSession " << this << dendl;
    // we should have been removed before we get destructed; see MonSessionMap::remove_session()
    ceph_assert(!item.is_on_list());
    ceph_assert(sub_map.empty());
    delete auth_handler;
  }

  bool is_capable(std::string service, int mask) {
    std::map<std::string,std::string> args;
    return caps.is_capable(
      g_ceph_context,
      CEPH_ENTITY_TYPE_MON,
      entity_name,
      service, "", args,
      mask & MON_CAP_R, mask & MON_CAP_W, mask & MON_CAP_X,
      get_peer_socket_addr());
  }

  const entity_addr_t& get_peer_socket_addr() {
    return socket_addr;
  }
};


struct MonSessionMap {
  xlist<MonSession*> sessions;
  std::map<std::string, xlist<Subscription*>* > subs;
  std::multimap<int, MonSession*> by_osd;
  FeatureMap feature_map; // type -> features -> count

  MonSessionMap() {}
  ~MonSessionMap() {
    while (!subs.empty()) {
      ceph_assert(subs.begin()->second->empty());
      delete subs.begin()->second;
      subs.erase(subs.begin());
    }
  }

  unsigned get_size() const {
    return sessions.size();
  }

  void remove_session(MonSession *s) {
    ceph_assert(!s->closed);
    for (std::map<std::string,Subscription*>::iterator p = s->sub_map.begin(); p != s->sub_map.end(); ++p) {
      p->second->type_item.remove_myself();
      delete p->second;
    }
    s->sub_map.clear();
    s->item.remove_myself();
    if (s->name.is_osd()) {
      for (auto p = by_osd.find(s->name.num());
	   p->first == s->name.num();
	   ++p)
	if (p->second == s) {
	  by_osd.erase(p);
	  break;
	}
    }
    if (s->con_features) {
      feature_map.rm(s->con_type, s->con_features);
    }
    s->closed = true;
    s->put();
  }

  MonSession *new_session(const entity_name_t& n,
			  const entity_addrvec_t& av,
			  Connection *c) {
    MonSession *s = new MonSession(c);
    ceph_assert(s);
    s->_ident(n, av);
    add_session(s);
    return s;
  }

  void add_session(MonSession *s) {
    sessions.push_back(&s->item);
    s->get();
    if (s->name.is_osd()) {
      by_osd.insert(std::pair<int,MonSession*>(s->name.num(), s));
    }
    if (s->con_features) {
      feature_map.add(s->con_type, s->con_features);
    }
  }

  MonSession *get_random_osd_session(OSDMap *osdmap) {
    // ok, this isn't actually random, but close enough.
    if (by_osd.empty())
      return 0;
    int n = by_osd.rbegin()->first + 1;
    int r = rand() % n;

    auto p = by_osd.lower_bound(r);
    if (p == by_osd.end())
      --p;

    if (!osdmap) {
      return p->second;
    }

    MonSession *s = NULL;

    auto b = p;
    auto f = p;
    bool backward = true, forward = true;
    while (backward || forward) {
      if (backward) {
        if (osdmap->is_up(b->first) &&
	    osdmap->get_addrs(b->first) == b->second->con->get_peer_addrs()) {
          s = b->second;
          break;
        }
        if (b != by_osd.begin())
          --b;
        else
          backward = false;
      }

      forward = (f != by_osd.end());
      if (forward) {
        if (osdmap->is_up(f->first)) {
          s = f->second;
          break;
        }
        ++f;
      }
    }

    return s;
  }

  void add_update_sub(MonSession *s, const std::string& what, version_t start, bool onetime, bool incremental_onetime) {
    Subscription *sub = 0;
    if (s->sub_map.count(what)) {
      sub = s->sub_map[what];
    } else {
      sub = new Subscription(s, what);
      s->sub_map[what] = sub;

      if (!subs.count(what))
	subs[what] = new xlist<Subscription*>;
      subs[what]->push_back(&sub->type_item);
    }
    sub->next = start;
    sub->onetime = onetime;
    sub->incremental_onetime = onetime && incremental_onetime;
  }

  void remove_sub(Subscription *sub) {
    sub->session->sub_map.erase(sub->type);
    sub->type_item.remove_myself();
    delete sub;
  }
};

inline std::ostream& operator<<(std::ostream& out, const MonSession& s)
{
  out << "MonSession(" << s.name << " " << s.addrs
      << " is " << (s.closed ? "closed" : "open")
      << " " << s.caps
      << ", features 0x" << std::hex << s.con_features << std::dec
      <<  " (" << ceph_release_name(ceph_release_from_features(s.con_features))
      << "))";
  return out;
}

#endif
