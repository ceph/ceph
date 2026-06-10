// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <map>
#include <set>
#include <string>
#include <string_view>

#include "common/RefCountedObj.h"
#include "include/utime.h"
#include "include/xlist.h"

#include "msg/Connection.h" // for ConnectionRef
#include "msg/msg_types.h"
#include "mon/FeatureMap.h"

#include "auth/AuthServiceHandler.h"

#include "MonCap.h"

class OSDMap;
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
  bool validated_stretch_connection = false;

  bool authenticated = false;  ///< true if auth handshake is complete

  std::map<std::string, Subscription*> sub_map;
  epoch_t osd_epoch = 0;       ///< the osdmap epoch sent to the mon client

  AuthServiceHandler *auth_handler = nullptr;
  EntityName entity_name;
  uint64_t global_id = 0;
  global_id_status_t global_id_status = global_id_status_t::NONE;

  ConnectionRef proxy_con;
  uint64_t proxy_tid = 0;

  std::string remote_host;                ///< remote host name
  std::map<std::string,std::string,std::less<>> last_config;    ///< most recently shared config
  bool any_config = false;

  MonSession(Connection *c);

  void _ident(const entity_name_t& n, const entity_addrvec_t& av);

  ~MonSession() override;

  bool is_capable(std::string service, int mask);

  std::vector<std::string> get_allowed_fs_names() const {
    return caps.allowed_fs_names();
  }

  bool fs_name_capable(std::string_view fsname, __u8 mask) {
    return caps.fs_name_capable(entity_name, fsname, mask);
  }

  const entity_addr_t& get_peer_socket_addr() {
    return socket_addr;
  }

  void dump(ceph::Formatter *f) const;
};


struct MonSessionMap {
  xlist<MonSession*> sessions;
  std::map<std::string, xlist<Subscription*>* > subs;
  std::multimap<int, MonSession*> by_osd;
  FeatureMap feature_map; // type -> features -> count

  MonSessionMap();
  ~MonSessionMap();

  unsigned get_size() const {
    return sessions.size();
  }

  void remove_session(MonSession *s);

  MonSession *new_session(const entity_name_t& n,
			  const entity_addrvec_t& av,
			  Connection *c);

  void add_session(MonSession *s);

  MonSession *get_random_osd_session(OSDMap *osdmap);

  void add_update_sub(MonSession *s, const std::string& what, version_t start, bool onetime, bool incremental_onetime);
  void remove_sub(Subscription *sub);
};

std::ostream& operator<<(std::ostream& out, const MonSession& s);

#endif
