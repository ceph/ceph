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

#ifndef __MMONOBSERVENOTIFY_H
#define __MMONOBSERVENOTIFY_H

#include "msg/Message.h"

class MMonObserveNotify : public Message {
 public:
  int32_t machine_id;
  bufferlist bl;
  version_t ver;
  bool is_latest;
  
  MMonObserveNotify() : Message(MSG_MON_OBSERVE_NOTIFY) {}
  MMonObserveNotify(int id, bufferlist& b, version_t v, bool l) :
    Message(MSG_MON_OBSERVE_NOTIFY), machine_id(id), bl(b), ver(v), is_latest(l) {}
    
  
  const char *get_type_name() { return "mon_observe_notify"; }
  void print(ostream& o) {
    o << "mon_observe_notify(v" << ver << " " << bl.length() << " bytes";
    if (is_latest)
      o << " latest";
    o << ")";
  }
  
  void encode_payload() {
    ::encode(machine_id, payload);
    ::encode(bl, payload);
    ::encode(ver, payload);
    ::encode(is_latest, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(machine_id, p);
    ::decode(bl, p);
    ::decode(ver, p);
    ::decode(is_latest, p);
  }
};

#endif
