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


#ifndef __MOSDPEERREQUEST_H
#define __MOSDPEERREQUEST_H

#include "msg/Message.h"


class MOSDPGPeerRequest : public Message {
  version_t       map_version;
  list<repgroup_t> pg_list;

 public:
  version_t get_version() { return map_version; }
  list<repgroup_t>& get_pg_list() { return pg_list; }

  MOSDPGPeerRequest() {}
  MOSDPGPeerRequest(version_t v, list<repgroup_t>& l) :
    Message(MSG_OSD_PG_PEERREQUEST) {
    this->map_version = v;
    pg_list.splice(pg_list.begin(), l);
  }
  
  char *get_type_name() { return "PGPR"; }

  void encode_payload() {
    ::encode(map_version, payload);
    ::encode(pg_list, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_version, p);
    ::decode(pg_list, p);
  }
};

#endif
