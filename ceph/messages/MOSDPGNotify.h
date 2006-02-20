// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __MOSDPGPEERNOTIFY_H
#define __MOSDPGPEERNOTIFY_H

#include "msg/Message.h"


class MOSDPGNotify : public Message {
  __uint64_t           map_version;
  map<pg_t, version_t> pg_list;   // pgid -> last_complete

 public:
  __uint64_t get_version() { return map_version; }
  map<pg_t, version_t>& get_pg_list() { return pg_list; }

  MOSDPGNotify() {}
  MOSDPGNotify(__uint64_t v, map<pg_t,version_t>& l) :
	Message(MSG_OSD_PG_NOTIFY) {
	this->map_version = v;
	pg_list = l;                          // FIXME
  }
  
  char *get_type_name() { return "PGnot"; }

  void encode_payload() {
	payload.append((char*)&map_version, sizeof(map_version));
	_encode(pg_list, payload);
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(map_version), (char*)&map_version);
	off += sizeof(map_version);
	_decode(pg_list, payload, off);
  }
};

#endif
