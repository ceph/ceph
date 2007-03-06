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

#ifndef __MOSDUPDATEREPLY_H
#define __MOSDUPDATEREPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDUpdateReply : public Message {
private:
  gid_t group;
  list<uid_t> updated_users;
public:
  MOSDUpdateReply () : Message(MSG_OSD_UPDATE_REPLY) { }
  MOSDUpdateReply(gid_t gid) : Message(MSG_OSD_UPDATE_REPLY),
			       group(gid) { }
  MOSDUpdateReply (gid_t gid, list<uid_t> users) :
    Message(MSG_OSD_UPDATE_REPLY), group(gid), updated_users(users) { }

  gid_t get_group() { return group; }
  list<uid_t>& get_list() { return updated_users; }

  virtual void encode_payload() {
    payload.append((char*)&group, sizeof(group));
    _encode(updated_users, payload);
  }
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(group), (char*)&group);
    off += sizeof(group);
    _decode(updated_users, payload, off);
  }
  virtual char *get_type_name() { return "oop_update_reply"; }
  void print(ostream& out) {
    out << "osd_update_reply(" << group
	<< ")";
  }
};

#endif
