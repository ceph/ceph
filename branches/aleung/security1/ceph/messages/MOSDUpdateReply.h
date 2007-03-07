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
  hash_t user_hash;
  list<uid_t> updated_users;
public:
  MOSDUpdateReply () : Message(MSG_OSD_UPDATE_REPLY) { }
  MOSDUpdateReply(gid_t gid) : Message(MSG_OSD_UPDATE_REPLY),
			       group(gid) { }
  MOSDUpdateReply(hash_t uhash) : Message(MSG_OSD_UPDATE_REPLY),
				  user_hash(uhash) { }
  MOSDUpdateReply (gid_t gid, list<uid_t> users) :
    Message(MSG_OSD_UPDATE_REPLY), group(gid), updated_users(users) { }
  MOSDUpdateReply(hash_t uhash, list<uid_t>& users) :
    Message(MSG_OSD_UPDATE_REPLY), user_hash(uhash), updated_users(users) { }

  gid_t get_group() { return group; }
  hash_t get_user_hash() { return user_hash; }
  list<uid_t>& get_list() { return updated_users; }

  virtual void encode_payload() {
    payload.append((char*)&group, sizeof(group));
    payload.append((char*)&user_hash, sizeof(user_hash));
    _encode(updated_users, payload);
  }
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(group), (char*)&group);
    off += sizeof(group);
    payload.copy(off, sizeof(user_hash), (char*)&user_hash);
    off += sizeof(user_hash);
    _decode(updated_users, payload, off);
  }
  virtual char *get_type_name() { return "oop_update_reply"; }
  void print(ostream& out) {
    out << "osd_update_reply(" << group
	<< ", " << user_hash
	<< ")";
  }
};

#endif
