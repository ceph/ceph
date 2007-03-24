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

#ifndef __MOSDUPDATE_H
#define __MOSDUPDATE_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "crypto/MerkleTree.h"

#define USER_HASH 0
#define FILE_HASH 1

class MOSDUpdate : public Message {
private:
  struct {
    gid_t group;
    hash_t uhash;

    entity_inst_t client;
    entity_inst_t asker;
  } update_st;
public:

  gid_t get_group() { return update_st.group; }
  hash_t get_hash() { return update_st.uhash; }

  entity_inst_t get_client_inst() { return update_st.client; }
  entity_inst_t get_asker() { return update_st.asker; }

  MOSDUpdate () { }

  MOSDUpdate(gid_t upGr) : Message(MSG_OSD_UPDATE) {
    memset(&update_st,0, sizeof(update_st));
    this->update_st.group = upGr;
  }

  MOSDUpdate(hash_t h) : Message(MSG_OSD_UPDATE) {
    memset(&update_st,0, sizeof(update_st));
    this->update_st.uhash = h;
  }

  MOSDUpdate(entity_inst_t asking_osd, entity_inst_t target_client,
	     gid_t upGr) : Message(MSG_OSD_UPDATE) {
    memset(&update_st,0, sizeof(update_st));
    this->update_st.group = upGr;
    this->update_st.client  = target_client;
    this->update_st.asker = asking_osd;
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(update_st), (char*)&update_st);
    off += sizeof(update_st);
  }
  virtual void encode_payload() {
    payload.append((char*)&update_st, sizeof(update_st));
  }
  
  virtual char *get_type_name() { return "oop_update"; }
  void print(ostream& out) {
    out << "osd_update(" << update_st.group
	<< ", " << update_st.uhash
	<< ")";
  }
};

#endif

