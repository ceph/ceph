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

#ifndef __MCLIENTUPDATE_H
#define __MCLIENTUPDATE_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "crypto/MerkleTree.h"

class MClientUpdate : public Message {
private:
  struct {
    hash_t user_hash;
    gid_t group;
    entity_inst_t asking_client;
  } mds_update_st;
public:
  MClientUpdate () : Message(MSG_CLIENT_UPDATE) { }
  MClientUpdate (gid_t gid) : Message(MSG_CLIENT_UPDATE) {
    memset(&mds_update_st, 0, sizeof(mds_update_st));
    this->mds_update_st.group = gid;
  }
  MClientUpdate (hash_t uhash) : Message(MSG_CLIENT_UPDATE) {
    memset(&mds_update_st, 0, sizeof(mds_update_st));
    this->mds_update_st.user_hash = uhash;
  }
  
  void set_group(gid_t ngroup) { mds_update_st.group = ngroup; }
  gid_t get_group () { return mds_update_st.group; }
  void set_user_hash(hash_t nhash) { mds_update_st.user_hash = nhash; }
  hash_t get_user_hash() { return mds_update_st.user_hash; }
  entity_inst_t& get_client_inst() { return mds_update_st.asking_client; }

  virtual void encode_payload() {
    payload.append((char*)&mds_update_st, sizeof(mds_update_st));
  }
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(mds_update_st), (char*)&mds_update_st);
    off += sizeof(mds_update_st);
  }
  virtual char *get_type_name() { return "mds_update"; }
  void print(ostream& out) {
    out << "mds_update(" << mds_update_st.group
	<< ", " << mds_update_st.user_hash
	<< ")";
  }
};

#endif
