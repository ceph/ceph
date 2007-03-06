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

class MClientUpdate : public Message {
private:
  struct {
    gid_t group;
    entity_inst_t asking_client;
  } mds_update_st;
public:
  MClientUpdate () : Message(MSG_CLIENT_UPDATE) { }
  MClientUpdate (gid_t gid) : Message(MSG_CLIENT_UPDATE) {
    memset(&mds_update_st, 0, sizeof(mds_update_st));
    this->mds_update_st.group = gid;
  }
  
  gid_t get_group () { return mds_update_st.group; }

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
	<< ")";
  }
};

#endif
