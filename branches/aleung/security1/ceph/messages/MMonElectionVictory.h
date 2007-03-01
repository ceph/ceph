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


#ifndef __MMONELECTIONVICTORY_H
#define __MMONELECTIONVICTORY_H

#include "msg/Message.h"


class MMonElectionVictory : public Message {
 public:
  //set<int> active_set;

  MMonElectionVictory(/*set<int>& as*/) : Message(MSG_MON_ELECTION_VICTORY)//,
	//active_set(as) 
	{}
  
  virtual char *get_type_name() { return "election_victory"; }
  
  void encode_payload() {
    //::_encode(active_set, payload);
  }
  void decode_payload() {
    //int off = 0;
    //::_decode(active_set, payload, off);
  }
};

#endif
