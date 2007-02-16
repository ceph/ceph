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


#ifndef __MMONELECTIONPROPOSE_H
#define __MMONELECTIONPROPOSE_H

#include "msg/Message.h"


class MMonElectionPropose : public Message {
 public:
  MMonElectionPropose() : Message(MSG_MON_ELECTION_PROPOSE) {}
  
  virtual char *get_type_name() { return "election_propose"; }

  void encode_payload() {}
  void decode_payload() {}

};

#endif
